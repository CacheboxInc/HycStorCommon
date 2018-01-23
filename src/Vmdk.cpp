#include <string>
#include <memory>
#include <vector>

#include <cstdint>
#include <sys/eventfd.h>

#include <roaring/roaring.hh>

#include "IDs.h"
#include "Utils.h"
#include "TgtTypes.h"
#include "RequestHandler.h"
#include "Vmdk.h"
#include "VirtualMachine.h"

namespace pio {
Vmdk::Vmdk(VmdkHandle handle, VmdkID&& id) : handle_(handle), id_(std::move(id)) {
}

Vmdk::~Vmdk() = default;

const VmdkID& Vmdk::GetID() const noexcept {
	return id_;
}

VmdkHandle Vmdk::GetHandle() const noexcept {
	return handle_;
}

ActiveVmdk::ActiveVmdk(VirtualMachine *vmp, VmdkHandle handle, VmdkID id,
		uint32_t block_size) : Vmdk(handle, std::move(id)), vmp_(vmp) {
	if (block_size & (block_size - 1)) {
		throw std::invalid_argument("Block Size is not power of 2.");
	}
	block_shift_ = PopCount(block_size-1);
}

ActiveVmdk::~ActiveVmdk() = default;

size_t ActiveVmdk::BlockShift() const {
	return block_shift_;
}

size_t ActiveVmdk::BlockSize() const {
	return 1ul << BlockShift();
}

size_t ActiveVmdk::BlockMask() const {
	return BlockSize() - 1;
}

void ActiveVmdk::SetEventFd(int eventfd) noexcept {
	eventfd_ = eventfd;
}

VirtualMachine* ActiveVmdk::GetVM() const noexcept {
	return vmp_;
}

void ActiveVmdk::RegisterRequestHandler(std::unique_ptr<RequestHandler> handler) {
	if (not headp_) {
		headp_ = std::move(handler);
		return;
	}

	headp_->RegisterNextRequestHandler(std::move(handler));
}

int ActiveVmdk::RequestComplete(std::unique_ptr<Request> reqp) {
	auto result = reqp->Complete();

	{
		std::lock_guard<std::mutex> lock(requests_.mutex_);
		requests_.complete_.emplace_back(std::move(reqp));
	}

	if (pio_likely(eventfd_ >= 0)) {
		auto rc = ::eventfd_write(eventfd_, 1);
		if (pio_unlikely(rc < 0)) {
			LOG(ERROR) << "Write to eventfd failed "
				<< "VmdkID " << GetID()
				<< "FD " << eventfd_;
		}
		/* ignore write failure */
		(void) rc;
	}
	return result;
}

uint32_t ActiveVmdk::GetRequestResult(RequestResult* resultsp,
		uint32_t nresults, bool *has_morep) {
	std::vector<std::unique_ptr<Request>> dst;

	{
		dst.reserve(nresults);
		std::lock_guard<std::mutex> lock(requests_.mutex_);
		*has_morep = requests_.complete_.size() > nresults;
		pio::MoveLastElements(dst, requests_.complete_, nresults);
	}

	auto i = 0;
	RequestResult* resultp = resultsp;
	for (const auto& reqp : dst) {
		resultp->privatep   = reqp->GetPrivateData();
		resultp->request_id = reqp->GetID();
		resultp->result     = reqp->GetResult();

		++i;
		++resultp;
	}

	return dst.size();
}

folly::Future<int> ActiveVmdk::Read(std::unique_ptr<Request> reqp) {
	assert(reqp);

	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}

	std::vector<RequestBlock*> failed;
	std::vector<RequestBlock*> process;
	process.reserve(reqp->NumberOfRequestBlocks());
	reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
		process.emplace_back(blockp);
		return true;
	});

	auto p = reqp.get();
	return headp_->Read(this, p, process, failed)
	.then([this, reqp = std::move(reqp), process = std::move(process),
			failed = std::move(failed)] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			return rc;
		} else if (pio_unlikely(not failed.empty())) {
			const auto blockp = failed.front();
			log_assert(blockp && blockp->IsFailed() && blockp->GetResult() != 0);
			return blockp->GetResult();
		}

		return RequestComplete(std::move(reqp));
	});
}

folly::Future<int> ActiveVmdk::Write(std::unique_ptr<Request> reqp,
		CheckPointID ckpt_id) {
	return WriteCommon(std::move(reqp), ckpt_id);
}

folly::Future<int> ActiveVmdk::WriteSame(std::unique_ptr<Request> reqp,
		CheckPointID ckpt_id) {
	return WriteCommon(std::move(reqp), ckpt_id);
}

folly::Future<int> ActiveVmdk::WriteCommon(std::unique_ptr<Request> reqp,
		CheckPointID ckpt_id) {
	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}

	std::vector<RequestBlock*> failed;
	std::vector<RequestBlock*> process;
	process.reserve(reqp->NumberOfRequestBlocks());
	reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
		process.emplace_back(blockp);
		return true;
	});

	auto p = reqp.get();
	return headp_->Write(this, p, process, failed)
	.then([this, reqp = std::move(reqp), process = std::move(process),
			failed = std::move(failed)] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			return rc;
		} else if (pio_unlikely(not failed.empty())) {
			const auto blockp = failed.front();
			log_assert(blockp && blockp->IsFailed() && blockp->GetResult() != 0);
			return blockp->GetResult();
		}

		return RequestComplete(std::move(reqp));
	});
}

folly::Future<int> ActiveVmdk::TakeCheckPoint(CheckPointID ckpt_id) {
	std::unordered_set<BlockID> blocks;
	BlockID start, end;

	{
		std::lock_guard<std::mutex> guard(blocks_.mutex_);
		std::exchange(blocks, blocks_.modified_);
		start = blocks_.min_;
		end = blocks_.max_;
		blocks_.modified_.clear();
		blocks_.min_ = kBlockIDMax;
		blocks_.max_ = kBlockIDMin;
	}

	auto checkpoint = std::make_unique<CheckPoint>(GetID(), ckpt_id);
	auto ckptp = checkpoint.get();
	ckptp->SetModifiedBlocks(std::move(blocks), start, end);

	auto s = ckptp->Serialize();

	{
		std::lock_guard<std::mutex> guard(checkpoints_.mutex_);
		checkpoints_.unflushed_.emplace_back(std::move(checkpoint));
	}
	return 0;
}

struct CheckPointHeader {
	VmdkID       vmdk_id;
	CheckPointID ckpt_id;
	BlockID      first;
	BlockID      last;
	uint32_t     bitset_size;
};

CheckPoint::CheckPoint(VmdkID vmdk_id, CheckPointID id) :
		vmdk_id_(std::move(vmdk_id)), self_(id) {
}

CheckPoint::~CheckPoint() = default;

std::unique_ptr<RequestBuffer> CheckPoint::Serialize() const {
	struct CheckPointHeader header;
	header.vmdk_id     = vmdk_id_;
	header.ckpt_id     = self_;
	header.first       = block_id_.first_;
	header.last        = block_id_.last_;
	header.bitset_size = blocks_bitset_->getSizeInBytes();

	auto size = sizeof(header) + header.bitset_size;
	size = AlignUpToBlockSize(size, kSectorSize);

	auto bufferp = NewRequestBuffer(size);
	if (pio_unlikely(bufferp)) {
		throw std::bad_alloc();
	}
	log_assert(bufferp->Size() >= size);

	auto dp = bufferp->Payload();
	::memcpy(dp, reinterpret_cast<void *>(&header), sizeof(header));
	dp += sizeof(header);
	blocks_bitset_->write(dp);

	return std::move(bufferp);
}

void CheckPoint::SetModifiedBlocks(std::unordered_set<BlockID>&& blocks,
		BlockID first, BlockID last) {
	blocks_bitset_ = std::make_unique<Roaring>();
	for (auto b : blocks) {
		blocks_bitset_->add(b);
	}
	blocks_bitset_->runOptimize();
	log_assert(blocks_bitset_->cardinality() == last - first + 1);
}
}