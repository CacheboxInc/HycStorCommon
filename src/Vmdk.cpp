#include <string>
#include <memory>

#include <cstdint>

#include <roaring/roaring.hh>

#include "IDs.h"
#include "Utils.h"
#include "RequestHandler.h"
#include "Vmdk.h"
#include "VirtualMachine.h"

namespace pio {
Vmdk::Vmdk(VmdkID&& vmdk_id) : vmdk_id_(std::move(vmdk_id)) {

}

Vmdk::~Vmdk() = default;

const VmdkID& Vmdk::GetVmdkID() const {
	return vmdk_id_;
}

ActiveVmdk::ActiveVmdk(VirtualMachine *vmp, VmdkID vmdk_id,
		uint32_t block_size) : Vmdk(std::move(vmdk_id)), vmp_(vmp) {
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

void ActiveVmdk::RegisterRequestHandler(std::unique_ptr<RequestHandler> handler) {
	if (not headp_) {
		headp_ = std::move(handler);
		return;
	}

	headp_->RegisterNextRequestHandler(std::move(handler));
}

folly::Future<int> ActiveVmdk::Read(RequestID req_id, void* bufp,
		size_t buf_size, Offset offset) {
	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}

	auto reqp = std::make_unique<Request>(req_id, this, Request::Type::kRead,
		bufp, buf_size, buf_size, offset);

	std::vector<RequestBlock*> process;
	process.reserve(reqp->NumberOfRequestBlocks());
	reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
		process.emplace_back(blockp);
		return true;
	});

	std::vector<RequestBlock*> failed;
	return headp_->Read(this, reqp.get(), process, failed)
	.then([reqp = std::move(reqp), process = std::move(process),
			failed = std::move(failed), bufp, buf_size, offset] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			return rc;
		} else if (pio_unlikely(not failed.empty())) {
			const auto blockp = failed.front();
			log_assert(blockp && blockp->IsFailed() && blockp->GetResult() != 0);
			return blockp->GetResult();
		}

		return reqp->Complete();
	});
}

folly::Future<int> ActiveVmdk::Write(RequestID req_id, CheckPointID ckpt_id,
		void* bufp, size_t buf_size, Offset offset) {
	return WriteCommon(req_id, Request::Type::kWrite, ckpt_id, bufp, buf_size,
		buf_size, offset);
}

folly::Future<int> ActiveVmdk::WriteSame(RequestID req_id, CheckPointID ckpt_id,
		void* bufp, size_t buf_size, size_t transfer_size, Offset offset) {
	return WriteCommon(req_id, Request::Type::kWriteSame, ckpt_id, bufp,
		buf_size, transfer_size, offset);
}

folly::Future<int> ActiveVmdk::WriteCommon(RequestID req_id, Request::Type type,
		CheckPointID ckpt_id, void* bufp, size_t buf_size, size_t transfer_size,
		Offset offset) {
	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}

	auto reqp = std::make_unique<Request>(req_id, this, type, bufp, buf_size,
		transfer_size, offset);

	std::vector<RequestBlock*> process;
	process.reserve(reqp->NumberOfRequestBlocks());
	reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
		process.emplace_back(blockp);
		return true;
	});

	std::vector<RequestBlock*> failed;
	return headp_->Write(this, reqp.get(), process, failed)
	.then([reqp = std::move(reqp), process = std::move(process),
			failed = std::move(failed), bufp, buf_size, transfer_size, offset]
			(int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			return rc;
		} else if (pio_unlikely(not failed.empty())) {
			const auto blockp = failed.front();
			log_assert(blockp && blockp->IsFailed() && blockp->GetResult() != 0);
			return blockp->GetResult();
		}

		return reqp->Complete();
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

	auto checkpoint = std::make_unique<CheckPoint>(vmdk_id_, ckpt_id);
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