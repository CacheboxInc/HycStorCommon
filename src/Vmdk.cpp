#include <string>
#include <memory>
#include <vector>
#include <numeric>

#include <cstdint>
#include <sys/eventfd.h>

#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "DaemonUtils.h"
#include "DaemonTgtTypes.h"
#include "VmdkConfig.h"
#include "RequestHandler.h"
#include "Vmdk.h"

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

ActiveVmdk::ActiveVmdk(VmdkHandle handle, VmdkID id, VirtualMachine *vmp,
		const std::string& config, std::shared_ptr<AeroSpikeConn> aero_conn)
		: Vmdk(handle, std::move(id)), vmp_(vmp),
		config_(std::make_unique<config::VmdkConfig>(config)),
		aero_conn_(aero_conn) {
	uint32_t block_size;
	if (not config_->GetBlockSize(block_size)) {
		block_size = kDefaultBlockSize;
	}
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

AeroSpikeConn* ActiveVmdk::GetAeroConnection() const noexcept {
	return aero_conn_.get();
}

void ActiveVmdk::SetEventFd(int eventfd) noexcept {
	eventfd_ = eventfd;
}

VirtualMachine* ActiveVmdk::GetVM() const noexcept {
	return vmp_;
}

const config::VmdkConfig* ActiveVmdk::GetJsonConfig() const noexcept {
	return config_.get();
}

void ActiveVmdk::RegisterRequestHandler(std::unique_ptr<RequestHandler> handler) {
	if (not headp_) {
		headp_ = std::move(handler);
		return;
	}

	headp_->RegisterNextRequestHandler(std::move(handler));
}

folly::Future<int> ActiveVmdk::Read(Request* reqp) {
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

	++stats_.reads_in_progress_;
	return headp_->Read(this, reqp, process, failed)
	.then([this, reqp, process = std::move(process),
			failed = std::move(failed)] (folly::Try<int>& result) mutable {
		if (result.hasException<std::exception>()) {
			reqp->SetResult(-ENOMEM, RequestStatus::kFailed);
		} else {
			auto rc = result.value();
			if (pio_unlikely(rc < 0)) {
				reqp->SetResult(rc, RequestStatus::kFailed);
			} else if (pio_unlikely(not failed.empty())) {
				const auto blockp = failed.front();
				log_assert(blockp && blockp->IsFailed() && blockp->GetResult() != 0);
				reqp->SetResult(blockp->GetResult(), RequestStatus::kFailed);
			}
		}

		--stats_.reads_in_progress_;
		return reqp->Complete();
	});
}

folly::Future<int> ActiveVmdk::Write(Request* reqp, CheckPointID ckpt_id) {
	return WriteCommon(reqp, ckpt_id);
}

folly::Future<int> ActiveVmdk::WriteSame(Request* reqp, CheckPointID ckpt_id) {
	return WriteCommon(reqp, ckpt_id);
}

int ActiveVmdk::WriteComplete(Request* reqp) {
	--stats_.writes_in_progress_;
	auto rc = reqp->Complete();
	if (pio_unlikely(rc < 0)) {
		return rc;
	}

	auto[start, end] = reqp->Blocks();
	std::vector<decltype(start)> modified(end - start + 1);
	std::iota(modified.begin(), modified.end(), start);

	std::lock_guard<std::mutex> lock(blocks_.mutex_);
	blocks_.modified_.insert(modified.begin(), modified.end());
	blocks_.min_ = std::min(blocks_.min_, start);
	blocks_.max_ = std::max(blocks_.max_, end);
	return 0;
}

void ActiveVmdk::CopyDirtyBlocksSet(std::unordered_set<BlockID>& blocks,
		BlockID& start, BlockID& end) {
	start = end = 0;
	blocks.clear();

	std::lock_guard<std::mutex> guard(blocks_.mutex_);
	if (blocks_.modified_.empty()) {
		log_assert(blocks_.min_ == kBlockIDMax and blocks_.max_ == kBlockIDMin);
		return;
	}
	log_assert(blocks_.min_ != kBlockIDMax);

	std::exchange(blocks, blocks_.modified_);
	start = blocks_.min_;
	end = blocks_.max_;

	blocks_.modified_.clear();
	blocks_.min_ = kBlockIDMax;
	blocks_.max_ = kBlockIDMin;
}

folly::Future<int> ActiveVmdk::WriteCommon(Request* reqp, CheckPointID ckpt_id) {
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

	++stats_.writes_in_progress_;
	return headp_->Write(this, reqp, ckpt_id, process, failed)
	.then([this, reqp, process = std::move(process),
			failed = std::move(failed)] (folly::Try<int>& result) mutable {
		if (result.hasException<std::exception>()) {
			reqp->SetResult(-ENOMEM, RequestStatus::kFailed);
		} else {
			auto rc = result.value();
			if (pio_unlikely(rc < 0)) {
				reqp->SetResult(rc, RequestStatus::kFailed);
			} else if (pio_unlikely(not failed.empty())) {
				const auto blockp = failed.front();
				log_assert(blockp && blockp->IsFailed() && blockp->GetResult() != 0);
				reqp->SetResult(blockp->GetResult(), RequestStatus::kFailed);
			}
		}

		return WriteComplete(reqp);
	});
}

folly::Future<int> ActiveVmdk::TakeCheckPoint(CheckPointID ckpt_id) {
	std::unordered_set<BlockID> blocks;
	BlockID start, end;
	CopyDirtyBlocksSet(blocks, start, end);

	auto checkpoint = std::make_unique<CheckPoint>(GetID(), ckpt_id);
	if (pio_unlikely(not checkpoint)) {
		return -ENOMEM;
	}
	checkpoint->SetModifiedBlocks(blocks, start, end);

#if 0
	/*
	 * TODO:
	 * The CheckPoint serialize at the moment, creates a binary blob, this is
	 * going to change to JSON.
	 */
	auto s = checkpoint->Serialize();
	(void) s;
#endif

	{
		std::lock_guard<std::mutex> guard(checkpoints_.mutex_);
		if (pio_likely(not checkpoints_.unflushed_.empty())) {
			const auto& x = checkpoints_.unflushed_.back();
			log_assert(*x < *checkpoint);
		}
		checkpoints_.unflushed_.emplace_back(std::move(checkpoint));
	}
	return 0;
}

const CheckPoint* ActiveVmdk::GetCheckPoint(CheckPointID ckpt_id) const {
	std::lock_guard<std::mutex> lock(checkpoints_.mutex_);
	auto it1 = pio::BinarySearch(checkpoints_.unflushed_.begin(),
		checkpoints_.unflushed_.end(), ckpt_id, []
				(const std::unique_ptr<CheckPoint>& ckpt, CheckPointID ckpt_id) {
			return ckpt->ID() < ckpt_id;
		});
	if (it1 != checkpoints_.unflushed_.end()) {
		return it1->get();
	}

	auto it2 = pio::BinarySearch(checkpoints_.flushed_.begin(),
		checkpoints_.flushed_.end(), ckpt_id, []
				(const std::unique_ptr<CheckPoint>& ckpt, CheckPointID ckpt_id) {
			return ckpt->ID() < ckpt_id;
		});
	if (it2 != checkpoints_.flushed_.end()) {
		return it2->get();
	}
	return nullptr;
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
	header.bitset_size = blocks_bitset_.getSizeInBytes();

	auto size = sizeof(header) + header.bitset_size;
	size = AlignUpToBlockSize(size, kSectorSize);

	auto bufferp = NewRequestBuffer(size);
	if (pio_unlikely(not bufferp)) {
		throw std::bad_alloc();
	}
	log_assert(bufferp->Size() >= size);

	auto dp = bufferp->Payload();
	::memcpy(dp, reinterpret_cast<void *>(&header), sizeof(header));
	dp += sizeof(header);
	blocks_bitset_.write(dp);

	return bufferp;
}

void CheckPoint::SetModifiedBlocks(const std::unordered_set<BlockID>& blocks,
		BlockID first, BlockID last) {
	log_assert(blocks_bitset_.isEmpty());
	for (auto b : blocks) {
		blocks_bitset_.add(b);
	}
	blocks_bitset_.runOptimize();
	block_id_.first_ = first;
	block_id_.last_ = last;
}

CheckPointID CheckPoint::ID() const noexcept {
	return self_;
}

bool CheckPoint::operator < (const CheckPoint& rhs) const noexcept {
	return self_ < rhs.self_;
}

std::pair<BlockID, BlockID> CheckPoint::Blocks() const noexcept {
	return std::make_pair(block_id_.first_, block_id_.last_);
}

const Roaring& CheckPoint::GetRoraringBitMap() const noexcept {
	return blocks_bitset_;
}

}