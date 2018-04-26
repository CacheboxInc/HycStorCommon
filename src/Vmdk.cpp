#include <string>
#include <memory>
#include <vector>
#include <numeric>

#include <cstdint>
#include <sys/eventfd.h>

#include <thrift/lib/cpp/protocol/TJSONProtocol.h>
#include <thrift/lib/cpp/transport/TBufferTransports.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "DaemonUtils.h"
#include "DaemonTgtTypes.h"
#include "VmdkConfig.h"
#include "RequestHandler.h"
#include "Vmdk.h"

namespace pio {

const std::string CheckPoint::kCheckPoint = "CheckPoint";

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
		const std::string& config)
		: Vmdk(handle, std::move(id)), vmp_(vmp),
		config_(std::make_unique<config::VmdkConfig>(config)) { 
	uint32_t block_size;
	if (not config_->GetBlockSize(block_size)) {
		block_size = kDefaultBlockSize;
	}
	if (block_size & (block_size - 1)) {
		throw std::invalid_argument("Block Size is not power of 2.");
	}
	block_shift_ = PopCount(block_size-1);

	if (config_->IsRamMetaDataKV()) {
		metad_kv_ = std::make_unique<RamMetaDataKV>();
	} else {
		metad_kv_ = std::make_unique<AeroMetaDataKV>();
	}
	if (not metad_kv_) {
		throw std::bad_alloc();
	}
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

	auto failed = std::make_unique<std::vector<RequestBlock*>>();
	auto process = std::make_unique<std::vector<RequestBlock*>>();
	process->reserve(reqp->NumberOfRequestBlocks());
	reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
		process->emplace_back(blockp);
		return true;
	});

	++stats_.reads_in_progress_;
	return headp_->Read(this, reqp, *process, *failed)
	.then([this, reqp, process = std::move(process),
			failed = std::move(failed)] (folly::Try<int>& result) mutable {
		if (result.hasException<std::exception>()) {
			reqp->SetResult(-ENOMEM, RequestStatus::kFailed);
		} else {
			auto rc = result.value();
			if (pio_unlikely(rc < 0)) {
				reqp->SetResult(rc, RequestStatus::kFailed);
			} else if (pio_unlikely(not failed->empty())) {
				const auto blockp = failed->front();
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

int ActiveVmdk::WriteComplete(Request* reqp, CheckPointID ckpt_id) {
	--stats_.writes_in_progress_;
	auto rc = reqp->Complete();
	if (pio_unlikely(rc < 0)) {
		return rc;
	}

	auto[start, end] = reqp->Blocks();
	std::vector<decltype(start)> modified(end - start + 1);
	std::iota(modified.begin(), modified.end(), start);

	std::lock_guard<std::mutex> lock(blocks_.mutex_);
	auto it = blocks_.modified_.find(ckpt_id);
	if (pio_unlikely(it == blocks_.modified_.end())) {
		blocks_.modified_.insert(std::make_pair(ckpt_id,
			std::unordered_set<BlockID>()));
		it = blocks_.modified_.find(ckpt_id);
	}
	log_assert(it != blocks_.modified_.end());

	it->second.insert(modified.begin(), modified.end());
	return 0;
}

std::optional<std::unordered_set<BlockID>>
ActiveVmdk::CopyDirtyBlocksSet(CheckPointID ckpt_id) {
	std::lock_guard<std::mutex> guard(blocks_.mutex_);
	auto it = blocks_.modified_.find(ckpt_id);
	if (pio_unlikely(it == blocks_.modified_.end())) {
		return {};
	}

	return it->second;
}

void ActiveVmdk::RemoveDirtyBlockSet(CheckPointID ckpt_id) {
	std::lock_guard<std::mutex> guard(blocks_.mutex_);
	auto it = blocks_.modified_.find(ckpt_id);
	if (pio_unlikely(it == blocks_.modified_.end())) {
		return;
	}
	blocks_.modified_.erase(it);
}

folly::Future<int> ActiveVmdk::WriteCommon(Request* reqp, CheckPointID ckpt_id) {
	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}

	auto failed = std::make_unique<std::vector<RequestBlock*>>();
	auto process = std::make_unique<std::vector<RequestBlock*>>();
	process->reserve(reqp->NumberOfRequestBlocks());
	reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
		process->emplace_back(blockp);
		return true;
	});

	++stats_.writes_in_progress_;
	return headp_->Write(this, reqp, ckpt_id, *process, *failed)
	.then([this, reqp, ckpt_id, process = std::move(process),
			failed = std::move(failed)] (folly::Try<int>& result) mutable {
		if (result.hasException<std::exception>()) {
			reqp->SetResult(-ENOMEM, RequestStatus::kFailed);
		} else {
			auto rc = result.value();
			if (pio_unlikely(rc < 0)) {
				reqp->SetResult(rc, RequestStatus::kFailed);
			} else if (pio_unlikely(not failed->empty())) {
				const auto blockp = failed->front();
				log_assert(blockp && blockp->IsFailed() && blockp->GetResult() != 0);
				reqp->SetResult(blockp->GetResult(), RequestStatus::kFailed);
			}
		}

		return WriteComplete(reqp, ckpt_id);
	});
}

folly::Future<int> ActiveVmdk::TakeCheckPoint(CheckPointID ckpt_id) {
	if (pio_unlikely(ckpt_id != checkpoints_.last_checkpoint_ + 1)) {
		LOG(ERROR) << "last_checkpoint_ " << checkpoints_.last_checkpoint_
			<< " cannot checkpoint for " << ckpt_id;
		return -EINVAL;
	}

	auto blocks = CopyDirtyBlocksSet(ckpt_id);
	auto checkpoint = std::make_unique<CheckPoint>(GetID(), ckpt_id);
	if (pio_unlikely(not checkpoint)) {
		return -ENOMEM;
	}

	if (pio_likely(blocks)) {
		checkpoint->SetModifiedBlocks(std::move(blocks.value()));
	}

	auto json = checkpoint->Serialize();
	auto key = checkpoint->SerializationKey();

	return metad_kv_->Write(std::move(key), std::move(json))
	.then([this, ckpt_id, checkpoint = std::move(checkpoint)] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			/* ignore serialized write status */
		} else {
			checkpoint->SetSerialized();
		}

		RemoveDirtyBlockSet(ckpt_id);
		std::lock_guard<std::mutex> guard(checkpoints_.mutex_);
		if (pio_likely(not checkpoints_.unflushed_.empty())) {
			const auto& x = checkpoints_.unflushed_.back();
			log_assert(*x < *checkpoint);
		}
		checkpoints_.unflushed_.emplace_back(std::move(checkpoint));
		checkpoints_.last_checkpoint_ = ckpt_id;
		return 0;
	});
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

CheckPoint::CheckPoint(VmdkID vmdk_id, CheckPointID id) :
		vmdk_id_(std::move(vmdk_id)), self_(id) {
}

CheckPoint::~CheckPoint() = default;

std::string CheckPoint::Serialize() const {
	auto bs = blocks_bitset_.getSizeInBytes();
	auto size = AlignUpToBlockSize(bs, kSectorSize);

	auto bufferp = NewRequestBuffer(size);
	if (pio_unlikely(not bufferp)) {
		throw std::bad_alloc();
	}
	log_assert(bufferp->Size() >= size);

	auto bitmap = bufferp->Payload();
	blocks_bitset_.write(bitmap);

	ondisk::CheckPointOnDisk ckpt_od;
	ckpt_od.set_id(ID());
	ckpt_od.set_vmdk_id(vmdk_id_);
	ckpt_od.set_bitmap(std::string(bitmap, bs));
	ckpt_od.set_start(block_id_.first_);
	ckpt_od.set_end(block_id_.last_);
	ckpt_od.set_flushed(false);

	using S2 = apache::thrift::SimpleJSONSerializer;
	return S2::serialize<std::string>(ckpt_od);
}

std::string CheckPoint::SerializationKey() const {
	std::string key;
	StringDelimAppend(key, ':', {kCheckPoint, vmdk_id_, std::to_string(ID())});
	return key;
}

void CheckPoint::SetModifiedBlocks(const std::unordered_set<BlockID>& blocks) {
	log_assert(blocks_bitset_.isEmpty());
	for (auto b : blocks) {
		blocks_bitset_.add(b);
	}
	blocks_bitset_.runOptimize();
	block_id_.first_ = blocks_bitset_.minimum();
	block_id_.last_ = blocks_bitset_.maximum();
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

const Roaring& CheckPoint::GetRoaringBitMap() const noexcept {
	return blocks_bitset_;
}

void CheckPoint::SetSerialized() noexcept {
	serialized_ = true;
}

bool CheckPoint::IsSerialized() const noexcept {
	return serialized_;
}

void CheckPoint::SetFlushed() noexcept {
	flushed_ = true;
}

bool CheckPoint::IsFlushed() const noexcept {
	return flushed_;
}

}