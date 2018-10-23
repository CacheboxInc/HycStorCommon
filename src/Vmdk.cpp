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
#include "VmdkConfig.h"
#include "RequestHandler.h"
#include "FlushManager.h"
#include "FlushInstance.h"
#include "Vmdk.h"
#include "Singleton.h"

using namespace ::hyc_thrift;
using namespace ::ondisk;

namespace pio {

const std::string CheckPoint::kCheckPoint = "CheckPoint";
uint32_t kFlushPendingLimit = 32;
#define MAX_FLUSH_SIZE_IO 1024 * 1024

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

	aux_info_ = std::make_unique<FlushAuxData>();
	if (not aux_info_) {
		throw std::bad_alloc();
	}

	if (not config_->GetCleanupOnWrite(cleanup_on_write_)) {
		cleanup_on_write_ = true;
	}

	config_->GetParentDisk(parentdisk_set_);
	if (!config_->GetParentDiskVmdkId(parentdisk_vmdkid_)) {
		parentdisk_vmdkid_.clear();
	}

	// Let this always be the last code block, pulling it up does not harm anything
	// but just for the sake of rule, let this be the last code block
	read_aheadp_ = NULL;
	if(config_->IsReadAheadEnabled()) {
		LOG(INFO) << "ReadAhead is enabled";
		read_aheadp_ = std::make_unique<ReadAhead>(this);
		if (not read_aheadp_) {
			throw std::bad_alloc();
		}
	}
	else {
		LOG(INFO) << "ReadAhead is disabled";
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

int ActiveVmdk::Cleanup() {
	if (not headp_) {
		return 0;
	}
	return headp_->Cleanup(this);
}

void ActiveVmdk::GetCacheStats(VmdkCacheStats* vmdk_stats) const noexcept {
	vmdk_stats->read_populates_ =  cache_stats_.read_populates_;
	vmdk_stats->cache_writes_   =  cache_stats_.cache_writes_;
	vmdk_stats->read_hits_      = cache_stats_.read_hits_;
	vmdk_stats->write_hits_     = cache_stats_.write_hits_;
	vmdk_stats->read_miss_      = cache_stats_.read_miss_;
	vmdk_stats->write_miss_     = cache_stats_.write_miss_;
	vmdk_stats->read_failed_    = cache_stats_.read_failed_;
	vmdk_stats->write_failed_   = cache_stats_.write_failed_;

	vmdk_stats->reads_in_progress_  = stats_.reads_in_progress_;
	vmdk_stats->writes_in_progress_ = stats_.writes_in_progress_;

	if(pio_likely(read_aheadp_)) {
		vmdk_stats->read_ahead_blks_ = read_aheadp_->StatsTotalReadAheadBlocks();
	}
}

CheckPointID ActiveVmdk::GetModifiedCheckPoint(BlockID block,
		const CheckPoints& min_max, bool& found) const {
	auto [min, max] = min_max;
	log_assert(min > MetaData_constants::kInvalidCheckPointID());

	{
		std::lock_guard<std::mutex> lock(blocks_.mutex_);
		for (; min <= max; --max) {
			if (auto it = blocks_.modified_.find(max);
					it == blocks_.modified_.end()) {
				break;
			} else {
				const auto& blocks = it->second;
				if (blocks.find(block) != blocks.end()) {
					return max;
				}
			}
		}
	}

	auto cur_max = MetaData_constants::kInvalidCheckPointID();
	size_t it_index;

	std::lock_guard<std::mutex> lock(checkpoints_.mutex_);
	auto eit = checkpoints_.unflushed_.end();
	for (auto it = checkpoints_.unflushed_.begin(); it != eit;) {
		if (it->get()->ID() > max || it->get()->ID() < min) {
			++it;
			continue;
		}

		if (it->get()->ID() > cur_max) {
			cur_max = it->get()->ID();
			it_index = it - checkpoints_.unflushed_.begin();
		}

		++it;
	}

	if (cur_max != MetaData_constants::kInvalidCheckPointID()) {
		auto it = checkpoints_.unflushed_.begin() + it_index;
		const auto& bitmap = it->get()->GetRoaringBitMap();
		if (bitmap.contains(block)) {
			return it->get()->ID();
		}
	}

	cur_max = MetaData_constants::kInvalidCheckPointID();
	eit = checkpoints_.flushed_.end();
	for (auto it = checkpoints_.flushed_.begin(); it != eit; ) {
		if (it->get()->ID() > max || it->get()->ID() < min) {
			++it;
			continue;
		}

		if (it->get()->ID() > cur_max) {
			cur_max = it->get()->ID();
			it_index = it - checkpoints_.flushed_.begin();
		}

		++it;
	}

	if (cur_max != MetaData_constants::kInvalidCheckPointID()) {
		auto it = checkpoints_.flushed_.begin() + it_index;
		const auto& bitmap = it->get()->GetRoaringBitMap();
		if (bitmap.contains(block)) {
			return it->get()->ID();
		}
	}

	/*
	 * FIXME:
	 * - Take initial CheckPoint when VMDK is first added
	 *   - Since code to take initial CheckPoint is missing, we return
	 *     MetaData_constants::kInvalidCheckPointID() + 1 from this function for now
	 * - Return MetaData_constants::kInvalidCheckPointID() from here
	 */

	found = false;
	return MetaData_constants::kInvalidCheckPointID() + 1;
}

void ActiveVmdk::SetReadCheckPointId(const std::vector<RequestBlock*>& blockps,
		const CheckPoints& min_max) const {
	for (auto blockp : blockps) {
		auto block = blockp->GetBlockID();
		bool found = true;
		/* find latest checkpoint in which the block is modified */
		auto ckpt_id = GetModifiedCheckPoint(block, min_max, found);
		log_assert(ckpt_id != MetaData_constants::kInvalidCheckPointID());
		blockp->SetReadCheckPointId(ckpt_id);
		blockp->AssignSet(found);
	}
}

folly::Future<int> ActiveVmdk::Read(Request* reqp, const CheckPoints& min_max) {
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
	SetReadCheckPointId(*process, min_max);

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

folly::Future<int> ActiveVmdk::BulkRead(const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {
	SetReadCheckPointId(process, min_max);
	++stats_.reads_in_progress_;

	auto failed = std::make_unique<std::vector<RequestBlock*>>();
	return headp_->BulkRead(this, requests, process, *failed)
	.then([this, failed = std::move(failed), &requests]
			(folly::Try<int>& result) mutable {
		auto failedp = failed.get();
		auto Fail = [&requests, &failedp] (int rc = -ENXIO, bool all = true) {
			if (all) {
				for (auto& request : requests) {
					request->SetResult(rc, RequestStatus::kFailed);
				}
			} else {
				for (auto& bp : *failedp) {
					auto reqp = bp->GetRequest();
					reqp->SetResult(bp->GetResult(), RequestStatus::kFailed);
				}
			}
		};
		if (pio_unlikely(result.hasException())) {
			Fail();
		} else {
			auto rc = result.value();
			if (pio_unlikely(rc < 0 || not failedp->empty())) {
				Fail(rc, not failedp->empty());
			}
		}

		int32_t res = 0;
		--stats_.reads_in_progress_;
		for (auto& reqp : requests) {
			auto rc = reqp->Complete();
			if (pio_unlikely(rc < 0)) {
				res = rc;
			}
		}
		return res;
	});
}

folly::Future<int> ActiveVmdk::Flush(Request* reqp, const CheckPoints& min_max) {
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

	SetReadCheckPointId(*process, min_max);

	++stats_.flushes_in_progress_;
	return headp_->Flush(this, reqp, *process, *failed)
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

		--stats_.flushes_in_progress_;
		return reqp->Complete();
	});
}

folly::Future<int> ActiveVmdk::Move(Request* reqp, const CheckPoints& min_max) {
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

	SetReadCheckPointId(*process, min_max);
	++stats_.moves_in_progress_;
	return headp_->Move(this, reqp, *process, *failed)
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

		--stats_.moves_in_progress_;
		return reqp->Complete();
	});
}

folly::Future<int> ActiveVmdk::Write(Request* reqp, CheckPointID ckpt_id) {
	return WriteCommon(reqp, ckpt_id);
}

folly::Future<int> ActiveVmdk::WriteSame(Request* reqp, CheckPointID ckpt_id) {
	return WriteCommon(reqp, ckpt_id);
}

int ActiveVmdk::WriteRequestComplete(Request* reqp, CheckPointID ckpt_id) {
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

int ActiveVmdk::WriteComplete(Request* reqp, CheckPointID ckpt_id) {
	--stats_.writes_in_progress_;
	return WriteRequestComplete(reqp, ckpt_id);
}

int ActiveVmdk::WriteComplete(
		const std::vector<std::unique_ptr<Request>>& requests,
		CheckPointID ckpt_id) {
	int ret = 0;
	--stats_.writes_in_progress_;
	for (auto& request : requests) {
		auto rc = WriteRequestComplete(request.get(), ckpt_id);
		if (pio_unlikely(rc < 0)) {
			ret = rc;
		}
	}
	return ret;
}

folly::Future<int> ActiveVmdk::BulkWrite(::ondisk::CheckPointID ckpt_id,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {
	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}

	++stats_.writes_in_progress_;
	auto failed = std::make_unique<std::vector<RequestBlock*>>();

	return headp_->BulkWrite(this, ckpt_id, requests, process, *failed)
	.then([this, ckpt_id, &requests, failed = std::move(failed)]
			(folly::Try<int>& result) mutable {
		auto failedp = failed.get();
		auto Fail = [&requests, failedp] (int rc = -ENXIO, bool all = true)
				mutable {
			if (all) {
				for (auto& request : requests) {
					request->SetResult(rc, RequestStatus::kFailed);
				}
			} else {
				for (auto blockp : *failedp) {
					auto reqp = blockp->GetRequest();
					reqp->SetResult(blockp->GetResult(), RequestStatus::kFailed);
				}
			}
		};

		if (pio_unlikely(result.hasException())) {
			Fail();
		} else {
			auto rc = result.value();
			if (pio_unlikely(rc < 0 || not failedp->empty())) {
				Fail(rc, failedp->empty());
			}
		}

		return WriteComplete(requests, ckpt_id);
	});
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

int ActiveVmdk::FlushStages(CheckPointID ckpt_id, bool perform_move) {

	auto rc = FlushStage(ckpt_id);
	if (rc) {
		return rc;
	}

	/* TBD: At this point searalize information at persistent storage
	 * specifying that flush stage is done and in case of failure retry
	 * we don't have to do it again.
	 *
	 * We should more relax about error conditions in Move stage because
	 * in context of retry we may not find some blocks in DIRTY namespace
	 * (already moved from DIRTY to CLEAN in previous attempts). A check
	 * can be added that the blocks those are not present in DIRTY
	 * should be in CLEAN namespace with same checkpoint id and in that
	 * case we can ignore errors.
	 */

	if (pio_likely(perform_move)) {
		rc = MoveStage(ckpt_id);
		if (rc) {
			return rc;
		}
	}

	/* TBD: Mark flush done for this checkpoint and searalize this
	 * information.
	 */

	return 0;
}

int ActiveVmdk::MoveStage(CheckPointID ckpt_id) {

	/*
	 * TBD :- Since a fix number of requests can under process
	 * at time, we can create those many records upfront to avoid
	 * creating and destroying it again and again (slab)
	 */

	aux_info_->InitState(FlushAuxData::FlushStageType::kMoveStage);
	int32_t size = BlockSize();

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == false);

	auto min_max = std::make_pair(ckpt_id, ckpt_id);
	const auto& bitmap = ckptp->GetRoaringBitMap();
	for (const auto& block : bitmap) {
		aux_info_->lock_.lock();
		if (aux_info_->failed_) {
			aux_info_->lock_.unlock();
			break;
		}

		if(aux_info_->pending_cnt_ >= kFlushPendingLimit) {
			/* Already submitted too much, wait for completion */
			aux_info_->sleeping_ = true;
			aux_info_->rendez_.TaskSleep(&aux_info_->lock_);
			if (aux_info_->failed_) {
				aux_info_->lock_.unlock();
				break;
			}
		}

		aux_info_->pending_cnt_++;
		aux_info_->lock_.unlock();
		auto iobuf = NewRequestBuffer(size);
		if (pio_unlikely(not iobuf)) {
			throw std::bad_alloc();
		}

		++aux_info_->reqid_;
		auto reqp = std::make_unique<Request>(aux_info_->reqid_, this,
				Request::Type::kMove,
				iobuf->Payload(), size, size, block * BlockSize());
		reqp->SetFlushReq();
		reqp->SetFlushCkptID(ckpt_id);

		this->Move(reqp.get(), min_max)
		.then([this, iobuf = std::move(iobuf), reqp = std::move(reqp)] (int rc) mutable {

			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << "Some of the Move request failed";
				aux_info_->failed_ = true;
			}

			if (aux_info_->sleeping_) {

				/*
				 * Sleeping in done context then wakeup from
				 * last completion otherwise wakeup to continue
				 * the pipeline
				 */

				if (aux_info_->done_) {
					if (!aux_info_->pending_cnt_) {
						aux_info_->sleeping_ = false;
						aux_info_->rendez_.TaskWakeUp();
					}
				} else {
					aux_info_->sleeping_ = false;
					aux_info_->rendez_.TaskWakeUp();
				}
			}

			++aux_info_->moved_blks_;
			aux_info_->lock_.unlock();
		});
	}

	/* Set done and wait for completion of all */
	aux_info_->lock_.lock();
	aux_info_->done_ = true;
	if (aux_info_->pending_cnt_) {
		aux_info_->sleeping_ = true;
		aux_info_->rendez_.TaskSleep(&aux_info_->lock_);
	}

	log_assert(aux_info_->sleeping_ == false);
	log_assert(aux_info_->pending_cnt_ == 0);
	aux_info_->lock_.unlock();

	return aux_info_->failed_;
}

int ActiveVmdk::FlushStage(CheckPointID ckpt_id) {

	/*
	 * TBD :- Since a fix number of requests can under process
	 * at time, we can create those many records upfront to avoid
	 * creating and destroying it again and again (slab)
	 */

	aux_info_->InitState(FlushAuxData::FlushStageType::kFlushStage);
	int32_t size = 0;

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == false);

	auto min_max = std::make_pair(ckpt_id, ckpt_id);
	int64_t prev_block = -1, cur_block = -1, start_block = -1;
	uint32_t block_cnt = 0;

	const auto& bitmap = ckptp->GetRoaringBitMap();
	for (const auto& block : bitmap) {

		/* TBD: Try to generate WAN (on prem disk) friendly
		 * IO's by doing look ahead in checkpoint
		 * bitmap and probably generating large sequential
		 * requests.
		 */

		if (start_block == -1) {
			start_block = block;
		}

		cur_block = block;
		if (prev_block == -1) {
			/* Initial, very first */
			prev_block = cur_block;
			block_cnt = 1;
			continue;
		} else if (prev_block + 1 == cur_block) {
			/* Sequential, can we accomadate it */
			if (BlockSize() * (block_cnt + 1) <= MAX_FLUSH_SIZE_IO) {
				prev_block = cur_block;
				block_cnt++;
				continue;
			}
		}

		aux_info_->lock_.lock();
		if (aux_info_->failed_) {
			aux_info_->lock_.unlock();
			break;
		}

		if (aux_info_->pending_cnt_ >= kFlushPendingLimit) {
			/* Already submitted too much, wait for a completion */
			aux_info_->sleeping_ = true;
			aux_info_->rendez_.TaskSleep(&aux_info_->lock_);
			if (aux_info_->failed_) {
				aux_info_->lock_.unlock();
				break;
			}
		}

		aux_info_->pending_cnt_++;
		aux_info_->lock_.unlock();

		size = block_cnt * BlockSize();
		LOG (ERROR) << "blk count::" << block_cnt
			<< ", start block::" << start_block << ", size::" << size;
		auto iobuf = NewRequestBuffer(size);
		if (pio_unlikely(not iobuf)) {
			throw std::bad_alloc();
			aux_info_->failed_ = true;
			break;
		}

		++aux_info_->reqid_;
		auto reqp = std::make_unique<Request>(aux_info_->reqid_, this,
				Request::Type::kRead,
				iobuf->Payload(), size, size, start_block * BlockSize());
		reqp->SetFlushReq();
		reqp->SetFlushCkptID(ckpt_id);

		this->Flush(reqp.get(), min_max)
		.then([this, iobuf = std::move(iobuf), reqp = std::move(reqp),
			block_cnt = block_cnt] (int rc) mutable {

			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << "Some of the flush request failed";
				aux_info_->failed_ = true;
			}

			if (aux_info_->sleeping_) {

				/*
				 * Sleeping in done context then wakeup from
				 * last completion otherwise wakeup to continue
				 * the pipeline
				 */

				if (aux_info_->done_) {
					if (!aux_info_->pending_cnt_) {
						aux_info_->sleeping_ = false;
						aux_info_->rendez_.TaskWakeUp();
					}
				} else {
					aux_info_->sleeping_ = false;
					aux_info_->rendez_.TaskWakeUp();
				}
			}

			aux_info_->flushed_blks_+= block_cnt;
			aux_info_->lock_.unlock();
		});

		/* Adjust the values before moving ahead */
		start_block = cur_block;
		prev_block = cur_block;
		block_cnt = 1;
		size = 0;
	}

	LOG (ERROR) << "blk count::" << block_cnt
		<< "start block::" << start_block << "size::" << size;
	/* Submit any last pending accumlated IOs */
	aux_info_->lock_.lock();
	if (aux_info_->failed_ || block_cnt == 0) {
		aux_info_->lock_.unlock();
	} else {
		aux_info_->pending_cnt_++;
		aux_info_->lock_.unlock();
		size = block_cnt * BlockSize();
		LOG (ERROR) << "blk count::" << block_cnt
			<< "start block::" << start_block << "size::" << size;
		auto iobuf = NewRequestBuffer(size);
		if (pio_unlikely(not iobuf)) {
			throw std::bad_alloc();
		}

		++aux_info_->reqid_;
		auto reqp = std::make_unique<Request>(aux_info_->reqid_, this,
			Request::Type::kRead,
			iobuf->Payload(), size, size, start_block * BlockSize());
		reqp->SetFlushReq();
		reqp->SetFlushCkptID(ckpt_id);

		this->Flush(reqp.get(), min_max)
		.then([this, iobuf = std::move(iobuf),
			reqp = std::move(reqp), block_cnt = block_cnt]
				(int rc) mutable {

			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << "Some of the flush requests are failed";
				aux_info_->failed_ = true;
			}

			if (aux_info_->sleeping_) {

				/*
				 * Sleeping in done context then wakeup from
				 * last completion otherwise wakeup to continue
				 * the pipeline
				 */

				if (aux_info_->done_) {
					if (!aux_info_->pending_cnt_) {
						aux_info_->sleeping_ = false;
						aux_info_->rendez_.TaskWakeUp();
					}
				} else {
					aux_info_->sleeping_ = false;
					aux_info_->rendez_.TaskWakeUp();
				}
			}

			aux_info_->flushed_blks_+= block_cnt;
			aux_info_->lock_.unlock();
		});
	}

	/* Set done and wait for completion of all */
	aux_info_->lock_.lock();
	aux_info_->done_ = true;
	if (aux_info_->pending_cnt_) {
		aux_info_->sleeping_ = true;
		aux_info_->rendez_.TaskSleep(&aux_info_->lock_);
	}

	log_assert(aux_info_->sleeping_ == false);
	log_assert(aux_info_->pending_cnt_ == 0);
	aux_info_->lock_.unlock();

	return aux_info_->failed_;
}

int ActiveVmdk::SetCkptBitmap(CheckPointID ckpt_id,
	std::unordered_set<::ondisk::BlockID>& blocks) {

	auto checkpoint = GetCheckPoint(ckpt_id);
	if (pio_unlikely(checkpoint == nullptr)) {
		LOG(ERROR) << __func__ << " No checkpoint information found"
			" for given checkpoint ID::" << ckpt_id;
		return -EINVAL;
	}

	if (pio_likely(blocks.size())) {

		/*
		 * TBD : Mark the checkpoint unserialized because after this point
		 * on disk state (if any) is inconsistent and checkpoint need to
		 * be serialized again post all the bitmap updates (in context of
		 * commit checkpoint). Serialized this information to disk.
		 */

		if (pio_unlikely(checkpoint->IsSerialized())) {
			LOG(ERROR) << __func__ << " Resetting the Serialized flag for VMDKID:" << GetID();
			checkpoint->UnsetSerialized();
		}

		checkpoint->SetModifiedBlocks(std::move(blocks));
	}

	return 0;
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

		std::lock_guard<std::mutex> guard(checkpoints_.mutex_);
		if (pio_likely(not checkpoints_.unflushed_.empty())) {
			const auto& x = checkpoints_.unflushed_.back();
			log_assert(*x < *checkpoint);
		}
		checkpoints_.unflushed_.emplace_back(std::move(checkpoint));
		checkpoints_.last_checkpoint_ = ckpt_id;
		RemoveDirtyBlockSet(ckpt_id);
		return 0;
	});
}

folly::Future<int> ActiveVmdk::CommitCheckPoint(CheckPointID ckpt_id) {

	LOG(ERROR) << __func__ << " Commit on checkpoint ID:" << ckpt_id;
	auto checkpoint = GetCheckPoint(ckpt_id);
	if (pio_unlikely(not checkpoint)) {
		return -ENOMEM;
	}

	/*
	 * Some of the disks may be untouched during bitmap update, doesn't
	 * need to seralized them again (serialized flag has not been flipped
	 * for them
	 */

	if (pio_unlikely(checkpoint->IsSerialized())) {
		LOG(ERROR) << __func__ << " Serialized flag is already set for VMDKID:" << GetID();
		return 0;
	} else {
		LOG(ERROR) << __func__ << " Serialized flag is false for VMDKID:" << GetID();
	}

	auto json = checkpoint->Serialize();
	auto key = checkpoint->SerializationKey();
	return metad_kv_->Write(std::move(key), std::move(json))
	.then([checkpoint = std::move(checkpoint)] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			/* ignore serialized write status */
		} else {
			checkpoint->SetSerialized();
		}

		return 0;
	});
}

CheckPoint* ActiveVmdk::GetCheckPoint(CheckPointID ckpt_id) const {
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

void CheckPoint::UnsetSerialized() noexcept {
	serialized_ = false;
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
