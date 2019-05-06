#include <string>
#include <memory>
#include <vector>
#include <numeric>
#include <atomic>

#include <cstdint>
#include <sys/eventfd.h>

#include <thrift/lib/cpp/protocol/TJSONProtocol.h>
#include <thrift/lib/cpp/transport/TBufferTransports.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <folly/fibers/Fiber.h>
#include <folly/fibers/Baton.h>


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
#if 1
#define USE_MOVE_VERSION2 1
#define USE_FLUSH_VERSION2 1
#endif

using namespace ::hyc_thrift;
using namespace ::ondisk;
const uint32_t kMaxFailureTolerateCnt = 10240000;

namespace pio {

const std::string CheckPoint::kCheckPoint = "M";

Vmdk::Vmdk(VmdkHandle handle, VmdkID&& id) : handle_(handle), id_(std::move(id)), disk_size_bytes_(0) {
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
		config_(std::make_unique<config::VmdkConfig>(config)),
		range_lock_ (std::make_unique<RangeLock::RangeLock>()) {
	if (not config_->GetVmdkUUID(vmdk_uuid_)) {
		throw std::invalid_argument("vmdk uuid is not set.");
	}

	uint32_t block_size;
	if (not config_->GetBlockSize(block_size)) {
		block_size = kDefaultBlockSize;
	}
	if (block_size & (block_size - 1)) {
		throw std::invalid_argument("Block Size is not power of 2.");
	}
	block_shift_ = PopCount(block_size-1);

	if (config_->IsRamMetaDataKV() || 1) {
		metad_kv_ = std::make_unique<RamMetaDataKV>();
	} else {
		LOG(ERROR) << __func__ << "KV store is AeroSpike";
		metad_kv_ = std::make_unique<AeroMetaDataKV>(this);
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

	ComputePreloadBlocks();
	
	stats_ = new VmdkCacheStats();
	delta_file_path_ = config_->GetDeltaFileTargetPath();
	if (delta_file_path_.length()) {
		delta_file_path_ += "/deltadisk_";
		delta_file_path_ += ":" ;
		delta_file_path_ += GetID();
	}

	config_->GetDiskSize(disk_size_bytes_);
	LOG(INFO) << "VmdkID =  " << id << " Disk Size = " << disk_size_bytes_;
	// Let this always be the last code block, pulling it up does not harm anything
	// but just for the sake of rule, let this be the last code block
	read_aheadp_ = NULL;
	auto disk_size_supported = ReadAhead::GetMinDiskSizeSupported();
	if(config_->IsReadAheadEnabled()) {
		if(disk_size_supported <= disk_size_bytes_) {
			read_aheadp_ = std::make_unique<ReadAhead>(this);
			if(not read_aheadp_) {
				throw std::bad_alloc();
			}
			LOG(INFO) << "ReadAhead is enabled";
		}
		else {
			LOG(WARNING) << "ReadAhead is disabled because DiskSize: " << disk_size_bytes_ 
				<< " is less than DiskSizeSupported: " << disk_size_supported;
		}
	}
	else {
		LOG(INFO) << "ReadAhead is disabled";
	}
}

ActiveVmdk::~ActiveVmdk() {
	/* destroy layers before rest of the ActiveVmdk object is destroyed */
	headp_ = nullptr;
	delete stats_;
}

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

RequestHandler* ActiveVmdk::GetRequestHandler(const char* namep) noexcept {
	return headp_->GetRequestHandler(namep);
}

int ActiveVmdk::Cleanup() {
	if (not headp_) {
		return 0;
	}
	return headp_->Cleanup(this);
}

const std::vector<PreloadBlock>& ActiveVmdk::GetPreloadBlocks() const noexcept {
	return preload_blocks_;
}

int ActiveVmdk::UpdatefdMap(const int64_t& snap_id, const int32_t& fd) {
	std::lock_guard<std::mutex> lock(mutex_);
	if (fd >= 0) {
		std::string key = GetID() + ":" + std::to_string(snap_id);
		fd_map_.emplace(std::make_pair(key, fd));
		return 0;
	}
	return -1;
}

int ActiveVmdk::CreateNewVmdkDeltaContext(int64_t snap_id) {

	/* TBD: FileTargetHandler with encryption and compression not supported,
	 * add a check here */

	std::string file_path = delta_file_path_;
	LOG(ERROR) << __func__ << "Initial Path is:" << file_path.c_str();

	/* TBD : Check that file doen't exist already, otherwise fail */
	file_path += ":";
	file_path += std::to_string(snap_id);
	LOG(ERROR) << __func__ << "Final Path is:" << file_path.c_str();
	auto fd = ::open(file_path.c_str(), O_RDWR | O_SYNC | O_DIRECT| O_CREAT, 0777);
	if (pio_unlikely(fd == -1)) {
		throw std::runtime_error("Delta File create failed");
	} else {
		#if 0
		if(ftruncate(fd, file_size_)) {
			throw std::runtime_error("Delta File truncate failed");
		}
		#endif
	}

	LOG(ERROR) << __func__ << "file fd_ is:" << fd;
	if (delta_fd_ >= 0) {
		LOG(ERROR) << "Update fd map for delta_fd:" << delta_fd_ << ", snap_id:" << snap_id;
		UpdatefdMap(snap_id - 1, delta_fd_);
	}

	/* Update the fd which is being used for write */
	delta_fd_ = fd;
	return 0;
}

void ActiveVmdk::ComputePreloadBlocks() {
	std::vector<std::pair<Offset, uint32_t>> pl;
	config_->GetPreloadBlocks(pl);
	if (pl.empty()) {
		return;
	}

	std::vector<BlockID> blocks;
	for (const auto& p : pl) {
		auto ids = pio::GetBlockIDs(p.first, p.second, BlockShift());
		for (const auto v : pio::iter::Range(ids.first, ids.second+1)) {
			blocks.emplace_back(v);
		}
	}

	RemoveDuplicate(blocks);
	MergeConsecutive(blocks.begin(), blocks.end(),
		std::back_insert_iterator(preload_blocks_),
		kBulkReadMaxSize >> BlockShift());
	if (VLOG_IS_ON(2)) {
		if (not preload_blocks_.empty()) {
			LOG(INFO) << "List of blocks to preload ";
			for (const auto& b : preload_blocks_) {
				LOG(INFO) << "start block " << b.first << " nblocks = " << b.second;
			}
		} else {
			LOG(INFO) << "no blocks to preload";
		}
	}
}

void ActiveVmdk::GetCacheStats(VmdkCacheStats* vmdk_stats) const noexcept {

	if (pio_likely(read_aheadp_)) {
		vmdk_stats->read_ahead_blks_ = read_aheadp_->StatsTotalReadAheadBlocks();
		vmdk_stats->rh_random_patterns_ = read_aheadp_->StatsTotalRandomPatterns();
		vmdk_stats->rh_strided_patterns_ = read_aheadp_->StatsTotalStridedPatterns();
		vmdk_stats->rh_correlated_patterns_ = read_aheadp_->StatsTotalCorrelatedPatterns();
		vmdk_stats->rh_dropped_reads_ = read_aheadp_->StatsTotalDroppedReads();

		auto app_reads = stats_->total_blk_reads_ - vmdk_stats->read_ahead_blks_;
		vmdk_stats->total_blk_reads_ = app_reads;

		atomic_store(&vmdk_stats->total_reads_, atomic_load(&(stats_->total_reads_)));
		vmdk_stats->read_miss_       = read_aheadp_->StatsTotalReadMissBlocks();
		vmdk_stats->read_hits_       = app_reads - vmdk_stats->read_miss_;
	} else {
		atomic_store(&vmdk_stats->total_blk_reads_, atomic_load(&stats_->total_blk_reads_));
		atomic_store(&vmdk_stats->total_reads_, atomic_load(&stats_->total_reads_));
		atomic_store(&vmdk_stats->read_miss_, atomic_load(&stats_->read_miss_));
		atomic_store(&vmdk_stats->read_hits_, atomic_load(&stats_->read_hits_));
	}

	atomic_store(&vmdk_stats->total_writes_, atomic_load(&stats_->total_writes_));
	atomic_store(&vmdk_stats->read_populates_, atomic_load(&stats_->read_populates_));
	atomic_store(&vmdk_stats->cache_writes_   , atomic_load(&stats_->cache_writes_));
	atomic_store(&vmdk_stats->read_failed_    , atomic_load(&stats_->read_failed_));
	atomic_store(&vmdk_stats->write_failed_   , atomic_load(&stats_->write_failed_));

	atomic_store(&vmdk_stats->reads_in_progress_, atomic_load(&stats_->reads_in_progress_));
	atomic_store(&vmdk_stats->writes_in_progress_, atomic_load(&stats_->writes_in_progress_));

	vmdk_stats->flushes_in_progress_ = stats_->FlushesInProgress();
	vmdk_stats->moves_in_progress_   = stats_->MovesInProgress();

	vmdk_stats->block_size_  = BlockSize();

	vmdk_stats->flushed_chkpnts_   = FlushedCheckpoints();
	vmdk_stats->unflushed_chkpnts_ = UnflushedCheckpoints();

	vmdk_stats->flushed_blocks_ = GetFlushedBlksCnt();
	vmdk_stats->moved_blocks_   = GetMovedBlksCnt() ;
	vmdk_stats->pending_blocks_ = GetPendingBlksCnt();

	atomic_store(&vmdk_stats->nw_bytes_write_   , atomic_load(&stats_->nw_bytes_write_));
	atomic_store(&vmdk_stats->nw_bytes_read_    , atomic_load(&stats_->nw_bytes_read_));
	atomic_store(&vmdk_stats->aero_bytes_write_ , atomic_load(&stats_->aero_bytes_write_));
	atomic_store(&vmdk_stats->aero_bytes_read_  , atomic_load(&stats_->aero_bytes_read_));

	atomic_store(&vmdk_stats->total_bytes_reads_  , atomic_load(&stats_->total_bytes_reads_));
	atomic_store(&vmdk_stats->total_bytes_writes_ , atomic_load(&stats_->total_bytes_writes_));

	atomic_store(&vmdk_stats->bufsz_before_compress , atomic_load(&stats_->bufsz_before_compress));
	atomic_store(&vmdk_stats->bufsz_after_compress , atomic_load(&stats_->bufsz_after_compress));
	atomic_store(&vmdk_stats->bufsz_before_uncompress ,
			atomic_load(&stats_->bufsz_before_uncompress));
	atomic_store(&vmdk_stats->bufsz_after_uncompress ,
			atomic_load(&stats_->bufsz_after_uncompress));
}

void ActiveVmdk::FillCacheStats(IOAVmdkStats& dest) const noexcept {
	stats_->GetDeltaCacheStats(old_stats_, dest);
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

	for (auto ckpt = max; min <= ckpt; --ckpt) {
		auto ckptp = GetCheckPoint(ckpt);
		if (not ckptp) {
			//log_assert(ckpt == min_max.second);
			continue;
		}
		const auto& bitmap = ckptp->GetRoaringBitMap();
		if (bitmap.contains(block)) {
			return ckptp->ID();
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
		/* Track number of reads per checkpoint */
		//GetVM()->IncCheckPointRef(ckpt_id);
	}
}

void ActiveVmdk::IncCheckPointRef(const std::vector<RequestBlock*>& blockps) {
	for (auto blockp : blockps) {
		auto ckpt_id = blockp->GetReadCheckPointId();
		log_assert(ckpt_id != MetaData_constants::kInvalidCheckPointID());
		/* Track number of reads per checkpoint */
		auto vmp = GetVM();
		if (pio_likely(vmp)) {
			vmp->IncCheckPointRef(ckpt_id);
			VLOG(5) << __func__ << "Inc::"
				<< blockp->GetAlignedOffset() << "::" << ckpt_id;
		}
	}
}

void ActiveVmdk::DecCheckPointRef(const std::vector<RequestBlock*>& blockps) {
	for (auto blockp : blockps) {
		auto ckpt_id = blockp->GetReadCheckPointId();
		log_assert(ckpt_id != MetaData_constants::kInvalidCheckPointID());
		/* Track number of reads per checkpoint */
		auto vmp = GetVM();
		if (pio_likely(vmp)) {
			vmp->DecCheckPointRef(ckpt_id);
			VLOG(5) << __func__ << "Dec::"
				<< blockp->GetAlignedOffset() << "::" << ckpt_id;
		}
	}
}

folly::Future<int> ActiveVmdk::Read(Request* reqp, const CheckPoints& min_max) {
	assert(reqp);
	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}
	auto[start, end] = reqp->Blocks();
	return TakeLockAndInvoke(start, end,
			[this, reqp, min_max = std::move(min_max)] () {

		auto failed = std::make_unique<std::vector<RequestBlock*>>();
		auto process = std::make_unique<std::vector<RequestBlock*>>();
		process->reserve(reqp->NumberOfRequestBlocks());
		reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
			process->emplace_back(blockp);
			return true;
		});
		SetReadCheckPointId(*process, min_max);
		IncCheckPointRef(*process);

		++stats_->reads_in_progress_;
		++stats_->total_reads_;
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

			--stats_->reads_in_progress_;
			stats_->IncrReadBytes(reqp->GetBufferSize());
			DecCheckPointRef(*process);
			return reqp->Complete();
		});
	});
}

int32_t ActiveVmdk::SetCompletionResults(const std::vector<std::unique_ptr<Request>>& requests,
	const std::vector<RequestBlock*>& failed, folly::Try<int>& result) {
	auto Fail = [&requests, &failed] (int rc = -ENXIO, bool all = true) {
	if (all) {
		for (auto& request : requests) {
			request->SetResult(rc, RequestStatus::kFailed);
		}
	} else {
		for (auto& bp : failed) {
			auto reqp = bp->GetRequest();
			reqp->SetResult(bp->GetResult(), RequestStatus::kFailed);
		}
	}
	};
	if (pio_unlikely(result.hasException())) {
		Fail();
	} else {
		auto rc = result.value();
		if (pio_unlikely(rc < 0 || not failed.empty())) {
			Fail(rc, not failed.empty());
		}
	}
	int32_t res = 0;
	stats_->reads_in_progress_ -= requests.size();
	for (auto& reqp : requests) {
		auto rc = reqp->Complete();
		if (pio_unlikely(rc < -1)) {
			res = rc;
		}
		stats_->IncrReadBytes(reqp->GetBufferSize());
	}
	return res;
}

folly::Future<int> ActiveVmdk::BulkRead(const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {
	if(requests[0]->IsReadAheadRequest()) {
		// TryLock implementation for ReadAhead
		// Selective-non-block-locking on all ranges in the Request vector
		// Emits locked_process which contains only those blocks for which a lock
		// was successfully acquired without blocking
		auto locked_process = std::make_unique<std::vector<RequestBlock*>>();
		return TryLockAndBulkInvoke(requests, process, *locked_process,
			[this, &requests, &locked_process, min_max = std::move(min_max)]() {
        	SetReadCheckPointId(*locked_process, min_max);
        	stats_->reads_in_progress_ += requests.size();
        	stats_->total_reads_ += requests.size();

        	auto failed = std::make_unique<std::vector<RequestBlock*>>();
			return headp_->BulkRead(this, requests, *locked_process, *failed)
        	.then([this, failed = std::move(failed), &requests, locked_process = std::move(locked_process)]
            	(folly::Try<int>& result) mutable {
            	return SetCompletionResults(requests, *failed, result);
        	});
    	});
	}
	return TakeLockAndBulkInvoke(requests,
	[this, &requests, &process, min_max = std::move(min_max)] () {
		SetReadCheckPointId(process, min_max);
		stats_->reads_in_progress_ += requests.size();
		stats_->total_reads_ += requests.size();

        auto failed = std::make_unique<std::vector<RequestBlock*>>();
        return headp_->BulkRead(this, requests, process, *failed)
        .then([this, failed = std::move(failed), &requests]
            (folly::Try<int>& result) mutable {
            return SetCompletionResults(requests, *failed, result);
        });
    });
}

folly::Future<int> ActiveVmdk::TruncateBlocks([[maybe_unused]] RequestID reqid,
		CheckPointID ckpt_id,
		const std::vector<TruncateReq>& requests) {
	auto TruncateLambda = [this, ckpt_id] (std::pair<BlockID, BlockID> range) mutable {
		std::vector<::ondisk::BlockID> to_delete;
		auto r = iter::Range(range.first, range.second+1);
		auto erased = RemoveModifiedBlocks(ckpt_id, r.begin(), r.end(), to_delete);
		if (erased == 0) {
			return folly::makeFuture(0);
		}

		std::vector<folly::Future<int>> futures;
		auto end = MergeConsecutiveIf(to_delete.begin(), to_delete.end(), 256,
				[this, &futures, ckpt_id] (BlockID block, uint16_t nblocks) mutable {
			futures.emplace_back(
				headp_->Delete(this, ckpt_id, {block, block+nblocks-1})
			);
			return true;
		});
		log_assert(end == to_delete.end() and not futures.empty());

		return folly::collectAll(std::move(futures))
		.then([] (const folly::Try<std::vector<folly::Try<int>>>& tries) mutable {
			if (pio_unlikely(tries.hasException())) {
				return -EIO;
			}
			const auto& vec = tries.value();
			for (const auto& tri : vec) {
				if (pio_unlikely(tri.hasException())) {
					return -EIO;
				}
				if (pio_unlikely(tri.value() < 0)) {
					return tri.value();
				}
			}
			return 0;
		});
	};

	if (pio_unlikely(not headp_)) {
		LOG(ERROR) << "IO Layers aren't registered";
		return -ENODEV;
	}

	std::vector<folly::Future<int>> futures;
	for (const auto& request : requests) {
		int64_t offset = request.get_offset();
		int64_t length = request.get_length();

		auto [block_start, block_end] = GetBlockIDs(offset, length, BlockShift());
		if (not IsBlockSizeAlgined(offset, BlockSize())) {
			if (++block_start > block_end) {
				continue;
			}
		}

		auto [_, end_offset] = BlockFirstLastOffset(block_end, BlockShift());
		if (end_offset != static_cast<uint64_t>(offset + length - 1)) {
			if (block_start >= block_end) {
				continue;
			}
			--block_end;
		}
		(void) _;
		log_assert(block_start <= block_end);

		futures.emplace_back(TruncateLambda({block_start, block_end}));
	}

	return folly::collectAll(std::move(futures))
	.then([] (const folly::Try<std::vector<folly::Try<int>>>& tries) {
		if (pio_unlikely(tries.hasException())) {
			return -EIO;
		}
		for (const auto& tri : tries.value()) {
			if (pio_unlikely(tri.hasException())) {
				return -EIO;
			} else if (pio_unlikely(tri.value())) {
				return tri.value();
			}
		}
		return 0;
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
	IncCheckPointRef(*process);

	++stats_->flushes_in_progress_;
	return headp_->Flush(this, reqp, *process, *failed)
	.then([this, reqp, process = std::move(process),
			failed = std::move(failed)] (folly::Try<int>& result) mutable {
		auto rc = 0;
		if (result.hasException<std::exception>()) {
			reqp->SetResult(-ENOMEM, RequestStatus::kFailed);
			rc = -ENOMEM;
		} else {
			rc = result.value();
			if (pio_unlikely(rc < 0)) {
				reqp->SetResult(rc, RequestStatus::kFailed);
			} else if (pio_unlikely(not failed->empty())) {
				const auto blockp = failed->front();
				log_assert(blockp && blockp->IsFailed() && blockp->GetResult() != 0);
				reqp->SetResult(blockp->GetResult(), RequestStatus::kFailed);
				rc = -EIO;
			}
		}

		this->DecCheckPointRef(*process);
		--stats_->flushes_in_progress_;
		return rc;
	});
}

folly::Future<int> ActiveVmdk::BulkFlush(const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {
	SetReadCheckPointId(process, min_max);
	IncCheckPointRef(process);

	auto failed = std::make_unique<std::vector<RequestBlock*>>();
	return headp_->BulkFlush(this, requests, process, *failed)
	.then([this, failed = std::move(failed), &requests, process]
			(folly::Try<int>& result) mutable {
		auto ret = 0;
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
			ret = -EIO;
		} else {
			auto rc = result.value();
			if (pio_unlikely(rc < 0 || not failedp->empty())) {
				VLOG(5) << __func__ << "Error::" << rc;
				Fail(rc, not failedp->empty());
				ret = rc;
			}
		}

		for (auto& reqp : requests) {
			if (pio_unlikely(reqp->IsFailed())) {
				ret = reqp->GetResult();
			}
		}
		DecCheckPointRef(process);
		return ret;
	});
}

folly::Future<int> ActiveVmdk::Move(Request* reqp, const CheckPoints& min_max) {
	assert(reqp);

	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}

	auto[start, end] = reqp->Blocks();
	return TakeLockAndInvoke(start, end,
			[this, reqp, min_max = std::move(min_max)] () {
		auto failed = std::make_unique<std::vector<RequestBlock*>>();
		auto process = std::make_unique<std::vector<RequestBlock*>>();

		process->reserve(reqp->NumberOfRequestBlocks());
		reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
			process->emplace_back(blockp);
			return true;
		});

		SetReadCheckPointId(*process, min_max);
		IncCheckPointRef(*process);
		++stats_->moves_in_progress_;
		return headp_->Move(this, reqp, *process, *failed)
		.then([this, reqp, process = std::move(process),
			failed = std::move(failed)] (folly::Try<int>& result) mutable {
			auto rc = 0;
			if (result.hasException<std::exception>()) {
				reqp->SetResult(-ENOMEM, RequestStatus::kFailed);
				rc = -ENOMEM;
			} else {
				rc = result.value();
				if (pio_unlikely(rc < 0)) {
					reqp->SetResult(rc, RequestStatus::kFailed);
				} else if (pio_unlikely(not failed->empty())) {
					const auto blockp = failed->front();
					log_assert(blockp && blockp->IsFailed() && blockp->GetResult() != 0);
					reqp->SetResult(blockp->GetResult(), RequestStatus::kFailed);
					rc = -EIO;
				}
			}

			--stats_->moves_in_progress_;
			this->DecCheckPointRef(*process);
			return rc;
		});
	});
}

folly::Future<int> ActiveVmdk::Write(Request* reqp, CheckPointID ckpt_id) {
	return WriteCommon(reqp, ckpt_id);
}

folly::Future<int> ActiveVmdk::WriteSame(Request* reqp, CheckPointID ckpt_id) {
	return WriteCommon(reqp, ckpt_id);
}

int32_t ActiveVmdk::RemoveModifiedBlocks(CheckPointID ckpt_id,
		iter::Range<BlockID>::iterator begin,
		iter::Range<BlockID>::iterator end,
		std::vector<BlockID>& removed) {
	removed.clear();
	std::lock_guard<std::mutex> lock(blocks_.mutex_);
	auto it = blocks_.modified_.find(ckpt_id);
	if (pio_unlikely(it == blocks_.modified_.end())) {
		return 0;
	}

	int32_t erased = 0;
	auto modified_end = it->second.end();
	for (; begin != end; ++begin) {
		auto erase = it->second.find(*begin);
		if (erase == modified_end) {
			continue;
		}
		removed.emplace_back(*erase);
		it->second.erase(erase);
		++erased;
	}
	return erased;
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
	--stats_->writes_in_progress_;
	stats_->IncrWriteBytes(reqp->GetBufferSize());
	return WriteRequestComplete(reqp, ckpt_id);
}

int ActiveVmdk::WriteComplete(
		const std::vector<std::unique_ptr<Request>>& requests,
		CheckPointID ckpt_id) {
	int ret = 0;
	--stats_->writes_in_progress_;
	for (auto& request : requests) {
		auto rc = WriteRequestComplete(request.get(), ckpt_id);
		if (pio_unlikely(rc < 0)) {
			ret = rc;
		}
		stats_->IncrWriteBytes(request->GetBufferSize());
	}
	return ret;
}

folly::Future<int> ActiveVmdk::BulkWrite(::ondisk::CheckPointID ckpt_id,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {
	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}

	return TakeLockAndBulkInvoke(requests,
		[this, &requests, &process, ckpt_id = std::move(ckpt_id)] () {
		stats_->writes_in_progress_ += requests.size();
		stats_->total_writes_ += requests.size();
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

	auto[start, end] = reqp->Blocks();
	return TakeLockAndInvoke(start, end,
			[this, reqp, ckpt_id = std::move(ckpt_id)] () {

		auto failed = std::make_unique<std::vector<RequestBlock*>>();
		auto process = std::make_unique<std::vector<RequestBlock*>>();
		process->reserve(reqp->NumberOfRequestBlocks());
		reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
			process->emplace_back(blockp);
			return true;
		});

		++stats_->writes_in_progress_;
		++stats_->total_writes_;
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
	});
}

folly::Future<int> ActiveVmdk::BulkFlush(const CheckPoints& min_max,
	std::unique_ptr<ReqVec> requests, std::unique_ptr<ReqBlockVec> process,
	std::unique_ptr<IOBufPtrVec> iobufs) {

	return BulkFlush(min_max, *requests, *process)
	.then([requests = std::move(requests), process = std::move(process),
			iobufs = std::move(iobufs)] (int rc) {
		if (rc) {
			VLOG(5) << __func__ << "Error::" << rc;
		}
		return rc;
	});
}

folly::Future<int>
ActiveVmdk::BulkFlush(ActiveVmdk* vmdkp,
		std::vector<ReadRequest>::const_iterator it,
		std::vector<ReadRequest>::const_iterator eit,
		CheckPointID ckpt_id) {

	static_assert(kBulkReadMaxSize >= 32*1024, "kBulkReadMaxSize too small");
	auto min_max = std::make_pair(ckpt_id, ckpt_id);
	auto AllocDS = [] (size_t nr) {
		auto process = std::make_unique<ReqBlockVec>();
		auto reqs = std::make_unique<ReqVec>();
		auto iobufs = std::make_unique<IOBufPtrVec>();
		reqs->reserve(nr);
		iobufs->reserve(nr);
		return std::make_tuple(std::move(reqs), std::move(process), std::move(iobufs));
	};

	auto ExtractParams = [] (const ReadRequest& rd) {
		return std::make_tuple(rd.get_reqid(), rd.get_size(), rd.get_offset());
	};

	auto NewRequest = [] (RequestID reqid, ActiveVmdk* vmdkp, folly::IOBuf* bufp,
			size_t size, int64_t offset, CheckPointID ckpt_id) {
		auto req = std::make_unique<Request>(reqid, vmdkp, Request::Type::kRead,
			bufp->writableData(), size, size, offset);
		bufp->append(size);
		req->SetFlushReq();
		req->SetFlushCkptID(ckpt_id);
		return req;
	};

	std::vector<folly::Future<int>> futures;
	::ondisk::BlockID prev_start = 0;
	::ondisk::BlockID prev_end = 0;
	std::unique_ptr<ReqBlockVec> process;
	std::unique_ptr<ReqVec> requests;
	std::unique_ptr<IOBufPtrVec> iobufs;

	auto pending = std::distance(it, eit);
	std::tie(requests, process, iobufs) = AllocDS(pending);
	if (pio_unlikely(not requests or not process or not iobufs)) {
		return -ENOMEM;
	}

	size_t read_size = 0;
	uint64_t total_read_size = 0;
	for (; it != eit; ++it) {
		auto [reqid, size, offset] = ExtractParams(*it);
		auto iobuf = folly::IOBuf::create(size);
		if (pio_unlikely(not iobuf)) {
			LOG(ERROR) << __func__ << "Allocating IOBuf for read failed";
			return -ENOMEM;
		}
		iobuf->unshare();
		iobuf->coalesce();
		log_assert(not iobuf->isChained());

		auto req = NewRequest(reqid, vmdkp, iobuf.get(), size, offset, ckpt_id);
		if (pio_unlikely(not req)) {
			LOG(ERROR) << __func__ << "Allocating IOBuf for read failed";
			return -ENOMEM;
		}
		log_assert(iobuf->computeChainDataLength() == static_cast<size_t>(size));

		auto reqp = req.get();
		--pending;

		if (reqp->HasUnalignedIO()) {
			/* In flush context IOs should not be unaligned */

			LOG(ERROR) << __func__ << "found partial IO, " << reqp->NumberOfRequestBlocks();
			#if 0
			for (const auto& blockp : reqp->request_blocks_) {
				LOG(ERROR) << __func__ << ", offset:" << blockp->GetOffset()
					<< ", aligned offset:" << blockp->GetAlignedOffset();
			}
			#endif

			log_assert(0);
			return -ENOMEM;
		}

		auto [cur_start, cur_end] = reqp->Blocks();
		log_assert(prev_start <= cur_start);

		if (read_size >= kBulkReadMaxSize ||
				(prev_end >= cur_start && not requests->empty())) {
			futures.emplace_back(
				BulkFlush(min_max, std::move(requests), std::move(process),
					std::move(iobufs)));
			log_assert(not process and not requests);
			std::tie(requests, process, iobufs) = AllocDS(pending+1);
			read_size = 0;
			if (pio_unlikely(not requests or not process or not iobufs)) {
				return -ENOMEM;
			}
		}

		process->reserve(process->size() + reqp->NumberOfRequestBlocks());
		reqp->ForEachRequestBlock([&] (RequestBlock* blockp) mutable {
			process->emplace_back(blockp);
			return true;
		});
		requests->emplace_back(std::move(req));
		iobufs->emplace_back(std::move(iobuf));

		read_size += size;
		prev_start = cur_start;
		prev_end = cur_end;
		total_read_size += size;
	}

	if (pio_likely(not requests->empty())) {
		futures.emplace_back(
		BulkFlush(min_max, std::move(requests), std::move(process),
			std::move(iobufs)));
	}

	return folly::collectAll(std::move(futures))
	.then([&, requests = std::move(requests), process = std::move(process),
		iobufs = std::move(iobufs)]
		(const std::vector<folly::Try<int>>& results) -> folly::Future<int> {
		for (auto& t : results) {
			if (pio_likely(t.hasValue() and t.value() != 0)) {
				VLOG(5) << __func__ << "Error::" << t.value();
				return -EIO;
			}
		}
		return 0;
	});
}

folly::Future<int> ActiveVmdk::BulkFlushStart(std::vector<ReadRequest>& in_reqs,
		CheckPointID ckpt_id) {

	std::unique_ptr<folly::Promise<int>> promise =
		std::make_unique<folly::Promise<int>>();
	auto f = promise->getFuture();
	BulkFlush(this, in_reqs.begin(), in_reqs.end(), ckpt_id)
	.then([promise = std::move(promise)] (int rc) mutable {
		promise->setValue(rc);
		return rc;
	});
	return f;
}

folly::Future<int> ActiveVmdk::BulkFlushStart(std::vector<FlushBlock>& blocks,
		CheckPointID ckpt_id) {

	::hyc_thrift::RequestID req_id = 0;
	std::vector<ReadRequest> requests;
	try {
		requests.reserve(blocks.size());
	} catch (const std::bad_alloc& e) {
		LOG(ERROR) << "memory allocation failed";
		return -ENOMEM;
	}

	std::transform(blocks.begin(), blocks.end(), std::back_inserter(requests),
		[&req_id, bs = BlockShift()] (const auto& block)
		-> ReadRequest {
		return {apache::thrift::FragileConstructor(), ++req_id,
			block.second << bs, block.first << bs};
	});

	return BulkFlush(this, requests.begin(), requests.end(), ckpt_id)
	.then([requests = std::move(requests)] (int rc) mutable {
		return rc;
	});
}

int ActiveVmdk::FlushStage(CheckPointID ckpt_id,
	uint32_t max_req_size, uint32_t max_pending_reqs) {

	/*
	 * TBD :- Since a fix number of requests can under process
	 * at time, we can create those many records upfront to avoid
	 * creating and destroying it again and again (slab)
	 */

	aux_info_->InitState(FlushAuxData::FlushStageType::kFlushStage);
	aux_info_->FlushStartedAt_ = std::chrono::steady_clock::now();
	int32_t size = 0;

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == false);

	auto min_max = std::make_pair(ckpt_id, ckpt_id);
	int64_t prev_block = -1, cur_block = -1, start_block = -1;
	uint32_t block_cnt = 0;
	auto vmdkid  = GetID();

	LOG (ERROR) << __func__ << vmdkid << "::" << " Flush stage start";
	const auto& bitmap = ckptp->GetRoaringBitMap();
	aux_info_->total_blks_ = bitmap.cardinality();

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
			if (BlockSize() * (block_cnt + 1) <= max_req_size) {
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

		if (aux_info_->pending_cnt_ >= max_pending_reqs) {
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
		LOG (ERROR) << __func__ << "::" << vmdkid << "::" << "blk count::" << block_cnt
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
			block_cnt = block_cnt, vmdkid] (int rc) mutable {

			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__ << " Some of the flush requests failed for vmdkid::" << vmdkid;
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

	LOG (ERROR) << __func__ << "::" << vmdkid << "::"
		<< " Outside loop blk count::" << block_cnt
		<< ", start block::" << start_block << ", size::" << size;
	/* Submit any last pending accumlated IOs */
	aux_info_->lock_.lock();
	if (aux_info_->failed_ || block_cnt == 0) {
		aux_info_->lock_.unlock();
	} else {
		aux_info_->pending_cnt_++;
		aux_info_->lock_.unlock();
		size = block_cnt * BlockSize();
		LOG (ERROR) << __func__ << "::" << vmdkid << "::"
			<< "Last set of blocks count::" << block_cnt
			<< ", start block::" << start_block
			<< ", size::" << size;
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
			reqp = std::move(reqp), block_cnt = block_cnt, vmdkid]
				(int rc) mutable {

			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__ << "Some of the flush requests failed for vmdkid::" << vmdkid;
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

	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::steady_clock::now() - aux_info_->FlushStartedAt_);
	aux_info_->FlushStageDuration_ = duration.count();
	aux_info_->lock_.unlock();

	LOG (ERROR) << __func__ << vmdkid << "::"
		<< "Flush stage End, total attempted flushed blocks count::"
		<< aux_info_->flushed_blks_
		<< ", status::" << aux_info_->failed_;
	return aux_info_->failed_;
}

int ActiveVmdk::FlushStage_v3(CheckPointID ckpt_id,
        uint32_t max_req_size, uint32_t max_pending_reqs) {

	/*
	 * TBD :- Since a fix number of requests can under process
	 * at time, we can create those many records upfront to avoid
	 * creating and destroying it again and again (slab)
	 */

	std::atomic<uint64_t> id = 0;
	aux_info_->InitState(FlushAuxData::FlushStageType::kFlushStage);
	aux_info_->FlushStartedAt_ = std::chrono::steady_clock::now();

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == false);

	int64_t prev_block = -1, cur_block = -1, start_block = -1,
			saved_start_block = -1, saved_prev_block = -1;
	uint32_t block_cnt = 0, saved_block_cnt = 0;
	auto vmdkid  = GetID();

	LOG (INFO) << __func__ << vmdkid << "::"
			<< " Flush stage start"
			<< ", max_pending_reqs::"
			<< max_pending_reqs;

	const auto& bitmap = ckptp->GetRoaringBitMap();
	aux_info_->total_blks_ = bitmap.cardinality();

	TrackFlushBlocks cur_blocks, saved_blocks;
	cur_blocks.id = id;
	uint32_t acc_size = 0, saved_acc_size = 0;
	auto blk_sz = BlockSize();

	auto it = bitmap.begin();
	auto eit = bitmap.end();
	bool processing_failed = false;
	bool done = false;
	uint64_t failed_processed = 0;

	while (true) {
		aux_info_->lock_.lock();
		if (pio_unlikely(!aux_info_->failed_list_.empty())) {
			LOG(ERROR) << __func__ << "failed blocks processing";
			processing_failed = true;

			/* save current context */
			saved_start_block = start_block;
			saved_prev_block = prev_block;
			saved_block_cnt = block_cnt;
			saved_acc_size = acc_size;
			saved_blocks = cur_blocks;

			/* TBD : Single call for front and pop_front */
			cur_blocks = aux_info_->failed_list_.front();
			acc_size = 0;
			block_cnt = 0;
			for (auto elm = cur_blocks.blocks.begin(); elm != cur_blocks.blocks.end(); elm++) {
				VLOG(5) << __func__ << vmdkid << "::" <<
					" found in failed list, start block:"
					<< elm->first << ", block_cnt:" << elm->second;
				acc_size += elm->second * blk_sz;
				block_cnt += elm->second;
			}
			failed_processed += acc_size;
			aux_info_->failed_list_.pop_front();
		} else if (pio_likely(it != eit)) {
			auto block = *it;
			if (pio_unlikely(start_block == -1)) {
				start_block = block;
			}

			cur_block = block;
			if (pio_unlikely(prev_block == -1)) {
				/* initial, very first */
				prev_block = cur_block;
				block_cnt = 1;
				++it;
				aux_info_->lock_.unlock();
				continue;
			} else if (prev_block + 1 == cur_block) {
				/* sequential, can we accomadate it */
				if (blk_sz * (block_cnt + 1) < max_req_size) {
					prev_block = cur_block;
					block_cnt++;
					++it;
					aux_info_->lock_.unlock();
					continue;
				}
			}

			/* add this range in the list */
			log_assert(block_cnt > 0);
			VLOG(5) << __func__ << "Adding start_block:"
					<< start_block << ", block_cnt:" << block_cnt;
			cur_blocks.blocks.push_back(std::make_pair(start_block, block_cnt));
			acc_size += block_cnt * blk_sz;
			block_cnt = 0;
			if (acc_size < max_req_size) {
				/* Start tracking other range */
				start_block = cur_block;
				prev_block = cur_block;
				block_cnt = 1;
				++it;
				aux_info_->lock_.unlock();
				continue;
			}
		} else if (it == eit && !done) { /* after itter end */
			VLOG(5) << __func__ << "End of itterator, done flag is not set";
			/* What already gathered */
			if (pio_unlikely(block_cnt)) {
				VLOG(5) << __func__ << "Adding last start_block:"
					<< start_block << ", block_cnt:" << block_cnt;
				cur_blocks.blocks.push_back(std::make_pair(start_block, block_cnt));
				acc_size += block_cnt * blk_sz;
			}

			block_cnt = 0;
			done = true;
		} else if (done) { /* Submitted all the blocks */
			VLOG(5) << __func__ << "Done flag set";
			if (aux_info_->failed_ == false && (aux_info_->pending_cnt_ || !aux_info_->failed_list_.empty())) {
				aux_info_->sleeping_ = true;
				aux_info_->rendez_.TaskSleep(&aux_info_->lock_);
				if (!aux_info_->failed_) {
					aux_info_->lock_.unlock();
					continue;
				}
			}

			VLOG(5) << __func__ << "Exiting..";
			aux_info_->lock_.unlock();
			break;
		} else {
			LOG(ERROR) << __func__ << "Should not be here";
			log_assert(0);
		}

		if (aux_info_->failed_) {
			LOG(ERROR) << __func__ << "::" << vmdkid << "::"
				<< "found failed state set, quitting";
			aux_info_->lock_.unlock();
			break;
		}

		if (aux_info_->pending_cnt_ >= max_pending_reqs) {
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

		BulkFlushStart(cur_blocks.blocks, ckpt_id)
		.then([this, vmdkid, acc_size = acc_size, blk_sz = blk_sz,
				cur_blocks = cur_blocks] (int rc) mutable {
			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			VLOG(5) << __func__ << ", acc size is::"
				<< acc_size << ", pending:"
				<< aux_info_->pending_cnt_;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				VLOG(5) << __func__ <<
					"[" << cur_blocks.id << "]" << " flush request failed for vmdkid::" << vmdkid
					<< ", remaining retry is::" << cur_blocks.retry_cnt;
				aux_info_->failure_cnt_++;
				if ((aux_info_->failure_cnt_ < kMaxFailureTolerateCnt || 1)
							&& cur_blocks.retry_cnt) {
					LOG(ERROR) << __func__ << "[" << cur_blocks.id << "]" <<
						" Retrying for failed flush request for vmdkid::" << vmdkid
						<< ", remaining retry is::" << cur_blocks.retry_cnt;
					for (auto elm = cur_blocks.blocks.begin(); elm != cur_blocks.blocks.end(); elm++) {
						VLOG(5) << __func__<< "[" << aux_info_->failure_cnt_
							<< "]Adding in failed list::" << elm->first
							<< "::" << elm->second;
					}
					cur_blocks.retry_cnt--;
					aux_info_->failed_list_.emplace_back(cur_blocks);

					/* Add delay of 100 ms in case of error. We had seen
					 * that on VMC->VM the network RTT is around 30 ms,
					 * add extra on top of it */
					usleep(100 * 1000);
				} else {
					LOG(ERROR) << __func__ <<
						"[" << cur_blocks.id << "]" <<
						" Setting flush failed for vmdkid::" << vmdkid
						<< ", remaining retry for cur block is::" << cur_blocks.retry_cnt;
					aux_info_->failed_ = true;
				}
			}

			aux_info_->pending_cnt_--;
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

			aux_info_->flushed_blks_+= acc_size / blk_sz;
			aux_info_->lock_.unlock();
		});

		if (processing_failed) {
			cur_blocks = saved_blocks;
			acc_size = saved_acc_size;
			start_block = saved_start_block;
			prev_block = saved_prev_block;
			block_cnt = saved_block_cnt;
			processing_failed = false;
		} else if (!done) {
			/* Adjust the values before moving ahead */
			start_block = cur_block;
			prev_block = cur_block;
			block_cnt = 1;

			/* Clear the elements of the vector */
			cur_blocks.blocks.clear();
			cur_blocks.retry_cnt = kFlushRetryCnt;
			cur_blocks.id = ++id;
			acc_size = 0;
			++it;
		} else {
			block_cnt = 0;
			acc_size = 0;
			cur_blocks.blocks.clear();
			cur_blocks.retry_cnt = kFlushRetryCnt;
			cur_blocks.id = ++id;
		}
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

	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::steady_clock::now() - aux_info_->FlushStartedAt_);
	aux_info_->FlushStageDuration_ = duration.count();
	aux_info_->lock_.unlock();

	LOG (INFO) << __func__ << vmdkid << "::"
		<< "Flush stage End, total attempted flushed blocks count::"
		<< aux_info_->flushed_blks_
		<< ", status::" << aux_info_->failed_
		<< ", failed_processed(in bytes)::" << failed_processed;
	return aux_info_->failed_;
}

int ActiveVmdk::FlushStage_v2(CheckPointID ckpt_id,
        uint32_t max_req_size, uint32_t max_pending_reqs) {

	/*
	 * TBD :- Since a fix number of requests can under process
	 * at time, we can create those many records upfront to avoid
	 * creating and destroying it again and again (slab)
	 */

	aux_info_->InitState(FlushAuxData::FlushStageType::kFlushStage);
	aux_info_->FlushStartedAt_ = std::chrono::steady_clock::now();

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == false);

	int64_t prev_block = -1, cur_block = -1, start_block = -1;
	uint32_t block_cnt = 0;
	auto vmdkid  = GetID();

	LOG (ERROR) << __func__ << vmdkid << "::"
			<< " Flush stage start"
			<< ", max_pending_reqs::"
			<< max_pending_reqs;

	const auto& bitmap = ckptp->GetRoaringBitMap();
	aux_info_->total_blks_ = bitmap.cardinality();

	std::vector <FlushBlock> blocks;
	uint32_t acc_size = 0;
	auto blk_sz = BlockSize();

	for (const auto& block : bitmap) {

		/* TBD: Try to generate WAN (on prem disk) friendly
		 * IO's by doing look ahead in checkpoint
		 * bitmap and probably generating large sequential
		 * requests.
		 */

		if (pio_unlikely(start_block == -1)) {
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
			if (blk_sz * (block_cnt + 1) <= max_req_size) {
				prev_block = cur_block;
				block_cnt++;
				continue;
			}
		}

		/* Non sequential or complete */
		/* add this range in the list*/
		#if 0
		LOG(ERROR) << __func__ << " Adding: start_offset::"
				<< start_block * blk_sz
				<< ", length" << block_cnt * blk_sz;
		#endif
		log_assert(block_cnt > 0);
		blocks.push_back(std::make_pair(start_block, block_cnt));
		acc_size += block_cnt * blk_sz;
		if (acc_size < max_req_size) {
			/* Start tracking other range */
			start_block = cur_block;
			prev_block = cur_block;
			block_cnt = 1;
			continue;
		}

		aux_info_->lock_.lock();
		if (aux_info_->failed_) {
			aux_info_->lock_.unlock();
			break;
		}

		if (aux_info_->pending_cnt_ >= max_pending_reqs) {
			/* Already submitted too much, wait for completion */
			aux_info_->sleeping_ = true;
			aux_info_->rendez_.TaskSleep(&aux_info_->lock_);
			if (aux_info_->failed_) {
				aux_info_->lock_.unlock();
				break;
			}
		}

		aux_info_->pending_cnt_++;
		#if 0
		LOG(ERROR) << __func__ << ", acc size is::"
				<< acc_size << ", pending:"
				<< aux_info_->pending_cnt_;
		#endif
		aux_info_->lock_.unlock();

		BulkFlushStart(blocks, ckpt_id)
		.then([this, vmdkid, acc_size = acc_size, blk_sz = blk_sz] (int rc) mutable {
			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
		#if 0
			LOG(ERROR) << __func__ << ", acc size is::"
				<< acc_size << ", pending:"
				<< aux_info_->pending_cnt_;
		#endif
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__
					<< " Some of the flush requests failed for vmdkid::"
					<< vmdkid;
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

			aux_info_->flushed_blks_+= acc_size / blk_sz;
			aux_info_->lock_.unlock();
		});

		/* Adjust the values before moving ahead */
		start_block = cur_block;
		prev_block = cur_block;
		block_cnt = 1;

		/* Clear the elements of the vector */
		blocks.clear();
		acc_size = 0;
	}

	/* Add the last set of blocks in the list */
	if (block_cnt) {
		#if 0
		LOG(ERROR) << __func__ << " Adding: start_offset::"
				<< start_block * blk_sz
				<< ", length" << block_cnt * blk_sz;
		#endif
		blocks.push_back(std::make_pair(start_block,
				block_cnt));
		acc_size += block_cnt * blk_sz;
	}

	LOG (ERROR) << __func__ << "::" << vmdkid << "::"
		<< " Outside loop, remaining blocks::"
		<< acc_size / blk_sz
		<< " In blocks:" << blocks.size() ;

	aux_info_->lock_.lock();
	if (aux_info_->failed_ || (blocks.size() == 0)) {
		aux_info_->lock_.unlock();
	} else {
		aux_info_->pending_cnt_++;
		#if 0
		LOG(ERROR) << __func__ << ", acc size is::"
				<< acc_size << ", pending:"
				<< aux_info_->pending_cnt_;
		#endif
		aux_info_->lock_.unlock();
		BulkFlushStart(blocks, ckpt_id)
		.then([this, vmdkid, acc_size = acc_size, blk_sz = blk_sz] (int rc) mutable {
			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			#if 0
			LOG(ERROR) << __func__ << ", acc size is::"
				<< acc_size << ", pending:"
				<< aux_info_->pending_cnt_;

			#endif
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__
					<< "Some of the flush requests failed for vmdkid::"
					<< vmdkid;
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

			aux_info_->flushed_blks_+= acc_size / blk_sz;
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

	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::steady_clock::now() - aux_info_->FlushStartedAt_);
	aux_info_->FlushStageDuration_ = duration.count();
	aux_info_->lock_.unlock();

	LOG (ERROR) << __func__ << vmdkid << "::"
		<< "Flush stage End, total attempted flushed blocks count::"
		<< aux_info_->flushed_blks_
		<< ", status::" << aux_info_->failed_;
	return aux_info_->failed_;
}

bool ActiveVmdk::IsCkptFlushed(CheckPointID ckpt_id) {
	std::lock_guard<std::mutex> guard(checkpoints_.mutex_);
	auto it = pio::BinarySearch(checkpoints_.flushed_.begin(),
			checkpoints_.flushed_.end(), ckpt_id, []
			(const std::unique_ptr<CheckPoint>& ckpt, CheckPointID ckpt_id) {
				return ckpt->ID() < ckpt_id;
			});
	if (it != checkpoints_.flushed_.end()) {
		return true;
	}

	return false;
}

folly::Future<int> ActiveVmdk::CkptBitmapMerge(CheckPointID ckpt_id) {

	if (pio_unlikely(!IsCkptFlushed(ckpt_id))) {
		LOG(ERROR) << __func__ << "Given checkpoint "
			<< ckpt_id << " is not into flushed checkpoint list";
		return -EINVAL;
	}

	if (pio_unlikely(!IsCkptFlushed(ckpt_id + 1))) {
		LOG(ERROR) << __func__ << "Given checkpoint appears as last flushed checkpoint,"
			<< " there is no subsquent checkpoint avaliable to merge";
		return -EINVAL;
	}

	/* Create vector of checkpoints to process */
	std::vector<CheckPointID> ckpt_vec;
	ckpt_vec.emplace_back(ckpt_id);
	ckpt_vec.emplace_back(ckpt_id + 1);

	/* Create a new checkpoint str */
	auto checkpoint = std::make_unique<CheckPoint>(GetID(), ckpt_id + 1);
	auto rc = checkpoint->CkptBitmapMerge(this, ckpt_vec);
	if (pio_unlikely(rc)) {
		LOG(ERROR) << __func__ << "Unions of bitmaps failed";
		return rc;
	}

	checkpoint->SetFlushed();
	auto json = checkpoint->Serialize();
	auto key = checkpoint->SerializationKey();

	/* Serialize the new bitmap, we are currently overwritting the existing one. */
	/* TBD: Record versioning is required */
	return metad_kv_->Write(std::move(key), std::move(json))
	.then([this, ckpt_id, checkpoint = std::move(checkpoint)] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			return folly::makeFuture(rc);
		} else {
			checkpoint->SetSerialized();
		}

		/* Replace older checkpoint str with new merged one so
		 * that all the new IOs can start using the Merged bitmap.
		 * TBD : We may need to quiesce the IOs on ckpt + 1 also
		 * so that older refrences drained out properly */

		std::lock_guard<std::mutex> guard(checkpoints_.mutex_);
		auto it = pio::BinarySearch(checkpoints_.flushed_.begin(),
				checkpoints_.flushed_.end(), ckpt_id + 1, []
				(const std::unique_ptr<CheckPoint>& ckpt, CheckPointID ckpt_id) {
					return ckpt->ID() < ckpt_id;
				});
		if (it == checkpoints_.flushed_.end()) {
			return folly::makeFuture(-EINVAL);
		}

		/* Keep track of checkpoint so that we can use it as reference at time of
		 * Data merging */

		checkpoints_.tracked_.emplace_back(std::move(*it));
		*it = std::move(checkpoint);
		return folly::makeFuture(0);
	});
}

int ActiveVmdk::ValidateCkpt(CheckPointID) {

#if 0
	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == true);
	const auto& bitmap = ckptp->GetRoaringBitMap();
	auto it = bitmap.begin();
	auto eit = bitmap.end();
	LOG(ERROR) << __func__ << "START...";
	while (true) {
		auto block = *it;
		LOG(ERROR) << __func__ << "Block::" << block;
		++it;
		if (pio_likely(it == eit)) {
			break;
		}
	}
	LOG(ERROR) << __func__ << "END...";
#endif
	return 0;
}

int ActiveVmdk::MergeStages(CheckPointID ckpt_id, MergeStageType type) {

	auto rc = 0;
	if (type == MergeStageType::kBitmapMergeStage) {
		auto f = CkptBitmapMerge(ckpt_id);
		f.wait();
		rc  = f.value();
		if(pio_likely(rc)) {
			LOG(ERROR) << __func__ << "Checkpoint bitmap merge failed for vmdkID:"
				<< GetID() << ", ckpt id:" << ckpt_id << ", rc:" << rc;
		} else {
			LOG(INFO) << __func__ << "Checkpoint bitmap merge successfully for vmdkID:"
				<< GetID() << ", ckpt id:" << ckpt_id << ", rc:" << rc;
			ValidateCkpt(ckpt_id + 1);
		}
	} else if (type == MergeStageType::kDataMergeStage) {

		/* Start promotion of data from old checkpoint ID to new one */
		auto val = ((checkpoints_.tracked_).begin())->get();

		/* 4th argument is to pass merge_context to Move function to distinguish between
		 * normal move post flush vs move in context of ckpt merge */
		rc = MoveStage_v3(ckpt_id, kMaxFlushIoSize, 4, true, val->GetRoaringBitMap());
		if (rc) {
			LOG(ERROR) << __func__ << "Checkpoint data merge failed for vmdkID:"
				<< GetID() << ", ckpt id:" << ckpt_id << ", rc:" << rc;
			return rc;
		}

		/* Cleanup, remove ckptid+1 from tracked str */
		for (auto it = (checkpoints_.tracked_).begin();
				it != (checkpoints_.tracked_).end();) {
			it = checkpoints_.tracked_.erase(it);
			if (it != (checkpoints_.tracked_).end())
				++it;
		}
		checkpoints_.tracked_.clear();

		/* Cleanup, remove ckptid from flushed ckpt list */
		std::lock_guard<std::mutex> guard(checkpoints_.mutex_);
		auto it = pio::BinarySearch(checkpoints_.flushed_.begin(),
				checkpoints_.flushed_.end(), ckpt_id, []
				(const std::unique_ptr<CheckPoint>& ckpt, CheckPointID ckpt_id) {
				return ckpt->ID() < ckpt_id;
				});
		if (it == checkpoints_.flushed_.end()) {
			rc = -EINVAL;
			LOG(ERROR) << __func__ << "Invalid configuration, "
				"failed to find checkpoint in flushed list for vmdkID:"
				<< GetID() << ", ckpt id:" << ckpt_id << ", rc:" << rc;
		} else {
			/* Remove pesistent str */
			auto checkpoint = it->get();
			auto key = checkpoint->SerializationKey();
			auto f = metad_kv_->Delete(std::move(key));
			f.wait();
			rc = f.value();
			checkpoints_.flushed_.erase(it);
		}
	}

	return rc;
}

int ActiveVmdk::FlushStages(CheckPointID ckpt_id, bool perform_flush,
		bool perform_move,
		uint32_t max_req_size, uint32_t max_pending_reqs) {

	auto rc = 0;
	if (pio_likely(perform_flush)) {

#ifdef USE_FLUSH_VERSION2
		rc = FlushStage_v3(ckpt_id, max_req_size, max_pending_reqs);
#else
		rc = FlushStage(ckpt_id, max_req_size, max_pending_reqs);
#endif

		if (rc) {
			return rc;
		}
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
#ifdef USE_MOVE_VERSION2
		Roaring tmp;
		rc = MoveStage_v3(ckpt_id, max_req_size, 4, false, tmp);
#else
		rc = MoveStage(ckpt_id, max_pending_reqs);
#endif
		if (rc) {
			return rc;
		}
	}

	/* TBD: Mark flush done for this checkpoint and searalize this
	 * information.
	 */

	return 0;
}

int ActiveVmdk::MoveStage_v2(CheckPointID ckpt_id,
	uint32_t max_req_size, uint32_t max_pending_reqs) {

	aux_info_->InitState(FlushAuxData::FlushStageType::kMoveStage);
	aux_info_->MoveStartedAt_ = std::chrono::steady_clock::now();
	int32_t size = 0;

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == false);

	auto min_max = std::make_pair(ckpt_id, ckpt_id);
	int64_t prev_block = -1, cur_block = -1, start_block = -1;
	uint32_t block_cnt = 0;
	auto vmdkid  = GetID();

	LOG (ERROR) << __func__ << vmdkid << "::" << " Move_v2 stage start";
	const auto& bitmap = ckptp->GetRoaringBitMap();
	aux_info_->total_blks_ = bitmap.cardinality();

	for (const auto& block : bitmap) {

		/* Try to generate large size IOs */
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
			if (BlockSize() * (block_cnt + 1) <= max_req_size) {
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

		if (aux_info_->pending_cnt_ >= max_pending_reqs) {
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
		VLOG(5) << __func__ << "::" << vmdkid << "::" << "blk count::" << block_cnt
			<< ", start block::" << start_block << ", size::" << size;
		auto iobuf = NewRequestBuffer(size);
		if (pio_unlikely(not iobuf)) {
			throw std::bad_alloc();
			aux_info_->failed_ = true;
			break;
		}

		++aux_info_->reqid_;
		auto reqp = std::make_unique<Request>(aux_info_->reqid_, this,
				Request::Type::kMove,
				iobuf->Payload(), size, size, start_block * BlockSize());
		reqp->SetFlushReq();
		reqp->SetFlushCkptID(ckpt_id);

		this->Move(reqp.get(), min_max)
		.then([this, iobuf = std::move(iobuf), reqp = std::move(reqp),
			block_cnt = block_cnt, vmdkid] (int rc) mutable {

			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__
					<< " Some of the move requests failed for vmdkid::"
					<< vmdkid;
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

			aux_info_->moved_blks_+= block_cnt;
			aux_info_->lock_.unlock();
		});

		/* Adjust the values before moving ahead */
		start_block = cur_block;
		prev_block = cur_block;
		block_cnt = 1;
		size = 0;
	}

	LOG (ERROR) << __func__ << "::" << vmdkid << "::"
		<< " Outside loop blk count::" << block_cnt
		<< ", start block::" << start_block << ", size::" << size;
	/* Submit any last pending accumlated IOs */
	aux_info_->lock_.lock();
	if (aux_info_->failed_ || block_cnt == 0) {
		aux_info_->lock_.unlock();
	} else {
		aux_info_->pending_cnt_++;
		aux_info_->lock_.unlock();
		size = block_cnt * BlockSize();
		LOG (ERROR) << __func__ << "::" << vmdkid << "::"
			<< "Last set of blocks count::" << block_cnt
			<< ", start block::" << start_block
			<< ", size::" << size;
		auto iobuf = NewRequestBuffer(size);
		if (pio_unlikely(not iobuf)) {
			throw std::bad_alloc();
		}

		++aux_info_->reqid_;
		auto reqp = std::make_unique<Request>(aux_info_->reqid_, this,
			Request::Type::kMove,
			iobuf->Payload(), size, size, start_block * BlockSize());
		reqp->SetFlushReq();
		reqp->SetFlushCkptID(ckpt_id);

		this->Move(reqp.get(), min_max)
		.then([this, iobuf = std::move(iobuf),
			reqp = std::move(reqp), block_cnt = block_cnt, vmdkid]
				(int rc) mutable {

			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__
					<< "Some of the move requests failed for vmdkid::"
					<< vmdkid;
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

			aux_info_->moved_blks_+= block_cnt;
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

	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::steady_clock::now() - aux_info_->MoveStartedAt_);
	aux_info_->MoveStageDuration_ = duration.count();
	aux_info_->lock_.unlock();
	LOG (ERROR) << __func__ << vmdkid << "::"
		<< "Move stage End, total attempted moved blocks count::"
		<< aux_info_->moved_blks_
		<< ", status::" << aux_info_->failed_;
	return aux_info_->failed_;
}

int ActiveVmdk::MoveStage(CheckPointID ckpt_id, uint32_t max_pending_reqs) {

	/*
	 * TBD :- Since a fix number of requests can under process
	 * at time, we can create those many records upfront to avoid
	 * creating and destroying it again and again (slab)
	 */

	aux_info_->InitState(FlushAuxData::FlushStageType::kMoveStage);
	aux_info_->MoveStartedAt_ = std::chrono::steady_clock::now();
	int32_t size = BlockSize();

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == false);

	auto min_max = std::make_pair(ckpt_id, ckpt_id);
	const auto& bitmap = ckptp->GetRoaringBitMap();
	aux_info_->total_blks_ = bitmap.cardinality();

	auto vmdkid = GetID();
	LOG (ERROR) << __func__ << vmdkid << "::" << "Move stage start";
	for (const auto& block : bitmap) {
		aux_info_->lock_.lock();
		if (aux_info_->failed_) {
			aux_info_->lock_.unlock();
			break;
		}

		if(aux_info_->pending_cnt_ >= max_pending_reqs) {
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
		.then([this, iobuf = std::move(iobuf), reqp = std::move(reqp), vmdkid] (int rc) mutable {

			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__ << "Some of the Move requests failed for vmdkid::" << vmdkid;
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

	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::steady_clock::now() - aux_info_->MoveStartedAt_);
	aux_info_->MoveStageDuration_ = duration.count();
	aux_info_->lock_.unlock();

	LOG (ERROR) << __func__ << vmdkid << "::"
		<< "Move stage End, total moved blocks count::"
		<< aux_info_->moved_blks_
		<< ", status::" << aux_info_->failed_;
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
			VLOG(5) << __func__ << " Resetting the Serialized flag for VMDKID:" << GetID();
			checkpoint->UnsetSerialized();
		}

		checkpoint->SetModifiedBlocks(std::move(blocks));
	}

	return 0;
}

/* === New Move start === */
folly::Future<int> ActiveVmdk::BulkMove(const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {

	return TakeLockAndBulkInvoke(requests,
			[this, &requests, &process, min_max = std::move(min_max)] () {
		SetReadCheckPointId(process, min_max);
		IncCheckPointRef(process);
		auto failed = std::make_unique<std::vector<RequestBlock*>>();
		return headp_->BulkMove(this, min_max.second, requests, process, *failed)
		.then([this, failed = std::move(failed), &requests, process]
				(folly::Try<int>& result) mutable {
			auto ret = 0;
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
				ret = -EIO;
			} else {
				auto rc = result.value();
				if (pio_unlikely(rc < 0 || not failedp->empty())) {
					LOG(ERROR) << __func__ << "Error::" << rc;
					Fail(rc, not failedp->empty());
					ret = rc;
				}
			}

			for (auto& reqp : requests) {
				if (pio_unlikely(reqp->IsFailed())) {
					ret = reqp->GetResult();
				}
			}
			this->DecCheckPointRef(process);
			return ret;
		});
	});
}

folly::Future<int> ActiveVmdk::BulkMove(const CheckPoints& min_max,
	std::unique_ptr<ReqVec> requests, std::unique_ptr<ReqBlockVec> process,
	std::unique_ptr<IOBufPtrVec> iobufs) {

	return BulkMove(min_max, *requests, *process)
	.then([requests = std::move(requests), process = std::move(process),
			iobufs = std::move(iobufs)] (int rc) {
		if (rc) {
			LOG(ERROR) << __func__ << "Error::" << rc;
		}
		return rc;
	});
}

folly::Future<int>
ActiveVmdk::BulkMove(ActiveVmdk* vmdkp,
		std::vector<ReadRequest>::const_iterator it,
		std::vector<ReadRequest>::const_iterator eit,
		CheckPointID ckpt_id, bool merge_context) {

	static_assert(kBulkReadMaxSize >= 32*1024, "kBulkReadMaxSize too small");
	auto min_max = std::make_pair(ckpt_id, ckpt_id);
	auto AllocDS = [] (size_t nr) {
		auto process = std::make_unique<ReqBlockVec>();
		auto reqs = std::make_unique<ReqVec>();
		auto iobufs = std::make_unique<IOBufPtrVec>();
		reqs->reserve(nr);
		iobufs->reserve(nr);
		return std::make_tuple(std::move(reqs), std::move(process), std::move(iobufs));
	};

	auto ExtractParams = [] (const ReadRequest& rd) {
		return std::make_tuple(rd.get_reqid(), rd.get_size(), rd.get_offset());
	};

	auto NewRequest = [] (RequestID reqid, ActiveVmdk* vmdkp, folly::IOBuf* bufp,
			size_t size, int64_t offset, CheckPointID ckpt_id, bool merge_context) {
		auto req = std::make_unique<Request>(reqid, vmdkp, Request::Type::kMove,
			bufp->writableData(), size, size, offset);
		bufp->append(size);
		req->SetFlushReq();
		req->SetFlushCkptID(ckpt_id);
		if (pio_unlikely(merge_context)) {
			VLOG(5) << __func__ << "Merge context is set, Write ckpt id is ::" << ckpt_id + 1;
			req->SetMoveWriteCkptID(ckpt_id + 1);
		} else {
			req->SetMoveWriteCkptID(ckpt_id);
		}
		req->SetMoveReadCkptID(ckpt_id);
		req->SetMergeContext(merge_context);
		return req;
	};

	std::vector<folly::Future<int>> futures;
	::ondisk::BlockID prev_start = 0;
	::ondisk::BlockID prev_end = 0;
	std::unique_ptr<ReqBlockVec> process;
	std::unique_ptr<ReqVec> requests;
	std::unique_ptr<IOBufPtrVec> iobufs;

	auto pending = std::distance(it, eit);
	std::tie(requests, process, iobufs) = AllocDS(pending);
	if (pio_unlikely(not requests or not process or not iobufs)) {
		return -ENOMEM;
	}

	size_t read_size = 0;
	uint64_t total_read_size = 0;
	for (; it != eit; ++it) {
		auto [reqid, size, offset] = ExtractParams(*it);
		auto iobuf = folly::IOBuf::create(size);
		if (pio_unlikely(not iobuf)) {
			LOG(ERROR) << __func__ << "Allocating IOBuf for read failed";
			return -ENOMEM;
		}
		iobuf->unshare();
		iobuf->coalesce();
		log_assert(not iobuf->isChained());

		auto req = NewRequest(reqid, vmdkp, iobuf.get(), size, offset, ckpt_id, merge_context);
		if (pio_unlikely(not req)) {
			LOG(ERROR) << __func__ << "Allocating IOBuf for read failed";
			return -ENOMEM;
		}
		log_assert(iobuf->computeChainDataLength() == static_cast<size_t>(size));

		auto reqp = req.get();
		--pending;

		if (reqp->HasUnalignedIO()) {
			/* In flush context IOs should not be unaligned */

			LOG(ERROR) << __func__ << "found partial IO, " << reqp->NumberOfRequestBlocks();
			#if 0
			for (const auto& blockp : reqp->request_blocks_) {
				LOG(ERROR) << __func__ << ", offset:" << blockp->GetOffset()
					<< ", aligned offset:" << blockp->GetAlignedOffset();
			}
			#endif

			log_assert(0);
			return -ENOMEM;
		}

		auto [cur_start, cur_end] = reqp->Blocks();
		log_assert(prev_start <= cur_start);

		if (read_size >= kBulkReadMaxSize ||
				(prev_end >= cur_start && not requests->empty())) {
			futures.emplace_back(
				BulkMove(min_max, std::move(requests), std::move(process),
					std::move(iobufs)));
			log_assert(not process and not requests);
			std::tie(requests, process, iobufs) = AllocDS(pending+1);
			read_size = 0;
			if (pio_unlikely(not requests or not process or not iobufs)) {
				return -ENOMEM;
			}
		}

		process->reserve(process->size() + reqp->NumberOfRequestBlocks());
		reqp->ForEachRequestBlock([&] (RequestBlock* blockp) mutable {
			process->emplace_back(blockp);
			return true;
		});
		requests->emplace_back(std::move(req));
		iobufs->emplace_back(std::move(iobuf));

		read_size += size;
		prev_start = cur_start;
		prev_end = cur_end;
		total_read_size += size;
	}

	if (pio_likely(not requests->empty())) {
		futures.emplace_back(
		BulkMove(min_max, std::move(requests), std::move(process),
			std::move(iobufs)));
	}

	return folly::collectAll(std::move(futures))
	.then([&, requests = std::move(requests), process = std::move(process),
		iobufs = std::move(iobufs)]
		(const std::vector<folly::Try<int>>& results) -> folly::Future<int> {
		for (auto& t : results) {
			if (pio_likely(t.hasValue() and t.value() != 0)) {
				LOG(ERROR) << __func__ << "Error::" << t.value();
				return -EIO;
			}
		}
		return 0;
	});
}

folly::Future<int> ActiveVmdk::BulkMoveStart(std::vector<FlushBlock>& blocks,
		CheckPointID ckpt_id, bool merge_context) {

	::hyc_thrift::RequestID req_id = 0;
	std::vector<ReadRequest> requests;
	try {
		requests.reserve(blocks.size());
	} catch (const std::bad_alloc& e) {
		LOG(ERROR) << "memory allocation failed";
		return -ENOMEM;
	}

	std::transform(blocks.begin(), blocks.end(), std::back_inserter(requests),
		[&req_id, bs = BlockShift()] (const auto& block)
		-> ReadRequest {
		return {apache::thrift::FragileConstructor(), ++req_id,
			block.second << bs, block.first << bs};
	});

	return BulkMove(this, requests.begin(), requests.end(), ckpt_id, merge_context)
	.then([requests = std::move(requests)] (int rc) mutable {
		return rc;
	});
}

int ActiveVmdk::MoveStage_v3(CheckPointID ckpt_id,
        uint32_t max_req_size, uint32_t max_pending_reqs,
	bool merge_context, const Roaring& ref_bitmap) {

	/*
	 * TBD :- Since a fix number of requests can under process
	 * at time, we can create those many records upfront to avoid
	 * creating and destroying it again and again (slab)
	 */

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	if (pio_likely(!merge_context)) {
		aux_info_->InitState(FlushAuxData::FlushStageType::kMoveStage);
		aux_info_->MoveStartedAt_ = std::chrono::steady_clock::now();
		log_assert(ckptp->IsFlushed() == false);
	}

	int64_t prev_block = -1, cur_block = -1, start_block = -1;
	uint32_t block_cnt = 0;
	auto vmdkid  = GetID();

	LOG (INFO) << __func__ << vmdkid << "::"
			<< " Move stage start"
			<< ", max_pending_reqs::"
			<< max_pending_reqs;

	const auto& bitmap = ckptp->GetRoaringBitMap();
	aux_info_->total_blks_ = bitmap.cardinality();

	std::vector <FlushBlock> blocks;
	uint32_t acc_size = 0;
	auto blk_sz = BlockSize();
	uint64_t exclude_blks = 0;

	for (const auto& block : bitmap) {

		/* TBD: Try to generate WAN (on prem disk) friendly
		 * IO's by doing look ahead in checkpoint
		 * bitmap and probably generating large sequential
		 * requests.
		 */

		if (pio_unlikely(merge_context)) {
			if (ref_bitmap.contains(block)) {
				exclude_blks++;
				continue;
			}
		}

		if (pio_unlikely(start_block == -1)) {
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
			if (blk_sz * (block_cnt + 1) <= max_req_size) {
				prev_block = cur_block;
				block_cnt++;
				continue;
			}
		}

		/* Non sequential or complete */
		/* add this range in the list*/
		log_assert(block_cnt > 0);
		blocks.push_back(std::make_pair(start_block, block_cnt));
		acc_size += block_cnt * blk_sz;
		if (acc_size < max_req_size) {
			/* Start tracking other range */
			start_block = cur_block;
			prev_block = cur_block;
			block_cnt = 1;
			continue;
		}

		aux_info_->lock_.lock();
		if (aux_info_->failed_) {
			aux_info_->lock_.unlock();
			break;
		}

		if (aux_info_->pending_cnt_ >= max_pending_reqs) {
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
		BulkMoveStart(blocks, ckpt_id, merge_context)
		.then([this, vmdkid, acc_size = acc_size, blk_sz = blk_sz] (int rc) mutable {
			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			#if 0
			LOG(ERROR) << __func__ << ", acc size is::"
				<< acc_size << ", pending:"
				<< aux_info_->pending_cnt_;
			#endif
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__
					<< " Some of the move requests failed for vmdkid::"
					<< vmdkid;
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

			aux_info_->moved_blks_+= acc_size / blk_sz;
			aux_info_->lock_.unlock();
		});

		/* Adjust the values before moving ahead */
		start_block = cur_block;
		prev_block = cur_block;
		block_cnt = 1;

		/* Clear the elements of the vector */
		blocks.clear();
		acc_size = 0;
	}

	/* Add the last set of blocks in the list */
	if (block_cnt) {
		blocks.push_back(std::make_pair(start_block,
				block_cnt));
		acc_size += block_cnt * blk_sz;
	}

	VLOG (5) << __func__ << "::" << vmdkid << "::"
		<< " Outside loop, remaining blocks::"
		<< acc_size / blk_sz
		<< " In blocks:" << blocks.size() ;

	aux_info_->lock_.lock();
	if (aux_info_->failed_ || (blocks.size() == 0)) {
		aux_info_->lock_.unlock();
	} else {
		aux_info_->pending_cnt_++;
		aux_info_->lock_.unlock();
		BulkMoveStart(blocks, ckpt_id, merge_context)
		.then([this, vmdkid, acc_size = acc_size, blk_sz = blk_sz] (int rc) mutable {
			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			#if 0
			LOG(ERROR) << __func__ << ", acc size is::"
				<< acc_size << ", pending:"
				<< aux_info_->pending_cnt_;
			#endif
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__
					<< "Some of the Move requests failed for vmdkid::"
					<< vmdkid;
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

			aux_info_->moved_blks_+= acc_size / blk_sz;
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

	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::steady_clock::now() - aux_info_->MoveStartedAt_);
	aux_info_->MoveStageDuration_ = duration.count();
	aux_info_->lock_.unlock();

	LOG (INFO) << __func__ << "vmid::" << vmdkid << "::"
		<< "Move stage End, total attempted moved blocks count::"
		<< aux_info_->moved_blks_
		<< ", status::" << aux_info_->failed_;

	if (merge_context) {
		LOG (INFO) << __func__ << "vmid::" << vmdkid << "::"
			<< "Move stage End, exclude blocks cnt is::" << exclude_blks;
	}
	return aux_info_->failed_;
}


/* END ==== */

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

	VLOG(5) << __func__ << " Commit on checkpoint ID:" << ckpt_id;
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
		VLOG(5) << __func__ << " Serialized flag is false for VMDKID:" << GetID();
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
	for (auto it = checkpoints_.unflushed_.begin();
		it != checkpoints_.unflushed_.end() ; ++it) {
		auto ckptp = it->get();
		if (pio_likely(ckptp->ID() == ckpt_id)) {
			return ckptp;
		}
	}

	for (auto it = checkpoints_.flushed_.begin();
		it != checkpoints_.flushed_.end() ; ++it) {
		auto ckptp = it->get();
		if (pio_likely(ckptp->ID() == ckpt_id)) {
			return ckptp;
		}
	}

	return nullptr;
}

#if 0
CheckPoint* ActiveVmdk::GetCheckPoint(CheckPointID ckpt_id) const {
	std::lock_guard<std::mutex> lock(checkpoints_.mutex_);
	CheckPoint* found_ckptp = nullptr;
	int where = 0;
	for (auto it = checkpoints_.unflushed_.begin();
		it != checkpoints_.unflushed_.end() ; ++it) {
			auto ckptp = it->get();
			if (ckptp->ID() == ckpt_id) {
				found_ckptp = ckptp;
				where = 1;
				break;
			}
	}

	if (found_ckptp == nullptr) {
		for (auto it = checkpoints_.flushed_.begin();
			it != checkpoints_.flushed_.end() ; ++it) {
				auto ckptp = it->get();
				if (ckptp->ID() == ckpt_id) {
					found_ckptp = ckptp;
					where = 2;
					break;
				}
		}
	}

	auto it1 = pio::BinarySearch(checkpoints_.unflushed_.begin(),
		checkpoints_.unflushed_.end(), ckpt_id, []
				(const std::unique_ptr<CheckPoint>& ckpt, CheckPointID ckpt_id) {
			return ckpt->ID() < ckpt_id;
		});
	if (it1 != checkpoints_.unflushed_.end()) {
		if (it1->get() != found_ckptp) {
			LOG(ERROR) << __func__ << GetID()
				<< "::Mismatch in search in unflushed checkpoint list.."
				"it->get : " << it1->get() << ", found_ckptp:" << found_ckptp <<
				"where:" << where;
			log_assert(0);
		}
		return it1->get();
	}

	auto it2 = pio::BinarySearch(checkpoints_.flushed_.begin(),
		checkpoints_.flushed_.end(), ckpt_id, []
				(const std::unique_ptr<CheckPoint>& ckpt, CheckPointID ckpt_id) {
			return ckpt->ID() < ckpt_id;
		});
	if (it2 != checkpoints_.flushed_.end()) {
		if (it2->get() != found_ckptp) {
			LOG(ERROR) << __func__ << GetID()
				<< "::Mismatch in search in flushed checkpoint list.."
				"it->get : " << it2->get() << ", found_ckptp:" << found_ckptp <<
				"where:" << where;
			log_assert(0);
		}
		return it2->get();
	}

	return nullptr;
}
#endif

folly::Future<int> ActiveVmdk::MoveUnflushedToFlushed(
		std::vector<::ondisk::CheckPointID>& vec_ckpts) {

	std::lock_guard<std::mutex> guard(checkpoints_.mutex_);
	if (pio_unlikely(checkpoints_.unflushed_.empty())) {
		LOG(ERROR) << "Unflushed checkpoint list is empty.";
		return ENOENT;
	}

	for (auto& elem : vec_ckpts) {
		auto it = pio::BinarySearch(checkpoints_.unflushed_.begin(),
			checkpoints_.unflushed_.end(), elem, []
					(const std::unique_ptr<CheckPoint>& ckpt,
					CheckPointID ckpt_id) {
				return ckpt->ID() < ckpt_id;
			});
		if (it == checkpoints_.unflushed_.end()) {
			//error case should not be here
			log_assert(0);
			return folly::makeFuture(ENOENT);
		}

		it->get()->SetFlushed();
		checkpoints_.flushed_.emplace_back(std::move(*it));
		checkpoints_.unflushed_.erase(it);
	}

	return folly::makeFuture(0);
}
uint64_t ActiveVmdk::FlushedCheckpoints() const noexcept {
	std::lock_guard<std::mutex> lock(checkpoints_.mutex_);
    return checkpoints_.flushed_.size();
}

::ondisk::CheckPointID ActiveVmdk::GetFlushedCheckPointID() const noexcept {
	return checkpoints_.flushed_.back()->ID();
}

uint64_t ActiveVmdk::UnflushedCheckpoints() const noexcept {
	std::lock_guard<std::mutex> lock(checkpoints_.mutex_);
    return checkpoints_.unflushed_.size();
}

void ActiveVmdk::GetUnflushedCheckpoints(
	std::vector<::ondisk::CheckPointID>& unflushed_ckpts) const noexcept {
	std::lock_guard<std::mutex> lock(checkpoints_.mutex_);
	auto& ckpt = checkpoints_.unflushed_.front();
	if (pio_likely(ckpt)) {
		unflushed_ckpts.push_back(ckpt->ID());
	}
}

void ActiveVmdk::GetFlushedCheckpoints(
	std::vector<::ondisk::CheckPointID>& flushed_ckpts) const noexcept {
	std::lock_guard<std::mutex> lock(checkpoints_.mutex_);
	for (const auto& ckpt : checkpoints_.flushed_) {
		flushed_ckpts.push_back(ckpt->ID());
	}
}

uint64_t ActiveVmdk::GetFlushedBlksCnt() const noexcept {
	if (not aux_info_) {
		return 0;
	}
	return aux_info_->GetFlushedBlksCnt();
}

uint64_t ActiveVmdk::GetMovedBlksCnt() const noexcept {
	if (not aux_info_) {
		return 0;
	}
	return aux_info_->GetMovedBlksCnt();
}

uint64_t ActiveVmdk::GetPendingBlksCnt() const noexcept {
	if (not aux_info_) {
		return 0;
	}
	return aux_info_->GetPendingBlksCnt();
}

uint64_t ActiveVmdk::GetTotalFlushBlksCnt() const noexcept {
	if (not aux_info_) {
		return 0;
	}
	return aux_info_->GetPendingBlksCnt();
}
CheckPoint::CheckPoint(VmdkID vmdk_id, CheckPointID id) :
	vmdk_id_(std::move(vmdk_id)), self_(id) {
}

int
CheckPoint::CkptBitmapMerge(ActiveVmdk *vmdkp, std::vector<CheckPointID> &ckpt_vec) {

	std::vector<const Roaring *> bitmap_vec;
	auto vmdkid = vmdkp->GetID();
	::ondisk::BlockID min = -1, max = -1;
	for (auto id : ckpt_vec) {
		auto ckptp = vmdkp->GetCheckPoint(id);
		if (pio_unlikely(ckptp == nullptr)) {
			LOG(ERROR) << __func__ << "Unable to find ckpt id:"
				<< id <<" for vmdkid:" << vmdkp->GetID();
			return -EINVAL;
		}

		vmdkp->ValidateCkpt(id);
		if (pio_unlikely(min == -1) || min > ckptp->block_id_.first_) {
			min = ckptp->block_id_.first_;
		}

		if (pio_unlikely(max == -1) || max < ckptp->block_id_.last_) {
			max = ckptp->block_id_.last_;
		}

		bitmap_vec.emplace_back(&ckptp->GetRoaringBitMap());
	}

	/* Create a Union of all extracted bitmaps */
	if(pio_unlikely(UnionRoaringBitmaps(bitmap_vec))) {
		LOG(ERROR) << __func__ << "Bitmap union operation failed for vmdkid:"
				<< vmdkid;
		return -ENOMEM;
	}

	/* TBD : Do we need this */
	blocks_bitset_.runOptimize();
        block_id_.first_ = min;
        block_id_.last_ = max;

	LOG(INFO) << __func__ << "For vmdkid:" << vmdkid
			<< ", post Union min block id is :"
			<< min << ", max is :" << max;
	return 0;
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

/*
 * Takes a vector of Roaring bitmaps and OR/Union those to yield
 * a merged Roaring bitmap instance
 */

int CheckPoint::UnionRoaringBitmaps(
	const std::vector<const Roaring*> roaring_bitmaps) {
	if(roaring_bitmaps.size() < 2) {
		LOG(WARNING) << __func__ << "Input vector size must be >= 2";
		return -ENOMEM;
	}

	blocks_bitset_ = Roaring::fastunion(roaring_bitmaps.size(),
				(const Roaring**)roaring_bitmaps.data());
	return 0;
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
