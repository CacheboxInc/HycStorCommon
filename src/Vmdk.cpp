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
const uint32_t kMaxFailureTolerateCnt = 102400;

namespace pio {

const std::string CheckPoint::kCheckPoint = "CheckPoint";

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
		config_(std::make_unique<config::VmdkConfig>(config)) {
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

	ComputePreloadBlocks();

	config_->GetDiskSize(disk_size_bytes_);
	LOG(INFO) << "VmdkID =  " << id << " Disk Size = " << disk_size_bytes_;

	// Let this always be the last code block, pulling it up does not harm anything
	// but just for the sake of rule, let this be the last code block
	read_aheadp_ = NULL;
	// To avail ReadAhead the disk size must be > 20MB
	if(config_->IsReadAheadEnabled() && disk_size_bytes_ > (20 * 1024 * 1024)) {
		read_aheadp_ = std::make_unique<ReadAhead>(this);
		if (not read_aheadp_) {
			throw std::bad_alloc();
		}
		LOG(INFO) << "ReadAhead is enabled";
	}
	else {
		LOG(INFO) << "ReadAhead is disabled";
	}
}

ActiveVmdk::~ActiveVmdk() {
	/* destroy layers before rest of the ActiveVmdk object is destroyed */
	headp_ = nullptr;
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

void ActiveVmdk::IncrReadBytes(size_t read_bytes) {
	cache_stats_.total_bytes_reads_ += read_bytes;
	return;
}

void ActiveVmdk::IncrWriteBytes(size_t write_bytes) {
	cache_stats_.total_bytes_writes_ += write_bytes;
	return;
}

void ActiveVmdk::IncrNwReadBytes(size_t read_bytes) {
	cache_stats_.nw_bytes_read_ += read_bytes;
	return;
}

void ActiveVmdk::IncrNwWriteBytes(size_t write_bytes) {
	cache_stats_.nw_bytes_write_ += write_bytes;
	return;
}

void ActiveVmdk::IncrAeroReadBytes(size_t read_bytes) {
	cache_stats_.aero_bytes_read_ += read_bytes;
	return;
}

void ActiveVmdk::IncrAeroWriteBytes(size_t write_bytes) {
	cache_stats_.aero_bytes_write_ += write_bytes;
	return;
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

const std::vector<PreloadBlock>& ActiveVmdk::GetPreloadBlocks() const noexcept {
	return preload_blocks_;
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
	preload_blocks_ = MergeConsecutive(blocks, kBulkReadMaxSize >> BlockShift());
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

		auto app_reads               = cache_stats_.total_blk_reads_ -
						vmdk_stats->read_ahead_blks_;
		vmdk_stats->total_blk_reads_ = app_reads;

		vmdk_stats->total_reads_     = cache_stats_.total_reads_;
		vmdk_stats->read_miss_       = read_aheadp_->StatsTotalReadMissBlocks();
		vmdk_stats->read_hits_       = app_reads - vmdk_stats->read_miss_;
	} else {
		vmdk_stats->total_blk_reads_ = cache_stats_.total_blk_reads_;
		vmdk_stats->total_reads_     = cache_stats_.total_reads_;
		vmdk_stats->read_miss_       = cache_stats_.read_miss_;
		vmdk_stats->read_hits_       = cache_stats_.read_hits_;
	}

	vmdk_stats->total_writes_   = cache_stats_.total_writes_;
	vmdk_stats->read_populates_ =  cache_stats_.read_populates_;
	vmdk_stats->cache_writes_   =  cache_stats_.cache_writes_;
	vmdk_stats->read_failed_    = cache_stats_.read_failed_;
	vmdk_stats->write_failed_   = cache_stats_.write_failed_;

	vmdk_stats->reads_in_progress_  = stats_.reads_in_progress_;
	vmdk_stats->writes_in_progress_ = stats_.writes_in_progress_;

	vmdk_stats->flushes_in_progress_ = FlushesInProgress();
	vmdk_stats->moves_in_progress_   = MovesInProgress();

	vmdk_stats->block_size_  = BlockSize();

	vmdk_stats->flushed_chkpnts_   = FlushedCheckpoints();
	vmdk_stats->unflushed_chkpnts_ = UnflushedCheckpoints();

	vmdk_stats->flushed_blocks_ = GetFlushedBlksCnt();
	vmdk_stats->moved_blocks_   = GetMovedBlksCnt() ;
	vmdk_stats->pending_blocks_ = GetPendingBlksCnt();
	vmdk_stats->dirty_blocks_   = GetDirtyBlockCount();
	vmdk_stats->clean_blocks_   = GetCleanBlockCount();

	vmdk_stats->nw_bytes_write_   = cache_stats_.nw_bytes_write_;
	vmdk_stats->nw_bytes_read_    = cache_stats_.nw_bytes_read_;
	vmdk_stats->aero_bytes_write_ = cache_stats_.aero_bytes_write_;
	vmdk_stats->aero_bytes_read_  = cache_stats_.aero_bytes_read_;

	vmdk_stats->total_bytes_reads_  = cache_stats_.total_bytes_reads_;
	vmdk_stats->total_bytes_writes_ = cache_stats_.total_bytes_writes_;

	vmdk_stats->bufsz_before_compress = cache_stats_.bufsz_before_compress;
	vmdk_stats->bufsz_after_compress = cache_stats_.bufsz_after_compress;
	vmdk_stats->bufsz_before_uncompress = cache_stats_.bufsz_before_uncompress;
	vmdk_stats->bufsz_after_uncompress = cache_stats_.bufsz_after_uncompress;
}

void ActiveVmdk::FillCacheStats(IOAVmdkStats& dest) const noexcept {
	VmdkCacheStats cur_stats;

	GetCacheStats(&cur_stats);
	cur_stats.GetCummulativeCacheStats(old_cache_stats_, dest);
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
			log_assert(ckpt == min_max.second);
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
	++cache_stats_.total_reads_;
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
		IncrReadBytes(reqp->GetBufferSize());
		return reqp->Complete();
	});
}

folly::Future<int> ActiveVmdk::BulkRead(const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {
	SetReadCheckPointId(process, min_max);
	stats_.reads_in_progress_ += requests.size();
	cache_stats_.total_reads_ += requests.size();

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
		stats_.reads_in_progress_ -= requests.size();
		for (auto& reqp : requests) {
			auto rc = reqp->Complete();
			if (pio_unlikely(rc < 0)) {
				res = rc;
			}
			IncrReadBytes(reqp->GetBufferSize());
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

		--stats_.flushes_in_progress_;
		return rc;
	});
}

folly::Future<int> ActiveVmdk::BulkFlush(const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {
	SetReadCheckPointId(process, min_max);

	auto failed = std::make_unique<std::vector<RequestBlock*>>();
	return headp_->BulkFlush(this, requests, process, *failed)
	.then([failed = std::move(failed), &requests]
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
		return ret;
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

		--stats_.moves_in_progress_;
		return rc;
	});
}

folly::Future<int> ActiveVmdk::Write(Request* reqp, CheckPointID ckpt_id) {
	return WriteCommon(reqp, ckpt_id);
}

folly::Future<int> ActiveVmdk::WriteSame(Request* reqp, CheckPointID ckpt_id) {
	return WriteCommon(reqp, ckpt_id);
}

void ActiveVmdk::SetBlocksModified(CheckPointID ckpt_id, Request* reqp) {
	auto [s, e] = reqp->Blocks();
	auto range_iter = iter::Range(s, e+1);

	std::lock_guard<std::mutex> lock(blocks_.mutex_);
	auto it = blocks_.modified_.find(ckpt_id);
	if (pio_unlikely(it == blocks_.modified_.end())) {
		bool inserted{};
		std::tie(it, inserted) = blocks_.modified_.insert(
			std::make_pair(ckpt_id, std::unordered_set<BlockID>())
		);
		log_assert(inserted);
	}
	it->second.insert(range_iter.begin(), range_iter.end());
}

void ActiveVmdk::SetBlocksModified(CheckPointID ckpt_id,
		const std::vector<std::unique_ptr<Request>>& requests) {
	std::lock_guard<std::mutex> lock(blocks_.mutex_);
	auto it = blocks_.modified_.find(ckpt_id);
	if (pio_unlikely(it == blocks_.modified_.end())) {
		bool inserted{};
		std::tie(it, inserted) = blocks_.modified_.insert(
			std::make_pair(ckpt_id, std::unordered_set<BlockID>())
		);
		log_assert(inserted);
	}
	for (const auto& request : requests) {
		auto [s, e] = request->Blocks();
		auto range_iter = pio::iter::Range(s, e+1);
		it->second.insert(range_iter.begin(), range_iter.end());
	}
}

int ActiveVmdk::WriteComplete(Request* reqp) {
	--stats_.writes_in_progress_;
	IncrWriteBytes(reqp->GetBufferSize());
	return reqp->Complete();
}

int ActiveVmdk::WriteComplete(const std::vector<std::unique_ptr<Request>>& requests) {
	int ret = 0;
	--stats_.writes_in_progress_;
	for (auto& request : requests) {
		auto rc = request->Complete();
		if (pio_unlikely(rc < 0)) {
			ret = rc;
		}
		IncrWriteBytes(request->GetBufferSize());
	}
	return ret;
}

folly::Future<int> ActiveVmdk::BulkWrite(::ondisk::CheckPointID ckpt_id,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {
	if (pio_unlikely(not headp_)) {
		return -ENXIO;
	}

	SetBlocksModified(ckpt_id, requests);

	stats_.writes_in_progress_ += requests.size();
	cache_stats_.total_writes_ += requests.size();
	auto failed = std::make_unique<std::vector<RequestBlock*>>();

	return headp_->BulkWrite(this, ckpt_id, requests, process, *failed)
	.then([this, &requests, failed = std::move(failed)]
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

		return WriteComplete(requests);
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

	SetBlocksModified(ckpt_id, reqp);

	auto failed = std::make_unique<std::vector<RequestBlock*>>();
	auto process = std::make_unique<std::vector<RequestBlock*>>();
	process->reserve(reqp->NumberOfRequestBlocks());
	reqp->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
		process->emplace_back(blockp);
		return true;
	});

	++stats_.writes_in_progress_;
	++cache_stats_.total_writes_;
	return headp_->Write(this, reqp, ckpt_id, *process, *failed)
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

		return WriteComplete(reqp);
	});
}

folly::Future<int> ActiveVmdk::BulkFlush(const CheckPoints& min_max,
	std::unique_ptr<ReqVec> requests, std::unique_ptr<ReqBlockVec> process,
	std::unique_ptr<IOBufPtrVec> iobufs) {

	return BulkFlush(min_max, *requests, *process)
	.then([requests = std::move(requests), process = std::move(process),
			iobufs = std::move(iobufs)] (int rc) {
		if (rc) {
			LOG(ERROR) << __func__ << "Error::" << rc;
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
				LOG(ERROR) << __func__ << "Error::" << t.value();
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

	aux_info_->InitState(FlushAuxData::FlushStageType::kFlushStage);
	aux_info_->FlushStartedAt_ = std::chrono::steady_clock::now();

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == false);

	int64_t prev_block = -1, cur_block = -1, start_block = -1,
			saved_start_block = -1, saved_prev_block = -1;
	uint32_t block_cnt = 0, saved_block_cnt = 0;
	auto vmdkid  = GetID();

	VLOG (5) << __func__ << vmdkid << "::"
			<< " Flush stage start"
			<< ", max_pending_reqs::"
			<< max_pending_reqs;

	const auto& bitmap = ckptp->GetRoaringBitMap();
	std::vector <FlushBlock> blocks, saved_blocks;
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
			saved_blocks = blocks;

			/* TBD : Single call for front and pop_front */
			blocks = aux_info_->failed_list_.front();
			acc_size = 0;
			block_cnt = 0;
			for (auto elm = blocks.begin(); elm != blocks.end(); elm++) {
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
			blocks.push_back(std::make_pair(start_block, block_cnt));
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
				blocks.push_back(std::make_pair(start_block, block_cnt));
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

		BulkFlushStart(blocks, ckpt_id)
		.then([this, vmdkid, acc_size = acc_size, blk_sz = blk_sz,
				blocks = blocks] (int rc) mutable {
			/* TBD : Free the created IO buffer */
			this->aux_info_->lock_.lock();
			VLOG(5) << __func__ << ", acc size is::"
				<< acc_size << ", pending:"
				<< aux_info_->pending_cnt_;
			aux_info_->pending_cnt_--;

			/* If some of the requests has failed then don't submit new ones */
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__ <<
					" flush request failed for vmdkid::" << vmdkid;
				aux_info_->failure_cnt_++;
				if (aux_info_->failure_cnt_ < kMaxFailureTolerateCnt) {
					LOG(ERROR) << __func__ <<
						" Retrying for failed flush request for vmdkid::" << vmdkid;
					for (auto elm = blocks.begin(); elm != blocks.end(); elm++) {
						VLOG(5) << __func__ 	<< "[" << aux_info_->failure_cnt_
							<< "]Adding in failed list::" << elm->first
							<< "::" << elm->second;
					}
					aux_info_->failed_list_.emplace_back(blocks);
				} else {
					LOG(ERROR) << __func__ <<
						" Setting flush failed for vmdkid::" << vmdkid;
					aux_info_->failed_ = true;
				}
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

		if (processing_failed) {
			blocks = saved_blocks;
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
			blocks.clear();
			acc_size = 0;
			++it;
		} else {
			block_cnt = 0;
			acc_size = 0;
			blocks.clear();
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

	aux_info_->lock_.unlock();

	VLOG (5) << __func__ << vmdkid << "::"
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
		rc = MoveStage_v3(ckpt_id, max_req_size, 4);
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
	SetReadCheckPointId(process, min_max);

	auto failed = std::make_unique<std::vector<RequestBlock*>>();
	return headp_->BulkMove(this, min_max.second, requests, process, *failed)
	.then([failed = std::move(failed), &requests]
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
		return ret;
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
		auto req = std::make_unique<Request>(reqid, vmdkp, Request::Type::kMove,
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

	return BulkMove(this, requests.begin(), requests.end(), ckpt_id)
	.then([requests = std::move(requests)] (int rc) mutable {
		return rc;
	});
}

int ActiveVmdk::MoveStage_v3(CheckPointID ckpt_id,
        uint32_t max_req_size, uint32_t max_pending_reqs) {

	/*
	 * TBD :- Since a fix number of requests can under process
	 * at time, we can create those many records upfront to avoid
	 * creating and destroying it again and again (slab)
	 */

	aux_info_->InitState(FlushAuxData::FlushStageType::kMoveStage);
	aux_info_->MoveStartedAt_ = std::chrono::steady_clock::now();

	auto ckptp = GetCheckPoint(ckpt_id);
	log_assert(ckptp != nullptr);
	log_assert(ckptp->IsFlushed() == false);

	int64_t prev_block = -1, cur_block = -1, start_block = -1;
	uint32_t block_cnt = 0;
	auto vmdkid  = GetID();

	VLOG (5) << __func__ << vmdkid << "::"
			<< " Move stage start"
			<< ", max_pending_reqs::"
			<< max_pending_reqs;

	const auto& bitmap = ckptp->GetRoaringBitMap();
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
		BulkMoveStart(blocks, ckpt_id)
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
		BulkMoveStart(blocks, ckpt_id)
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

	VLOG (5) << __func__ << vmdkid << "::"
		<< "Move stage End, total attempted moved blocks count::"
		<< aux_info_->moved_blks_
		<< ", status::" << aux_info_->failed_;
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

uint64_t ActiveVmdk::WritesInProgress() const noexcept {
	return stats_.writes_in_progress_;
}

uint64_t ActiveVmdk::ReadsInProgress() const noexcept {
	return stats_.reads_in_progress_;
}

uint64_t ActiveVmdk::FlushesInProgress() const noexcept {
	return stats_.flushes_in_progress_;
}

uint64_t ActiveVmdk::MovesInProgress() const noexcept {
	return stats_.moves_in_progress_;
}

uint64_t ActiveVmdk::FlushedCheckpoints() const noexcept {
	std::lock_guard<std::mutex> lock(checkpoints_.mutex_);
    return checkpoints_.flushed_.size();
}

uint64_t ActiveVmdk::UnflushedCheckpoints() const noexcept {
	std::lock_guard<std::mutex> lock(checkpoints_.mutex_);
    return checkpoints_.unflushed_.size();
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

uint64_t ActiveVmdk::GetReadHits() const noexcept {
	return 0;
}

uint64_t ActiveVmdk::GetReadMisses() const noexcept {
	return 0;
}

uint64_t ActiveVmdk::GetDirtyBlockCount() const noexcept {
	return 0;
}

uint64_t ActiveVmdk::GetCleanBlockCount() const noexcept {
	return 0;
}

void ActiveVmdk::GetVmdkInfo(st_vmdk_stats& vmdk_stats) {
    vmdk_stats.writes_in_progress = WritesInProgress();
    vmdk_stats.reads_in_progress = ReadsInProgress();
    vmdk_stats.flushes_in_progress = FlushesInProgress();
    vmdk_stats.moves_in_progress = MovesInProgress();
    vmdk_stats.block_size = BlockSize();
    vmdk_stats.block_shift = BlockShift();
    vmdk_stats.block_mask = BlockMask();
    vmdk_stats.flushed_chkpnts = FlushedCheckpoints();
    vmdk_stats.unflushed_chkpnts = UnflushedCheckpoints();
    vmdk_stats.flushed_blocks = GetFlushedBlksCnt();
    vmdk_stats.moved_blocks = GetMovedBlksCnt() ;
    vmdk_stats.pending_blocks = GetPendingBlksCnt();
    vmdk_stats.read_misses = GetReadMisses();
    vmdk_stats.read_hits = GetReadHits();
    vmdk_stats.dirty_blocks = GetDirtyBlockCount();
    vmdk_stats.clean_blocks = GetCleanBlockCount();
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
