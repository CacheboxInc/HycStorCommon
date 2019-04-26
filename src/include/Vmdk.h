#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include <mutex>
#include <unordered_set>
#include <string>

#include <cstdint>

#include <folly/futures/Future.h>
#include <roaring/roaring.hh>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/MetaData_constants.h"
#include "DaemonCommon.h"
#include "IDs.h"
#include "VmdkCacheStats.h"
#include "VirtualMachine.h"
#include "Request.h"
#include "RequestHandler.h"
#include "AeroConn.h"
#include "MetaDataKV.h"
#include "QLock.h"
#include "Rendez.h"
#include "ReadAhead.h"
#include "RangeLock.h"
#include "CkptMergeInstance.h"

namespace pio {

using FlushBlock = std::pair<::ondisk::BlockID, uint16_t>;
const uint16_t kFlushRetryCnt = 10;

struct TrackFlushBlocks {
	std::vector <FlushBlock> blocks;
	uint64_t id{0};
	uint16_t retry_cnt{kFlushRetryCnt};
};

using PreloadBlock = std::pair<::ondisk::BlockID, uint16_t>;
using FailedElms = std::vector <TrackFlushBlocks>;

/* forward declaration for Pimpl */
namespace config {
	class VmdkConfig;
	class AeroConfig;
	class FlushConfig;
}

class CheckPoint {
public:
	CheckPoint(::ondisk::VmdkID vmdk_id, ::ondisk::CheckPointID id);
	void SetModifiedBlocks(const std::unordered_set<::ondisk::BlockID>& blocks);
	std::string Serialize() const;
	std::string SerializationKey() const;
	~CheckPoint();
	bool operator < (const CheckPoint& rhs) const noexcept;
	::ondisk::CheckPointID ID() const noexcept;

	std::pair<::ondisk::BlockID, ::ondisk::BlockID> Blocks() const noexcept;
	const Roaring& GetRoaringBitMap() const noexcept;
	int UnionRoaringBitmaps(const std::vector<const Roaring*> roaring_bitmaps);
	void SetSerialized() noexcept;
	void UnsetSerialized() noexcept;
	bool IsSerialized() const noexcept;
	void SetFlushed() noexcept;
	bool IsFlushed() const noexcept;
	int CkptBitmapMerge(ActiveVmdk *vmdkp, std::vector<CheckPointID> &ckpt_vec);
private:
	::ondisk::VmdkID vmdk_id_;
	::ondisk::CheckPointID self_;
	Roaring blocks_bitset_;
	struct {
		::ondisk::BlockID first_{0};
		::ondisk::BlockID last_{0};
	} block_id_;

	bool serialized_{false};
	bool flushed_{false};
public:
	static const std::string kCheckPoint;
};

class FlushAuxData {
public:
	enum class FlushStageType {
                kFlushStage,
                kMoveStage,
        };

	QLock lock_;
	Rendez rendez_;
	uint64_t pending_cnt_{0};
	uint64_t flushed_blks_{0};
	uint64_t moved_blks_{0};
	bool sleeping_{false};
	bool done_{false};
	bool failed_{false};
	std::atomic<uint64_t> reqid_{0};
	uint32_t failure_cnt_{0};
	std::list<TrackFlushBlocks> failed_list_;

	FlushStageType stage_in_progress_;
	std::chrono::steady_clock::time_point FlushStartedAt_;
	uint64_t FlushStageDuration_{0};
	std::chrono::steady_clock::time_point MoveStartedAt_;
	uint64_t MoveStageDuration_{0};

public:
	void InitState(FlushStageType type) {
		/* TBD: rendez reset */
		pending_cnt_ = 0;
		sleeping_ = false;
		done_ = false;
		failed_ = false;
		failure_cnt_ = 0;
		failed_list_.clear();
		stage_in_progress_ = type;
		if (type == FlushStageType::kFlushStage) {
			flushed_blks_ = 0;
			moved_blks_ = 0;

		} else {
			moved_blks_ = 0;
		}
	}

	uint64_t GetFlushedBlksCnt() {
		return flushed_blks_;
	}

	uint64_t GetMovedBlksCnt() {
		return moved_blks_;
	}

	uint64_t GetPendingBlksCnt() {
		return pending_cnt_;
	}

	enum FlushStageType GetStageInProgress() {
		return stage_in_progress_;
	}
};

class Vmdk {
public:
	Vmdk(VmdkHandle handle, ::ondisk::VmdkID&& vmdk_id);
	virtual ~Vmdk();
	const ::ondisk::VmdkID& GetID() const noexcept;
	VmdkHandle GetHandle() const noexcept;
	int64_t GetDiskSize() const {
		return disk_size_bytes_;
	}
protected:
	VmdkHandle handle_;
	::ondisk::VmdkID id_;
	int64_t disk_size_bytes_;
};

class ActiveVmdk : public Vmdk {
public:
	ActiveVmdk(VmdkHandle handle, ::ondisk::VmdkID vmdk_id, VirtualMachine *vmp,
		const std::string& config);
	virtual ~ActiveVmdk();

	void RegisterRequestHandler(std::unique_ptr<RequestHandler> handler);
	int Cleanup();
	void SetEventFd(int eventfd) noexcept;

	folly::Future<int> Read(Request* reqp, const CheckPoints& min_max);
	folly::Future<int> Flush(Request* reqp, const CheckPoints& min_max);
	folly::Future<int> BulkFlush(ActiveVmdk* vmdkp,
		std::vector<ReadRequest>::const_iterator it,
		std::vector<ReadRequest>::const_iterator eit,
		ondisk::CheckPointID ckpt_id);
	folly::Future<int> BulkFlush(const CheckPoints& min_max,
		std::unique_ptr<ReqVec> requests,
		std::unique_ptr<ReqBlockVec> process,
		std::unique_ptr<IOBufPtrVec> iobufs);
	folly::Future<int> BulkFlushStart(std::vector<ReadRequest>& in_reqs,
				ondisk::CheckPointID ckpt_id);
	folly::Future<int> BulkFlushStart(std::vector<FlushBlock>& blocks,
				ondisk::CheckPointID ckpt_id);
	folly::Future<int> BulkFlush(
		const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process);
	folly::Future<int> Move(Request* reqp, const CheckPoints& min_max);
	folly::Future<int> BulkMoveStart(std::vector<FlushBlock>& blocks,
				ondisk::CheckPointID ckpt_id, bool merge_context);
	folly::Future<int> BulkMove(ActiveVmdk* vmdkp,
		std::vector<ReadRequest>::const_iterator it,
		std::vector<ReadRequest>::const_iterator eit,
		ondisk::CheckPointID ckpt_id, bool merge_context);
	folly::Future<int> BulkMove(const CheckPoints& min_max,
		std::unique_ptr<ReqVec> requests,
		std::unique_ptr<ReqBlockVec> process,
		std::unique_ptr<IOBufPtrVec> iobufs);
	folly::Future<int> BulkMove(
		const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process);
	folly::Future<int> Write(Request* reqp, ::ondisk::CheckPointID ckpt_id);
	folly::Future<int> WriteSame(Request* reqp, ::ondisk::CheckPointID ckpt_id);
	folly::Future<int> TakeCheckPoint(::ondisk::CheckPointID check_point);
	folly::Future<int> CommitCheckPoint(::ondisk::CheckPointID check_point);
	int FlushStages(::ondisk::CheckPointID check_point, bool perform_flush,
			bool perform_move, uint32_t, uint32_t);
	int MergeStages(::ondisk::CheckPointID check_point, MergeStageType type);
	int FlushStage(::ondisk::CheckPointID check_point, uint32_t, uint32_t);
	folly::Future<int> CkptBitmapMerge(::ondisk::CheckPointID check_point);
	int ValidateCkpt(CheckPointID ckpt_id);
	int FlushStage_v2(::ondisk::CheckPointID check_point, uint32_t, uint32_t);
	int FlushStage_v3(::ondisk::CheckPointID check_point, uint32_t, uint32_t);
	int MoveStage(::ondisk::CheckPointID check_point, uint32_t);
	int MoveStage_v2(::ondisk::CheckPointID check_point, uint32_t, uint32_t);
	int MoveStage_v3(::ondisk::CheckPointID check_point, uint32_t, uint32_t, bool, const Roaring& );
	int FlushStages(::ondisk::CheckPointID check_point, bool perform_flush, bool perform_move);
	int FlushStage(::ondisk::CheckPointID check_point);
	int MoveStage(::ondisk::CheckPointID check_point);
	CheckPoint* GetCheckPoint(::ondisk::CheckPointID ckpt_id) const;
	folly::Future<int> MoveUnflushedToFlushed(std::vector<::ondisk::CheckPointID>&);

	
	uint64_t FlushedCheckpoints() const noexcept;
	uint64_t UnflushedCheckpoints() const noexcept;
	uint64_t GetFlushedBlksCnt() const noexcept;
	uint64_t GetMovedBlksCnt() const noexcept;
	uint64_t GetPendingBlksCnt() const noexcept;
	uint64_t GetReadHits() const noexcept;
	uint64_t GetReadMisses() const noexcept;
	uint64_t GetDirtyBlockCount() const noexcept;
	uint64_t GetCleanBlockCount() const noexcept;


	folly::Future<int> BulkWrite(::ondisk::CheckPointID ckpt_id,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process);
	folly::Future<int> BulkRead(const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process);
	folly::Future<int> TruncateBlocks(RequestID reqid, CheckPointID ckpt_id,
		const std::vector<TruncateReq>& requests);

	int SetCkptBitmap(::ondisk::CheckPointID ckpt_id,
		std::unordered_set<::ondisk::BlockID>& blocks);

	const std::vector<PreloadBlock>& GetPreloadBlocks() const noexcept;
	void GetUnflushedCheckpoints(std::vector<::ondisk::CheckPointID>& unflushed_ckpts) const noexcept;
	void GetFlushedCheckpoints(std::vector<::ondisk::CheckPointID>& flushed_ckpts) const noexcept;
	bool IsCkptFlushed(::ondisk::CheckPointID ckpt_id);
	void IncCheckPointRef(const std::vector<RequestBlock*>& blockps);
	void DecCheckPointRef(const std::vector<RequestBlock*>& blockps);
public:
	int32_t delta_fd_{-1};
	int CreateNewVmdkDeltaContext(int64_t snap_id);
	std::string delta_file_path_;
	size_t BlockSize() const;
	size_t BlockShift() const;
	size_t BlockMask() const;
	bool CleanupOnWrite() const {
		return cleanup_on_write_;
	};

	const std::string GetParentDiskSet() const {
		return parentdisk_set_;
	};

	const ::ondisk::VmdkID GetParentDiskVmdkId() const {
		return parentdisk_vmdkid_;
	};

	const ::ondisk::VmdkUUID& GetUUID() const noexcept {
		return vmdk_uuid_;
	};

	VirtualMachine* GetVM() const noexcept;
	const config::VmdkConfig* GetJsonConfig() const noexcept;

	VmdkCacheStats* stats_;

	void GetCacheStats(VmdkCacheStats* vmdk_stats) const noexcept;
	void FillCacheStats(::ondisk::IOAVmdkStats& dest) const noexcept;
private:
	folly::Future<int> WriteCommon(Request* reqp, ::ondisk::CheckPointID ckpt_id);
	int WriteRequestComplete(Request* reqp, ::ondisk::CheckPointID ckpt_id);
	int WriteComplete(Request* reqp, ::ondisk::CheckPointID ckpt_id);
	int WriteComplete(const std::vector<std::unique_ptr<Request>>& requests,
		::ondisk::CheckPointID ckpt_id);
	std::optional<std::unordered_set<::ondisk::BlockID>>
		CopyDirtyBlocksSet(::ondisk::CheckPointID ckpt_id);
	void RemoveDirtyBlockSet(::ondisk::CheckPointID ckpt_id);
	::ondisk::CheckPointID GetModifiedCheckPoint(::ondisk::BlockID block,
		const CheckPoints& min_max, bool& found) const;
	void ComputePreloadBlocks();
	int32_t RemoveModifiedBlocks(CheckPointID ckpt_id,
		iter::Range<BlockID>::iterator begin,
		iter::Range<BlockID>::iterator end,
		std::vector<BlockID>& removed);
public:
	void SetReadCheckPointId(const std::vector<RequestBlock*>& blockps,
		const CheckPoints& min_max) const;
	int UpdatefdMap(const int64_t& snap_id, const int32_t& fd);
	mutable std::mutex mutex_;
	std::unordered_map<std::string, int32_t> fd_map_;

private:
	VirtualMachine *vmp_{nullptr};
	//Vmdk           *parentp_{nullptr};
	int            eventfd_{-1};

	uint32_t block_shift_{0};
	bool cleanup_on_write_{true};
	std::unique_ptr<config::VmdkConfig> config_;
	::ondisk::VmdkUUID vmdk_uuid_;

	struct {
		mutable std::mutex mutex_;
		std::unordered_map<::ondisk::CheckPointID, std::unordered_set<::ondisk::BlockID>> modified_;
	} blocks_;

	struct {
		/* Last successful ::ondisk::CheckPointID on VMDK */
		std::atomic<::ondisk::CheckPointID> last_checkpoint_{
			::ondisk::MetaData_constants::kInvalidCheckPointID()
		};
		mutable std::mutex mutex_;
		std::vector<std::unique_ptr<CheckPoint>> unflushed_;
		std::vector<std::unique_ptr<CheckPoint>> flushed_;
		std::vector<std::unique_ptr<CheckPoint>> tracked_;
	} checkpoints_;

	std::unique_ptr<MetaDataKV> metad_kv_{nullptr};

	std::vector<PreloadBlock> preload_blocks_;
	mutable VmdkCacheStats old_stats_;

public:
	std::unique_ptr<RequestHandler> headp_{nullptr};
	std::unique_ptr<FlushAuxData> aux_info_{nullptr};
	std::unique_ptr<ReadAhead> read_aheadp_{nullptr};
	std::string parentdisk_set_;
	::ondisk::VmdkID parentdisk_vmdkid_;

private:
	static constexpr uint32_t kDefaultBlockSize{4096};

private:
	template <typename Func>
	auto TakeLockAndInvoke(::ondisk::BlockID start, ::ondisk::BlockID end, Func&& func) {
		auto lock = std::make_unique<RangeLock::LockGuard>(range_lock_.get(), start, end);
		return lock->Lock()
		.then([lock = std::move(lock), func = std::forward<Func>(func)] (int rc) mutable {
			if (pio_unlikely(not lock->IsLocked() or rc < 0)) {
				LOG(ERROR) << "Failed to take lock";
				return folly::makeFuture(rc ? rc : -EINVAL);
			}
			return func()
			.then([lock = std::move(lock)] (auto& rc) {
				return std::move(rc);
			});
		});
	}

	template <typename Func>
	auto TryLockAndBulkInvoke(const std::vector<std::unique_ptr<Request>>& requests,
        /*[IN]*/const std::vector<RequestBlock*>& process,
		/*[OUT]*/std::vector<RequestBlock*>& locked_process, 
		Func&& func) {
		assert(locked_process.empty());
		assert(!process.empty());
		(void)locked_process;
		(void)process;
		auto g = std::make_unique<RangeLock::LockGuard>(range_lock_.get(), Ranges(requests));
		std::vector<pio::RangeLock::range_t> ranges;
		bool full_lock = g->SelectiveLock(ranges);
		
		// Temporary code to prove that two reads on same offset works
		// After several successful CHO runs on master this code and related
		// stats in ReadAhead class may be safely removed.
		// Moreover, once dropping off unlocked message works we will no longer require
		// this code and related stats
		if(!full_lock && requests[0]->IsReadAheadRequest()) {
			int unlocked_reads = process.size() - ranges.size();
			assert(unlocked_reads > 0);
			assert(this->read_aheadp_ != NULL);
			this->read_aheadp_->UpdateTotalUnlockedReads(unlocked_reads);	
		}
		if(pio_unlikely(not g->IsLocked() || ranges.empty())) {
			LOG(WARNING) << "Couldn't acquire lock on even a single block";
			return folly::makeFuture(-EINVAL);
		}
		
		// Dropping of messages does not work right now and hence is postponed till next iteration
		// Dropping of messages encounters a crash and the traces depict that in AeroOps.cpp
		// while actually submitting the batch request we hit count mismatch which is not yet fully
		// analyzed.
#if 0
		try {
			locked_process.reserve(ranges.size());
		} 
		catch(const std::bad_alloc& e) {
			return folly::makeFuture(-ENOMEM);
		}
		for(const auto& block : process) {
			assert(block != nullptr);
			auto block_id = block->GetBlockID();
			if(full_lock || std::find(ranges.begin(), ranges.end(), 
				pio::RangeLock::range_t(block_id, block_id)) != ranges.end()) {
				LOG(ERROR) << "Pushing into locked_process, block ID = " << block_id;
				locked_process.emplace_back(block);
			}
		}
		auto l_size = locked_process.size();
		auto p_size = process.size();
		assert(!locked_process.empty() 
			&& (full_lock ? (l_size == p_size) : (l_size < p_size)));
		(void)l_size;
		(void)p_size;
#endif
		return func()
		.then([g = std::move(g) 
		/*,locked_process = std::move(locked_process)*/] (int rc) {
			return rc;
		});
	}

	std::vector<pio::RangeLock::range_t> Ranges(
			const std::vector<std::unique_ptr<Request>>& requests) {
		std::vector<pio::RangeLock::range_t> ranges;
		ranges.reserve(requests.size());
		for (const auto& request : requests) {
			ranges.emplace_back(request->Blocks());
		}
		return ranges;
	}

	template <typename Func>
	auto TakeLockAndBulkInvoke(const std::vector<std::unique_ptr<Request>>& requests,
			Func&& func) {
		auto g = std::make_unique<RangeLock::LockGuard>(range_lock_.get(), Ranges(requests));
		return g->Lock()
		.then([g = std::move(g), func = std::forward<Func>(func) ]
				(int rc) mutable {
			if (pio_unlikely(not g->IsLocked() || rc < 0)) {
				LOG(ERROR) << "Failed to take lock";
				return folly::makeFuture(rc ? rc : -EINVAL);
			}
			return func()
			.then([g = std::move(g)] (int rc) {
				return rc;
			});
		});

	}
private:
	const std::unique_ptr<RangeLock::RangeLock> range_lock_{nullptr};

};

class SnapshotVmdk : public Vmdk {
public:
	SnapshotVmdk(VmdkHandle handle, ::ondisk::VmdkID vmdk_id) :
			Vmdk(handle, std::move(vmdk_id)) {
	}
private:
	//Vmdk*               parentp_{nullptr};
	std::vector<Vmdk *> chidren_;
};

}
