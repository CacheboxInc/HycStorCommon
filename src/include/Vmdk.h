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

namespace pio {

using PreloadBlock = std::pair<::ondisk::BlockID, uint16_t>;
using FlushBlock = std::pair<::ondisk::BlockID, uint16_t>;

/* forward declaration for Pimpl */
namespace config {
	class VmdkConfig;
	class AeroConfig;
	class FlushConfig;
}

/* Vmdk statistics */
typedef struct __st_vmdk_stats__ {
    uint64_t    writes_in_progress;
    uint64_t    reads_in_progress;
    uint64_t    flushes_in_progress;
    uint64_t    moves_in_progress;
    uint64_t    block_size;
    uint64_t    block_shift;
    uint64_t    block_mask;
    uint64_t    flushed_chkpnts;
    uint64_t    unflushed_chkpnts;
    uint64_t    flushed_blocks;
    uint64_t    moved_blocks;
    uint64_t    pending_blocks;
    uint64_t    read_misses;
    uint64_t    read_hits;
    uint64_t    dirty_blocks;
    uint64_t    clean_blocks;
}st_vmdk_stats;

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
	void SetSerialized() noexcept;
	void UnsetSerialized() noexcept;
	bool IsSerialized() const noexcept;
	void SetFlushed() noexcept;
	bool IsFlushed() const noexcept;
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
public:
	void InitState(FlushStageType type) {
		/* TBD: rendez reset */
		pending_cnt_ = 0;
		sleeping_ = false;
		done_ = false;
		failed_ = false;
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
				ondisk::CheckPointID ckpt_id);
	folly::Future<int> BulkMove(ActiveVmdk* vmdkp,
		std::vector<ReadRequest>::const_iterator it,
		std::vector<ReadRequest>::const_iterator eit,
		ondisk::CheckPointID ckpt_id);
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
	int FlushStage(::ondisk::CheckPointID check_point, uint32_t, uint32_t);
	int FlushStage_v2(::ondisk::CheckPointID check_point, uint32_t, uint32_t);
	int MoveStage(::ondisk::CheckPointID check_point, uint32_t);
	int MoveStage_v2(::ondisk::CheckPointID check_point, uint32_t, uint32_t);
	int MoveStage_v3(::ondisk::CheckPointID check_point, uint32_t, uint32_t);
	CheckPoint* GetCheckPoint(::ondisk::CheckPointID ckpt_id) const;

	/* Functions to gathering Vmdk statistics at this point in time */
	void GetVmdkInfo(st_vmdk_stats& vmdk_stats);
	uint64_t WritesInProgress() const noexcept;
	uint64_t ReadsInProgress() const noexcept;
	uint64_t FlushesInProgress() const noexcept;
	uint64_t MovesInProgress() const noexcept;
	uint64_t FlushedCheckpoints() const noexcept;
	uint64_t UnflushedCheckpoints() const noexcept;
	uint64_t GetFlushedBlksCnt() const noexcept;
	uint64_t GetMovedBlksCnt() const noexcept;
	uint64_t GetPendingBlksCnt() const noexcept;
	uint64_t GetReadHits() const noexcept;
	uint64_t GetReadMisses() const noexcept;
	uint64_t GetDirtyBlockCount() const noexcept;
	uint64_t GetCleanBlockCount() const noexcept;

	void IncrReadBytes(size_t read_bytes);
	void IncrWriteBytes(size_t write_bytes);
	void IncrNwReadBytes(size_t read_bytes);
	void IncrNwWriteBytes(size_t write_bytes);
	void IncrAeroReadBytes(size_t read_bytes);
	void IncrAeroWriteBytes(size_t write_bytes);

	folly::Future<int> BulkWrite(::ondisk::CheckPointID ckpt_id,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process);
	folly::Future<int> BulkRead(const CheckPoints& min_max,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process);

	int SetCkptBitmap(::ondisk::CheckPointID ckpt_id,
		std::unordered_set<::ondisk::BlockID>& blocks);

	const std::vector<PreloadBlock>& GetPreloadBlocks() const noexcept;
public:
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

	std::atomic<unsigned long long> w_total_latency{0};
	std::atomic<uint64_t> w_io_count{0};
	std::atomic<uint64_t> w_io_blks_count{0};

	std::atomic<unsigned long long> r_total_latency{0};
	std::atomic<uint64_t> r_io_count{0};
	std::atomic<uint64_t> r_io_blks_count{0};
	std::atomic<uint64_t> r_pending_count{0}, w_pending_count{0};
	std::mutex r_stat_lock_, w_stat_lock_;

	std::atomic<unsigned long long> w_aero_total_latency_{0};
	std::atomic<unsigned long long> r_aero_total_latency_{0};
	std::atomic<uint64_t> w_aero_io_blks_count_{0};
	std::atomic<uint64_t> r_aero_io_blks_count_{0};
	std::mutex r_aero_stat_lock_, w_aero_stat_lock_;

	struct {
		std::atomic<uint64_t> total_reads_{0};
		std::atomic<uint64_t> total_writes_{0};
		std::atomic<size_t> total_bytes_reads_{0};
		std::atomic<size_t> total_bytes_writes_{0};

		std::atomic<uint64_t> parent_blks_{0};
		std::atomic<uint64_t> read_populates_{0};
		std::atomic<uint64_t> cache_writes_{0};

		std::atomic<uint64_t> read_hits_{0};
		std::atomic<uint64_t> read_miss_{0};

		std::atomic<uint64_t> read_failed_{0};
		std::atomic<uint64_t> write_failed_{0};

		std::atomic<size_t> nw_bytes_write_{0};
		std::atomic<size_t> nw_bytes_read_{0};
		std::atomic<size_t> aero_bytes_write_{0};
		std::atomic<size_t> aero_bytes_read_{0};

		std::atomic<uint64_t> bufsz_before_compress{0};
		std::atomic<uint64_t> bufsz_after_compress{0};
		std::atomic<uint64_t> bufsz_before_uncompress{0};
		std::atomic<uint64_t> bufsz_after_uncompress{0};
	} cache_stats_;

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
public:
	void SetReadCheckPointId(const std::vector<RequestBlock*>& blockps,
		const CheckPoints& min_max) const;

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
	} checkpoints_;

	std::unique_ptr<MetaDataKV> metad_kv_{nullptr};

	struct {
		std::atomic<uint64_t> writes_in_progress_{0};
		std::atomic<uint64_t> reads_in_progress_{0};
		std::atomic<uint64_t> flushes_in_progress_{0};
		std::atomic<uint64_t> moves_in_progress_{0};
	} stats_;

	std::vector<PreloadBlock> preload_blocks_;
	mutable VmdkCacheStats old_cache_stats_;

public:
	std::unique_ptr<RequestHandler> headp_{nullptr};
	std::unique_ptr<FlushAuxData> aux_info_{nullptr};
	std::unique_ptr<ReadAhead> read_aheadp_{nullptr};
	std::string parentdisk_set_;
	::ondisk::VmdkID parentdisk_vmdkid_;

private:
	static constexpr uint32_t kDefaultBlockSize{4096};
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
