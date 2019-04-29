#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include <mutex>

#include <cstdint>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/MetaData_constants.h"
#include "gen-cpp2/StorRpc_types.h"
#include "DaemonCommon.h"
#include "AeroConn.h"
#include "RecurringTimer.h"
#include "Analyzer.h"
#include "Rest.h"
#include "ArmConfig.h"
#include "VmSync.h"
#include "ArmSync.h"

using namespace ::hyc_thrift;

namespace pio {
/* forward declaration for Pimpl */
class VmSync;
class VmdkCacheStats;
namespace config {
	class VmConfig;
	class ArmConfig;
}

using ReqBlockVec = std::vector<RequestBlock*>;
using ReqVec = std::vector<std::unique_ptr<Request>>;
using IOBufPtrVec = std::vector<IOBufPtr>;
using ReadResultVec = std::vector<ReadResult>;

struct Stun {
	Stun();
	folly::Future<int> GetFuture();
	void SetPromise(int result);
	folly::Promise<int> promise;
	folly::FutureSplitter<int> futures;
};

using CheckPointResult = std::pair<::ondisk::CheckPointID, int>;

class VirtualMachine {
public:
	VirtualMachine(VmdkHandle handle, ::ondisk::VmID vm_id, const std::string& config);
	~VirtualMachine();

	void NewVmdk(ActiveVmdk* vmdkp);
	int RemoveVmdk(ActiveVmdk* vmdkp);
	int VmdkCount();
	const std::vector<ActiveVmdk *>& GetAllVmdks() const noexcept;

	ActiveVmdk* FindVmdk(const ::ondisk::VmdkID& vmdk_id) const;

	RequestID NextRequestID();

	folly::Future<int> Write(ActiveVmdk* vmdkp, Request* reqp);
	folly::Future<int> WriteSame(ActiveVmdk* vmdkp, Request* reqp);
	folly::Future<int> Read(ActiveVmdk* vmdkp, Request* reqp);
	folly::Future<CheckPointResult> TakeCheckPoint();
	folly::Future<int> CommitCheckPoint(::ondisk::CheckPointID ckpt_id);
	int FlushStart(::ondisk::CheckPointID ckpt_id,bool perform_flush,
			bool perform_move, uint32_t, uint32_t);
	folly::Future<int> MoveUnflushedToFlushed(std::vector<::ondisk::CheckPointID>&);
	int MergeStart(::ondisk::CheckPointID ckpt_id);
	folly::Future<int> CreateNewVmDeltaContext(int64_t snap_id);
	int FlushStart(::ondisk::CheckPointID ckpt_id, bool perform_flush, bool perform_move);
	int FlushStatus(FlushStats &flush_stat);
	int AeroCacheStats(AeroStats *aero_statsp, AeroSpikeConn *aerop);
	int GetVmdkParentStats(AeroSpikeConn *aerop, ActiveVmdk* vmdkp,
		VmdkCacheStats *vmdk_stats);
	folly::Future<int> Stun(::ondisk::CheckPointID ckpt_id);
	folly::Future<int> RStun(::ondisk::CheckPointID ckpt_id);
	std::vector <::ondisk::VmdkID> GetVmdkIDs();
	::ondisk::CheckPointID GetCurCkptID() const;

	folly::Future<int> BulkWrite(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process);

	folly::Future<std::unique_ptr<ReadResultVec>>
	BulkRead(ActiveVmdk* vmdkp,
		std::unique_ptr<std::vector<::hyc_thrift::ReadRequest>> in_reqs);

	folly::Future<std::unique_ptr<ReadResultVec>> BulkRead(ActiveVmdk* vmdkp,
		std::vector<ReadRequest>::const_iterator it,
		std::vector<ReadRequest>::const_iterator eit,
		bool is_read_ahead_req);

	folly::Future<int> TruncateBlocks(ActiveVmdk* vmdkp,
                RequestID reqid, const std::vector<TruncateReq>& requests);

	int GetUnflushedCheckpoints(std::vector<::ondisk::CheckPointID>& unflushed_ckpts);
	int SerializeCheckpoints(int64_t snap_id, const std::vector<int64_t>& vec_ckpts);
	int GetflushedCheckpoints(std::vector<::ondisk::CheckPointID>& flushed_ckpts);
	int DeSerializeCheckpoints(const std::vector<int64_t>& vec_ckpts);
	int DeSerializeCheckpoint(::ondisk::CheckPointID ckpt_id);
	int64_t GetSnapID(ActiveVmdk* vmdkp, const uint64_t& ckpt_id);

	bool AddVmSync(std::unique_ptr<VmSync> sync);
	VmSync* GetVmSync(VmSync::Type type) noexcept;
	friend std::ostream& operator << (std::ostream& os, const VirtualMachine& vm);
public:
	void AddVmdk(ActiveVmdk* vmdkp);
	folly::Future<int> StartPreload(const ::ondisk::VmdkID& id);
	const ::ondisk::VmID& GetID() const noexcept;
	const ::ondisk::VmUUID& GetUUID() const noexcept;
	VmHandle GetHandle() const noexcept;
	const config::VmConfig* GetJsonConfig() const noexcept;

	int SetArmJsonConfig(const std::string&);
	void UnsetArmJsonConfig();
	const config::ArmConfig* GetArmJsonConfig() const noexcept;

	Analyzer* GetAnalyzer() noexcept;
	folly::Future<RestResponse> RestCall(_ha_instance* instancep,
		std::string ep, std::string body);
	int StartTimer(struct _ha_instance *instancep, folly::EventBase* basep);

	const std::string GetSetName() const {
		return setname_;
	};

	void SetHaInstancePtr(void *instancep) { ha_instancep_ = instancep;}
	void *GetHaInstancePtr() { return ha_instancep_;}
	void IncCheckPointRef(CheckPointID &ckpt_id);
	void DecCheckPointRef(CheckPointID &ckpt_id);
private:
	ActiveVmdk* FindVmdk(VmdkHandle vmdk_handle) const;

private:
	folly::Future<int> StartPreload(ActiveVmdk* vmdkp);
	void WriteComplete(::ondisk::CheckPointID ckpt_id);
	void CheckPointComplete(::ondisk::CheckPointID ckpt_id);
	void FlushComplete(::ondisk::CheckPointID ckpt_id);
	void MergeComplete(::ondisk::CheckPointID ckpt_id);

	void PostIOStats(_ha_instance* instancep);
	void PostFingerPrintStats(_ha_instance* instancep);

	folly::Future<ReadResultVec> BulkRead(ActiveVmdk* vmdkp,
		std::unique_ptr<ReqVec> requests, std::unique_ptr<ReqBlockVec> process,
		std::unique_ptr<IOBufPtrVec> iobufs, size_t read_size);
	std::string setname_;

private:
	VmdkHandle handle_;
	::ondisk::VmID vm_id_;
	::ondisk::VmUUID vm_uuid_;
	std::atomic<RequestID> request_id_{0};
	std::unique_ptr<config::VmConfig> config_;
	std::unique_ptr<config::ArmConfig> armconfig_{nullptr};
	void *ha_instancep_{nullptr};

	Analyzer analyzer_;
	RecurringTimer timer_;

	struct {
		std::atomic_flag in_progress_ = ATOMIC_FLAG_INIT;
		std::atomic<::ondisk::CheckPointID> checkpoint_id_{
			::ondisk::MetaData_constants::kInvalidCheckPointID()+1
		};

		mutable std::mutex mutex_, r_mutex_;
		std::unordered_map<::ondisk::CheckPointID, std::atomic<uint64_t>> writes_per_checkpoint_;
		std::unordered_map<::ondisk::CheckPointID, std::atomic<uint64_t>> reads_per_checkpoint_;
		std::unordered_map<::ondisk::CheckPointID, std::unique_ptr<struct Stun>> stuns_;
		std::unordered_map<::ondisk::CheckPointID, std::unique_ptr<struct Stun>> r_stuns_;
	} checkpoint_;

	struct {
		mutable std::mutex mutex_;
		std::vector<ActiveVmdk *> list_;
	} vmdk_;

	struct {
		mutable std::mutex mutex_;
		std::unordered_map<VmSync::Type, std::unique_ptr<VmSync>> list_;
	} sync_;

	struct {
		std::atomic<uint64_t> writes_in_progress_{0};
		std::atomic<uint64_t> reads_in_progress_{0};
		std::atomic<uint64_t> flushs_in_progress_{0};

		std::atomic<uint64_t> bulk_reads_{0};
		std::atomic<uint64_t> bulk_read_sz_{0};

		std::atomic<uint64_t> bulk_writes_{0};
		std::atomic<uint64_t> bulk_write_sz_{0};
	} stats_;

	std::atomic_flag flush_in_progress_ = ATOMIC_FLAG_INIT;
	std::unordered_map<std::string, int64_t> snap_ckpt_map_;

	std::unique_ptr<ArmSync> armsync_;
};

}
