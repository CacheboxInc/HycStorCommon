#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/MetaData_types.h"
#include "DaemonTgtInterface.h"
#include "Singleton.h"
#include "VirtualMachine.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "VmdkFactory.h"
#include "VmManager.h"
#include "FlushInstance.h"
#include "FlushConfig.h"
#include <chrono>

using namespace ::ondisk;

namespace pio {

const std::string CheckPoint::kCheckPoint = "M";
const uint32_t kMaxPendingFlushReqs = 32;
const uint32_t kMaxFlushIoSize = 256 * 1024;

FlushInstance::FlushInstance(VmID vmid, const std::string& config):
	vmid_(std::move(vmid)),
	config_(std::make_unique<config::FlushConfig>(config)) {
	start_time_ = std::chrono::steady_clock::now();
	start_time_system_ = std::chrono::system_clock::now();
}

FlushInstance::~FlushInstance() {
}

int FlushInstance::StartFlush(const VmID& vmid) {

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vmid);
	log_assert(vmp != nullptr);

	/* Get list of checkpoints to operate on */
	std::vector<::ondisk::CheckPointID> vec_ckpts;
	auto rc = vmp->GetUnflushedCheckpoints(vec_ckpts);
	if(pio_unlikely(rc)) {
		LOG(ERROR) << __func__ <<
			"Failed to Get List of Unflushed Checkpoints for vmid:"
			<< vmid;
		return rc;
	}

	bool perform_move;
	if (not GetJsonConfig()->GetMoveAllowedStatus(perform_move)) {
		perform_move = true;
	}

	bool perform_flush;
	if (not GetJsonConfig()->GetFlushAllowedStatus(perform_flush)) {
		perform_flush = true;
	}

	uint32_t max_pending_reqs = 0;
	if (not GetJsonConfig()->GetMaxPendingReqsCnt(max_pending_reqs)) {
		max_pending_reqs = kMaxPendingFlushReqs;
	}

	uint32_t max_req_size = 0;
	if (not GetJsonConfig()->GetMaxReqSize(max_req_size)) {
		max_req_size = kMaxFlushIoSize;
	}

	if (max_req_size > kMaxFlushIoSize) {
		max_req_size = kMaxFlushIoSize;
	}

	VLOG(5) << __func__ << " max_req_size:" << max_req_size  <<
			", max_pending_reqs:" << max_pending_reqs;

	for(auto const& ckpt_id : vec_ckpts) {
		LOG(INFO) << __func__ << " Starting flush for Checkpoint ID is:" << ckpt_id;
		auto rc = vmp->FlushStart(ckpt_id, perform_flush, perform_move,
						max_req_size, max_pending_reqs);
		if (pio_unlikely(rc)) {
			LOG(INFO) << __func__ << " flush failed for Checkpoint ID is:"
						<< ckpt_id << ", rc is:" << rc;
			return rc;
		}

		LOG(INFO) << __func__ << " flush successful for Checkpoint ID is:" << ckpt_id;
	}

	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		LOG(ERROR) << " Invalid VmID = " << vmid;
		return -EINVAL;
	}

	/*
	 * RTO/RPO workflow will do invokations for flush and Move stages separately.
	 * if we are in move stage (which is final stage) then move flushed checkpoints
	 * from unflushed to flushed checkpoints list.
	 */

	if (perform_flush && not perform_move) {
		LOG(INFO) << __func__ << " Only flush triggered move not triggered";
		return 0;
	}

	/* TBD : Pass ckptids with below function so that we operate on only the
	 * ckpts those has been flushed during this flush instance and not touch
	 * any other ckpts those has been created afterwards.
	 */

	auto ret = pio::MoveUnflushedToFlushed(vm_handle);
	if (pio_unlikely(ret)) {
		LOG(ERROR) << " Moving checkpoint from unflushed to flushed list failed."
			<< " VmID: " << vmid;
		return ret;
	}

	return 0;
}

int FlushInstance::FlushStatus(const VmID& vmid, FlushStats &flush_stat) {
	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vmid);
	log_assert(vmp != nullptr);

	/* Append start and elapsed time in stat */
	auto ms = std::chrono::time_point_cast<std::chrono::milliseconds>(start_time_);
	flush_stat.emplace("-1", std::make_pair(ms.time_since_epoch().count(), ElapsedTime()));

	auto rc = vmp->FlushStatus(flush_stat);
	if (pio_unlikely(rc)) {
		return rc;
	}

	return 0;
}

config::FlushConfig* FlushInstance::GetJsonConfig() const noexcept {
	return config_.get();
}

}
