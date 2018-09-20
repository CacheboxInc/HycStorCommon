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

FlushInstance::FlushInstance(VmID vmid, const std::string& config):
	vmid_(std::move(vmid)),
	config_(std::make_unique<config::FlushConfig>(config)) {
	start_time_ = std::chrono::steady_clock::now();
}

FlushInstance::~FlushInstance() {
}

int FlushInstance::StartFlush(const VmID& vmid) {

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vmid);
	log_assert(vmp != nullptr);

	/*
	 * TBD : Get list of unflushed checkpoints and try to flush them.
	 * Unflushed checkpoints should be per VM and should not be
	 * different independently per disk
	 */

	auto f = vmp->TakeCheckPoint();
	f.wait();
	auto [ckpt_id, rc] = f.value();
	if (pio_unlikely(rc)) {
		return rc;
	}

	bool perform_move;
	if (not GetJsonConfig()->GetMoveAllowedStatus(perform_move)) {
		perform_move = true;
	}

	rc = vmp->FlushStart(ckpt_id, perform_move);
	if (pio_unlikely(rc)) {
		return rc;
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
