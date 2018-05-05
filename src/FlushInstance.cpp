#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "DaemonTgtInterface.h"
#include "Singleton.h"
#include "VirtualMachine.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "VmdkFactory.h"
#include "VmManager.h"
#include "FlushInstance.h"

namespace pio {

FlushInstance::FlushInstance(VmID vmid):vmid_(std::move(vmid)) {
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

	rc = vmp->FlushStart(ckpt_id);
	if (pio_unlikely(rc)) {
		return rc;
	}

	return 0;
}

}
