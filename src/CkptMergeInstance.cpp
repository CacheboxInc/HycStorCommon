#include "Singleton.h"
#include "CkptMergeManager.h"
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/aerospike_info.h>
#include "VmManager.h"
#include "VirtualMachine.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "VmdkFactory.h"
#include "CkptMergeInstance.h"
#include <chrono>
#include "DaemonTgtInterface.h"

namespace pio {

CkptMergeInstance::CkptMergeInstance(VmID vmid):
	vmid_(std::move(vmid)) {
	start_time_ = std::chrono::steady_clock::now();
}

CkptMergeInstance::~CkptMergeInstance() {
}

int CkptMergeInstance::StartCkptMerge(const VmID& vmid, const CheckPointID& ckpt_id) {

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vmid);
	log_assert(vmp != nullptr);

	if (pio_unlikely(ckpt_id == MetaData_constants::kInvalidCheckPointID() + 1)) {
		LOG(ERROR) << __func__
			<< " Merging for very first checkpoint (ID:" << ckpt_id
			<< ") is not allowed";
		return -EINVAL;
	}

	LOG(INFO) << __func__ << " Starting merge for Checkpoint ID :" << ckpt_id;
	auto rc = vmp->MergeStart(ckpt_id);
	if (pio_unlikely(rc)) {
		LOG(INFO) << __func__ << " CkptMerge failed for Checkpoint ID:"
					<< ckpt_id << ", rc is:" << rc;
		return rc;
	}

	LOG(INFO) << __func__
		<< " Starting Data delete operation in background for Checkpoint ID :"
		<< ckpt_id;
	pio::NewScanReq(vmid, ckpt_id);
	return 0;
}

int CkptMergeInstance::CkptMergeStatus(CkptMergeStats &merge_stat) {
	/* Append start and elapsed time in stat */
	merge_stat.running = true;
	return 0;
}

}
