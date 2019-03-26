#include "VirtualMachine.h"
#include "Vmdk.h"
#include "VmSync.h"
#include "DataCopier.h"

namespace pio {
VmdkSync::VmdkSync(ActiveVmdk* vmdkp) noexcept :
		vmdkp_(vmdkp),
		sync_(vmdkp, 0) {
}

void VmdkSync::SetSyncSource(RequestHandlerPtrVec&& source) {
	sync_.SetDataSource(std::forward<RequestHandlerPtrVec>(source));
}

void VmdkSync::SetSyncDest(RequestHandlerPtrVec&& dest) {
	sync_.SetDataDestination(std::forward<RequestHandlerPtrVec>(dest));
}

int VmdkSync::SetCheckPointBatch(const CkptBatch& batch, bool* restart) noexcept {
	CheckPointPtrVec ckpts;

	auto [begin, progress, end] = batch;
	(void) progress;
	for (auto b = begin; b <= end; ++b) {
		CheckPoint* ckptp = vmdkp_->GetCheckPoint(b);
		if (pio_unlikely(not ckptp)) {
			return -EINVAL;
		}
		ckpts.emplace_back(ckptp);
	}

	return sync_.SetCheckPoints(std::move(ckpts), restart);
}

void VmdkSync::SyncStatus(bool* is_stoppedp, int* resultp) const noexcept {
	sync_.GetStatus(is_stoppedp, resultp);
}

void VmdkSync::GetCheckPointSummary(
			::ondisk::CheckPointID* donep,
			::ondisk::CheckPointID* progressp,
			::ondisk::CheckPointID* scheduledp
		) const noexcept {
	auto stats = sync_.GetStats();
	*donep = stats.cbt_sync_done;
	*progressp = stats.cbt_sync_in_progress;
	*scheduledp = stats.cbt_sync_scheduled;
}

int VmdkSync::SyncStart() {
	return sync_.Start();
}

VmSync::VmSync(VirtualMachine* vmp,
			const CkptBatch& batch,
			uint16_t batch_size
		) noexcept :
			vmp_(vmp),
			ckpt_batch_(batch),
			ckpt_batch_size_(batch_size) {
	ckpt_latest_ = vmp->GetCurCkptID();
}

VmSync::~VmSync() noexcept {
}

void VmSync::NewCheckPointCreated(::ondisk::CheckPointID id) {
	VLOG(5) << "VmSync: updated Sync CBT ID from " << ckpt_latest_
		<< " to " << id;
	ckpt_latest_ = id;
}

int VmSync::SetVmdkToSync(std::vector<std::unique_ptr<VmdkSync>> vmdks) noexcept {
	if (pio_unlikely(not vmdks_.empty())) {
		LOG(ERROR) << "VmSync: Sync targets already configured";
		return -EINVAL;
	}
	vmdks_.swap(vmdks);
	return 0;
}

void VmSync::SyncStatus(bool* stopped, int* result) const noexcept {
	*stopped = true;
	*result = 0;

	std::lock_guard<std::mutex> lock(mutex_);
	for (const auto& vmdk_sync : vmdks_) {
		bool s;
		int rc;
		vmdk_sync->SyncStatus(&s, &rc);
		if (pio_unlikely(rc < 0)) {
			*result = rc;
		}
		if (not s) {
			*stopped = false;
		}
	}
}

int VmSync::SyncRestart() {
	int res;
	for (auto& vmdk_sync : vmdks_) {
		auto rc = vmdk_sync->SyncStart();
		if (pio_unlikely(rc < 0)) {
			res = rc;
		}
	}
	return res;
}

int VmSync::SyncStart() {
	std::lock_guard<std::mutex> lock(mutex_);
	/* ensure existing sync is stopped */
	bool stopped;
	int result;
	SyncStatus(&stopped, &result);
	if (pio_unlikely(result < 0)) {
		LOG(ERROR) << "VmSync: previous sync failed. Restarting it";
		return SyncRestart();
	}
	if (not stopped) {
		LOG(INFO) << "VmSync: previous sync is already running for VmID "
			<< vmp_->GetID()
			<< " batch " << std::get<0>(ckpt_batch_) << ','
			<< std::get<1>(ckpt_batch_) << ','
			<< std::get<2>(ckpt_batch_);
		return 0;
	}

	/* validate the progress of existing sync */
	auto expected_scheduled = std::get<2>(ckpt_batch_);
	for (const auto& vmdk_sync : vmdks_) {
		::ondisk::CheckPointID done;
		::ondisk::CheckPointID progress;
		::ondisk::CheckPointID scheduled;

		vmdk_sync->GetCheckPointSummary(&done, &progress, &scheduled);
		if (not (done == progress and done == scheduled and done == expected_scheduled)) {
			LOG(ERROR) << "VmSync: fatal error "
				<< " done " << done
				<< " progress " << progress
				<< " scheduled " << scheduled
				<< " expected scheduled " << expected_scheduled;
			return -EINVAL;
		}
	}

	++expected_scheduled;
	auto sync_till = std::min(expected_scheduled + ckpt_batch_size_, ckpt_latest_);
	CkptBatch ckpt_batch{expected_scheduled, 0, sync_till};
	int res = 0;
	for (auto& vmdk_sync : vmdks_) {
		bool restart = false;
		auto rc = vmdk_sync->SetCheckPointBatch(ckpt_batch, &restart);
		if (pio_unlikely(rc < 0)) {
			res = rc;
			continue;
		}
		log_assert(restart == true);
	}
	if (pio_unlikely(res < 0)) {
		return res;
	}
	ckpt_batch_ = ckpt_batch;
	return SyncRestart();
}
}
