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
	log_assert(begin <= end);

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

DataSync::Stats VmdkSync::GetStats() const noexcept {
	return sync_.GetStats();
}

void VmdkSync::GetCheckPointSummary(
			::ondisk::CheckPointID* donep,
			::ondisk::CheckPointID* progressp,
			::ondisk::CheckPointID* scheduledp
		) const noexcept {
	auto stats = GetStats();
	*donep = stats.cbt_sync_done;
	*progressp = stats.cbt_sync_in_progress;
	*scheduledp = stats.cbt_sync_scheduled;
}

folly::Future<int> VmdkSync::SyncStart() {
	return sync_.Start();
}

VmSync::VmSync(VirtualMachine* vmp, VmSync::Type type,
			const ::ondisk::CheckPointID base,
			uint16_t batch_size
		) noexcept :
			vmp_(vmp),
			type_(type),
			ckpt_base_(base),
			ckpt_batch_({base, 0, base}),
			ckpt_batch_size_(batch_size) {
}

const VmSync::Type& VmSync::GetSyncType() const noexcept {
	return type_;
}

#if 0
VmSync::VmSync(VirtualMachine* vmp,
			SynceCookie::Cookie cookie
		) :
			vmp_(vmp) {
	cookie_.SetCookie(cookie);

	ckpt_base_ = cookie_.GetCheckPointBase();
	ckpt_batch_ = cookie_.GetCheckPointBatch();
	ckpt_batch_size_ = cookie_.GetCheckPointBatchSize();

#if 0
	TODO
	====
	make sure UUID in Cookie is correct
#endif
}
#endif

VmSync::~VmSync() noexcept {
}

void VmSync::SyncTill(::ondisk::CheckPointID till) noexcept {
	till = std::max(ckpt_sync_till_, till);
	VLOG(5) << "VmSync: updated Sync CBT ID from " << ckpt_sync_till_
		<< " to " << till;
	ckpt_sync_till_ = till;
}

int VmSync::SetVmdkToSync(std::vector<std::unique_ptr<VmdkSync>> vmdks) noexcept {
	if (pio_unlikely(not vmdks_.empty())) {
		LOG(ERROR) << "VmSync: Sync targets already configured";
		return -EINVAL;
	}
	vmdks_.swap(vmdks);
	return 0;
}

void VmSync::SyncStatusLocked(bool* stopped, int* result) const noexcept {
	*stopped = true;
	*result = 0;
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

std::vector<DataSync::Stats> VmSync::GetStats() const noexcept {
	std::vector<DataSync::Stats> ret;
	for (const auto& vmdk_sync : vmdks_) {
		ret.emplace_back(vmdk_sync->GetStats());
	}
	return ret;
}

void VmSync::SyncStatus(bool* stopped, int* result) const noexcept {
	std::lock_guard<std::mutex> lock(mutex_);
	SyncStatusLocked(stopped, result);
}

folly::Future<int> VmSync::SyncRestart() {
	std::vector<folly::Future<int>> futures;
	futures.reserve(vmdks_.size());

	for (auto& vmdk_sync : vmdks_) {
		futures.emplace_back(vmdk_sync->SyncStart());
	}

	return folly::collectAll(std::move(futures))
	.then([] (folly::Try<std::vector<folly::Try<int>>>& tries) {
		if (pio_unlikely(tries.hasException())) {
			LOG(ERROR) << "VmSync: starting sync failed";
			return -EIO;
		}
		for (const auto& tri : tries.value()) {
			if (pio_unlikely(tri.hasException())) {
				LOG(ERROR) << "VmSync: starting sync failed";
				return -EIO;
			}
			if (pio_unlikely(tri.value() < 0)) {
				LOG(ERROR) << "VmSync: starting sync failed with error "
					<< tri.value();
				return tri.value();
			}
		}
		return 0;
	});
}

int VmSync::ValidateSyncProgress() noexcept {
	auto expected_scheduled = std::get<2>(ckpt_batch_);
	if (expected_scheduled == ckpt_base_) {
		return 0;
	}
	for (const auto& vmdk_sync : vmdks_) {
		::ondisk::CheckPointID done;
		::ondisk::CheckPointID progress;
		::ondisk::CheckPointID scheduled;

		vmdk_sync->GetCheckPointSummary(&done, &progress, &scheduled);
		if (not (done == scheduled and done == expected_scheduled)) {
			LOG(ERROR) << "VmSync: fatal error "
				<< " done " << done
				<< " progress " << progress
				<< " scheduled " << scheduled
				<< " expected scheduled " << expected_scheduled;
			return -EINVAL;
		}
	}
	return 0;
}

bool VmSync::Setup() {
	std::lock_guard<std::mutex> lock(mutex_);
	bool stopped;
	int result;
	SyncStatusLocked(&stopped, &result);
	if (pio_unlikely(result < 0)) {
		LOG(ERROR) << "VmSync: earlier sync has failed.";
		return true;
	}
	if (not stopped) {
		LOG(INFO) << "VmSync: earlier sync is running for VmID "
			<< vmp_->GetID()
			<< " batch " << std::get<0>(ckpt_batch_) << ','
			<< std::get<1>(ckpt_batch_) << ','
			<< std::get<2>(ckpt_batch_);
		return false;
	}

	auto rc = ValidateSyncProgress();
	if (pio_unlikely(rc < 0)) {
		return false;
	}

	auto expected_scheduled = std::get<2>(ckpt_batch_) + 1;
	if (expected_scheduled > ckpt_sync_till_) {
		return false;
	}

	auto sync_till = std::min(expected_scheduled + ckpt_batch_size_, ckpt_sync_till_);
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
		return false;
	}
	ckpt_batch_ = ckpt_batch;
	return true;
}

int VmSync::SyncStart() {
	bool restart = Setup();
	if (not restart) {
		return 0;
	}

	LOG(INFO) << "VmSync: starting sync from CBT " << std::get<0>(ckpt_batch_)
		<< " till CBT " << std::get<2>(ckpt_batch_);
	(void) SyncRestart()
	.then([this] (int rc) mutable {
		if (pio_likely(rc == 0)) {
			SyncStart();
		}
	});
	return 0;
}
}
