#pragma once

#include <vector>
#include <memory>
#include <mutex>

#include "DataSync.h"
#include "DataMoverCommonTypes.h"

namespace pio {
class VirtualMachine;
class ActiveVmdk;
class VmSync;

class VmdkSync {
public:
	VmdkSync(const VmdkSync&) = delete;
	VmdkSync(VmdkSync&&) = delete;
	VmdkSync& operator = (const VmdkSync&) = delete;
	VmdkSync& operator = (VmdkSync&&) = delete;

	VmdkSync(ActiveVmdk* vmdkp) noexcept;

	void SetSyncSource(RequestHandlerPtrVec&& source);
	void SetSyncDest(RequestHandlerPtrVec&& dest);
  
	DataSync::Stats GetStats() const noexcept;

	friend class VmSync;
private:
	void SyncStatus(bool* is_stoppedp, int* resultp) const noexcept;
	int SetCheckPointBatch(const CkptBatch& batch, bool* restartp) noexcept;
	void GetCheckPointSummary(::ondisk::CheckPointID* donep,
		::ondisk::CheckPointID* progressp,
		::ondisk::CheckPointID* scheduledp) const noexcept;
	int SyncStart();
private:
	ActiveVmdk* vmdkp_;
	DataSync sync_;
};

class VmSync {
protected:
	VmSync(VirtualMachine* vmp,
		const ::ondisk::CheckPointID base,
		uint16_t batch_size) noexcept;

	/* TODO: need constructor using SynceCookie */

public:
	virtual ~VmSync() noexcept;

	virtual void SetCheckPoints(::ondisk::CheckPointID latest,
		::ondisk::CheckPointID flushed) = 0;

	void SyncStatus(bool* stopped, int* result) const noexcept;
	int SyncStart();
private:
	void SyncStatusLocked(bool* is_stoppedp, int* resultp) const noexcept;
	int SyncRestart();
protected:
	int SetVmdkToSync(std::vector<std::unique_ptr<VmdkSync>> vmdks) noexcept;
	void SyncTill(::ondisk::CheckPointID id) noexcept;

private:
	VirtualMachine* vmp_{};
	::ondisk::CheckPointID ckpt_base_{};
	CkptBatch ckpt_batch_{kCkptBatchInitial};
	uint16_t ckpt_batch_size_{0};
	::ondisk::CheckPointID ckpt_sync_till_{};

	mutable std::mutex mutex_;
	std::vector<std::unique_ptr<VmdkSync>> vmdks_;
};

}
