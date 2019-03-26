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

	friend class VmSync;
private:
	int SetCheckPointBatch(const CkptBatch& batch, bool* restartp) noexcept;
	void SyncStatus(bool* is_stoppedp, int* resultp) const noexcept;
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
		const CkptBatch& batch,
		uint16_t batch_size) noexcept;

	/* TODO: need constructor using SynceCookie */

public:
	virtual ~VmSync() noexcept;

	void NewCheckPointCreated(::ondisk::CheckPointID);
	void SyncStatus(bool* stopped, int* result) const noexcept;
	int SyncStart();
private:
	int SyncRestart();
protected:
	int SetVmdkToSync(std::vector<std::unique_ptr<VmdkSync>> vmdks) noexcept;

protected:
	VirtualMachine* vmp_{};

private:
	CkptBatch ckpt_batch_{kCkptBatchInitial};
	uint16_t ckpt_batch_size_{0};
	::ondisk::CheckPointID ckpt_latest_{};

	mutable std::mutex mutex_;
	std::vector<std::unique_ptr<VmdkSync>> vmdks_;
};

}
