#pragma once

#include <vector>
#include <unordered_map>
#include <memory>

#include "RequestHandler.h"
#include "VmSync.h"

namespace pio {
class VddkTargetHandler;

using VddkPathInfo = int;
using VCenter = int;
using VCenterInfo = int;
using VddkTarget = std::unique_ptr<VddkTargetHandler>;
using VddkPathInfoMap = std::unordered_map<::ondisk::VmdkID, VddkPathInfo>;

class VddkTargetHandler : public virtual RequestHandler {
public:
	VddkTargetHandler(ActiveVmdk* vmdkp,
				VCenter* vcp,
				const VddkPathInfo& path
			) noexcept :
				RequestHandler("vddk", nullptr) {
		(void) vmdkp;
		(void) vcp;
		(void) path;
	}

	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
			const std::vector<RequestBlock*>& process,
			std::vector<RequestBlock *>& failed) override {
		(void) vmdkp;
		(void) reqp;
		(void) process;
		(void) failed;
		return 0;
	}

	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
			CheckPointID ckpt, const std::vector<RequestBlock*>& process,
			std::vector<RequestBlock *>& failed) override {
		(void) vmdkp;
		(void) reqp;
		(void) ckpt;
		(void) process;
		(void) failed;
		return 0;
	}

	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp,
			Request *reqp, const std::vector<RequestBlock*>& process,
			std::vector<RequestBlock *>& failed) override {
		(void) vmdkp;
		(void) reqp;
		(void) process;
		(void) failed;
		return 0;
	}

	virtual folly::Future<int> BulkWrite(ActiveVmdk* vmdkp,
			::ondisk::CheckPointID ckpt,
			const std::vector<std::unique_ptr<Request>>& requests,
			const std::vector<RequestBlock*>& process,
			std::vector<RequestBlock*>& failed) override {
		(void) vmdkp;
		(void) ckpt;
		(void) requests;
		(void) process;
		(void) failed;
		return 0;
	}

	virtual folly::Future<int> BulkRead(ActiveVmdk* vmdkp,
			const std::vector<std::unique_ptr<Request>>& requests,
			const std::vector<RequestBlock*>& process,
			std::vector<RequestBlock*>& failed) override {
		(void) vmdkp;
		(void) requests;
		(void) process;
		(void) failed;
		return 0;
	}

	virtual folly::Future<int> BulkReadPopulate(ActiveVmdk* vmdkp,
			const std::vector<std::unique_ptr<Request>>& requests,
			const std::vector<RequestBlock*>& process,
			std::vector<RequestBlock*>& failed) override {
		(void) vmdkp;
		(void) requests;
		(void) process;
		(void) failed;
		return 0;
	}
};

class ArmSync : public virtual VmSync {
public:
	ArmSync(const ArmSync&) = delete;
	ArmSync(ArmSync&&) = delete;
	ArmSync& operator = (const ArmSync&) = delete;
	ArmSync& operator = (ArmSync&&) = delete;

	ArmSync(VirtualMachine*, const CkptBatch&, uint16_t batch_size) noexcept;
	virtual ~ArmSync() noexcept;

	int VCenterConnnect(VCenterInfo&& info);
	int SyncStart(const VddkPathInfoMap& paths);

private:
	std::unordered_map<::ondisk::VmdkID, VddkTarget>
		CreateVddkTargets(const VddkPathInfoMap& paths);
	std::unordered_map<::ondisk::VmdkID, RequestHandlerPtrVec>
		FindSyncSource();

private:
	VirtualMachine* vmp_{};
	std::unique_ptr<VCenter> vcenter_;

	std::unordered_map<::ondisk::VmdkID, VddkTarget> targets_;
};

}
