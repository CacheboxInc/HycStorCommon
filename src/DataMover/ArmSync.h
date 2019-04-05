#pragma once

#include <vector>
#include <unordered_map>
#include <memory>

#include "RequestHandler.h"
#include "VmSync.h"
#include "ArmConfig.h"
#include "VddkLib.h"
#include "VddkTargetHandler.h"

namespace vddk {
class VCenter;
}

namespace pio {
class VddkTargetHandler;

using VddkPathInfo = config::arm_config::vmdk_info;
using VCenterInfo = vc_ns::vc_info;
using VddkTargetHandlerPtr = std::unique_ptr<VddkTargetHandler>;
using VddkPathInfoMap = config::arm_config::vmdk_info_map;


class ArmSync : public virtual VmSync {
public:
	ArmSync(const ArmSync&) = delete;
	ArmSync(ArmSync&&) = delete;
	ArmSync& operator = (const ArmSync&) = delete;
	ArmSync& operator = (ArmSync&&) = delete;

	ArmSync(VirtualMachine*, const ::ondisk::CheckPointID base,
		uint16_t batch_size) noexcept;
	// ArmSync(VirtualMachine*, SynceCookie::Cookie cookie) noexcept;
	virtual ~ArmSync() noexcept;

	int VCenterConnnect(std::string&& moid, VCenterInfo&& info);
	int SyncStart(const VddkPathInfoMap& paths);
	void SetCheckPoints(CheckPointID latest, CheckPointID flushed);

private:
	std::unordered_map<::ondisk::VmdkID, VddkTargetHandlerPtr>
		CreateVddkTargets(const VddkPathInfoMap& paths);
	std::unordered_map<::ondisk::VmdkID, RequestHandlerPtrVec>
		FindSyncSource();

private:
	VirtualMachine* vmp_{};
	std::unique_ptr<vddk::VCenter> vcenter_;

	std::unordered_map<::ondisk::VmdkID, VddkTargetHandlerPtr> targets_;
};

}
