#pragma once

#include <memory>
#include <unordered_map>
#include <string>

#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "DaemonTgtTypes.h"
#include "DaemonCommon.h"
#include "SpinLock.h"

namespace pio {
class VmManager {
public:
	::hyc_thrift::VmHandle CreateInstance(VmID vmid, const std::string& config);
	VirtualMachine* GetInstance(const VmID& vmid);
	VirtualMachine* GetInstance(const ::hyc_thrift::VmHandle& handle);
	void FreeInstance(const ::hyc_thrift::VmHandle& handle);
private:
	SpinLock mutex_;
	::hyc_thrift::VmHandle handle_{0};
	std::unordered_map<VmID, std::unique_ptr<VirtualMachine>> ids_;
	std::unordered_map<::hyc_thrift::VmHandle, VirtualMachine*> handles_;
};
}