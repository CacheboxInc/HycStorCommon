#pragma once

#include <memory>
#include <unordered_map>
#include <string>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "DaemonCommon.h"
#include "SpinLock.h"

namespace pio {
class VmManager {
public:
	::hyc_thrift::VmHandle CreateInstance(::ondisk::VmID vmid, const std::string& config);
	VirtualMachine* GetInstance(const ::ondisk::VmID& vmid);
	VirtualMachine* GetInstance(const ::hyc_thrift::VmHandle& handle);
	int FreeInstance(const ::hyc_thrift::VmHandle& handle);
private:
	SpinLock mutex_;
	::hyc_thrift::VmHandle handle_{0};
	std::unordered_map<::ondisk::VmID, std::unique_ptr<VirtualMachine>> ids_;
	std::unordered_map<::hyc_thrift::VmHandle, VirtualMachine*> handles_;
};
}
