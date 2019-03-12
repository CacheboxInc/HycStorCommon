#include <memory>
#include <unordered_map>
#include <string>

#include <folly/futures/Future.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "IDs.h"
#include "DaemonCommon.h"
#include "SpinLock.h"
#include "VirtualMachine.h"
#include "VmManager.h"

using namespace ::hyc_thrift;
using namespace ::ondisk;

namespace pio {
VmHandle VmManager::CreateInstance(VmID vmid, const std::string& config) {
	try {
		std::lock_guard<SpinLock> lock(mutex_);
		if (auto it = ids_.find(vmid); pio_unlikely(it != ids_.end())) {
			assert(0);
		}
		auto handle = ++handle_;
		auto vm = std::make_unique<VirtualMachine>(handle, vmid, config);
		handles_.insert(std::make_pair(handle, vm.get()));
		ids_.insert(std::make_pair(std::move(vmid), std::move(vm)));
		return handle;
	} catch (const std::bad_alloc& e) {
		return StorRpc_constants::kInvalidVmHandle();
	}
	return StorRpc_constants::kInvalidVmHandle();
}

VirtualMachine* VmManager::GetInstance(const VmID& vmid) {
	std::lock_guard<SpinLock> lock(mutex_);
	if (auto it = ids_.find(vmid); pio_likely(it != ids_.end())) {
		return it->second.get();
	}
	return nullptr;
}

std::vector<VirtualMachine*> VmManager::GetAllVMs() {
	std::vector<VirtualMachine*> vms;
	std::lock_guard<SpinLock> lock(mutex_);
	for (auto it: handles_) {
		vms.push_back(it.second);
	}
	return vms;
}

VirtualMachine* VmManager::GetInstance(const VmHandle& handle) {
	std::lock_guard<SpinLock> lock(mutex_);
	if (auto it = handles_.find(handle); pio_likely(it != handles_.end())) {
		return it->second;
	}
	return nullptr;
}

int VmManager::FreeInstance(const VmHandle& handle) {
	LOG(ERROR) << __func__ << "START";
	std::lock_guard<SpinLock> lock(mutex_);
	auto it1 = handles_.find(handle);
	if (pio_unlikely(it1 == handles_.end())) {
		LOG(ERROR) << __func__ << "Haven't found given VM";
		return 1;
	}

	auto it2 = ids_.find(it1->second->GetID());
	assert(pio_likely(it2 != ids_.end()));

	handles_.erase(it1);
	ids_.erase(it2);
	LOG(ERROR) << __func__ << "Given VM has removed";
	return 0;
}

}
