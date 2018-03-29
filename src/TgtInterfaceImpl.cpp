#include <deque>
#include <string>
#include <atomic>
#include <mutex>

#include <folly/fibers/Fiber.h>
#include <folly/futures/Future.h>
#include <glog/logging.h>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtInterface.h"
#include "Request.h"
#include "ThreadPool.h"
#include "VirtualMachine.h"
#include "Vmdk.h"
#include "BlockTraceHandler.h"
#include "CacheHandler.h"
#include "VmdkConfig.h"
#include "VmdkFactory.h"
#include "Singleton.h"

using namespace ::hyc_thrift;

namespace pio {
using namespace folly;

struct vms {
	std::mutex mutex_;
	std::atomic<VmHandle> handle_{0};
	std::unordered_map<VmID, std::unique_ptr<VirtualMachine>> ids_;
	std::unordered_map<VmHandle, VirtualMachine*> handles_;
} g_vms;

struct {
	std::once_flag initialized_;
} g_init_;

int InitStordLib(void) {
	try {
		std::call_once(g_init_.initialized_, [=] () mutable {
			SingletonHolder<VmdkManager>::CreateInstance();
		});
	} catch (const std::exception& e) {
		return -1;
	}
	return 0;
}

int DeinitStordLib(void) {
	return 0;
}

static VirtualMachine* VmFromVmID(const std::string& vmid) {
	std::lock_guard<std::mutex> lock(g_vms.mutex_);
	auto it = g_vms.ids_.find(vmid);
	if (pio_unlikely(it == g_vms.ids_.end())) {
		return kInvalidVmHandle;
	}
	return it->second.get();
}

static VirtualMachine* VmFromVmHandle(VmHandle handle) {
	std::lock_guard<std::mutex> lock(g_vms.mutex_);
	auto it = g_vms.handles_.find(handle);
	if (pio_unlikely(it == g_vms.handles_.end())) {
		return nullptr;
	}
	return it->second;
}

static void RemoveVmHandleLocked(VmHandle handle) {
	auto it1 = g_vms.handles_.find(handle);
	if (it1 != g_vms.handles_.end()) {
		auto it2 = g_vms.ids_.find(it1->second->GetID());
		if (it2 != g_vms.ids_.end()) {
			g_vms.ids_.erase(it2);
		}
		g_vms.handles_.erase(it1);
	}
}

void RemoveVm(VmHandle handle) {
	std::lock_guard<std::mutex> lock(g_vms.mutex_);
	RemoveVmHandleLocked(handle);
}

VmHandle NewVm(VmID vmid, const std::string& config) {
	std::lock_guard<std::mutex> lock(g_vms.mutex_);

	auto handle = ++g_vms.handle_;
	try {
		if (auto it = g_vms.ids_.find(vmid);
				pio_unlikely(it != g_vms.ids_.end())) {
			LOG(ERROR) << __func__ << "vmid " << vmid << " already present.";
			return kInvalidVmHandle;
		}

		auto vmp = std::make_unique<VirtualMachine>(handle, vmid, config);
		g_vms.handles_.insert(std::make_pair(handle, vmp.get()));
		g_vms.ids_.insert(std::make_pair(std::move(vmid), std::move(vmp)));
		return handle;
	} catch (const std::bad_alloc& e) {
		LOG(ERROR) << __func__ <<  " vmid " << vmid
			<< " failed because of std::bad_alloc exception";
		RemoveVmHandleLocked(handle);
		return kInvalidVmHandle;
	}
}

VmdkHandle NewActiveVmdk(VmHandle vm_handle, VmdkID vmdkid,
		const std::string& config) {
	auto managerp = SingletonHolder<VmdkManager>::GetInstance();

	if (auto vmdkp = managerp->GetInstance(vmdkid); pio_unlikely(vmdkp)) {
		/* VMDK already present */
		return kInvalidVmdkHandle;
	}

	auto vmp = VmFromVmHandle(vm_handle);
	if (pio_unlikely(not vmp)) {
		return kInvalidVmdkHandle;
	}

	auto handle = managerp->CreateInstance<ActiveVmdk>(std::move(vmdkid), vmp,
		config);
	if (pio_unlikely(handle == kInvalidVmdkHandle)) {
		return handle;
	}

	try {
		auto vmdkp = dynamic_cast<ActiveVmdk*>(managerp->GetInstance(handle));
		if (pio_unlikely(vmdkp == nullptr)) {
			throw std::runtime_error("Fatal error");
		}

		vmdkp->RegisterRequestHandler(std::make_unique<BlockTraceHandler>());
		auto ch = std::make_unique<CacheHandler>(vmdkp->GetJsonConfig());
		vmdkp->RegisterRequestHandler(std::move(ch));

		vmp->AddVmdk(vmdkp);
	} catch (const std::exception& e) {
		managerp->FreeVmdkInstance(handle);
		LOG(ERROR) << "Failed to add VMDK";
		handle = kInvalidVmdkHandle;
	}
	return handle;
}

VmHandle GetVmHandle(const std::string& vmid) {
	try {
		auto vmp = pio::VmFromVmID(vmid);
		if (pio_unlikely(not vmp)) {
			return kInvalidVmHandle;
		}
		return vmp->GetHandle();
	} catch (const std::exception& e) {
		return kInvalidVmHandle;
	}
}

VmdkHandle GetVmdkHandle(const std::string& vmdkid) {
	try {
		auto vmdkp =
			SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdkid);
		if (pio_unlikely(not vmdkp)) {
			return kInvalidVmdkHandle;
		}
		return vmdkp->GetHandle();
	} catch (const std::exception& e) {
		return kInvalidVmdkHandle;
	}
}

void RemoveVmdk(VmdkHandle handle) {
	try {
		auto managerp = SingletonHolder<VmdkManager>::GetInstance();
		managerp->FreeVmdkInstance(handle);
	} catch (const std::exception& e) {

	}
}

}
