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

using namespace ::hyc_thrift;

namespace pio {
using namespace folly;

struct vms {
	std::mutex mutex_;
	std::atomic<VmHandle> handle_{0};
	std::unordered_map<VmID, std::unique_ptr<VirtualMachine>> ids_;
	std::unordered_map<VmHandle, VirtualMachine*> handles_;
} g_vms;

struct vmdks {
	std::mutex mutex_;
	std::atomic<VmdkHandle> handle_{0};
	std::unordered_map<VmdkID, std::unique_ptr<Vmdk>> ids_;
	std::unordered_map<VmdkHandle, ActiveVmdk*> handles_;
} g_vmdks;

struct {
	std::once_flag initialized_;
	std::unique_ptr<ThreadPool> pool_;
} g_thread_;

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

ActiveVmdk* VmdkFromVmdkHandle(VmdkHandle handle) {
	std::lock_guard<std::mutex> lock(g_vmdks.mutex_);
	auto it = g_vmdks.handles_.find(handle);
	if (pio_unlikely(it == g_vmdks.handles_.end())) {
		return nullptr;
	}
	return it->second;
}

static Vmdk* VmdkFromVmdkID(const std::string& vmdkid) {
	std::lock_guard<std::mutex> lock(g_vmdks.mutex_);
	auto it = g_vmdks.ids_.find(vmdkid);
	if (pio_unlikely(it == g_vmdks.ids_.end())) {
		return kInvalidVmHandle;
	}
	return it->second.get();
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

	VLOG(1) << __func__ << " " << vmid << " config " << config;

	auto handle = ++g_vms.handle_;

	try {
		auto it = g_vms.ids_.find(vmid);
		if (pio_unlikely(it != g_vms.ids_.end())) {
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

static void RemoveVmdkHandleLocked(VmdkHandle handle) {
	auto it1 = g_vmdks.handles_.find(handle);
	if (it1 != g_vmdks.handles_.end()) {
		auto it2 = g_vmdks.ids_.find(it1->second->GetID());
		if (it2 != g_vmdks.ids_.end()) {
			g_vmdks.ids_.erase(it2);
		}
		g_vmdks.handles_.erase(it1);
	}
}

void RemoveVmdk(VmdkHandle handle) {
	std::lock_guard<std::mutex> lock(g_vmdks.mutex_);
	RemoveVmdkHandleLocked(handle);
}

VmdkHandle NewActiveVmdk(VmHandle vm_handle, VmdkID vmdkid,
		const std::string& config) {
	{
		/* VMDK already present */
		auto vmdkp = VmdkFromVmdkID(vmdkid);
		if (pio_unlikely(vmdkp)) {
			return kInvalidVmdkHandle;
		}
	}

	auto vmp = VmFromVmHandle(vm_handle);
	if (pio_unlikely(not vmp)) {
		return kInvalidVmdkHandle;
	}

	std::lock_guard<std::mutex> lock(g_vmdks.mutex_);
	auto handle = ++g_vmdks.handle_;

	try {
		auto vmdkp = std::make_unique<ActiveVmdk>(vmp, handle, vmdkid, config);
		auto p = vmdkp.get();
		g_vmdks.handles_.insert(std::make_pair(handle, p));
		g_vmdks.ids_.insert(std::make_pair(std::move(vmdkid), std::move(vmdkp)));

		p->RegisterRequestHandler(std::make_unique<BlockTraceHandler>());
		p->RegisterRequestHandler(std::make_unique<CacheHandler>(p->GetJsonConfig()));

		vmp->AddVmdk(p);
		return handle;
	} catch (const std::bad_alloc& e) {
		RemoveVmdkHandleLocked(handle);
		return kInvalidVmdkHandle;
	} catch (const boost::property_tree::ptree_error& e) {
		RemoveVmdkHandleLocked(handle);
		LOG(ERROR) << "Invalid configuration.";
		return kInvalidVmdkHandle;
	}
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
		auto vmdkp = pio::VmdkFromVmdkID(vmdkid);
		if (pio_unlikely(not vmdkp)) {
			return kInvalidVmdkHandle;
		}
		return vmdkp->GetHandle();
	} catch (const std::exception& e) {
		return kInvalidVmdkHandle;
	}
}
}
