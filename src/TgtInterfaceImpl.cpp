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
#include "VmManager.h"
#include "Singleton.h"

using namespace ::hyc_thrift;

namespace pio {
using namespace folly;

struct {
	std::once_flag initialized_;
} g_init_;

int InitStordLib(void) {
	try {
		std::call_once(g_init_.initialized_, [=] () mutable {
			SingletonHolder<VmdkManager>::CreateInstance();
			SingletonHolder<VmManager>::CreateInstance();
		});
	} catch (const std::exception& e) {
		return -1;
	}
	return 0;
}

int DeinitStordLib(void) {
	return 0;
}

VmHandle NewVm(VmID vmid, const std::string& config) {
	auto managerp = SingletonHolder<VmManager>::GetInstance();
	if (auto vmp = managerp->GetInstance(vmid); pio_unlikely(vmp)) {
		LOG(ERROR) << "VirtualMachine already present";
		return kInvalidVmHandle;
	}

	try {
		return managerp->CreateInstance(std::move(vmid), config);
	} catch (...) {
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

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vm_handle);
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
		auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vmid);
		if (pio_unlikely(not vmp)) {
			return kInvalidVmHandle;
		}
		return vmp->GetHandle();
	} catch (const std::exception& e) {
		return kInvalidVmHandle;
	}
}

void RemoveVm(VmHandle handle) {
	try {
		SingletonHolder<VmManager>::GetInstance()->FreeInstance(handle);
	} catch (...) {

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
