#include <deque>
#include <string>
#include <atomic>
#include <mutex>

#include <folly/fibers/Fiber.h>
#include <folly/futures/Future.h>
#include <glog/logging.h>

#include "TgtInterface.h"
#include "Request.h"
#include "ThreadPool.h"
#include "VirtualMachine.h"
#include "Vmdk.h"
#include "BlockTraceHandler.h"
#include "CacheHandler.h"

namespace pio {
using namespace folly;

/* global data structures for library */
struct {
	std::mutex mutex_;
	std::atomic<VmHandle> handle_{0};
	std::unordered_map<VmID, std::unique_ptr<VirtualMachine>> ids_;
	std::unordered_map<VmHandle, VirtualMachine*> handles_;
} g_vms;

struct {
	std::mutex mutex_;
	std::atomic<VmdkHandle> handle_{0};
	std::unordered_map<VmdkID, std::unique_ptr<Vmdk>> ids_;
	std::unordered_map<VmdkHandle, ActiveVmdk*> handles_;
} g_vmdks;

struct {
	std::once_flag initialized_;
	std::unique_ptr<ThreadPool> pool_;
} g_thread_;

static int InitializeLibrary() {
	auto cores = std::thread::hardware_concurrency();

	try {
		std::call_once(g_thread_.initialized_, [=] () mutable {
			google::InitGoogleLogging("TGTD-StorageLibrary");
			g_thread_.pool_ = std::make_unique<ThreadPool>(cores);
			g_thread_.pool_->CreateThreads();

			VLOG(1) << "library initialized "
				<< "Cores = " << cores
				<< "Threads = " << g_thread_.pool_->Stats().nthreads_;
		});
	} catch (const std::exception& e) {
		g_thread_.pool_.release();
		return -ENOMEM;
	}

	log_assert(g_thread_.pool_);
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

static ActiveVmdk* VmdkFromVmdkHandle(VmdkHandle handle) {
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

static void RemoveVm(VmHandle handle) {
	std::lock_guard<std::mutex> lock(g_vms.mutex_);
	RemoveVmHandleLocked(handle);
}

static VmHandle NewVm(std::string vmid) {
	std::lock_guard<std::mutex> lock(g_vms.mutex_);

	auto handle = ++g_vms.handle_;

	try {
		auto it = g_vms.ids_.find(vmid);
		if (pio_unlikely(it != g_vms.ids_.end())) {
			return kInvalidVmHandle;
		}

		auto vmp = std::make_unique<VirtualMachine>(handle, vmid);
		g_vms.handles_.insert(std::make_pair(handle, vmp.get()));
		g_vms.ids_.insert(std::make_pair(std::move(vmid), std::move(vmp)));
		return handle;
	} catch (const std::bad_alloc& e) {
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

static void RemoveVmdk(VmdkHandle handle) {
	std::lock_guard<std::mutex> lock(g_vmdks.mutex_);
	RemoveVmdkHandleLocked(handle);
}

static VmdkHandle NewActiveVmdk(VmHandle vm_handle, std::string vmdkid) {
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
		auto vmdkp = std::make_unique<ActiveVmdk>(vmp, handle, vmdkid, 4096);
		auto p = vmdkp.get();
		g_vmdks.handles_.insert(std::make_pair(handle, p));
		g_vmdks.ids_.insert(std::make_pair(std::move(vmdkid), std::move(vmdkp)));

		p->RegisterRequestHandler(std::make_unique<BlockTraceHandler>());
		p->RegisterRequestHandler(std::make_unique<CacheHandler>());

		vmp->AddVmdk(p);
		return handle;
	} catch (const std::bad_alloc& e) {
		RemoveVmdkHandleLocked(handle);
		return kInvalidVmdkHandle;
	}
}

static int SetVmdkEventFd(VmdkHandle handle, int eventfd) {
	auto vmdkp = VmdkFromVmdkHandle(handle);
	if (pio_unlikely(not vmdkp)) {
		return -EINVAL;
	}
	vmdkp->SetEventFd(eventfd);
	return 0;
}

static RequestID ScheduleRead(VmdkHandle handle, const void* privatep,
		char *bufferp, size_t length, uint64_t offset) {
	auto vmdkp = VmdkFromVmdkHandle(handle);
	if (pio_unlikely(not vmdkp)) {
		VLOG(1) << "Invalid VMDK handle " << handle;
		return kInvalidRequestID;
	}

	auto vmp = vmdkp->GetVM();
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << "ActiveVmdk without VM " << handle;
		return kInvalidRequestID;
	}

	auto id = vmp->NextRequestID();
	auto reqp = std::make_unique<Request>(id, vmdkp, Request::Type::kRead,
		bufferp, length, length, offset);
	reqp->SetPrivateData(privatep);

	/* Add fiber task */
	g_thread_.pool_->AddTask([vmdkp, vmp, reqp = std::move(reqp)] () mutable {
		auto rc = vmp->Read(vmdkp, std::move(reqp)).get();
		(void) rc;
	});

	return id;
}

static RequestID ScheduleWrite(VmdkHandle handle, const void* privatep,
		char *bufferp, size_t length, uint64_t offset) {
	auto vmdkp = VmdkFromVmdkHandle(handle);
	if (pio_unlikely(not vmdkp)) {
		VLOG(1) << "Invalid VMDK handle " << handle;
		return kInvalidRequestID;
	}

	auto vmp = vmdkp->GetVM();
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << "ActiveVmdk without VM " << handle;
		return kInvalidRequestID;
	}

	auto id = vmp->NextRequestID();
	auto reqp = std::make_unique<Request>(id, vmdkp, Request::Type::kWrite,
		bufferp, length, length, offset);
	reqp->SetPrivateData(privatep);

	/* Add fiber task */
	g_thread_.pool_->AddTask([vmdkp, vmp, reqp = std::move(reqp)] () mutable {
		auto rc = vmp->Write(vmdkp, std::move(reqp)).get();
		(void) rc;
	});

	return id;
}

static RequestID ScheduleWriteSame(VmdkHandle handle, const void* privatep,
		char *bufferp, size_t buffer_length, size_t transfer_length,
		uint64_t offset) {
	auto vmdkp = VmdkFromVmdkHandle(handle);
	if (pio_unlikely(not vmdkp)) {
		VLOG(1) << "Invalid VMDK handle " << handle;
		return kInvalidRequestID;
	}

	auto vmp = vmdkp->GetVM();
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << "ActiveVmdk without VM " << handle;
		return kInvalidRequestID;
	}

	auto id = vmp->NextRequestID();
	auto reqp = std::make_unique<Request>(id, vmdkp, Request::Type::kWriteSame,
		bufferp, buffer_length, transfer_length, offset);
	reqp->SetPrivateData(privatep);

	/* Add fiber task */
	g_thread_.pool_->AddTask([vmdkp, vmp, reqp = std::move(reqp)] () mutable {
		auto rc = vmp->WriteSame(vmdkp, std::move(reqp)).get();
		(void) rc;
	});

	return id;
}

static int RequestAbort(VmdkHandle handle, RequestID request_id) {
	auto vmdkp = VmdkFromVmdkHandle(handle);
	if (pio_unlikely(not vmdkp)) {
		VLOG(1) << "Invalid VMDK handle " << handle;
		return -1;
	}

	/* TODO: */
	return 0;
}

static uint32_t GetCompleteRequests(VmdkHandle handle, RequestResult *resultsp,
		uint32_t nresults, bool *has_morep) {
	auto vmdkp = VmdkFromVmdkHandle(handle);
	if (pio_unlikely(not vmdkp)) {
		VLOG(1) << "Invalid VMDK handle " << handle;
		return kInvalidRequestID;
	}

	auto vmp = vmdkp->GetVM();
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << "ActiveVmdk without VM " << handle;
		return kInvalidRequestID;
	}

	return vmp->GetRequestResult(vmdkp, resultsp, nresults, has_morep);
}
}

int InitializeLibrary() {
	try {
		return pio::InitializeLibrary();
	} catch (const std::exception& e) {
		return -1;
	}
}

VmHandle NewVm(const char* vmidp) {
	try {
		return pio::NewVm(vmidp);
	} catch (const std::exception& e) {
		return kInvalidVmHandle;
	}
}

VmHandle GetVmHandle(const char* vmidp) {
	try {
		auto vmp = pio::VmFromVmID(vmidp);
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
		pio::RemoveVm(handle);
		return;
	} catch (const std::exception& e) {
		return;
	}
}

VmdkHandle NewActiveVmdk(VmHandle vm_handle, const char* vmdkid) {
	try {
		return pio::NewActiveVmdk(vm_handle, vmdkid);
	} catch (const std::exception& e) {
		return kInvalidVmdkHandle;
	}
}

void RemoveVmdk(VmdkHandle handle) {
	try {
		pio::RemoveVmdk(handle);
		return;
	} catch (const std::exception& e) {
		return;
	}
}

VmdkHandle GetVmdkHandle(const char* vmdkidp) {
	try {
		auto vmdkp = pio::VmdkFromVmdkID(vmdkidp);
		if (pio_unlikely(not vmdkp)) {
			return kInvalidVmdkHandle;
		}
		return vmdkp->GetHandle();
	} catch (const std::exception& e) {
		return kInvalidVmdkHandle;
	}
}

int SetVmdkEventFd(VmdkHandle handle, int eventfd) {
	try {
		return pio::SetVmdkEventFd(handle, eventfd);
	} catch (const std::exception& e) {
		return -EINVAL;
	}
}

RequestID ScheduleRead(VmdkHandle handle, const void* privatep,
		char *bufferp, size_t length, uint64_t offset) {
	try {
		return pio::ScheduleRead(handle, privatep, bufferp, length, offset);
	} catch (const std::exception& e) {
		return kInvalidRequestID;
	}
}

RequestID ScheduleWrite(VmdkHandle handle, const void* privatep,
		char *bufferp, size_t length, uint64_t offset) {
	try {
		return pio::ScheduleWrite(handle, privatep, bufferp, length, offset);
	} catch (const std::exception& e) {
		return kInvalidRequestID;
	}
}

RequestID ScheduleWriteSame(VmdkHandle handle, const void* privatep,
		char *bufferp, size_t buffer_length, uint64_t transfer_length,
		uint64_t offset) {
	try {
		return pio::ScheduleWriteSame(handle, privatep, bufferp, buffer_length,
			transfer_length, offset);
	} catch (const std::exception& e) {
		return kInvalidRequestID;
	}
}

int RequestAbort(VmdkHandle handle, RequestID request_id) {
	try {
		return pio::RequestAbort(handle, request_id);
	} catch (const std::exception& e) {
		return -1;
	}
}

uint32_t GetCompleteRequests(VmdkHandle handle, RequestResult *resultsp,
		uint32_t nresults, bool *has_morep) {
	try {
		return pio::GetCompleteRequests(handle, resultsp, nresults, has_morep);
	} catch (const std::exception& e) {
		*has_morep = true;
		return 0;
	}
}