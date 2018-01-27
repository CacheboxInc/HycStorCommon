#include <algorithm>

#include <folly/futures/Future.h>

#include "TgtTypes.h"
#include "VirtualMachine.h"
#include "Vmdk.h"

namespace pio {
VirtualMachine::VirtualMachine(VmHandle handle, VmID vm_id) : handle_(handle),
		vm_id_(vm_id) {
}

VirtualMachine::~VirtualMachine() = default;

const VmID& VirtualMachine::GetID() const noexcept {
	return vm_id_;
}

VmHandle VirtualMachine::GetHandle() const noexcept {
	return handle_;
}

RequestID VirtualMachine::NextRequestID() {
	return ++request_id_;
}

ActiveVmdk* VirtualMachine::FindVmdk(const VmdkID& vmdk_id) const {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	auto eit = vmdk_.list_.end();
	auto it = std::find_if(vmdk_.list_.begin(), eit,
		[vmdk_id] (const auto& vmdkp) {
			return vmdkp->GetID() == vmdk_id;
		});
	if (pio_unlikely(it == eit)) {
		return nullptr;
	}
	return *it;
}

ActiveVmdk* VirtualMachine::FindVmdk(VmdkHandle vmdk_handle) const {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	auto eit = vmdk_.list_.end();
	auto it = std::find_if(vmdk_.list_.begin(), eit,
		[vmdk_handle] (const auto& vmdkp) {
			return vmdkp->GetHandle() == vmdk_handle;
		});
	if (pio_unlikely(it == eit)) {
		return nullptr;
	}
	return *it;
}

void VirtualMachine::AddVmdk(ActiveVmdk* vmdkp) {
	log_assert(vmdkp);
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	vmdk_.list_.emplace_back(vmdkp);
}

folly::Future<CheckPointID> VirtualMachine::TakeCheckPoint() {
	CheckPointID c = ++checkpoint_.checkpoint_id_;

	std::vector<folly::Future<int>> futures;
	std::lock_guard lock(vmdk_.mutex_);
	for (const auto& vmdkp : vmdk_.list_) {
		auto fut = vmdkp->TakeCheckPoint(c - 1);
		futures.emplace_back(std::move(fut));
	}

	return folly::collectAll(std::move(futures))
	.then([&c] (const std::vector<folly::Try<int>>& results) {
		return c - 1;
	});
}

folly::Future<int> VirtualMachine::Write(ActiveVmdk* vmdkp,
		std::unique_ptr<Request> reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		++checkpoint_.writes_per_checkpoint_[checkpoint_.checkpoint_id_];
		return checkpoint_.checkpoint_id_;
	} ();

	stats_.writes_in_progress_.fetch_add(std::memory_order_relaxed);
	return vmdkp->Write(std::move(reqp), ckpt_id)
	.then([this, ckpt_id] (int rc) mutable {
		stats_.writes_in_progress_.fetch_sub(std::memory_order_relaxed);

		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		auto c = --checkpoint_.writes_per_checkpoint_[ckpt_id];
		if (not c && ckpt_id != checkpoint_.checkpoint_id_) {
			checkpoint_.writes_per_checkpoint_.erase(ckpt_id);
		}
		return rc;
	});
}

folly::Future<int> VirtualMachine::Read(ActiveVmdk* vmdkp,
		std::unique_ptr<Request> reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	++stats_.reads_in_progress_;
	return vmdkp->Read(std::move(reqp))
	.then([this] (int rc) {
		--stats_.reads_in_progress_;
		return rc;
	});
}

folly::Future<int> VirtualMachine::WriteSame(ActiveVmdk* vmdkp,
		std::unique_ptr<Request> reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		++checkpoint_.writes_per_checkpoint_[checkpoint_.checkpoint_id_];
		return checkpoint_.checkpoint_id_;
	} ();

	++stats_.writes_in_progress_;
	return vmdkp->WriteSame(std::move(reqp), ckpt_id)
	.then([this, ckpt_id] (int rc) mutable {
		--stats_.writes_in_progress_;

		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		auto c = --checkpoint_.writes_per_checkpoint_[ckpt_id];
		if (not c && ckpt_id != checkpoint_.checkpoint_id_) {
			checkpoint_.writes_per_checkpoint_.erase(ckpt_id);
		}
		return rc;
	});
}

uint32_t VirtualMachine::GetRequestResult(ActiveVmdk* vmdkp,
		RequestResult* resultsp, uint32_t nresults, bool *has_morep) const {
	log_assert(vmdkp && resultsp && has_morep);

	*has_morep = false;
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	return vmdkp->GetRequestResult(resultsp, nresults, has_morep);
}

}