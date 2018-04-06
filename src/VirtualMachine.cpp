#include <algorithm>

#include <folly/futures/Future.h>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "VirtualMachine.h"
#include "Vmdk.h"
#include "VmConfig.h"


namespace pio {

VirtualMachine::VirtualMachine(VmHandle handle, VmID vm_id,
		const std::string& config) : handle_(handle), vm_id_(std::move(vm_id)),
		config_(std::make_unique<config::VmConfig>(config)) {
}

VirtualMachine::~VirtualMachine() = default;

const VmID& VirtualMachine::GetID() const noexcept {
	return vm_id_;
}

VmHandle VirtualMachine::GetHandle() const noexcept {
	return handle_;
}

const config::VmConfig* VirtualMachine::GetJsonConfig() const noexcept {
	return config_.get();
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
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (const auto& vmdkp : vmdk_.list_) {
		auto fut = vmdkp->TakeCheckPoint(c - 1);
		futures.emplace_back(std::move(fut));
	}

	return folly::collectAll(std::move(futures))
	.then([&c] (const std::vector<folly::Try<int>>& results) {
		return c - 1;
	});
}

folly::Future<int> VirtualMachine::Write(ActiveVmdk* vmdkp, Request* reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		++checkpoint_.writes_per_checkpoint_[checkpoint_.checkpoint_id_];
		return checkpoint_.checkpoint_id_;
	} ();

	stats_.writes_in_progress_.fetch_add(std::memory_order_relaxed);
	return vmdkp->Write(reqp, ckpt_id)
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

folly::Future<int> VirtualMachine::Read(ActiveVmdk* vmdkp, Request* reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	++stats_.reads_in_progress_;
	return vmdkp->Read(reqp)
	.then([this] (int rc) {
		--stats_.reads_in_progress_;
		return rc;
	});
}

folly::Future<int> VirtualMachine::WriteSame(ActiveVmdk* vmdkp, Request* reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		++checkpoint_.writes_per_checkpoint_[checkpoint_.checkpoint_id_];
		return checkpoint_.checkpoint_id_;
	} ();

	++stats_.writes_in_progress_;
	return vmdkp->WriteSame(reqp, ckpt_id)
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

}