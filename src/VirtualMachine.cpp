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

void VirtualMachine::CheckPointComplete(CheckPointID ckpt_id) {
	log_assert(ckpt_id != kInvalidCheckPointID);
	checkpoint_.in_progress_.clear();
}

folly::Future<CheckPointResult> VirtualMachine::TakeCheckPoint() {
	if (checkpoint_.in_progress_.test_and_set()) {
		return std::make_pair(kInvalidCheckPointID, -EAGAIN);
	}

	CheckPointID ckpt_id = (++checkpoint_.checkpoint_id_) - 1;
	return Stun(ckpt_id)
	.then([this, ckpt_id] (int rc) mutable -> folly::Future<CheckPointResult> {
		if (pio_unlikely(rc < 0)) {
			return std::make_pair(kInvalidCheckPointID, rc);
		}

		std::vector<folly::Future<int>> futures;
		futures.reserve(vmdk_.list_.size());
		{
			std::lock_guard<std::mutex> lock(vmdk_.mutex_);
			for (const auto& vmdkp : vmdk_.list_) {
				auto fut = vmdkp->TakeCheckPoint(ckpt_id);
				futures.emplace_back(std::move(fut));
			}
		}

		return folly::collectAll(std::move(futures))
		.then([this, ckpt_id] (const std::vector<folly::Try<int>>& results)
				-> folly::Future<CheckPointResult> {
			for (auto& t : results) {
				if (pio_likely(t.hasValue() and t.value() == 0)) {
					continue;
				} else {
					return std::make_pair(kInvalidCheckPointID, t.value());
				}
			}
			CheckPointComplete(ckpt_id);
			return std::make_pair(ckpt_id, 0);;
		});
	});
}

folly::Future<int> VirtualMachine::Write(ActiveVmdk* vmdkp, Request* reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		auto ckpt_id = checkpoint_.checkpoint_id_.load();
		++checkpoint_.writes_per_checkpoint_[ckpt_id];
		return ckpt_id;
	} ();

	++stats_.writes_in_progress_;
	return vmdkp->Write(reqp, ckpt_id)
	.then([this, ckpt_id] (int rc) mutable {
		WriteComplete(ckpt_id);
		return rc;
	});
}

folly::Future<int> VirtualMachine::Read(ActiveVmdk* vmdkp, Request* reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpts = std::make_pair(kInvalidCheckPointID + 1,
		checkpoint_.checkpoint_id_.load());
	++stats_.reads_in_progress_;
	return vmdkp->Read(reqp, ckpts)
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
		auto ckpt_id = checkpoint_.checkpoint_id_.load();
		++checkpoint_.writes_per_checkpoint_[ckpt_id];
		return ckpt_id;
	} ();

	++stats_.writes_in_progress_;
	return vmdkp->WriteSame(reqp, ckpt_id)
	.then([this, ckpt_id] (int rc) mutable {
		WriteComplete(ckpt_id);
		return rc;
	});
}

void VirtualMachine::WriteComplete(CheckPointID ckpt_id) {
	--stats_.writes_in_progress_;

	std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
	log_assert(checkpoint_.writes_per_checkpoint_[ckpt_id] > 0);
	auto c = --checkpoint_.writes_per_checkpoint_[ckpt_id];
	if (pio_unlikely(not c and ckpt_id != checkpoint_.checkpoint_id_)) {
		log_assert(ckpt_id < checkpoint_.checkpoint_id_);
		auto it = checkpoint_.stuns_.find(ckpt_id);
		if (pio_unlikely(it == checkpoint_.stuns_.end())) {
			return;
		}
		auto stun = std::move(it->second);
		checkpoint_.stuns_.erase(it);
		stun->SetPromise(0);
	}
}

folly::Future<int> VirtualMachine::Stun(CheckPointID ckpt_id) {
	std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
	if (auto it = checkpoint_.writes_per_checkpoint_.find(ckpt_id);
			it == checkpoint_.writes_per_checkpoint_.end() ||
			it->second == 0) {
		return 0;
	} else {
		log_assert(it->second > 0);
	}

	if (auto it = checkpoint_.stuns_.find(ckpt_id);
			pio_likely(it == checkpoint_.stuns_.end())) {
		auto stun = std::make_unique<struct Stun>();
		auto fut = stun->GetFuture();
		checkpoint_.stuns_.emplace(ckpt_id, std::move(stun));
		return fut;
	} else {
		return it->second->GetFuture();
	}
}

Stun::Stun() : promise(), futures(promise.getFuture()) {
}

folly::Future<int> Stun::GetFuture() {
	return futures.getFuture();
}

void Stun::SetPromise(int result) {
	promise.setValue(result);
}

}