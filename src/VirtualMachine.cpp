#include <algorithm>

#include <folly/futures/Future.h>

#include "VirtualMachine.h"
#include "Vmdk.h"

namespace pio {
VirtualMachine::VirtualMachine(VmID vm_id) : vm_id_(vm_id) {
}

VirtualMachine::~VirtualMachine() = default;

ActiveVmdk* VirtualMachine::FindVmdk(const VmdkID& vmdk_id) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (const auto& vmdkp : vmdk_.list_) {
		if (vmdkp->GetVmdkID() == vmdk_id) {
			return vmdkp.get();
		}
	}
	return nullptr;
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

folly::Future<int> VirtualMachine::Write(const VmdkID& vmdk_id, RequestID req_id,
		void *bufferp, size_t buf_size, Offset offset) {
	auto vmdkp = FindVmdk(vmdk_id);
	if (pio_unlikely(not vmdkp)) {
		throw std::invalid_argument("Unknown VmdkID");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		++checkpoint_.writes_per_checkpoint_[checkpoint_.checkpoint_id_];
		return checkpoint_.checkpoint_id_;
	} ();

	++stats_.writes_in_progress_;
	return vmdkp->Write(req_id, ckpt_id, bufferp, buf_size, offset)
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

folly::Future<int> VirtualMachine::Read(const VmdkID& vmdk_id, RequestID req_id,
		void *bufp, size_t buf_size, Offset offset) {
	auto vmdkp = FindVmdk(vmdk_id);
	if (pio_unlikely(not vmdkp)) {
		throw std::invalid_argument("Unknown VmdkID");
	}

	++stats_.reads_in_progress_;
	return vmdkp->Read(req_id, bufp, buf_size, offset)
	.then([this] (int rc) {
		--stats_.reads_in_progress_;
		return rc;
	});
}

folly::Future<int> VirtualMachine::WriteSame(const VmdkID& vmdk_id,
		RequestID req_id, void *bufp, size_t buf_size, size_t transfer_size,
		Offset offset) {
	auto vmdkp = FindVmdk(vmdk_id);
	if (pio_unlikely(not vmdkp)) {
		throw std::invalid_argument("Unknown VmdkID");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		++checkpoint_.writes_per_checkpoint_[checkpoint_.checkpoint_id_];
		return checkpoint_.checkpoint_id_;
	} ();

	++stats_.writes_in_progress_;
	return vmdkp->WriteSame(req_id, ckpt_id, bufp, buf_size, transfer_size, offset)
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