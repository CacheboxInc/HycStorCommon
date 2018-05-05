#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include <mutex>

#include <cstdint>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

#include "IDs.h"
#include "DaemonCommon.h"

using namespace ::hyc_thrift;

namespace pio {
/* forward declaration for Pimpl */
namespace config {
	class VmConfig;
}

struct Stun {
	Stun();
	folly::Future<int> GetFuture();
	void SetPromise(int result);
	folly::Promise<int> promise;
	folly::FutureSplitter<int> futures;
};

using CheckPointResult = std::pair<CheckPointID, int>;

class VirtualMachine {
public:
	VirtualMachine(VmdkHandle handle, VmID vm_id, const std::string& config);
	~VirtualMachine();

	void AddVmdk(ActiveVmdk* vmdkp);
	RequestID NextRequestID();

	folly::Future<int> Write(ActiveVmdk* vmdkp, Request* reqp);
	folly::Future<int> WriteSame(ActiveVmdk* vmdkp, Request* reqp);
	folly::Future<int> Read(ActiveVmdk* vmdkp, Request* reqp);
	folly::Future<int> Flush(ActiveVmdk* vmdkp, Request* reqp, const CheckPoints& min_max);
	folly::Future<CheckPointResult> TakeCheckPoint();
	int FlushStart(CheckPointID ckpt_id);
	folly::Future<int> Stun(CheckPointID ckpt_id);

public:
	const VmID& GetID() const noexcept;
	VmdkHandle GetHandle() const noexcept;
	const config::VmConfig* GetJsonConfig() const noexcept;

private:
	ActiveVmdk* FindVmdk(const VmdkID& vmdk_id) const;
	ActiveVmdk* FindVmdk(VmdkHandle vmdk_handle) const;
	void WriteComplete(CheckPointID ckpt_id);
	void CheckPointComplete(CheckPointID ckpt_id);
	void FlushComplete(CheckPointID ckpt_id);

private:
	VmdkHandle handle_;
	VmID vm_id_;
	std::atomic<RequestID> request_id_{0};
	std::unique_ptr<config::VmConfig> config_;

	struct {
		std::atomic_flag in_progress_ = ATOMIC_FLAG_INIT;
		std::atomic<CheckPointID> checkpoint_id_{kInvalidCheckPointID+1};

		mutable std::mutex mutex_;
		std::unordered_map<CheckPointID, std::atomic<uint64_t>> writes_per_checkpoint_;
		std::unordered_map<CheckPointID, std::unique_ptr<struct Stun>> stuns_;
	} checkpoint_;

	struct {
		mutable std::mutex mutex_;
		std::vector<ActiveVmdk *> list_;
	} vmdk_;

	struct {
		std::atomic<uint64_t> writes_in_progress_{0};
		std::atomic<uint64_t> reads_in_progress_{0};
		std::atomic<uint64_t> flushs_in_progress_{0};
	} stats_;

	std::atomic_flag flush_in_progress_ = ATOMIC_FLAG_INIT;
};
}
