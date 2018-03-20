#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include <mutex>

#include <cstdint>

#include "IDs.h"
#include "DaemonCommon.h"

using namespace ::hyc_thrift;


namespace pio {
/* forward declaration for Pimpl */
namespace config {
	class VmConfig;
}


class VirtualMachine {
public:
	VirtualMachine(VmdkHandle handle, VmID vm_id, const std::string& config);
	~VirtualMachine();

	void AddVmdk(ActiveVmdk* vmdkp);
	RequestID NextRequestID();

	folly::Future<int> Write(ActiveVmdk* vmdkp, Request* reqp);
	folly::Future<int> WriteSame(ActiveVmdk* vmdkp, Request* reqp);
	folly::Future<int> Read(ActiveVmdk* vmdkp, Request* reqp);
	folly::Future<CheckPointID> TakeCheckPoint();

public:
	const VmID& GetID() const noexcept;
	VmdkHandle GetHandle() const noexcept;
	const config::VmConfig* GetJsonConfig() const noexcept;

private:
	ActiveVmdk* FindVmdk(const VmdkID& vmdk_id) const;
	ActiveVmdk* FindVmdk(VmdkHandle vmdk_handle) const;

private:
	VmdkHandle handle_;
	VmID vm_id_;
	std::atomic<RequestID> request_id_{0};
	std::unique_ptr<config::VmConfig> config_;

	struct {
		mutable std::mutex mutex_;
		std::atomic<CheckPointID> checkpoint_id_{kInvaluCheckPointID};
		std::unordered_map<CheckPointID, std::atomic<uint64_t>> writes_per_checkpoint_;
	} checkpoint_;

	struct {
		mutable std::mutex mutex_;
		std::vector<ActiveVmdk *> list_;
	} vmdk_;

	struct {
		std::atomic<uint64_t> writes_in_progress_{0};
		std::atomic<uint64_t> reads_in_progress_{0};
	} stats_;
};
}
