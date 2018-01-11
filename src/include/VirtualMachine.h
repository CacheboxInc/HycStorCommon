#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include <mutex>

#include <cstdint>

#include "IDs.h"
#include "Common.h"

namespace pio {
class VirtualMachine {
public:
	VirtualMachine(VmID vm_id);
	~VirtualMachine();

	ActiveVmdk* FindVmdk(const VmdkID& vmdk_id);

	folly::Future<int> Write(const VmdkID& vmdk_id, RequestID req_id,
		void *bufferp, size_t buf_size, Offset offset);
	folly::Future<int> WriteSame(const VmdkID& vmdk_id, RequestID req_id,
		void *bufp, size_t buf_size, size_t transfer_size, Offset offset);
	folly::Future<int> Read(const VmdkID& vmdk_id, RequestID req_id, void *bufp,
		size_t buf_size, Offset offset);
	folly::Future<CheckPointID> TakeCheckPoint();
private:

private:
	VmID vm_id_;
	struct {
		std::mutex mutex_;
		std::atomic<CheckPointID> checkpoint_id_{kInvaluCheckPointID};
		std::unordered_map<CheckPointID, std::atomic<uint64_t>> writes_per_checkpoint_;
	} checkpoint_;

	struct {
		std::mutex mutex_;
		std::vector<std::unique_ptr<ActiveVmdk>> list_;
	} vmdk_;

	struct {
		std::atomic<uint64_t> writes_in_progress_{0};
		std::atomic<uint64_t> reads_in_progress_{0};
	} stats_;
};
}
