#pragma once

#include <string>
#include <optional>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "iostats.h"

namespace folly {
class EventBase;
}

namespace pio {
class ActiveVmdk;

namespace analyzer {
struct VmdkInfo {
	VmdkInfo(const ActiveVmdk* vmdkp, ::io_vm_handle_t handle) :
		vmdkp_(vmdkp), handle_(handle) {
	}
	const ActiveVmdk* vmdkp_{};
	::io_vmdk_handle_t handle_;
};
}

class Analyzer {
public:
	Analyzer(const ::ondisk::VmID& vm_id, uint32_t l1_ticks,
		uint32_t l2_ticks, uint32_t l3_ticks);
	~Analyzer();
	void SetVmUUID(const ::ondisk::VmUUID& vm_uuid);
	::io_vmdk_handle_t RegisterVmdk(const ActiveVmdk* vmdkp);
	void UnregisterVmdk(const ::ondisk::VmdkID& vmdkid);
	bool Read(::io_vmdk_handle_t handle, int64_t latency, Offset offset,
		size_t nsectors, uint32_t queue_depth);
	bool Write(::io_vmdk_handle_t handle, int64_t latency, Offset offset,
		size_t nsectors, uint32_t queue_depth);
	void SetTimerTicked();
	std::optional<std::string> GetIOStats(const int32_t service_index);
	std::optional<std::string> GetFingerPrintStats();

	friend std::ostream& operator << (std::ostream& os, const Analyzer& analyzer);
private:
	std::string GetVmdkIOStat(const ::io_vmdk_handle_t handle,
		const io_op_t op, const io_level_t level);
	std::optional<std::string> GetIOStats(const ::io_level_t level,
		const analyzer::VmdkInfo& info);
	std::string GetStordStats(const analyzer::VmdkInfo& info,
		const int32_t service_index, const ::io_level_t level);
	std::optional<std::string> GetFingerPrintStats(const ::io_level_t level,
		analyzer::VmdkInfo& info);
private:
	const ::ondisk::VmID& vm_id_;
	::ondisk::VmUUID vm_uuid_;

	struct {
		uint64_t ticks_{0};
		const uint32_t l1_ticks_{};
		const uint32_t l2_ticks_{};
		const uint32_t l3_ticks_{};
	};

	int64_t ioa_tag_{};
	::io_vm_handle_t vm_handle_{};

	struct {
		mutable std::mutex mutex_;
		std::vector<analyzer::VmdkInfo> list_;
	} vmdk_;
};
}
