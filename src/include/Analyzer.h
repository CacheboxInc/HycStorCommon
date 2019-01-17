#pragma once

#include <string>
#include <optional>
#include <vector>
#include <chrono>
#include <utility>

#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "iostats.h"
#include "RecurringTimer.h"

struct _ha_instance;

namespace folly {
class EventBase;
}

namespace pio {
class Analyzer {
public:
	Analyzer(const ::ondisk::VmID& vm_id, uint32_t l1_ticks, uint32_t l2_ticks,
		uint32_t l3_ticks);
	~Analyzer();
	::io_vmdk_handle_t RegisterVmdk(const ::ondisk::VmdkID& vmdkid);
	void StartTimer(_ha_instance *instancep, folly::EventBase* basep);
	bool Read(::io_vmdk_handle_t handle, int64_t latency, Offset offset,
		size_t nsectors, uint32_t queue_depth);
	bool Write(::io_vmdk_handle_t handle, int64_t latency, Offset offset,
		size_t nsectors, uint32_t queue_depth);
	void SetTimerTicked();
	std::optional<std::string> GetIOStats();
	std::optional<std::string> GetFingerPrintStats();

	friend std::ostream& operator << (std::ostream& os, const Analyzer& analyzer);
private:
	int PostRest(_ha_instance* instancep, std::string&& ep, std::string&& body);
	std::string GetVmdkIOStat(const ::io_vmdk_handle_t handle,
		const io_op_t op, const io_level_t level);
	std::optional<::ondisk::IOAVmdkStats> GetIOStats(const ::ondisk::VmdkID& id,
		const ::io_vmdk_handle_t& handle, const io_level_t level);
	std::string RemoveDataField(std::string&& body);
	std::string GetVmdkFingerPrint(const ::io_vmdk_handle_t& handle,
		const io_op_t op, const io_level_t level);
	std::optional<::ondisk::IOAVmdkFingerPrint> GetFingerPrintStats(
		const ::ondisk::VmdkID& vmdk_id,
		const ::io_vmdk_handle_t& handle, const io_level_t level);
private:
	const ::ondisk::VmID& vm_id_;

	struct {
		uint64_t ticks_{0};
		const uint32_t l1_ticks_{};
		const uint32_t l2_ticks_{};
		const uint32_t l3_ticks_{};
	};

	int64_t ioa_tag_{};
	::io_vm_handle_t vm_handle_{};

	std::vector<std::pair<::ondisk::VmdkID, ::io_vmdk_handle_t>> vmdks_;
};
}
