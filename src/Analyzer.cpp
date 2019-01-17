#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp/protocol/TJSONProtocol.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "Analyzer.h"
#include "halib.h"
#include "CommonMacros.h"

using namespace ::ondisk;

namespace pio {
template <typename T, typename = int>
struct HasData : std::false_type {
};

template <typename T>
struct HasData<T, decltype((void) T::data, 0)> : std::true_type {
};

inline constexpr ::io_level_t operator | (::io_level_t a, ::io_level_t b) {
	return static_cast<::io_level_t>(static_cast<uint32_t>(a) |
		static_cast<uint32_t>(b));
}

constexpr static auto kDefaultIoaLevels = ::io_level_t::IOSTATS_LEVEL1 |
	::io_level_t::IOSTATS_LEVEL2 | ::io_level_t::IOSTATS_LEVEL3;

std::ostream& operator << (std::ostream& os, const Analyzer& analyzer) {
	os << "{IOA Handle " << analyzer.vm_handle_
		<< " VMDKs = [ ";
	for (const auto& handle : analyzer.vmdks_) {
		os << handle.first << ',' << handle.second << ' ';
	}
	os << "]}";
	return os;
}

Analyzer::Analyzer(const VmID& vm_id, uint32_t l1_ticks, uint32_t l2_ticks,
		uint32_t l3_ticks) : vm_id_(vm_id), l1_ticks_(l1_ticks),
		l2_ticks_(l2_ticks), l3_ticks_(l3_ticks) {
	io_params_t params;
	params.mrc_window = 12;
	params.trace_chunk_len = 720;
	params.max_trace_chunks = 10;
	::iostats_init(&params);

	auto tp = std::chrono::system_clock::now();
	ioa_tag_ =  tp.time_since_epoch().count();
	vm_handle_ = ::iostats_register_vm(vm_id_.c_str(), ioa_tag_);
}

Analyzer::~Analyzer() {
	for (auto& handle : vmdks_) {
		::iostats_unregister_vmdk(handle.second);
	}
	::iostats_unregister_vm(vm_handle_);
	::iostats_finalize();
}

::io_vmdk_handle_t Analyzer::RegisterVmdk(const VmdkID& vmdkid) {
	auto h = ::iostats_register_vmdk(vm_handle_, vmdkid.c_str(), kDefaultIoaLevels);
	vmdks_.emplace_back(vmdkid, h);
	return h;
}

void Analyzer::SetTimerTicked() {
	++ticks_;
}

std::string Analyzer::GetVmdkIOStat(const ::io_vmdk_handle_t handle,
		const io_op_t op, const io_level_t level) {
	io_statsbuf_t stats;
	stats.size = 0;
	stats.alloced = false;
	stats.data = nullptr;

	::iostats_snap(handle, op, level);
	bool s = ::iostats_get_statsbuf(handle, op, nullptr, &stats);
	if (pio_unlikely(not s)) {
		return std::string();
	}

	log_assert(stats.data);
	std::string res(stats.data, stats.size);
	::iostats_free_statsbuf(&stats);
	return res;
}

std::optional<IOAVmdkStats>
Analyzer::GetIOStats(const VmdkID& id, const ::io_vmdk_handle_t& handle,
		const ::io_level_t level) {
	IOAVmdkStats stat;
	stat.set_vmdk_id(id);
	stat.set_vm_id(vm_id_);
	stat.set_tag(ioa_tag_);

	stat.set_read_iostats(GetVmdkIOStat(handle, IOSTATS_READ, level));
	if (pio_unlikely(stat.read_iostats.empty())) {
		return {};
	}

	stat.set_write_iostats(GetVmdkIOStat(handle, IOSTATS_WRITE, level));
	if (pio_unlikely(stat.write_iostats.empty())) {
		return {};
	}
	return {std::move(stat)};
}

std::string Analyzer::RemoveDataField(std::string&& body) {
	static_assert(HasData<IOAVmStats>::value,
		"IOAVmStats must have data field");
	static_assert(HasData<IOAVmFPrintStats>::value,
		"IOAVmFPrintStats must have data field");

	json_error_t  error;

	auto jdata = json_loads(body.c_str(), JSON_DISABLE_EOF_CHECK, &error);
	log_assert(jdata);

	auto r_data = json_object_get(jdata, "data");
	log_assert(r_data);

	auto data_body = json_dumps(r_data, 0);
	log_assert(data_body);

	std::string res(data_body);
	std::free(data_body);
	json_decref(jdata);
	return res;
}

std::optional<std::string> Analyzer::GetIOStats() {
	if (ticks_ % l1_ticks_ != 0) {
		return {};
	}

	::io_level_t level = ::io_level_t::IOSTATS_LEVEL1;
	if (ticks_ % l2_ticks_ == 0) {
		level = level | ::io_level_t::IOSTATS_LEVEL2;
	}

	IOAVmStats stats;
	int i = 0;
	for (auto& vmdk : vmdks_) {
		auto s = GetIOStats(vmdk.first, vmdk.second, level);
		if (pio_likely(s)) {
			stats.data.emplace(std::to_string(i), std::move(s.value()));
		}
		++i;
	}

	using S2 = apache::thrift::SimpleJSONSerializer;
	auto json_body = S2::serialize<std::string>(stats);
	if (json_body.empty()) {
		return {};
	}
	return RemoveDataField(std::move(json_body));
}

std::string Analyzer::GetVmdkFingerPrint(const ::io_vmdk_handle_t& handle,
		const io_op_t op, const io_level_t level) {
	io_fprintbuf_t stats;
	stats.size = 0;
	stats.alloced = false;
	stats.data = nullptr;

	::iostats_snap(handle, op, level);
	bool s = ::iostats_get_fprintbuf(handle, op, nullptr, &stats);
	if (not s) {
		return std::string();
	}

	log_assert(stats.data);
	std::string res(stats.data, stats.size);
	::iostats_free_fprintbuf(&stats);
	return res;
}

std::optional<IOAVmdkFingerPrint> Analyzer::GetFingerPrintStats(
		const VmdkID& vmdk_id, const ::io_vmdk_handle_t& handle,
		const ::io_level_t level) {
	IOAVmdkFingerPrint stat;
	stat.set_vmdk_id(vmdk_id);
	stat.set_vm_id(vm_id_);
	stat.set_tag(ioa_tag_);

	stat.set_read_fprints(GetVmdkFingerPrint(handle, IOSTATS_READ, level));
	if (pio_unlikely(stat.read_fprints.empty())) {
		return {};
	}

	stat.set_write_fprints(GetVmdkFingerPrint(handle, IOSTATS_WRITE, level));
	if (pio_unlikely(stat.read_fprints.empty())) {
		return {};
	}
	return {std::move(stat)};
}

std::optional<std::string> Analyzer::GetFingerPrintStats() {
	if (ticks_ % l3_ticks_ != 0) {
		return {};
	}

	::io_level_t level = ::io_level_t::IOSTATS_LEVEL3;
	if (ticks_ % l1_ticks_ == 0) {
		level = level | ::io_level_t::IOSTATS_LEVEL1;
	}
	if (ticks_ % l2_ticks_ == 0) {
		level = level | ::io_level_t::IOSTATS_LEVEL2;
	}

	IOAVmFPrintStats stats;
	int i = 0;
	for (auto& vmdk : vmdks_) {
		auto s = GetFingerPrintStats(vmdk.first, vmdk.second, level);
		if (pio_likely(s)) {
			stats.data.emplace(std::to_string(i), std::move(s.value()));
		}
		++i;
	}

	using S2 = apache::thrift::SimpleJSONSerializer;
	auto json_body = S2::serialize<std::string>(stats);
	if (json_body.empty()) {
		return {};
	}
	return RemoveDataField(std::move(json_body));
}

bool Analyzer::Read(::io_vmdk_handle_t handle, int64_t latency, Offset offset,
		size_t nsectors, uint32_t queue_depth) {
#ifndef NDEBUG
	auto it = std::find_if(vmdks_.begin(), vmdks_.end(),
			[&handle] (const auto& x) {
		return handle == x.second;
	});
	log_assert(it != vmdks_.end());
#endif
	return ::iostats_add_record(handle, ::io_op_t::IOSTATS_READ, latency,
		offset, nsectors, queue_depth);
}

bool Analyzer::Write(::io_vmdk_handle_t handle, int64_t latency, Offset offset,
		size_t nsectors, uint32_t queue_depth) {
#ifndef NDEBUG
	auto it = std::find_if(vmdks_.begin(), vmdks_.end(),
			[&handle] (const auto& x) {
		return handle == x.second;
	});
	log_assert(it != vmdks_.end());
#endif
	return ::iostats_add_record(handle, ::io_op_t::IOSTATS_WRITE, latency,
		offset, nsectors, queue_depth);
}
}
