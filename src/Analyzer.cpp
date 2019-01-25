#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp/protocol/TJSONProtocol.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "Vmdk.h"
#include "Analyzer.h"
#include "halib.h"
#include "CommonMacros.h"

using namespace ::ondisk;

namespace pio {
using namespace analyzer;

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
	std::lock_guard<std::mutex> lock(analyzer.vmdk_.mutex_);

	os << "{IOA Handle " << analyzer.vm_handle_
		<< " VMDKs = [ ";
	for (const auto& info : analyzer.vmdk_.list_) {
		const ActiveVmdk* vmdkp = info.vmdkp_;
		if (not vmdkp) {
			continue;
		}
		os << vmdkp->GetID() << ',' << info.handle_ << ' ';
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
}

Analyzer::~Analyzer() {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (auto& handle : vmdk_.list_) {
		::iostats_unregister_vmdk(handle.handle_);
	}
	::iostats_unregister_vm(vm_handle_);
	::iostats_finalize();
}

void Analyzer::SetVmUUID(const VmUUID& vm_uuid) {
	vm_uuid_ = vm_uuid;
	vm_handle_ = ::iostats_register_vm(vm_uuid_.c_str(), ioa_tag_);
}

::io_vmdk_handle_t Analyzer::RegisterVmdk(const ActiveVmdk* vmdkp) {
	auto h = ::iostats_register_vmdk(vm_handle_, vmdkp->GetUUID().c_str(),
		kDefaultIoaLevels);
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	vmdk_.list_.emplace_back(vmdkp, h);
	return h;
}

void Analyzer::UnregisterVmdk(const VmdkID& vmdkid) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (auto& info : vmdk_.list_) {
		auto vmdkp = info.vmdkp_;
		if (vmdkp and vmdkp->GetID() == vmdkid) {
			info.vmdkp_ = nullptr;
			break;
		}
	}
}

void Analyzer::SetTimerTicked() {
	++ticks_;
}

std::string Analyzer::GetStordStats(const analyzer::VmdkInfo& info,
		const int32_t service_index, const ::io_level_t level) {
	auto handle = info.handle_;
	const ActiveVmdk* vmdkp = info.vmdkp_;
	log_assert(vmdkp);

	prepare_stats(handle, level);
	auto jsonp = ::iostats_get_stats(handle);
	if (pio_unlikely(not jsonp)) {
		return std::string();
	}

	using ManagedJson = std::unique_ptr<json_t, void (*) (json_t*)>;
	auto managed_json = ManagedJson(jsonp, json_decref);

	IOAVmdkStats stats;
	stats.set_service_index(service_index);
	vmdkp->FillCacheStats(stats);

	using S2 = apache::thrift::SimpleJSONSerializer;
	auto string_stats = S2::serialize<std::string>(stats);

	auto rc = json_object_set_new(jsonp, "stord-stats",
			json_string(string_stats.c_str()));
	if (pio_unlikely(rc)) {
		return std::string();
	}

	auto strp = json_dumps(jsonp, 0);
	if (pio_unlikely(not strp)) {
		return std::string();
	}

	std::string res(strp);
	std::free(strp);
	return res;
}

std::optional<std::string> Analyzer::GetIOStats(const int32_t service_index) {
	if (ticks_ % l1_ticks_ != 0) {
		return {};
	}

	::io_level_t level = ::io_level_t::IOSTATS_LEVEL1;
	if (ticks_ % l2_ticks_ == 0) {
		level = level | ::io_level_t::IOSTATS_LEVEL2;
	}

	IOAVmStats stats;
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		int i = 0;
		for (auto& info : vmdk_.list_) {
			const ActiveVmdk* vmdkp = info.vmdkp_;
			if (pio_unlikely(vmdkp == nullptr)) {
				continue;
			}

			auto vmdk_stats = GetStordStats(info, service_index, level);
			if (pio_unlikely(vmdk_stats.empty())) {
				continue;
			}

			stats.data.emplace(std::to_string(i), std::move(vmdk_stats));
			++i;
		}

		if (pio_unlikely(i == 0)) {
			return {};
		}
	}

	using S2 = apache::thrift::SimpleJSONSerializer;
	return S2::serialize<std::string>(stats);
}

std::optional<std::string>
Analyzer::GetFingerPrintStats(const ::io_level_t level, VmdkInfo& info) {
	auto handle = info.handle_;

	io_fprintbuf_t stats;
	stats.size = 0;
	stats.alloced = false;
	stats.data = nullptr;

	prepare_stats(handle, level);

	auto s = ::iostats_get_fprintbuf(handle, &stats);
	if (pio_unlikely(not s or not stats.data)) {
		return {};
	}

	std::string res(stats.data, stats.size);
	::iostats_free_fprintbuf(&stats);
	return res;
}

std::optional<std::string> Analyzer::GetFingerPrintStats() {
	if (ticks_ % l3_ticks_ != 0) {
		return {};
	}

	::io_level_t level = ::io_level_t::IOSTATS_LEVEL3;

	IOAVmFPrintStats stats;
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		int i = 0;
		for (auto& info : vmdk_.list_) {
			const ActiveVmdk* vmdkp = info.vmdkp_;
			if (pio_unlikely(not vmdkp)) {
				continue;
			}

			auto s = GetFingerPrintStats(level, info);
			if (pio_unlikely(not s)) {
				continue;
			}
			stats.data.emplace(std::to_string(i), std::move(s.value()));
			++i;
		}

		if (pio_unlikely(i == 0)) {
			return {};
		}
	}

	using S2 = apache::thrift::SimpleJSONSerializer;
	return S2::serialize<std::string>(stats);
}

bool Analyzer::Read(::io_vmdk_handle_t handle, int64_t latency, Offset offset,
		size_t nsectors, uint32_t queue_depth) {
#ifndef NDEBUG
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		auto it = std::find_if(vmdk_.list_.begin(), vmdk_.list_.end(),
				[&handle] (const auto& x) {
			return handle == x.handle_;
		});
		log_assert(it != vmdk_.list_.end());
	}
#endif
	return ::iostats_add_record(handle, ::io_op_t::IOSTATS_READ, latency,
		offset, nsectors, queue_depth);
}

bool Analyzer::Write(::io_vmdk_handle_t handle, int64_t latency, Offset offset,
		size_t nsectors, uint32_t queue_depth) {
#ifndef NDEBUG
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		auto it = std::find_if(vmdk_.list_.begin(), vmdk_.list_.end(),
				[&handle] (const auto& x) {
			return handle == x.handle_;
		});
		log_assert(it != vmdk_.list_.end());
	}
#endif
	return ::iostats_add_record(handle, ::io_op_t::IOSTATS_WRITE, latency,
		offset, nsectors, queue_depth);
}
}
