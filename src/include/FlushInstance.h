#pragma once

#include <cstdint>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

#include "IDs.h"
#include "DaemonCommon.h"
#include <algorithm>
#include <chrono>
#include "gen-cpp2/MetaData_types.h"
#include "FlushConfig.h"

namespace pio {

class FlushInstance {
public:
	FlushInstance(::ondisk::VmID vmid_, const std::string& config);
	~FlushInstance();

	uint64_t flushed_blocks_{0};
	uint64_t moved_blocks_{0};
	int StartFlush(const ::ondisk::VmID& vmid);
	int FlushStatus(const ::ondisk::VmID& vmid, FlushStats &flush_stat);
	uint64_t ElapsedTime() {
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
					(std::chrono::steady_clock::now() - start_time_);
		return duration.count();
	}
	config::FlushConfig* GetJsonConfig() const noexcept;
private:
	::ondisk::VmID vmid_;
	std::chrono::steady_clock::time_point start_time_;
	std::unique_ptr<config::FlushConfig> config_;
};
}
