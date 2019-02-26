#pragma once

#include <thread>
#include <chrono>

#include <aerospike/aerospike.h>
#include <aerospike/as_monitor.h>

#include <folly/io/async/EventBase.h>

#include "AeroConfig.h"
#include "RecurringTimer.h"
#include "RequestHandler.h"

struct as_event_loop;

namespace pio {

class WorkScheduler;

class AeroSpikeConn{
public:
	using ConfigTag = uint64_t;
	AeroSpikeConn(AeroClusterID cluster_id, const std::string& config);
	~AeroSpikeConn();
	int Connect();
	int Disconnect();
	config::AeroConfig* GetJsonConfig() const noexcept;

	void HandleServerOverload(ConfigTag tag);
	void UnthrottleClient();
	void GetEventLoopAndTag(as_event_loop** looppp, ConfigTag* tagp) const;
	WorkScheduler* GetWorkScheduler() noexcept;
private:
	void SetEventLoopDepth(const uint32_t depth) const noexcept;
private:
	AeroClusterID cluster_id_;
	RecurringTimer reconfig_timer_;
	std::unique_ptr<WorkScheduler> work_scheduler_;

	struct {
		mutable std::mutex mutex_;
		uint16_t io_depth_{1};
		uint16_t prev_unthrottle_{1};
		uint64_t gen_count_{1};
	} reconf_;

	std::unique_ptr<config::AeroConfig> config_;
public:
	aerospike  as_;
	as_monitor as_mon_;
	bool       as_started_{false};
};
}
