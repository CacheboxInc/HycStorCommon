#include <cerrno>
#include <iostream>
#include <iterator>
#include "AeroConn.h"
#include <folly/futures/Future.h>
#include "string.h"
#include <stdlib.h>
#include <algorithm>

namespace pio {

AeroSpikeConn::AeroSpikeConn(AeroClusterID cluster_id, const std::string& config): 
        cluster_id_(cluster_id), 
        config_(std::make_unique<config::AeroConfig>(config)) {}

AeroSpikeConn::~AeroSpikeConn() {
	this->Disconnect();
}

config::AeroConfig* AeroSpikeConn::GetJsonConfig() const noexcept {
	return config_.get();
}

int GetFileLimit()
{
	struct rlimit limit;
	if (getrlimit(RLIMIT_NOFILE, &limit) != 0) {
		LOG(ERROR) << "getrlimit() failed with errno:" << errno;
		return -1;
	}
	return limit.rlim_cur;
}

int AeroSpikeConn::Connect() {
	static constexpr uint16_t kAeroEventLoops = 4;
	static constexpr uint16_t kMaxCommandsInProgress = 800 / kAeroEventLoops;
	log_assert(as_started_ == false);

	auto aeroconf = this->GetJsonConfig();
	std::string clusterips = aeroconf->GetAeroIPs();
	int host_count;
	if (pio_likely(clusterips.size())) {
		LOG(ERROR) << "AeroSpike host IPs::" << clusterips;
		host_count = std::count(clusterips.begin(), clusterips.end(), ',') + 1;
	} else { 
		LOG(ERROR) << "Unable to get Aerospike host IPs";
		return -EINVAL;
	}

	uint32_t port;
	auto ret = aeroconf->GetAeroPort(port);
	if (pio_likely(ret)) {
		LOG(ERROR) << "AeroSpike Port::" << port;
	} else {
		LOG(ERROR) << "Unable to get Aerospike port Number";
		return -EINVAL;
	}

	as_error err;
	as_policy_event policy;
	as_policy_event_init(&policy);
	policy.max_commands_in_process = kMaxCommandsInProgress;
	auto status = as_create_event_loops(&err, &policy, kAeroEventLoops, nullptr);
	if (pio_unlikely(status != AEROSPIKE_OK)) {
		LOG (ERROR) << "Aerospike event loop creation failed.";
		return -EINVAL;
	}

	as_config cfg;
	as_config_init(&cfg);
	if (pio_unlikely(host_count  == 1)) {
		as_config_add_host(&cfg, clusterips.c_str(), port);
	} else {
		auto status = as_config_add_hosts(&cfg, clusterips.c_str(), port);
		if (pio_unlikely(status == false)) {
			LOG(ERROR) << "Error in adding host(s) " << clusterips
				<< "in client config.";
			return -EINVAL;
		}
	}

	/*
	 * If the current soft limit is high enough then
	 * increase the number of outstanding commands
	 */

	if (pio_likely(GetFileLimit() >= 16 * 1024)) {
		cfg.max_conns_per_node = 3000;
		cfg.async_max_conns_per_node = 3000;
		cfg.pipe_max_conns_per_node = 3000;
	} else {
		cfg.async_max_conns_per_node = 400;
	}

	LOG(ERROR) << __func__ << "Max async commands limit:"
		<< cfg.async_max_conns_per_node;
	/* Setting this policy so that key get stored in records */
	cfg.policies.write.key = AS_POLICY_KEY_SEND;
	cfg.policies.read.key = AS_POLICY_KEY_SEND;
	cfg.policies.operate.key = AS_POLICY_KEY_SEND;
	cfg.policies.remove.key = AS_POLICY_KEY_SEND;
	cfg.policies.apply.key = AS_POLICY_KEY_SEND;

	/* TODO: See if we need to set for all types of policies */
	/* Timeout setting */
	cfg.policies.write.base.total_timeout = 5000;// 5secs
	cfg.policies.write.base.max_retries = 0;
	cfg.policies.write.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.read.base.total_timeout = 5000; // 5secs
	cfg.policies.read.base.max_retries = 10;
	cfg.policies.read.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.operate.base.total_timeout = 5000; // 5secs
	cfg.policies.operate.base.max_retries = 10;
	cfg.policies.operate.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.remove.base.total_timeout = 5000; // 5secs
	cfg.policies.remove.base.max_retries = 0;
	cfg.policies.remove.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.batch.base.total_timeout = 5000; // 5secs
	cfg.policies.batch.base.max_retries = 10;
	cfg.policies.batch.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.apply.base.total_timeout = 5000; // 5secs
	cfg.policies.apply.base.max_retries = 0;
	cfg.policies.apply.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.scan.base.total_timeout = 5000; // 5secs
	cfg.policies.scan.base.max_retries = 0;
	cfg.policies.scan.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.query.base.total_timeout = 5000; // 5secs
	cfg.policies.query.base.max_retries = 0;
	cfg.policies.query.base.sleep_between_retries = 300; // 300ms 

	aerospike_init(&this->as_, &cfg);
	if (aerospike_connect(&this->as_, &err) != AEROSPIKE_OK) {
		VLOG(1) << "Connection with aerospike server failed.";
		aerospike_destroy(&this->as_);
		as_event_close_loops();
		return 1;
	}

	as_monitor_init(&this->as_mon_);
	this->as_started_ = true;
	VLOG(1) << "Connected to Aerospike Server succesfully";
	return 0;
}

int AeroSpikeConn::Disconnect() {

	as_error err;
	VLOG(1) << "In aerospike disconnect";
	if (this->as_started_ == false) {
		VLOG(1)<<"Not connected with aerospike server";
		return 0;
	}

	VLOG(1)<<"Calling as_monitor_destroy";
	as_monitor_destroy(&this->as_mon_);
	VLOG(1)<<"Calling aerospike_close";
	aerospike_close(&this->as_, &err);
	VLOG(1)<<"Calling aerospike_destroy";
	aerospike_destroy(&this->as_);
	VLOG(1)<<"Calling as_event_close_loops";
	as_event_close_loops();
	this->as_started_ = false;
	VLOG(1) << "Aerospike Disconnection done";
	return 0;
}
}
