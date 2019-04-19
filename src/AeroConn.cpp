#include <cerrno>
#include <iterator>
#include <algorithm>
#include <chrono>

#include <folly/futures/Future.h>

#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_event.h>
#include <aerospike/as_log.h>
#include <AeroConfig.h>

#include "AeroConn.h"
#include "WorkScheduler.h"

namespace pio {
using namespace std::chrono_literals;

static constexpr uint16_t kAeroEventLoops = 4;
static constexpr uint16_t kMinCommandsInProgress = 4;
static constexpr uint16_t kMaxCommandsInProgress = 1024 / kAeroEventLoops;
static constexpr uint16_t kStartIoDepth = 128 / kAeroEventLoops;
static constexpr auto kTickSeconds = 30s;

AeroSpikeConn::AeroSpikeConn(AeroClusterID cluster_id,
		const std::string& config) :
		cluster_id_(std::move(cluster_id)),
		reconfig_timer_(kTickSeconds),
		config_(std::make_unique<config::AeroConfig>(config)) {
	reconf_.io_depth_ = kStartIoDepth;
	reconf_.prev_unthrottle_ = 1;
	reconf_.gen_count_ = 1;
}

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


bool AeroClientLogCallback(as_log_level level, const char *func, const char *file,
    uint32_t line, const char *fmt, ...)
{
    char msg[1024] = {0};
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(msg, 1024, fmt, ap);
    msg[1023] = '\0';
    va_end(ap);

    LOG(ERROR) << "file:" << file << " line:" << line << " func:" << func << " level:" << level
       << " msg:" << msg;

    return 0;

}

void AeroClusterChangeEventFnPtr(as_cluster_event* event_data) {
	std::string change_details;
	auto this_ptr_ = (reinterpret_cast<AeroSpikeConn *>(event_data->udata));

	if(this_ptr_->as_started_ == false) {
		LOG(INFO) << "Returning as we haven't yet connected to server";
	} else {
		LOG(INFO) << "Got cluster change event, about to take action accordingly";
		switch(event_data->type) {
			case AS_CLUSTER_ADD_NODE:
				change_details="added";
				this_ptr_->UnthrottleClient();
				break;
			case AS_CLUSTER_REMOVE_NODE:
				change_details="removed";
				AeroSpikeConn::ConfigTag tag;
				this_ptr_->GetEventLoopAndTag(nullptr, &tag);
				this_ptr_->HandleServerOverload(tag);
				break;
			case AS_CLUSTER_DISCONNECTED:
				change_details="disconnected";
				break;
			default:
				change_details="surprized";
		}
		LOG(INFO) << "Cluster Node " << event_data->node_address << " got " 
			<< change_details;
	}
}

int AeroSpikeConn::Connect() {
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
	policy.max_commands_in_process = reconf_.io_depth_;
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

	cfg.policies.read.replica = AS_POLICY_REPLICA_SEQUENCE; 
	cfg.policies.write.replica = AS_POLICY_REPLICA_SEQUENCE; //default is MASTER 
	cfg.policies.write.commit_level = AS_POLICY_COMMIT_LEVEL_MASTER; //default is ALL
	//cfg.tender_interval = 1800000; //1000 (1 sec) * 60 * 30 = 30 mins

	/* TODO: See if we need to set for all types of policies */
	/* Timeout setting */
	/* we are setting total_timeout to 0 so that we would wait inifinitely
	 * till transaction is completed on server side. This makes assumption on 
	 * server side that for single record transactions would be completed in 1 sec
	 * Hopefully after we bring in replication factor, we see no timeouts in 
	 * aerospike server log file. Hint: ticker.c
	 */
	cfg.policies.write.base.total_timeout = 0;
	cfg.policies.write.base.max_retries = 0;
	cfg.policies.write.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.read.base.total_timeout = 0;
	cfg.policies.read.base.max_retries = 10;
	cfg.policies.read.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.operate.base.total_timeout = 0;
	cfg.policies.operate.base.max_retries = 10;
	cfg.policies.operate.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.remove.base.total_timeout = 0;
	cfg.policies.remove.base.max_retries = 0;
	cfg.policies.remove.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.batch.base.total_timeout = 0;
	cfg.policies.batch.base.max_retries = 10;
	cfg.policies.batch.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.apply.base.total_timeout = 0;
	cfg.policies.apply.base.max_retries = 0;
	cfg.policies.apply.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.scan.base.total_timeout = 0;
	cfg.policies.scan.base.max_retries = 0;
	cfg.policies.scan.base.sleep_between_retries = 300; // 300ms 

	cfg.policies.query.base.total_timeout = 0;
	cfg.policies.query.base.max_retries = 0;
	cfg.policies.query.base.sleep_between_retries = 300; // 300ms 

	as_config_set_cluster_event_callback(&cfg, AeroClusterChangeEventFnPtr, this);

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

	//uncomment to enable aerospike client logs
	//as_log_set_level(AS_LOG_LEVEL_INFO);
	//as_log_set_callback(AeroClientLogCallback);

	work_scheduler_ = std::make_unique<WorkScheduler>();
	reconfig_timer_.AttachToEventBase(work_scheduler_->GetEventBase());
	reconfig_timer_.ScheduleTimeout([this] () mutable {
		this->UnthrottleClient();
		return true;
	});
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

void AeroSpikeConn::GetEventLoopAndTag(as_event_loop** looppp, ConfigTag* tagp) const {
	std::lock_guard<std::mutex> lock(reconf_.mutex_);
	if (tagp) {
		*tagp = reconf_.gen_count_;
	}
	if (looppp) {
		*looppp = as_event_loop_get();
	}
}

void AeroSpikeConn::SetEventLoopDepth(const uint32_t depth) const noexcept {
	as_event_loop* loopp;
	for (int i = 0; (loopp = as_event_loop_get_by_index(i)) != nullptr; ++i) {
		loopp->max_commands_in_process = depth;
	}
}

void AeroSpikeConn::HandleServerOverload(ConfigTag tag) {
	std::lock_guard<std::mutex> lock(reconf_.mutex_);
	if (reconf_.gen_count_ > tag) {
		return;
	}

	reconf_.prev_unthrottle_ = 1;
	const uint16_t old_depth = reconf_.io_depth_;
	reconf_.io_depth_ = std::max(static_cast<uint16_t>(old_depth/2),
		kMinCommandsInProgress);
	if (old_depth == reconf_.io_depth_) {
		return;
	}

	SetEventLoopDepth(reconf_.io_depth_);
	++reconf_.gen_count_;
	LOG(INFO) << "KV Store IODeth Reconfigured from " << old_depth
		<< " to " << reconf_.io_depth_;
}

void AeroSpikeConn::UnthrottleClient() {
	std::lock_guard<std::mutex> lock(reconf_.mutex_);
	as_event_loop* loopp{};
	bool delay_queue_empty = true;
	for (int i = 0; (loopp = as_event_loop_get_by_index(i)) != nullptr; ++i) {
		if (not as_queue_empty(&loopp->delay_queue)) {
			delay_queue_empty = false;
			break;
		}
	}
	if (delay_queue_empty) {
		return;
	}

	const auto old_depth = reconf_.io_depth_;
	const auto increment = reconf_.prev_unthrottle_ << 1;
	reconf_.io_depth_ = std::min(static_cast<uint16_t>(old_depth + increment),
			kMaxCommandsInProgress);
	if (reconf_.io_depth_ == old_depth) {
		return;
	}

	reconf_.prev_unthrottle_ = increment;
	SetEventLoopDepth(reconf_.io_depth_);
	++reconf_.gen_count_;
	LOG(INFO) << "KV Store IODeth Reconfigured from " << old_depth
		<< " to " << reconf_.io_depth_;
}

WorkScheduler* AeroSpikeConn::GetWorkScheduler() noexcept {
	return work_scheduler_.get();
}
}
