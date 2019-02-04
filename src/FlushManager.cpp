#include <memory>
#include <unordered_map>
#include <string>

#include <folly/futures/Future.h>

#include "jansson.h"
#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "DaemonCommon.h"
#include "SpinLock.h"
#include "FlushInstance.h"
#include "FlushManager.h"
#include "ThreadPool.h"

using namespace ::ondisk;

namespace pio {
int FlushManager::CreateInstance(struct _ha_instance *) {

	auto rc = InitFlushManager();
	if (pio_unlikely(rc)) {
		return rc;
	}
	return 0;
}

void FlushManager::DestroyInstance() {
	//TBD: Quiescing of IO's
	DeinitFlushManager();
}

FlushInstance* FlushManager::GetInstance(const VmID& vmid) {
	std::lock_guard<SpinLock> lock(mutex_);
	if (auto it = instances_.find(vmid); pio_likely(it != instances_.end())) {
		return it->second.get();
	}

	return nullptr;
}

int FlushManager::InitFlushManager() {
	auto cores = std::thread::hardware_concurrency();
	try {
		std::call_once(threadpool_.initialized_, [=] () mutable {
				threadpool_.pool_ = std::make_unique<ThreadPool>(cores);
				threadpool_.pool_->CreateThreads();
				});
	} catch (const std::exception& e) {
		threadpool_.pool_ = nullptr;
		return -ENOMEM;
	}

	return 0;
}

void FlushManager::DeinitFlushManager() {
	threadpool_.pool_ = nullptr;
}

int FlushManager::NewInstance(VmID vmid, const std::string& config) {
	try {
		auto fi = std::make_unique<FlushInstance>(vmid, config);
		instances_.insert(std::make_pair(std::move(vmid), std::move(fi)));
		return 0;
	} catch (const std::bad_alloc& e) {
		return -ENOMEM;
	}
}

void FlushManager::PopulateHistory(const VmID& vmid, FlushInstance *fi, int status) {

	instance_history h;
	h.status      = status;
	h.FlushedBlks = 0;
	h.MovedBlks   = 0;

	pio::FlushStats flush_stat;
	FlushStats::iterator itr;
	bool first = true;

	auto rc = fi->FlushStatus(vmid, flush_stat);
	if(pio_unlikely(rc)) {
		LOG(ERROR) << "Failed to get stats. Not populating history";
		return;
	}

	for (itr = flush_stat.begin(); itr != flush_stat.end(); ++itr) {
		if (pio_unlikely(first)) {
			h.StartedAt   = fi->GetStartTime();
			h.RunDuration = (itr->second).second;
			first = false;
		} else if (itr->first == "-2") {
			h.FlushDuration = (itr->second).first;
			h.MoveDuration  = (itr->second).second;
		} else if (itr->first == "-3") {
			h.FlushBytes = (itr->second).first;
			h.MoveBytes  = (itr->second).second;
		} else {
			h.FlushedBlks += (itr->second).first;
			h.MovedBlks   += (itr->second).second;
		}
	}

	LOG(INFO) << "Populating history for vmid: " << vmid
		<< "\nh.FlushedBlks: " << h.FlushedBlks << "h.MovedBlks" << h.MovedBlks;

	int cnt = flush_history_.count(vmid);
	if (pio_likely(cnt > 4)) {
		auto itr = flush_history_.find(vmid);
		flush_history_.erase(itr);
	}

	flush_history_.emplace(std::make_pair(vmid, std::move(h)));
	return;
}

void FlushManager::FreeHistory(const VmID& vmid) {
	auto cnt = flush_history_.count(vmid);
	auto erased = flush_history_.erase(vmid);

	if (pio_unlikely(cnt != erased)) {
		LOG(ERROR) << "Flush history not erased completely!!";
		return;
	}
	LOG(INFO) << "Flush history erased for vmid:" << vmid;
	return;
}

int FlushManager::GetHistory(const VmID& vmid, void *history_p) {

	auto itr1 = flush_history_.find(vmid);
	if (itr1 == flush_history_.end()) {
		return 1;
	}

	auto history_param = reinterpret_cast<json_t *>(history_p);
	auto i = flush_history_.count(vmid) - 1;
	for (auto itr = flush_history_.find(vmid); itr != flush_history_.end(); itr++) {
		json_t *all_params = json_object();
		auto h = itr -> second;
		auto st = std::chrono::system_clock::to_time_t(h.StartedAt);

		auto end_time = h.StartedAt + std::chrono::milliseconds(h.RunDuration);
		auto et = std::chrono::system_clock::to_time_t(end_time);
		if (pio_unlikely(h.status)) {
			json_object_set_new(all_params, "Status",
				json_string("Flush failed!!"));
			json_object_set_new(all_params, "Flush Started at",
				json_string(strtok(std::ctime(&st), "\n")));
		} else {
			json_object_set_new(all_params, "flushed_blks_cnt",
				json_integer(h.FlushedBlks));
			json_object_set_new(all_params, "moved_blks_cnt",
				json_integer(h.MovedBlks));
			json_object_set_new(all_params, "flush_duration(ms)",
				json_integer(h.FlushDuration));
			json_object_set_new(all_params, "move_duration(ms)",
				json_integer(h.MoveDuration));
			json_object_set_new(all_params, "Flush Started at",
				json_string(strtok(std::ctime(&st), "\n")));
			json_object_set_new(all_params, "Flush Ended at",
				json_string(strtok(std::ctime(&et), "\n")));
			json_object_set_new(all_params, "Total flush time(ms)",
				json_integer(h.RunDuration));
			json_object_set_new(all_params, "Status",
				json_string("Flush Completed"));
		}

		auto *all_param_str = json_dumps(all_params, JSON_ENCODE_ANY);
		json_object_set_new(history_param, std::to_string(i).c_str(), json_string(all_param_str));
		json_object_clear(all_params);
		json_decref(all_params);
		::free(all_param_str);
		i--;
	}
	return 0;
}

void FlushManager::FreeInstance(const VmID& vmid, int status) {
	std::lock_guard<SpinLock> lock(mutex_);


	auto it = instances_.find(vmid);
	if (it != instances_.end()) {
		PopulateHistory(vmid, it->second.get(), status);
		instances_.erase(it);
	}
}

}
