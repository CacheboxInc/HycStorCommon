#include <memory>
#include <unordered_map>
#include <string>

#include <folly/futures/Future.h>

#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "DaemonCommon.h"
#include "SpinLock.h"
#include "FlushInstance.h"
#include "FlushManager.h"
#include "ThreadPool.h"

using namespace ::ondisk;

namespace pio {
int FlushManager::CreateInstance(struct _ha_instance *ha_instance) {

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

void FlushManager::PopulateHistory(const VmID& vmid, FlushInstance *fi) {

	instance_history h;
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


int FlushManager::GetHistory(const VmID& vmid, std::ostringstream& fh) {
	for (auto itr = flush_history_.find(vmid); itr != flush_history_.end(); itr++) {
		auto h = itr -> second;
		auto st = std::chrono::system_clock::to_time_t(h.StartedAt);

		auto end_time = h.StartedAt + std::chrono::milliseconds(h.RunDuration);
		auto et = std::chrono::system_clock::to_time_t(end_time);

		fh << "(flushed_blks_cnt: "<< h.FlushedBlks << ", moved_blks_cnt: "
			<< h.MovedBlks << ", Flush Started at: " <<
			strtok(std::ctime(&st), "\n") << ", Flush Ended at: " <<
			strtok(std::ctime(&et), "\n") << ", Total flush time(ms): "
			<< h.RunDuration << "), ";
	}
	return 0;
}

void FlushManager::FreeInstance(const VmID& vmid) {
	std::lock_guard<SpinLock> lock(mutex_);


	auto it = instances_.find(vmid);
	if (it != instances_.end()) {
		PopulateHistory(vmid, it->second.get());
		instances_.erase(it);
	}
}

}
