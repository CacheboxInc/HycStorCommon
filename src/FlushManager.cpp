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

int FlushManager::NewInstance(VmID vmid) {
	try {
		auto fi = std::make_unique<FlushInstance>(vmid);
		instances_.insert(std::make_pair(std::move(vmid), std::move(fi)));
		return 0;
	} catch (const std::bad_alloc& e) {
		return -ENOMEM;
	}
}

void FlushManager::FreeInstance(const VmID& vmid) {
	std::lock_guard<SpinLock> lock(mutex_);
	auto it = instances_.find(vmid);
	if (it != instances_.end()) {
		instances_.erase(it);
	}
}

}
