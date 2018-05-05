#include <memory>
#include <unordered_map>
#include <string>

#include <folly/futures/Future.h>

#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "DaemonTgtTypes.h"
#include "DaemonCommon.h"
#include "SpinLock.h"
#include "FlushInstance.h"
#include "FlushManager.h"
#include "ThreadPool.h"

namespace pio {
int FlushManager::CreateInstance() {

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
	/* TBD : More sensible error codes to retrun */
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
