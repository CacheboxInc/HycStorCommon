#include "CkptMergeInstance.h"
#include "CkptMergeManager.h"
#include "ThreadPool.h"

using namespace ::ondisk;

namespace pio {
int CkptMergeManager::CreateInstance(struct _ha_instance *) {
	auto rc = InitCkptMergeManager();
	if (pio_unlikely(rc)) {
		return rc;
	}
	return 0;
}

void CkptMergeManager::DestroyInstance() {
	//TBD: Quiescing of IO's
	DeinitCkptMergeManager();
}

CkptMergeInstance* CkptMergeManager::GetInstance(const VmID& vmid) {
	std::lock_guard<SpinLock> lock(mutex_);
	if (auto it = instances_.find(vmid); pio_likely(it != instances_.end())) {
		return it->second.get();
	}
	return nullptr;
}

int CkptMergeManager::InitCkptMergeManager() {
	auto cores = std::thread::hardware_concurrency();
	try {
		std::call_once(threadpool_.initialized_, [=] () mutable {
			/* Hardcore value 2 for now */
			if (pio_likely(cores > 1)) {
				cores = 2;
			}
			threadpool_.pool_ = std::make_unique<ThreadPool>(cores);
			threadpool_.pool_->CreateThreads();
		});
	} catch (const std::exception& e) {
		threadpool_.pool_ = nullptr;
		return -ENOMEM;
	}

	return 0;
}

void CkptMergeManager::DeinitCkptMergeManager() {
	threadpool_.pool_ = nullptr;
}

int CkptMergeManager::NewInstance(VmID vmid) {
	try {
		auto mi = std::make_unique<CkptMergeInstance>(vmid);
		instances_.insert(std::make_pair(std::move(vmid), std::move(mi)));
		return 0;
	} catch (const std::bad_alloc& e) {
		return -ENOMEM;
	}
}

void CkptMergeManager::FreeInstance(const VmID& vmid) {
	std::lock_guard<SpinLock> lock(mutex_);
	auto it = instances_.find(vmid);
	if (it != instances_.end()) {
		instances_.erase(it);
	}
}
}
