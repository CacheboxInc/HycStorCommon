#include "ScanManager.h"

using namespace ::ondisk;

namespace pio {
ScanInstance* ScanManager::GetInstance(const AeroClusterID& cluster_id) {
	std::lock_guard<SpinLock> lock(mutex_);
	if (auto it = instances_.find(cluster_id); pio_likely(it != instances_.end())) {
		return it->second.get();
	}

	return nullptr;
}

int ScanManager::NewInstance(AeroClusterID cluster_id) {
	try {
		auto fi = std::make_unique<ScanInstance>(cluster_id);
		instances_.insert(std::make_pair(std::move(cluster_id), std::move(fi)));
		return 0;
	} catch (const std::bad_alloc& e) {
		return -ENOMEM;
	}
}

void ScanManager::FreeInstance(const AeroClusterID& cluster_id) {
	std::lock_guard<SpinLock> lock(mutex_);
	auto it = instances_.find(cluster_id);
	if (it != instances_.end()) {
		instances_.erase(it);
	}
}

}
