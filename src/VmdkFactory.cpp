#include <memory>

#include "VmdkFactory.h"
#include "IDs.h"

using namespace ::hyc_thrift;

namespace pio {
Vmdk* VmdkManager::GetInstance(::hyc_thrift::VmdkHandle handle) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = handles_.find(handle);
	if (pio_unlikely(it == handles_.end())) {
		return nullptr;
	}
	return it->second;
}

Vmdk* VmdkManager::GetInstance(const VmdkID& vmdkid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = ids_.find(vmdkid);
	if (pio_unlikely(it == ids_.end())) {
		return nullptr;
	}
	return it->second.get();
}

void VmdkManager::FreeVmdkInstance(::hyc_thrift::VmdkHandle handle) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto it1 = handles_.find(handle);
	if (pio_unlikely(it1 == handles_.end())) {
		return;
	}
	const auto& id = it1->second->GetID();
	handles_.erase(it1);

	auto it2 = ids_.find(id);
	assert(pio_unlikely(it2 != ids_.end()));
	ids_.erase(it2);
}
}