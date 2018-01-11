#include <memory>
#include <mutex>

#include "Common.h"
#include "Request.h"
#include "Vmdk.h"
#include "RamCache.h"

namespace pio {

void RamCache::Read(ActiveVmdk *vmdkp, void *bufferp, Offset offset) {
	Key key = offset;

	std::lock_guard<std::mutex> guard(mutex_);
	auto it = cache_.find(key);
	if (it == cache_.end()) {
		auto destp = NewRequestBuffer(vmdkp->BlockSize());
		auto dp = destp->Payload();
		::memset(dp, 0, destp->Size());
		cache_.emplace(key, std::move(destp));

		it = cache_.find(key);
	}

	log_assert(it != cache_.end());
	auto srcp = it->second->Payload();
	::memcpy(bufferp, srcp, vmdkp->BlockSize());
}

void RamCache::Write(ActiveVmdk *vmdkp, void *bufferp, Offset offset) {
	Key key = offset;

	std::lock_guard<std::mutex> guard(mutex_);
	auto it = cache_.find(key);
	if (it != cache_.end()) {
		auto dp = it->second.get();
		::memcpy(dp->Payload(), bufferp, dp->Size());
		return;
	}

	auto destp = NewRequestBuffer(vmdkp->BlockSize());
	auto dp = destp->Payload();
	::memcpy(dp, bufferp, destp->Size());
	cache_.emplace(key, std::move(destp));
}

}