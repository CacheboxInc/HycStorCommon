#include <memory>
#include <mutex>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "DaemonCommon.h"
#include "Request.h"
#include "Vmdk.h"
#include "RamCache.h"

namespace pio {

bool RamCache::Read(ActiveVmdk *vmdkp, void *bufferp, Offset offset) {
	Key key = offset;

	std::lock_guard<std::mutex> guard(mutex_);
	auto it = cache_.find(key);
	if (it == cache_.end()) {
		return false;
	}

	log_assert(it != cache_.end());
	auto srcp = it->second->Payload();
	::memcpy(bufferp, srcp, vmdkp->BlockSize());
	return true;
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