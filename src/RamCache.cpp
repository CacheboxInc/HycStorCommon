#include <memory>
#include <mutex>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonCommon.h"
#include "Request.h"
#include "Vmdk.h"
#include "RamCache.h"

namespace pio {

std::pair<std::unique_ptr<RequestBuffer>, bool>
RamCache::Read(ActiveVmdk *vmdkp, Offset offset) {
	Key key = offset;

	std::lock_guard<std::mutex> guard(mutex_);
	auto it = cache_.find(key);
	if (it == cache_.end()) {
		return std::make_pair(nullptr, false);
	}

	log_assert(it != cache_.end());
	auto bufferp = CloneRequestBuffer(it->second.get());
	if (pio_unlikely(not bufferp)) {
		return std::make_pair(nullptr, true);
	}
	return std::make_pair(std::move(bufferp), true);
}

void RamCache::Write(ActiveVmdk *vmdkp, void *bufferp, Offset offset, size_t size) {
	Key key = offset;

	std::lock_guard<std::mutex> guard(mutex_);
	auto destp = NewRequestBuffer(size);
	auto dp = destp->Payload();

	::memcpy(dp, bufferp, destp->Size());
	cache_.insert_or_assign(key, std::move(destp));
}

}