#pragma once

#include <unordered_map>

#include "IDs.h"
#include "Common.h"

namespace pio {
class RamCache {
public:
	enum class ReadStatus {
		kCacheHit,
		kCacheMiss,
	};

	using Key = Offset;
	void Read(ActiveVmdk *vmdkp, void *bufferp, Offset offset);
	void Write(ActiveVmdk *vmdkp, void *bufferp, Offset offset);
private:
	std::mutex mutex_;
	std::unordered_map<Key, std::unique_ptr<RequestBuffer>> cache_;
};
}