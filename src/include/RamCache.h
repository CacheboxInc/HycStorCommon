#pragma once

#include <unordered_map>
#include <mutex>

#include "IDs.h"
#include "DaemonCommon.h"

namespace pio {
class RamCache {
public:
	enum class ReadStatus {
		kCacheHit,
		kCacheMiss,
	};

	using Key = Offset;
	std::pair<std::unique_ptr<RequestBuffer>, bool>
		Read(ActiveVmdk *vmdkp, Offset offset);
	void Write(ActiveVmdk *vmdkp, void *bufferp, Offset offset, size_t size);

	template <typename ForwardIt>
	void Erase(ForwardIt begin, ForwardIt end) {
		std::lock_guard<std::mutex> lock(mutex_);
		for (; begin != end; ++begin) {
			cache_.erase(*begin);
		}
	}

	size_t Size() const noexcept;
private:
	std::mutex mutex_;
	std::unordered_map<Key, std::unique_ptr<RequestBuffer>> cache_;
};
}
