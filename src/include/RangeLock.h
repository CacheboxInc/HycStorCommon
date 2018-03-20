#pragma once

#include <set>
#include <utility>
#include <mutex>
#include <memory>

#include <cstdint>

#include "SpinLock.h"

namespace folly {
/* forward declaration for pimpl */
template <typename T>
class FutureSplitter;
}

namespace pio {
namespace RangeLock {

struct RangeCompare;

class Range {
public:
	Range(const std::pair<uint64_t, uint64_t>& range);
	folly::Future<int> GetFuture() const;
	std::unique_ptr<folly::Promise<int>> MovePromise() const;

	friend struct RangeCompare;
private:
	std::pair<uint64_t, uint64_t>      range_;
	struct {
		mutable SpinLock mutex_;
		mutable std::unique_ptr<folly::Promise<int>> promise_;
		mutable std::unique_ptr<folly::FutureSplitter<int>> futures_;
	} details_;
};

struct RangeCompare {
	using is_transparent = void;

	bool operator () (const Range& lhs, const Range& rhs) const;
	bool operator () (const Range& lhs,
		const std::pair<uint64_t, uint64_t>& rhs) const;
	bool operator () (const std::pair<uint64_t, uint64_t>& lhs,
		const Range& rhs) const;
};

class RangeLock {
public:
	folly::Future<int> Lock(const std::pair<uint64_t, uint64_t>& range);
	void Unlock(const std::pair<uint64_t, uint64_t>& range);
	bool TryLock(const std::pair<uint64_t, uint64_t>& range);
private:
	bool IsRangeLocked(const std::pair<uint64_t, uint64_t>& range) const;
	void LockRange(const std::pair<uint64_t, uint64_t>& range);
private:
	std::mutex                    mutex_;
	std::set<Range, RangeCompare> ranges_;
};

class LockGuard {
public:
	LockGuard(RangeLock* lockp, uint64_t start, uint64_t end);
	LockGuard(LockGuard& rhs) = delete;
	LockGuard(LockGuard&& rhs);
	LockGuard& operator = (const LockGuard& rhs) = delete;
	LockGuard& operator == (LockGuard&& rhs);

	~LockGuard();
	folly::Future<int> Lock();
private:
	RangeLock* lockp_{nullptr};
	bool       is_locked_{false};
	std::pair<uint64_t, uint64_t> range_;
};

}
}