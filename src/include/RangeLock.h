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
using range_t = std::pair<uint64_t, uint64_t>;

struct RangeCompare;

class Range {
public:
	Range(const range_t& range);
	folly::Future<int> GetFuture() const;
	std::unique_ptr<folly::Promise<int>> MovePromise() const;

	friend struct RangeCompare;
private:
	range_t range_;
	struct {
		mutable SpinLock mutex_;
		mutable std::unique_ptr<folly::Promise<int>> promise_;
		mutable std::unique_ptr<folly::FutureSplitter<int>> futures_;
	} details_;
};

struct RangeCompare {
	using is_transparent = void;

	bool operator () (const Range& lhs, const Range& rhs) const;
	bool operator () (const Range& lhs, const range_t& rhs) const;
	bool operator () (const range_t& lhs, const Range& rhs) const;
};

class RangeLock {
public:
	folly::Future<int> Lock(const range_t& range);
	void Unlock(const range_t& range);
	bool TryLock(const range_t& range);
	void SelectiveLock(/*[IN]*/const std::vector<range_t>& ranges_in,                                                                                                     
                /*[OUT]*/std::vector<range_t>& ranges_out);	
private:
	bool IsRangeLocked(const range_t& range) const;
	void LockRange(const range_t& range);
private:
	std::mutex                    mutex_;
	std::set<Range, RangeCompare> ranges_;
};

class LockGuard {
public:
	LockGuard(RangeLock* lockp, uint64_t start, uint64_t end) noexcept;
	LockGuard(RangeLock* lockp, std::vector<range_t> ranges)
#ifdef NDEBUG
noexcept
#endif
;

	LockGuard(LockGuard&& rhs) noexcept;

	LockGuard(LockGuard& rhs) = delete;
	LockGuard& operator = (const LockGuard& rhs) = delete;
	LockGuard& operator == (LockGuard&& rhs) = delete;

	~LockGuard();
	folly::Future<int> Lock();
	bool SelectiveLock(std::vector<range_t>& ranges);
	bool IsLocked() const noexcept;

private:
	folly::Future<int> LockIterate(std::vector<range_t>::iterator it,
		std::vector<range_t>::iterator eit);

private:
	RangeLock* lockp_{nullptr};
	bool       is_locked_{false};
	std::vector<range_t> ranges_;
};

}
}
