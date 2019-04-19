#include <memory>
#include <mutex>
#include <utility>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

#include "CommonMacros.h"
#include "RangeLock.h"

namespace pio { namespace RangeLock {
bool RangeCompare::operator() (const Range& lhs, const Range& rhs) const {
	return lhs.range_.second < rhs.range_.first;
}

bool RangeCompare::operator() (const Range& lhs, const range_t& rhs) const {
	return lhs.range_.second < rhs.first;
}

bool RangeCompare::operator() (const range_t& lhs, const Range& rhs) const {
	return lhs.second < rhs.range_.first;
}

Range::Range(const range_t& range) : range_(range) {
}

folly::Future<int> Range::GetFuture() const {
	if (details_.futures_) {
		return details_.futures_->getFuture();
	}

	std::lock_guard<SpinLock> lock(details_.mutex_);
	if (details_.futures_) {
		return details_.futures_->getFuture();
	}
	details_.promise_ = std::make_unique<folly::Promise<int>>();
	details_.futures_ =
		std::make_unique<folly::FutureSplitter<int>>(
			details_.promise_->getFuture()
		);
	return details_.futures_->getFuture();
}

std::unique_ptr<folly::Promise<int>> Range::MovePromise() const {
	return std::move(details_.promise_);
}

void RangeLock::SelectiveLock(/*[IN]*/const std::vector<range_t>& ranges_in, 
				/*[OUT]*/std::vector<range_t>& ranges_out) {
	std::lock_guard<std::mutex> l(mutex_);
	ranges_out.reserve(ranges_in.size());
	for(auto it = ranges_in.begin(); it != ranges_in.end(); ++it) {
		if(!IsRangeLocked(*it)) {
			LockRange(*it);
			ranges_out.emplace_back(*it);
		}
	}
}

void RangeLock::LockRange(const range_t& range) {
	ranges_.emplace(range);
}

folly::Future<int> RangeLock::Lock(const range_t& range) {
	std::lock_guard<std::mutex> l(mutex_);
	auto it = ranges_.find(range);
	if (it == ranges_.end()) {
		LockRange(range);
		return folly::makeFuture(0);
	}

	return it->GetFuture()
	.then([this, &range] () {
		return this->Lock(range);
	});
}

void RangeLock::Unlock(const range_t& range) {
	std::unique_lock<std::mutex> l(mutex_);
	auto it = ranges_.find(range);
	if (pio_unlikely(it == ranges_.end())) {
		assert(0);
		return;
	}

	auto promise = it->MovePromise();
	ranges_.erase(it);
	l.unlock();

	if (promise) {
		promise->setValue(0);
	}
}

bool RangeLock::IsRangeLocked(const range_t& range) const {
	return ranges_.find(range) != ranges_.end();
}

bool RangeLock::TryLock(const range_t& range) {
	std::lock_guard<std::mutex> l(mutex_);
	if (IsRangeLocked(range)) {
		return false;
	}
	LockRange(range);
	return true;
}

LockGuard::LockGuard(RangeLock* lockp, uint64_t start, uint64_t end) noexcept :
		lockp_(lockp) {
	ranges_.emplace_back(start, end);
}

LockGuard::LockGuard(RangeLock* lockp, std::vector<range_t> ranges)
#ifdef NDEBUG
noexcept
#endif
:
		lockp_(lockp), ranges_(std::move(ranges)) {
	struct {
		bool operator() (const range_t& f, const range_t& s) {
			return f.first < s.first;
		}
	} RangeLessThan;

	if (not std::is_sorted(ranges_.begin(), ranges_.end(), RangeLessThan)) {
		std::sort(ranges_.begin(), ranges_.end(), RangeLessThan);
	}

#ifndef NDEBUG
	/* ensure no range coincides */
	bool first = true;
	range_t prev;
	for (const auto& r : ranges_) {
		if (first) {
			prev = r;
			first = false;
			continue;
		}

		if (prev.second >= r.first) {
			throw std::runtime_error("Will cause deadlock");
		}
		prev = r;
	}
#endif
}

LockGuard::LockGuard(LockGuard&& rhs) noexcept :
		lockp_(rhs.lockp_),
		is_locked_(rhs.is_locked_),
		ranges_(std::move(rhs.ranges_)) {
	rhs.is_locked_ = false;
	rhs.ranges_.clear();
}

LockGuard::~LockGuard() {
	if (is_locked_) {
		auto it = ranges_.rbegin();
		auto eit = ranges_.rend();
		for (; it != eit; ++it) {
			lockp_->Unlock(*it);
		}
		is_locked_ = false;
	}
}

folly::Future<int> LockGuard::LockIterate(std::vector<range_t>::iterator it,
		std::vector<range_t>::iterator eit) {
	if (pio_unlikely(it == eit)) {
		return folly::makeFuture(0);
	}

	return lockp_->Lock(*it)
	.then([this, it = std::move(it), eit = std::move(eit)] (int rc) mutable {
		if (pio_unlikely(rc)) {
			return folly::makeFuture(rc < 0 ? rc : -rc);
		}

		++it;
		if (it == eit) {
			return folly::makeFuture(0);
		}
		return LockIterate(std::move(it), std::move(eit));
	});
}

folly::Future<int> LockGuard::Lock() {
	auto it = ranges_.begin();
	auto eit = ranges_.end();
	return LockIterate(std::move(it), std::move(eit))
	.then([this] (int rc) {
		this->is_locked_ = rc == 0;
		return rc;
	});
}

bool LockGuard::SelectiveLock(/*[OUT]*/std::vector<range_t>& ranges) {
	lockp_->SelectiveLock(ranges_, ranges);
	is_locked_ = ranges.size() > 0;
	bool full_lock = ranges_.size() == ranges.size();
	if(!full_lock) {
		ranges_= ranges;
	}
	return full_lock;
}

bool LockGuard::IsLocked() const noexcept {
	return is_locked_;
}

}}
