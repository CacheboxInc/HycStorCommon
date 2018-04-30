#include <memory>
#include <mutex>
#include <utility>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

#include "RangeLock.h"

namespace pio { namespace RangeLock {

bool RangeCompare::operator() (const Range& lhs, const Range& rhs) const {
	return lhs.range_.second < rhs.range_.first;
}

bool RangeCompare::operator() (const Range& lhs,
		const std::pair<uint64_t, uint64_t>& rhs) const {
	return lhs.range_.second < rhs.first;
}

bool RangeCompare::operator() (const std::pair<uint64_t, uint64_t>& lhs,
		const Range& rhs) const {
	return lhs.second < rhs.range_.first;
}

Range::Range(const std::pair<uint64_t, uint64_t>& range) : range_(range) {
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

void RangeLock::LockRange(const std::pair<uint64_t, uint64_t>& range) {
	ranges_.emplace(range);
}

folly::Future<int> RangeLock::Lock(const std::pair<uint64_t, uint64_t>& range) {
	std::lock_guard<std::mutex> l(mutex_);
	auto it = ranges_.find(range);
	if (it == ranges_.end()) {
		LockRange(range);
		return folly::makeFuture(0);
	}

	return it->GetFuture()
	.then([range = std::move(range), this] () {
		return this->Lock(std::move(range));
	});
}

void RangeLock::Unlock(const std::pair<uint64_t, uint64_t>& range) {
	std::unique_lock<std::mutex> l(mutex_);
	auto it = ranges_.find(range);
	if (it == ranges_.end()) {
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

bool RangeLock::IsRangeLocked(const std::pair<uint64_t, uint64_t>& range) const {
	return ranges_.find(range) != ranges_.end();
}

bool RangeLock::TryLock(const std::pair<uint64_t, uint64_t>& range) {
	std::lock_guard<std::mutex> l(mutex_);
	if (IsRangeLocked(range)) {
		return false;
	}
	LockRange(range);
	return true;
}

LockGuard::LockGuard(RangeLock* lockp, uint64_t start, uint64_t end) :
		lockp_(lockp), range_(start, end) {
}

LockGuard::~LockGuard() {
	if (is_locked_) {
		lockp_->Unlock(range_);
		is_locked_ = false;
	}
}

folly::Future<int> LockGuard::Lock() {
	return lockp_->Lock(range_)
	.then([this] (int rc) {
		this->is_locked_ = true;
		return rc;
	});
}

bool LockGuard::IsLocked() const noexcept {
	return is_locked_;
}

}}