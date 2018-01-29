#include <mutex>
#include <folly/fibers/Baton.h>

#include "QLock.h"

namespace pio {
QLock::QLock() {

}

QLock::~QLock() {

}

void QLock::lock() {
	while (lock_flag_.test_and_set(std::memory_order_acquire)) {
		auto baton = std::make_unique<folly::fibers::Baton>();
		auto batonp = baton.get();
		{
			std::lock_guard<SpinLock> lock_(waiters_.mutex_);
			waiters_.queue_.push(batonp);
		}
		batonp->wait();
	}
}

void QLock::unlock() {
	lock_flag_.clear(std::memory_order_release);

	std::lock_guard<SpinLock> lock_(waiters_.mutex_);
	if (not waiters_.queue_.empty()) {
		auto batonp = waiters_.queue_.front();
		waiters_.queue_.pop();
		batonp->post();
	}
}

}