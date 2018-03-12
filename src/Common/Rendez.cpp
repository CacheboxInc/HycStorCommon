#include <memory>
#include <folly/fibers/Baton.h>

#include "QLock.h"
#include "Rendez.h"

namespace pio {
Rendez::Rendez() {

}

Rendez::~Rendez() {
	TaskWakeUpAll();
}

void Rendez::TaskSleep(QLock* qlockp) {
	auto baton = std::make_unique<folly::fibers::Baton>();
	auto batonp = baton.get();
	{
		std::lock_guard<SpinLock> lock_(waiters_.mutex_);
		waiters_.queue_.push(batonp);
	}

	if (not qlockp) {
		batonp->wait();
	} else {
		qlockp->unlock();
		batonp->wait();
		qlockp->lock();
	}
}

size_t Rendez::WakeUp(size_t count) {
	size_t woken = 0;
	for (; not waiters_.queue_.empty() and woken < count; ++woken) {
		auto batonp = waiters_.queue_.front();
		waiters_.queue_.pop();
		batonp->post();
	}
	return woken;
}

bool Rendez::TaskWakeUp() {
	std::lock_guard<SpinLock> lock_(waiters_.mutex_);
	return WakeUp(1) >= 1;
}

size_t Rendez::TaskWakeUpAll() {
	std::lock_guard<SpinLock> lock_(waiters_.mutex_);
	return WakeUp(waiters_.queue_.size());
}

bool Rendez::IsSleeping() const {
	std::lock_guard<SpinLock> lock_(waiters_.mutex_);
	return not waiters_.queue_.empty();
}

}