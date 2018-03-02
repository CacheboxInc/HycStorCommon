#pragma once

#include <atomic>
#include <queue>

#include "SpinLock.h"

/* forward declaration */
namespace folly {
	namespace fibers {
		class Baton;
	}
}

namespace hyc {
class QLock {
public:
	QLock();
	~QLock();

	void lock();
	void unlock();

private:
	std::atomic_flag lock_flag_{ATOMIC_FLAG_INIT};
	struct {
		SpinLock mutex_;
		std::queue<folly::fibers::Baton *> queue_;
	} waiters_;
};
}
