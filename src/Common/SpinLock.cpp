#include <cassert>
#include <thread>

#include "SpinLock.h"

namespace pio {
SpinLock::SpinLock() {
}

void SpinLock::lock() {
	while (flag_.test_and_set(std::memory_order_acquire)) {
		std::this_thread::yield();
	}
}

void SpinLock::unlock() {
	flag_.clear(std::memory_order_release);
}
}