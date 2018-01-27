#include <mutex>
#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "SpinLock.h"

using namespace pio;

TEST(SpinLockTests, NoThread) {
	SpinLock spin;

	for (auto i = 0; i < 10; ++i) {
		spin.lock();
		spin.unlock();
	}

	for (auto i = 0; i < 10; ++i) {
		std::lock_guard<SpinLock> lock(spin);
	}
}

TEST(SpinLockTests, Threaded) {
	const uint64_t kThreads = 64u;
	const uint64_t kLoop = 32u;
	SpinLock spin;
	uint64_t count = 0;

	std::vector<std::thread> threads;
	threads.reserve(kThreads);
	for (auto i = 0u; i < kThreads; ++i) {
		threads.emplace_back(std::thread([&] () mutable {
			for (auto i = 0u; i < kLoop; ++i) {
				std::lock_guard<SpinLock> lock(spin);
				++count;
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}));
	}

	for (auto& thread : threads) {
		thread.join();
	}

	EXPECT_EQ(count, kLoop * kThreads);
}