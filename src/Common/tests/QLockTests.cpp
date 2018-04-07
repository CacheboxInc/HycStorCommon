#include <mutex>
#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <folly/fibers/Baton.h>

#include "ThreadPool.h"
#include "QLock.h"

using namespace pio;

TEST(QLockTests, NoContention) {
	ThreadPool pool(std::thread::hardware_concurrency());
	pool.CreateThreads();

	QLock mutex;
	folly::Promise<int> promise;

	pool.AddTask([&mutex, &promise] () mutable {
		for (auto i = 0; i < 20; ++i) {
			std::lock_guard<QLock> lock_(mutex);
		}
		promise.setValue(0);
	});

	auto future = promise.getFuture();
	future.wait();
	EXPECT_TRUE(future.isReady());
	EXPECT_EQ(future.get(), 0);
}

TEST(QLockTests, Contention) {
	const auto kMaxTasks = 128;
	const auto kMaxLocks = 8ul << 10;

	ThreadPool pool(std::thread::hardware_concurrency());
	pool.CreateThreads();

	QLock mutex;
	uint64_t count = 0;
	std::vector<folly::Future<int>> futures;

	for (auto i = 0u; i < kMaxTasks; ++i) {
		folly::Promise<int> promise;
		futures.emplace_back(promise.getFuture());
		pool.AddTask([&, promise = std::move(promise)] () mutable {
			for (auto i = 0ul; i < kMaxLocks; ++i) {
				std::lock_guard<QLock> lock(mutex);
				++count;
			}

			promise.setValue(0);
		});
	}

	folly::collectAll(std::move(futures))
	.then([&] (const std::vector<folly::Try<int>>& tries) mutable{
		for (const auto& t : tries) {
			EXPECT_EQ(t.value(), 0);
		}
	})
	.wait();

	EXPECT_EQ(count, kMaxTasks * kMaxLocks);
}
