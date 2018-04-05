#include <vector>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "ThreadPool.h"
#include "Rendez.h"
#include "QLock.h"

using namespace pio;

TEST(RendezTest, BlockedSleep) {
	ThreadPool pool(std::thread::hardware_concurrency());
	pool.CreateThreads();
	Rendez rendez;

	for (auto i = 0; i < 5; ++i) {
		EXPECT_FALSE(rendez.IsSleeping());
		folly::Promise<int> promise;

		pool.AddTask([&rendez, &promise] () mutable {
			/* Sleep on rendez */
			rendez.TaskSleep();
			promise.setValue(0);
		});

		/* wait for one second and ensure promise is not fullfilled */
		auto future = promise.getFuture();
		future.wait(std::chrono::milliseconds(1000));
		EXPECT_FALSE(future.isReady());
		EXPECT_TRUE(rendez.IsSleeping());

		/* wakeup fiber sleeping on rendez */
		rendez.TaskWakeUp();

		/* future should now be fullfilled */
		future.wait(std::chrono::milliseconds(1000));
		EXPECT_TRUE(future.isReady());
	}
}

TEST(RendezTest, SimpleSleep) {
	ThreadPool pool(std::thread::hardware_concurrency());
	pool.CreateThreads();

	folly::Promise<int> p1;
	folly::Promise<int> p2;

	std::vector<folly::Future<int>> futures;
	futures.emplace_back(p1.getFuture());
	futures.emplace_back(p2.getFuture());

	Rendez rendez;
	pool.AddTask([&rendez, &p1] () mutable {
		rendez.TaskSleep();
		p1.setValue(0);
	});

	/* ensure first task is started and it is sleeping on rendez */
	while (not rendez.IsSleeping());

	pool.AddTask([&rendez, &p2] () mutable {
		rendez.TaskWakeUp();
		p2.setValue(0);
	});

	auto start = std::chrono::steady_clock::now();
	auto f = folly::collectAll(std::move(futures))
	.wait(std::chrono::milliseconds(1000));
	auto elapsed = std::chrono::steady_clock::now() - start;

	EXPECT_TRUE(f.isReady());
	EXPECT_LE(elapsed, std::chrono::milliseconds(1000));
}

TEST(RendezTest, TaskWakeUpAll) {
	ThreadPool pool(std::thread::hardware_concurrency());
	pool.CreateThreads();

	Rendez rendez;
	std::vector<folly::Future<int>> futures;

	for (auto i = 0; i < 10; ++i) {
		folly::Promise<int> promise;
		futures.emplace_back(promise.getFuture());
		pool.AddTask([&rendez, promise = std::move(promise)] () mutable {
			rendez.TaskSleep();
			promise.setValue(0);
		});
	}

	auto start = std::chrono::steady_clock::now();
	auto f = folly::collectAll(std::move(futures))
	.wait(std::chrono::milliseconds(1000));
	auto elapsed = std::chrono::steady_clock::now() - start;

	EXPECT_GE(elapsed, std::chrono::milliseconds(1000));
	EXPECT_TRUE(rendez.IsSleeping());
	EXPECT_FALSE(f.isReady());

	rendez.TaskWakeUpAll();


	{
		auto start = std::chrono::steady_clock::now();
		f.wait(std::chrono::milliseconds(1000));
		auto elapsed = std::chrono::steady_clock::now() - start;
		EXPECT_LT(elapsed, std::chrono::milliseconds(1000));
		EXPECT_TRUE(f.isReady());
		EXPECT_FALSE(rendez.IsSleeping());
	}
}

TEST(RendezTest, QLock) {
	if (std::thread::hardware_concurrency() <= 1) {
		/* The test cannot be run on single core machine */
		return;
	}

	const int kLoop = 1ull << 18;
	ThreadPool pool(std::thread::hardware_concurrency());
	pool.CreateThreads();

	std::vector<folly::Future<int>> futures;
	std::atomic<bool> stop{false};

	QLock mutex;
	Rendez rendez;
	{
		auto promise = std::make_unique<folly::Promise<int>>();
		futures.emplace_back(promise->getFuture());
		pool.AddTask([promise = std::move(promise), &rendez, &mutex, &stop] ()
				mutable {
			for (auto i = 0; i < kLoop; ++i) {
				mutex.lock();
				rendez.TaskSleep(&mutex);
				mutex.unlock();
			}
			promise->setValue(0);
			stop = true;
		});
	}
	{
		auto promise = std::make_unique<folly::Promise<int>>();
		futures.emplace_back(promise->getFuture());
		pool.AddTask([promise = std::move(promise), &rendez, &mutex, &stop] ()
				mutable {
			while (not stop) {
				mutex.lock();
				rendez.TaskWakeUp();
				mutex.unlock();
			}
			promise->setValue(0);
		});
	}

	auto f = folly::collectAll(std::move(futures));
	f.wait(std::chrono::milliseconds(5000));
	EXPECT_TRUE(f.isReady());
}