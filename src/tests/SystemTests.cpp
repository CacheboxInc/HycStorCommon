#include <thread>
#include <atomic>

#include <folly/futures/Future.h>

#include "ThreadPool.h"

#include <gtest/gtest.h>
#include <glog/logging.h>

using namespace hyc;

TEST(SystemTest, Cores) {
	auto cores = std::thread::hardware_concurrency();
	EXPECT_GT(cores, 0);
	VLOG(1) << "Number of cores " << cores;
}

TEST(SystemTest, CreateThreads) {
	auto cores = std::thread::hardware_concurrency();
	EXPECT_GT(cores, 0);

	auto *threadsp = new std::thread[cores];
	std::atomic<uint16_t> thread_initialized{0};
	for (auto i = 0u; i < cores; ++i) {
		threadsp[i] = std::thread([&] () mutable {
			++thread_initialized;
		});
	}

	for (auto i = 0u; i < cores; ++i) {
		threadsp[i].join();
	}
	delete []threadsp;
	EXPECT_EQ(thread_initialized, cores);
}

TEST(ThreadPoolTests, Exception) {
	/* ThreadPool with no threads */
	EXPECT_THROW(ThreadPool pool(0), std::invalid_argument);

	/* ThreadPool.AddTask() without a thread already started */
	ThreadPool pool(1);
	EXPECT_THROW(pool.AddTask([&] () { }), std::invalid_argument);
}

TEST(ThreadPoolTests, CreateAndDestroy) {
	for (auto i = 1; i < 8; ++i) {
		ThreadPool pool(i);
		pool.CreateThreads();

		auto stats = pool.Stats();
		EXPECT_EQ(stats.nthreads_, i);
		EXPECT_EQ(stats.remote_task_scheduled_, 0);
		EXPECT_EQ(stats.local_task_scheduled_, 0);
	}

	{
		auto cores = std::thread::hardware_concurrency();
		ThreadPool pool(cores);
		pool.CreateThreads();

		auto stats = pool.Stats();
		EXPECT_EQ(stats.nthreads_, cores);
		EXPECT_EQ(stats.remote_task_scheduled_, 0);
		EXPECT_EQ(stats.local_task_scheduled_, 0);
	}
}

TEST(ThreadPoolTests, ScheduleRemoteTasks) {
	std::vector<folly::Future<int>> futures;
	const auto kRemoteTasks = 64;

	auto cores = std::thread::hardware_concurrency();
	ThreadPool pool(cores);
	pool.CreateThreads();

	std::atomic<uint64_t> task_ran{0};
	for (auto i = 0; i < kRemoteTasks; ++i) {
		folly::Promise<int> promise;
		futures.push_back(promise.getFuture());
		pool.AddTask([&task_ran, promise = std::move(promise)] () mutable {
			++task_ran;
			promise.setValue(0);
		});
	}

	folly::collectAll(std::move(futures))
	.then([] (const std::vector<folly::Try<int>>& tries) {
		for (const auto& t : tries) {
			EXPECT_EQ(t.value(), 0);
		}
	})
	.wait();

	EXPECT_EQ(task_ran, kRemoteTasks);

	auto stats = pool.Stats();
	EXPECT_EQ(stats.nthreads_, cores);
	EXPECT_EQ(stats.remote_task_scheduled_, kRemoteTasks);
	EXPECT_EQ(stats.local_task_scheduled_, 0);
}

TEST(ThreadPoolTests, ScheduleLocalTasks) {
	std::vector<folly::Future<int>> remote_futures;
	const auto kRemoteTasks = 32;
	const auto kLocalTasks = 512;

	auto cores = std::thread::hardware_concurrency();
	ThreadPool pool(cores);
	pool.CreateThreads();

	std::atomic<uint64_t> remote_task_ran{0};
	std::atomic<uint64_t> local_task_ran{0};
	for (auto i = 0; i < kRemoteTasks; ++i) {
		folly::Promise<int> promise;
		remote_futures.push_back(promise.getFuture());
		pool.AddTask([&, promise = std::move(promise)] () mutable {
			++remote_task_ran;

			std::vector<folly::Future<int>> local_futures;
			for (auto i = 0; i < kLocalTasks; ++i) {
				folly::Promise<int> p;
				local_futures.push_back(p.getFuture());
				pool.AddTask([&, p = std::move(p)] () mutable {
					++local_task_ran;
					p.setValue(0);
				});
			}

			folly::collectAll(std::move(local_futures))
			.then([promise = std::move(promise)]
					(const std::vector<folly::Try<int>>& tries) mutable {
				for (const auto& t : tries) {
					EXPECT_EQ(t.value(), 0);
				}
				promise.setValue(0);
			});
		});
	}

	folly::collectAll(std::move(remote_futures))
	.then([] (const std::vector<folly::Try<int>>& tries) {
		for (const auto& t : tries) {
			EXPECT_EQ(t.value(), 0);
		}
	})
	.wait();

	EXPECT_EQ(remote_task_ran, kRemoteTasks);
	EXPECT_EQ(local_task_ran, kLocalTasks * kRemoteTasks);

	auto stats = pool.Stats();
	EXPECT_EQ(stats.nthreads_, cores);
	EXPECT_EQ(stats.remote_task_scheduled_, kRemoteTasks);
	EXPECT_EQ(stats.local_task_scheduled_, kLocalTasks * kRemoteTasks);
}

TEST(ThreadPoolTests, SchedulerThread) {
	std::thread handle([&] () mutable {
		std::vector<folly::Future<int>> futures;

		auto cores = std::thread::hardware_concurrency();
		ThreadPool pool(cores);
		pool.CreateThreads();

		folly::Promise<int> promise;
		auto fut = promise.getFuture();
		auto task_ran = 0;

		pool.AddTask([&task_ran, promise = std::move(promise)] () mutable {
			++task_ran;
			promise.setValue(0);
		});

		fut.wait();
		EXPECT_TRUE(fut.isReady());
		EXPECT_EQ(fut.get(), 0);
		EXPECT_EQ(task_ran, 1);

		auto stats = pool.Stats();
		EXPECT_EQ(stats.nthreads_, cores);
		EXPECT_EQ(stats.remote_task_scheduled_, 1);
		EXPECT_EQ(stats.local_task_scheduled_, 0);
	});
	handle.join();
}
