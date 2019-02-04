#include <chrono>
#include <thread>
#include <algorithm>

#include <gtest/gtest.h>
#include <folly/futures/FutureSplitter.h>

#include "RangeLock.h"

using namespace pio;

using Guard = RangeLock::LockGuard;

TEST(RangeLockTest, LockGuard_Basic_InLoop) {
	pio::RangeLock::RangeLock range_lock;

	for (auto i = 0; i < 10; ++i) {
		pio::RangeLock::LockGuard lock(&range_lock, 0, 1);
		auto f = lock.Lock();
		EXPECT_TRUE(f.isReady());
	}
}

TEST(RangeLockTest, LockGuard_LockRange_Sorted) {
	pio::RangeLock::RangeLock range_lock;

	std::vector<pio::RangeLock::range_t> ranges(10);
	std::generate(ranges.begin(), ranges.end(), [n = 0] () mutable {
		auto p = n++;
		return std::make_pair(p*2, (p*2)+1);
	});

	pio::RangeLock::LockGuard lock(&range_lock, std::move(ranges));
	auto f = lock.Lock();
	EXPECT_TRUE(f.isReady());
}

TEST(RangeLockTest, LockGuard_LockRange_Sorted_Threaded) {
	using Guard = pio::RangeLock::LockGuard;
	using namespace std::chrono_literals;

	pio::RangeLock::RangeLock range_lock;

	std::vector<pio::RangeLock::range_t> ranges(10);
	std::generate(ranges.begin(), ranges.end(), [n = 0] () mutable {
		auto p = n++;
		return std::make_pair(p*2, (p*2)+1);
	});

	auto cores = std::thread::hardware_concurrency();
	std::vector<std::thread> threads;
	for (auto c = cores; c > 0; --c) {
		threads.emplace_back(std::thread([&range_lock, &ranges] () {
			const auto kLoop = 128;
			for (auto i = 0; i < kLoop; ++i) {
				auto r = ranges;
				auto g = Guard(&range_lock, std::move(r));
				auto f = g.Lock();
				f.wait(1s);
				EXPECT_TRUE(f.isReady());
			}
		}));
	}

	for (auto& thread : threads) {
		thread.join();
	}
}

TEST(RangeLockTest, LockGuard_LockRange_NotSorted) {
	pio::RangeLock::RangeLock range_lock;

	std::vector<pio::RangeLock::range_t> ranges(10);
	std::generate(ranges.begin(), ranges.end(), [n = 0] () mutable {
		auto p = n++;
		return std::make_pair(p*2, (p*2)+1);
	});

	std::random_shuffle(ranges.begin(), ranges.end());

	pio::RangeLock::LockGuard lock(&range_lock, std::move(ranges));
	auto f = lock.Lock();
	EXPECT_TRUE(f.isReady());
}

TEST(RangeLockTest, LockGuard_LockRange_Deadlock) {
	using namespace std::chrono_literals;

	pio::RangeLock::RangeLock range_lock;

	std::vector<pio::RangeLock::range_t> ranges(10);
	std::generate(ranges.begin(), ranges.end(), [n = 0] () mutable {
		/* adding coninciding ranges will result in deadlock */
		return std::make_pair(n, n+1);
	});

	std::random_shuffle(ranges.begin(), ranges.end());

#ifdef NDEBUG
	pio::RangeLock::LockGuard lock(&range_lock, std::move(ranges));
	auto f = lock.Lock();
	f.wait(100ms);
	EXPECT_FALSE(f.isReady());
	f.cancel();
#else
	EXPECT_ANY_THROW(pio::RangeLock::LockGuard lock(&range_lock, std::move(ranges)));
#endif
}

TEST(RangeLockTest, LockGuard_BasicConcurrent_InLock) {
	pio::RangeLock::RangeLock range_lock;

	for (auto i = 0; i < 10; ++i) {
		pio::RangeLock::LockGuard lock_outside(&range_lock, 0, 1);

		auto lock_func = [&lock_outside, &range_lock] {
			pio::RangeLock::LockGuard lock_inside(&range_lock, 0, 10);
			auto f1 = lock_inside.Lock();
			EXPECT_TRUE(f1.isReady());

			auto f2 = lock_outside.Lock();
			EXPECT_FALSE(f2.isReady());
			return f2;
		};

		auto f2 = lock_func();
		EXPECT_TRUE(f2.isReady());
	}
}

TEST(RangeLockTest, LockGuard_Future_Fullfilled) {
	const uint32_t kLocks = 100;
	auto range = std::make_pair(0, 1);

	pio::RangeLock::RangeLock range_lock;
	uint32_t locked = 0;
	for (uint32_t i = 0; i < kLocks; ++i) {
		auto guard = std::make_unique<Guard>(&range_lock, range.first, range.second);
		auto fut = guard->Lock()
		.then([guard = std::move(guard), &locked] () {
			EXPECT_TRUE(guard->IsLocked());
			++locked;
			return 0;
		});

		EXPECT_TRUE(fut.isReady());
		EXPECT_EQ(locked, i+1);

		auto locked = range_lock.TryLock(range);
		EXPECT_TRUE(locked);
		range_lock.Unlock(range);
	}
	EXPECT_EQ(locked, kLocks);
}

struct ThreadArgs {
	pio::RangeLock::RangeLock *range_lockp;
	std::vector<uint64_t>     per_thread_counter;
	uint64_t                  lock_range_start;
	uint64_t                  lock_range_end;
	size_t                    ntimes_to_lock;
	bool                      is_locked;
};

static void yield_thread(std::chrono::microseconds us) {
	auto start = std::chrono::high_resolution_clock::now();
	auto end = start + us;
	do {
		std::this_thread::yield();
	} while (std::chrono::high_resolution_clock::now() < end);
}

static void thread_lock_range(struct ThreadArgs *argsp, size_t thread_index) {
	std::vector<uint64_t>& counter_vecp = argsp->per_thread_counter;
	pio::RangeLock::RangeLock *range_lockp = argsp->range_lockp;
	uint64_t first = argsp->lock_range_start;
	uint64_t last = argsp->lock_range_end;
	size_t to_lock = argsp->ntimes_to_lock;

	for (; to_lock > 0; --to_lock) {
		auto guard = std::make_unique<Guard>(range_lockp, first, last);
		auto f = guard->Lock()
		.then([guard = std::move(guard), &counter_vecp, &thread_index, &argsp]
					(int) mutable {
			EXPECT_TRUE(guard->IsLocked());
			EXPECT_FALSE(argsp->is_locked);
			argsp->is_locked = true;
			counter_vecp[thread_index] += 1;
			yield_thread(std::chrono::microseconds(1000));
			argsp->is_locked = false;
			return 0;
		});
		EXPECT_EQ(f.get(), 0);
	}

}

TEST(RangeLockTest, LockGuard_Multiple_Threads) {
	pio::RangeLock::RangeLock range_lock;
	const auto kNThreads = 10;
	const size_t kTimesToLockRange = 100;

	ThreadArgs args;
	args.range_lockp = &range_lock;
	args.per_thread_counter.reserve(kNThreads);
	args.lock_range_start = 0;
	args.lock_range_end = 4095;
	args.ntimes_to_lock = kTimesToLockRange;
	args.is_locked = false;

	std::vector<std::thread> threads;
	std::vector<uint64_t> lock_counter;

	threads.reserve(kNThreads);
	lock_counter.reserve(kNThreads);

	for (size_t i = 0; i < kNThreads; ++i) {
		threads.emplace_back(std::thread(thread_lock_range, &args, i));
	}

	for (auto& thread : threads) {
		thread.join();
	}

	for (auto counter : lock_counter) {
		EXPECT_EQ(counter, kTimesToLockRange);
	}
}

TEST(RangeLockTest, Basic_TryLock) {
	pio::RangeLock::RangeLock range_lock;

	for (auto i = 0; i < 1; ++i) {
		auto range = std::make_pair(10, 200);

		EXPECT_TRUE(range_lock.TryLock(range));
		EXPECT_FALSE(range_lock.TryLock(range));

		range_lock.Unlock(range);

		EXPECT_TRUE(range_lock.TryLock(range));
		EXPECT_FALSE(range_lock.TryLock(range));

		range_lock.Unlock(range);
	}

	auto range = std::make_pair(0, 200);
	EXPECT_TRUE(range_lock.TryLock(range));
	for (auto i = range.first; i <= range.second; ++i) {
		auto r = std::make_pair(i, i);
		EXPECT_FALSE(range_lock.TryLock(r));
	}
}

TEST(RangeLockTest, Basic_TryLock_Fail) {
	pio::RangeLock::RangeLock range_lock;

	auto locked_range = std::make_pair(100, 150);
	auto locked = range_lock.TryLock(locked_range);
	EXPECT_TRUE(locked);

	{
		auto range = std::make_pair(50, locked_range.first);
		auto locked = range_lock.TryLock(range);
		EXPECT_FALSE(locked);
	}

	{
		auto range = std::make_pair(50, locked_range.first + 1);
		auto locked = range_lock.TryLock(range);
		EXPECT_FALSE(locked);
	}

	{
		auto range = std::make_pair(locked_range.first + 1, locked_range.second - 1);
		auto locked = range_lock.TryLock(range);
		EXPECT_FALSE(locked);
	}

	{
		auto range = std::make_pair(locked_range.second, locked_range.second);
		auto locked = range_lock.TryLock(range);
		EXPECT_FALSE(locked);
	}

	{
		auto range = std::make_pair(locked_range.first, locked_range.first);
		auto locked = range_lock.TryLock(range);
		EXPECT_FALSE(locked);
	}

	{
		auto range = std::make_pair(locked_range.second, locked_range.second + 10);
		auto locked = range_lock.TryLock(range);
		EXPECT_FALSE(locked);
	}
}
