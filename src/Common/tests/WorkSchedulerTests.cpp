#include <chrono>
#include <memory>

#include <folly/executors/ManualExecutor.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "WorkScheduler.h"

using namespace std::chrono_literals;
using namespace folly;
using namespace pio;

TEST(WorkSchedulerTest, BasicUse) {
	WorkScheduler scheduler;
	const auto kDelay = 10ms;
	int count = 0;

	auto basep = scheduler.GetEventBase();
	auto now = basep->now();
	auto f_int = scheduler.ScheduleAsync(kDelay, [&count] () mutable -> folly::Future<int> {
		return ++count;
	});
	EXPECT_EQ(count, 0);
	for (int i = 0; i < 10; ++i) {
		f_int.wait(1ms);
		if (f_int.isReady()) {
			EXPECT_LE(now + kDelay, basep->now());
			break;
		}
		EXPECT_FALSE(f_int.isReady());
	}
	f_int.wait();
	EXPECT_EQ(count, 1);
	EXPECT_TRUE(f_int.isReady());
	EXPECT_EQ(f_int.value(), 1);

	count = 0;
	auto f_vector = scheduler.ScheduleAsync(kDelay, [&count] () mutable {
		std::vector<int> rc;
		for (int i = 0; i < 10; ++i) {
			rc.emplace_back(++count);
		}
		return folly::makeFuture(std::move(rc));
	});
	EXPECT_EQ(count, 0);
	f_vector.wait();
	EXPECT_TRUE(f_int.isReady());
	for (const auto no : f_vector.value()) {
		EXPECT_LE(no, count);
	}
}

TEST(WorkSchedulerTest, DestructImmediate) {
	auto scheduler = std::make_unique<WorkScheduler>();

	auto f_int = scheduler->ScheduleAsync(100ms, [] () mutable {
		return folly::makeFuture(0);
	});
	scheduler = nullptr;
	EXPECT_EQ(f_int.value(), 0);
	// EXPECT_FALSE(f_int.isReady());
}
