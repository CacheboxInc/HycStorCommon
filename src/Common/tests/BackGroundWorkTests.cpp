#include <atomic>
#include <string>
#include <chrono>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "Work.h"
#include "BackGroundWorker.h"

using namespace pio;
using namespace std::chrono_literals;

struct StringWork {
	StringWork(std::string str, std::atomic<uint32_t>* counterp) : str_(std::move(str)),
			counterp_(counterp) {
	}

	void DoWork() {
		counterp_->fetch_add(1);
	}

	std::string str_;
	std::atomic<uint32_t>* counterp_;
};

struct IntWork {
	IntWork(int32_t no, std::atomic<uint32_t>* counterp) : no_(no),
			counterp_(counterp) {
	}

	void DoWork() {
		counterp_->fetch_add(1);
		do_work_called_ = true;
	}

	int32_t no_{};
	std::atomic<uint32_t>* counterp_;
	bool do_work_called_{false};
};

TEST(BackGroundWorkerTests, Throws) {
	EXPECT_THROW(BackGroundWorkers(-1), std::invalid_argument);
	EXPECT_THROW(BackGroundWorkers(0), std::invalid_argument);
}

TEST(BackGroundWorkerTests, WorkNow) {
	const int kWorks = 10;
	BackGroundWorkers worker(1);
	EXPECT_FALSE(worker.HasWork());

	std::atomic<uint32_t> counter{0};
	std::vector<std::shared_ptr<Work>> works;
	std::vector<std::shared_ptr<StringWork>> str_works;
	std::vector<std::shared_ptr<IntWork>> int_works;
	for (auto i = 0; i < kWorks; ++i) {
		auto sw = std::make_shared<StringWork>(std::to_string(i), &counter);
		auto w = std::make_shared<Work>(sw);
		works.emplace_back(std::move(w));
		str_works.emplace_back(std::move(sw));

		auto iw = std::make_shared<IntWork>(i, &counter);
		w = std::make_shared<Work>(iw);
		works.emplace_back(std::move(w));
		int_works.emplace_back(std::move(iw));
	}

	for (auto w : works) {
		worker.WorkAdd(w);
	}

	::sleep(1);
	EXPECT_FALSE(worker.HasWork());
	EXPECT_EQ(counter.load(), kWorks * 2);
	for (const auto& w : works) {
		EXPECT_TRUE(w->IsComplete());
	}
}

TEST(BackGroundWorkerTests, WorkLater) {
	const int kWorks = 5;
	const auto kMaxSecs = std::chrono::seconds(kWorks);
	BackGroundWorkers worker(1);
	EXPECT_FALSE(worker.HasWork());

	std::vector<std::shared_ptr<Work>> works;
	std::vector<std::shared_ptr<StringWork>> str_works;
	std::vector<std::shared_ptr<IntWork>> int_works;
	std::atomic<uint32_t> counter{0};
	for (auto [sec, j] = std::make_pair(0s, 0); sec < kMaxSecs; ++sec, ++j) {
		auto name = std::to_string(kWorks - j);
		auto sw = std::make_shared<StringWork>(name, &counter);
		auto w = std::make_shared<Work>(sw, Work::Clock::now() + (kMaxSecs - sec));
		works.emplace_back(std::move(w));
		str_works.emplace_back(std::move(sw));

		auto iw = std::make_shared<IntWork>(j, &counter);
		w = std::make_shared<Work>(iw, Work::Clock::now() + sec);
		works.emplace_back(std::move(w));
		int_works.emplace_back(std::move(iw));
	}

	for (auto w : works) {
		worker.WorkAdd(w);
	}

	EXPECT_TRUE(worker.HasWork());
	for (auto i = 0; i < kWorks; ++i) {
		EXPECT_TRUE(worker.HasWork());
		::sleep(1);
	}
	EXPECT_FALSE(worker.HasWork());
	EXPECT_EQ(counter.load(), kWorks * 2);
	for (const auto& w : works) {
		EXPECT_TRUE(w->IsComplete());
	}
}

TEST(BackGroundWorkerTests, WorkDelete) {
	std::atomic<uint32_t> counter{0};
	BackGroundWorkers worker(2);

	{
		auto iw1 = std::make_shared<IntWork>(1, &counter);
		auto w1 = std::make_shared<Work>(iw1, Work::Clock::now() + 1s);
		worker.WorkAdd(w1);
		/* setting shared_ptr to nullptr deletes the work */
		w1 = nullptr;

		auto iw2 = std::make_shared<IntWork>(1, &counter);
		auto w2 = std::make_shared<Work>(iw2, Work::Clock::now() + 1s);
		worker.WorkAdd(w2);
		EXPECT_TRUE(worker.HasWork());

		::sleep(2);
		EXPECT_FALSE(worker.HasWork());
		EXPECT_EQ(counter.load(), 1);
		EXPECT_TRUE(w2->IsComplete());
		EXPECT_TRUE(iw2->do_work_called_);
		EXPECT_FALSE(iw1->do_work_called_);
		counter.store(0);
	}

	{
		const auto kLoop = 3;
		for (auto i = 0; i < kLoop; ++i) {
			std::vector<std::shared_ptr<IntWork>> int_works;
			std::vector<std::shared_ptr<Work>> works;
			auto sec = 0s;
			for (auto j = 0; j < kLoop; ++j, ++sec) {
				auto iw = std::make_shared<IntWork>(1, &counter);
				auto w = std::make_shared<Work>(iw, Work::Clock::now() + sec);
				worker.WorkAdd(w);
				int_works.emplace_back(std::move(iw));
				if (i != j) {
					works.emplace_back(std::move(w));
				}
			}

			::sleep(kLoop);

			for (auto j = 0; j < kLoop; ++j) {
				const auto& w = int_works[j];
				if (i == j) {
					EXPECT_FALSE(w->do_work_called_);
				} else {
					EXPECT_TRUE(w->do_work_called_);
				}
			}
		}
	}
}

TEST(BackGroundWorkerTests, WorkCancel) {
	std::atomic<uint32_t> counter{0};
	BackGroundWorkers worker(2);

	{
		auto iw1 = std::make_shared<IntWork>(2, &counter);
		auto w1 = std::make_shared<Work>(iw1, Work::Clock::now() + 2s);
		worker.WorkAdd(w1);
		w1->Cancel();

		auto iw2 = std::make_shared<IntWork>(1, &counter);
		auto w2 = std::make_shared<Work>(iw2, Work::Clock::now() + 1s);
		worker.WorkAdd(w2);
		EXPECT_TRUE(worker.HasWork());

		::sleep(2);
		EXPECT_FALSE(worker.HasWork());
		EXPECT_FALSE(iw1->do_work_called_);
		EXPECT_TRUE(iw2->do_work_called_);
		EXPECT_EQ(counter.load(), 1);
		counter.store(0);
	}

	{
		auto iw1 = std::make_shared<IntWork>(1, &counter);
		auto w1 = std::make_shared<Work>(iw1, Work::Clock::now() + 1s);
		worker.WorkAdd(w1);
		w1->Cancel();

		auto iw2 = std::make_shared<IntWork>(2, &counter);
		auto w2 = std::make_shared<Work>(iw2, Work::Clock::now() + 2s);
		worker.WorkAdd(w2);
		EXPECT_TRUE(worker.HasWork());

		::sleep(3);
		EXPECT_FALSE(worker.HasWork());
		EXPECT_FALSE(iw1->do_work_called_);
		EXPECT_TRUE(iw2->do_work_called_);
		EXPECT_EQ(counter.load(), 1);
		counter.store(0);
	}
}

TEST(BackGroundWorkerTests, DestructorTest) {
	auto cores = std::thread::hardware_concurrency();
	auto worker = std::make_unique<BackGroundWorkers>(cores ? cores : 1);
	std::atomic<uint32_t> counter{0};
	auto iw1 = std::make_shared<IntWork>(2, &counter);
	auto w1 = std::make_shared<Work>(iw1, Work::Clock::now() + 2s);
	worker->WorkAdd(w1);

	worker = nullptr;

	EXPECT_TRUE(counter.load() == 0);
	EXPECT_FALSE(iw1->do_work_called_);
	EXPECT_FALSE(w1->IsComplete());
}

struct TimedWork {
	TimedWork(std::chrono::milliseconds delay) : delay_(delay) {

	}

	void DoWork() {
		end = Work::Clock::now();
		do_work_called_ = true;
	}

	Work::TimePoint start{Work::Clock::now()};
	Work::TimePoint end{Work::Clock::now()};
	std::chrono::milliseconds delay_;
	bool do_work_called_{false};
};

TEST(BackGroundWorkerTests, TimeTest) {
	auto cores = std::thread::hardware_concurrency();
	BackGroundWorkers worker(cores ? cores : 1);

	{
		EXPECT_FALSE(worker.HasWork());
		auto tw1 = std::make_shared<TimedWork>(10ms);
		auto w1 = std::make_shared<Work>(tw1, Work::Clock::now() + tw1->delay_);
		auto tw2 = std::make_shared<TimedWork>(100ms);
		auto w2 = std::make_shared<Work>(tw2, Work::Clock::now() + tw2->delay_);
		worker.WorkAdd(w1);
		worker.WorkAdd(w2);
		::sleep(1);

		EXPECT_FALSE(worker.HasWork());
		EXPECT_TRUE(tw1->do_work_called_);
		EXPECT_TRUE(tw2->do_work_called_);
		EXPECT_GE(tw1->end, tw1->start + tw1->delay_);
		EXPECT_GE(tw2->end, tw2->start + tw2->delay_);
		EXPECT_GE(tw2->end, tw1->end);
	}

	{
		EXPECT_FALSE(worker.HasWork());
		auto tw1 = std::make_shared<TimedWork>(10ms);
		auto w1 = std::make_shared<Work>(tw1, Work::Clock::now() + tw1->delay_);
		auto tw2 = std::make_shared<TimedWork>(100ms);
		auto w2 = std::make_shared<Work>(tw2, Work::Clock::now() + tw2->delay_);
		worker.WorkAdd(w2);
		worker.WorkAdd(w1);
		::sleep(1);

		EXPECT_FALSE(worker.HasWork());
		EXPECT_TRUE(tw1->do_work_called_);
		EXPECT_TRUE(tw2->do_work_called_);
		EXPECT_GE(tw1->end, tw1->start + tw1->delay_);
		EXPECT_GE(tw2->end, tw2->start + tw2->delay_);
		EXPECT_GE(tw2->end, tw1->end);
	}

	{
		std::vector<std::vector<std::chrono::milliseconds>> tests = {
			{10ms, 100ms},
			{100ms, 10ms},
			{10ms, 100ms, 20ms, 90ms},
			{100ms, 10ms, 90ms, 20ms},
			{10ms, 20ms, 90ms, 100ms},
			{100ms, 90ms, 20ms, 10ms},
		};
		EXPECT_TRUE(tests.size() == 6);

		for (const auto& test : tests) {
			EXPECT_FALSE(worker.HasWork());
			std::vector<std::shared_ptr<TimedWork>> timed_works;
			for (const auto& ms : test) {
				timed_works.emplace_back(std::make_shared<TimedWork>(ms));
			}

			std::vector<std::shared_ptr<Work>> works;
			for (auto tw : timed_works) {
				auto w = std::make_shared<Work>(tw, Work::Clock::now() + tw->delay_);
				works.emplace_back(w);
				worker.WorkAdd(w);
			}
			::sleep(1);

			EXPECT_FALSE(worker.HasWork());
			for (auto tw : timed_works) {
				EXPECT_TRUE(tw->do_work_called_);
				EXPECT_GE(tw->end, tw->start + tw->delay_);
			}
		}
	}
}
