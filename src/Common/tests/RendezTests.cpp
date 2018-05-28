#include <vector>
#include <memory>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "ThreadPool.h"
#include "Rendez.h"
#include "QLock.h"
#include "Work.h"
#include "BackGroundWorker.h"

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

using WorkId = uint64_t;

class WorkStats {
public:
	void WorkScheduled() {
		std::lock_guard<QLock> lock(qlock);
		++outstanding_;
	}

	void WorkComplete() {
		std::lock_guard<QLock> lock(qlock);
		--outstanding_;
		rendez.TaskWakeUp();
	}

	void WaitForWorkCompletion() {
		qlock.lock();
		if (not IsWorkScheduled()) {
			qlock.unlock();
			return;
		}
		rendez.TaskSleep(&qlock);
		qlock.unlock();
	}

	bool IsWorkScheduled() const noexcept {
		return not (outstanding_ == 0);
	}

	uint64_t GetWorkScheduledCount() const noexcept {
		return outstanding_.load();
	}

private:
	QLock qlock;
	Rendez rendez;
	std::atomic<uint64_t> outstanding_{0};
};

struct RendezWork {
	RendezWork(WorkStats* work_statsp, uint64_t id) :
			work_statsp_(work_statsp), work_id(id) {
	}

	void DoWork() {
		lock.lock();
		auto rc = rendez.TaskWakeUp();
		EXPECT_EQ(rc, 1);
		complete = true;
		lock.unlock();
		work_statsp_->WorkComplete();
	}

	WorkStats* work_statsp_{nullptr};
	uint64_t work_id{0};
	QLock lock;
	Rendez rendez;
	bool complete{false};
};

TEST(RendezTest, Producer_BackGroundConsumer) {
	using namespace std::chrono_literals;

	auto cores = std::thread::hardware_concurrency();
	auto threads = cores / 2 ? cores / 2 : 1;
	ThreadPool pool(threads);
	pool.CreateThreads();

	folly::Promise<int> promise;
	auto fut = promise.getFuture();
	pool.AddTask([promise = std::move(promise), threads, &pool] () mutable {
		const auto kWorks = 1ull << 20;
		const auto kDepth = 8u;
		WorkStats work_stats;
		WorkId to_submit{kDepth};
		BackGroundWorkers worker(threads);
		std::atomic<uint64_t> outstanding{0};

		for (auto id = 0ull; id < kWorks or work_stats.IsWorkScheduled(); ) {
			for (WorkId r = 0; r < to_submit; ++r, ++id) {
				//auto delay = ::rand() % 100;
				auto rendez_work = std::make_shared<RendezWork>(&work_stats, id);
				auto work = std::make_shared<Work>(rendez_work);
				work_stats.WorkScheduled();

				auto lockp = &rendez_work->lock;
				lockp->lock();
				worker.WorkAdd(work);
				pool.AddTask([rendez_work, work, &outstanding, lockp] () mutable {
					auto rendezp = &rendez_work->rendez;
					EXPECT_FALSE(rendez_work->complete);
					rendezp->TaskSleep(lockp);
					lockp->unlock();
					EXPECT_TRUE(rendez_work->complete);
					--outstanding;
				});
				++outstanding;
				if ((id & (id - 1)) == 0) {
					std::cout << "submitted " << work_stats.GetWorkScheduledCount()
						<< " ID " << id
						<< " outstanding " << outstanding.load()
						<< std::endl;
				}
			}

			work_stats.WaitForWorkCompletion();
			auto submitted = work_stats.GetWorkScheduledCount();
			EXPECT_LT(submitted, kDepth);
			to_submit = kDepth - submitted;
			if (id >= kWorks) {
				to_submit = 0;
			}
		}

		EXPECT_FALSE(work_stats.IsWorkScheduled());
		EXPECT_FALSE(worker.HasWork());
		promise.setValue(0);
	});

	fut.wait();
}