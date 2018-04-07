#pragma once

#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include <folly/fibers/Fiber.h>
#include <folly/fibers/FiberManager.h>
#include <folly/fibers/GenericBaton.h>
#include <folly/io/async/EventBase.h>

#include "DaemonCommon.h"

namespace pio {

class Thread {
public:
	Thread(uint32_t /* unused */) noexcept;
	~Thread();

	Thread(const Thread&) = delete;
	Thread(Thread&&) = delete;
	Thread operator = (const Thread&) = delete;
	Thread operator = (Thread&&) = delete;

	void Loop();
	void StartThread();
	folly::fibers::FiberManager *GetFiberManager() const noexcept;

private:
	void WaitUntilReady() const;
	void SetFiberManager(folly::fibers::FiberManager *managerp) noexcept;
	void SetEventBase(folly::EventBase* basep) noexcept;
	void SetReady();
	folly::EventBase* GetEventBase() const noexcept;

private:
	struct {
		mutable std::mutex mutex_;
		mutable std::condition_variable cv_;
		bool complete_{false};
	} start_;

	struct {
		folly::fibers::GenericBaton baton_;
	} stop_;

	folly::fibers::FiberManager *managerp_{nullptr};
	folly::EventBase* basep_{nullptr};

	/*
	 * =============
	 * || CAUTION ||
	 * =============
	 * thread handle must be last member of class Thread. This ensures even
	 * before thread is started all members of class Thread are properly
	 * initialized.
	 */
	std::thread handle_;
};

struct ThreadPoolStats {
	uint64_t nthreads_;
	uint64_t remote_task_scheduled_;
	uint64_t local_task_scheduled_;
};

class ThreadPool {
public:
	ThreadPool(uint32_t nthreads);
	~ThreadPool();

	ThreadPool(const ThreadPool& rhs) = delete;
	ThreadPool(ThreadPool&& rhs) = delete;
	ThreadPool operator = (const ThreadPool&) = delete;
	ThreadPool operator = (ThreadPool&&) = delete;

	void CreateThreads();
	ThreadPoolStats Stats() const noexcept;

	template<typename Lambda>
	void AddTask(Lambda&& func) {
		if (pio_unlikely(threads_.empty())) {
			throw std::invalid_argument("Threads not started");
		}

		if (not IsRunningInThreadPool()) {
			auto t = stats_.remote_task_scheduled_
				.fetch_add(1,std::memory_order_relaxed) % nthreads_;
			auto managerp = threads_[t]->GetFiberManager();
			managerp->addTaskRemote(std::forward<Lambda>(func));
		} else {
			stats_.local_task_scheduled_.fetch_add(1, std::memory_order_relaxed);
			auto threadp = GetThread();
			auto managerp = threadp->GetFiberManager();
			managerp->addTask(std::forward<Lambda>(func));
		}
	}

private:
	bool IsRunningInThreadPool() const noexcept;
	Thread* GetThread() const noexcept;
private:
	uint16_t nthreads_{0};
	std::vector<std::unique_ptr<Thread>> threads_;

	struct {
		std::atomic<uint64_t> remote_task_scheduled_{0};
		std::atomic<uint64_t> local_task_scheduled_{0};
	} stats_;
};
}