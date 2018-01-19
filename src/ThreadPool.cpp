#include <atomic>
#include <thread>
#include <vector>

#include <folly/fibers/FiberManager.h>
#include <folly/fibers/EventBaseLoopController.h>
#include <folly/io/async/EventBaseManager.h>

#include "ThreadPool.h"
#include "Common.h"

using namespace folly::fibers;

namespace pio {
thread_local std::atomic<Thread *> this_threadp{nullptr};

ThreadPool::ThreadPool(uint32_t nthreads) : nthreads_(nthreads) {
	if (not nthreads_) {
		throw std::invalid_argument("Number of threads must be > 0");
	}
}

ThreadPool::~ThreadPool() {
	while (not threads_.empty()) {
		threads_.pop_back();
	}
}

void ThreadPool::CreateThreads() {
	threads_.reserve(nthreads_);
	for (auto i = 0; i < nthreads_; ++i) {
		auto thread = std::make_unique<Thread>(i);
		thread->StartThread();
		threads_.emplace_back(std::move(thread));
	}
}

bool ThreadPool::IsRunningInThreadPool() const {
	auto threadp = GetThread();
	return threadp != nullptr;
}

Thread* ThreadPool::GetThread() const {
	return this_threadp.load(std::memory_order_relaxed);
}

ThreadPoolStats ThreadPool::Stats() const {
	ThreadPoolStats stats;
	stats.nthreads_ = nthreads_;
	stats.remote_task_scheduled_ = stats_.remote_task_scheduled_.load();
	stats.local_task_scheduled_ = stats_.local_task_scheduled_.load();
	return stats;
}

Thread::Thread(uint32_t id) noexcept : id_(id) {
}

Thread::~Thread() {
	this->stop_.baton_.post();
	handle_.join();
}

void Thread::Loop() {
	folly::EventBase base;

	FiberManager manager(std::make_unique<EventBaseLoopController>());
	SetFiberManager(&manager);
	SetEventBase(&base);

	auto& controller =
		dynamic_cast<EventBaseLoopController&>(manager.loopController());
	controller.attachEventBase(base);

	manager.addTask([&] () mutable {
		this->stop_.baton_.wait();
		base.terminateLoopSoon();
	});

	SetReady();
	base.loopForever();
}

void Thread::StartThread() {
	handle_ = std::thread([&] () mutable {
		this_threadp.store(this, std::memory_order_release);
		this->Loop();
	});
	WaitUntilReady();
}

void Thread::SetFiberManager(folly::fibers::FiberManager *managerp) noexcept {
	managerp_ = managerp;
}

FiberManager* Thread::GetFiberManager() const noexcept {
	return managerp_;
}

folly::EventBase* Thread::GetEventBase() const noexcept {
	return basep_;
}

void Thread::SetEventBase(folly::EventBase* basep) noexcept {
	basep_ = basep;
}

void Thread::WaitUntilReady() const {
	std::unique_lock<std::mutex> lock(start_.mutex_);
	while (not start_.complete_) {
		start_.cv_.wait(lock);
	}
	auto basep = GetEventBase();
	assert(basep != nullptr);

	basep->waitUntilRunning();
}

void Thread::SetReady() {
	std::unique_lock<std::mutex> lock(start_.mutex_);
	start_.complete_ = true;
	start_.cv_.notify_all();
}

}