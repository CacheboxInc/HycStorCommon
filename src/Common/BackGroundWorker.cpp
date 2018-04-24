#include <cassert>
#include <exception>

#include <glog/logging.h>

#include "BackGroundWorker.h"
#include "Work.h"
#include "DaemonCommon.h"

namespace pio {

BackGroundWorkers::BackGroundWorkers(int32_t nthreads) {
	if (nthreads <= 0) {
		throw std::invalid_argument("Number of threads must be specified");
	}
	for (auto i = 0; i < nthreads; ++i) {
		threads_.emplace_back(std::thread([this] () mutable {
			this->Run();
		}));
	}
}

BackGroundWorkers::~BackGroundWorkers() {
	std::unique_lock<std::mutex> lock(work_.mutex_);
	stop_ = true;
	work_.cv_.notify_all();
	lock.unlock();
	for (auto& thread : threads_) {
		thread.join();
	}
}

void BackGroundWorkers::CheckDelayedWork() {
	auto now = Work::Clock::now();
	while (not work_.delayed_.empty()) {
		auto work = work_.delayed_.front().lock();
		if (not work or work->IsCancelled() or work->RunAt() <= now) {
			auto w = std::move(work_.delayed_.front());
			work_.delayed_.pop_front();
			work_.runable_.emplace_back(std::move(w));
			continue;
		}
		break;
	}
}

bool BackGroundWorkers::HasWork() const noexcept {
	return not (work_.delayed_.empty() and work_.runable_.empty());
}

bool BackGroundWorkers::HasReadyWork() const noexcept {
	return not work_.runable_.empty();
}

void BackGroundWorkers::Run() {
	while (1) {
		std::unique_lock<std::mutex> lock(work_.mutex_);
		if (pio_unlikely(stop_)) {
			break;
		}

		CheckDelayedWork();

		if (not HasWork()) {
			work_.cv_.wait(lock);
			continue;
		} else if (work_.runable_.empty()) {
			auto work = work_.delayed_.front().lock();
			log_assert(work);
			auto rc = work_.cv_.wait_until(lock, work->RunAt());
			if (rc == std::cv_status::no_timeout) {
				/* high priority work added */
				continue;
			}
			continue;
		}
		log_assert(HasReadyWork());

		std::vector<std::weak_ptr<Work>> runnable_works;
		runnable_works.reserve(work_.runable_.size());
		std::move(work_.runable_.begin(), work_.runable_.end(),
			std::back_inserter(runnable_works));
		work_.runable_.clear();
		lock.unlock();

		for (auto& w : runnable_works) {
			auto work = w.lock();
			if (not work or work->IsCancelled()) {
				continue;
			}
			work->DoWork();
		}
	}
}

void BackGroundWorkers::WorkAdd(std::shared_ptr<Work> work) {
	if (not work->IsDelayed()) {
		std::lock_guard<std::mutex> lock(work_.mutex_);
		work_.runable_.emplace_back(work);
		work_.cv_.notify_one();
		return;
	}

	WorkAddWithDelay(std::move(work));
}

void BackGroundWorkers::WorkAddWithDelay(std::shared_ptr<Work> nw) {
	auto notify = true;
	auto inserted = false;

	std::lock_guard<std::mutex> lock(work_.mutex_);
	auto eit = work_.delayed_.end();
	for (auto it = work_.delayed_.begin(); it != eit; ++it) {
		auto work = it->lock();
		if (not work) {
			continue;
		}
		if (work->RunAt() > nw->RunAt()) {
			work_.delayed_.insert(it, std::move(nw));
			inserted = true;
			break;
		}
		notify = false;
	}

	if (not inserted) {
		work_.delayed_.emplace_back(nw);
	}

	if (notify) {
		work_.cv_.notify_one();
	}
}
} // namespace pio
