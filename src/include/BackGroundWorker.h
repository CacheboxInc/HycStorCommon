#pragma once

#include <deque>
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace pio {
class Work;

class BackGroundWorkers {
public:
	BackGroundWorkers(int32_t nthreads);
	~BackGroundWorkers();
	void WorkAdd(std::shared_ptr<Work> work);
	bool HasWork() const noexcept;
	bool HasReadyWork() const noexcept;
private:
	void Run();
	void CheckDelayedWork();
	void WorkAddWithDelay(std::shared_ptr<Work> nw);
private:
	std::vector<std::thread> threads_;
	bool stop_{false};

	struct {
		mutable std::mutex mutex_;
		std::condition_variable cv_;
		/* delayed_ is sorted on time at which work should run */
		std::deque<std::weak_ptr<Work>> delayed_;
		std::deque<std::weak_ptr<Work>> runable_;
	} work_;
};
} // namespace pio
