#pragma once

#include <chrono>
#include <thread>

#include <folly/io/async/EventBase.h>
#include <folly/futures/Future.h>

namespace pio {
class WorkScheduler {
public:
	WorkScheduler();
	~WorkScheduler();
	folly::EventBase* GetEventBase() noexcept;

	template <typename Lambda>
	void Schedule(std::chrono::milliseconds&& delay, Lambda&& func) {
		base_.schedule(std::forward<Lambda>(func), delay);
	}

	template <typename Lambda>
	auto ScheduleAsync(const std::chrono::milliseconds& delay, Lambda&& func)
			-> folly::Future<typename decltype(func())::value_type> {
		folly::Promise<typename decltype(func())::value_type> promise;
		auto f = promise.getFuture();
		base_.runInEventBaseThread([this, promise = std::move(promise),
				func = std::move(func), delay] () mutable {
			base_.schedule([promise = std::move(promise),
					func = std::move(func)] () mutable {
				func()
				.then([promise = std::move(promise)] (auto& rc) mutable {
					promise.setValue(rc);
				});
			}, delay);
		});
		return f;
	}

private:
	void ThreadMain();
private:
	std::thread thread_;
	folly::EventBase base_;
};
}
