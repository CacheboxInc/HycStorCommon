#pragma once

#include <chrono>
#include <folly/io/async/AsyncTimeout.h>

namespace pio {

using TimeoutFunc = std::function<bool (void)>;

class RecurringTimer : public folly::AsyncTimeout {
public:
	RecurringTimer(std::chrono::milliseconds milli);
	RecurringTimer(folly::EventBase* basep, std::chrono::milliseconds milli);
	virtual ~RecurringTimer();
	void timeoutExpired() noexcept override;
	void ScheduleTimeout(TimeoutFunc func);
	void AttachToEventBase(folly::EventBase* basep);
private:
	void Cancel();
private:
	folly::EventBase* basep_{nullptr};
	bool cancel_{false};
	TimeoutFunc func_;
	std::chrono::milliseconds milli_;
};
}
