#include <cassert>
#include <folly/io/async/EventBase.h>

#include "RecurringTimer.h"

namespace pio {
RecurringTimer::RecurringTimer(std::chrono::milliseconds milli) :
		folly::AsyncTimeout(), milli_(std::move(milli)) {
}

RecurringTimer::RecurringTimer(folly::EventBase* basep,
		std::chrono::milliseconds milli) : folly::AsyncTimeout(basep),
		basep_(basep), milli_(std::move(milli)) {
}

RecurringTimer::~RecurringTimer() {
	Cancel();
}

void RecurringTimer::AttachToEventBase(folly::EventBase* basep) {
	assert(not basep_);
	basep_ = basep;
	basep_->runInEventBaseThreadAndWait([this] () mutable {
		attachEventBase(basep_);
	});
}

void RecurringTimer::Cancel() {
	cancel_ = true;
	if (not isScheduled()) {
		return;
	}
	basep_->runInEventBaseThreadAndWait([this] () mutable {
		cancelTimeout();
	});
}

void RecurringTimer::timeoutExpired() noexcept {
	if (not cancel_ and func_()) {
		ScheduleTimeout(func_);
	}
}

void RecurringTimer::ScheduleTimeout(TimeoutFunc func) {
	if (cancel_) {
		return;
	}
	func_ = func;
	basep_->runInEventBaseThread([this] () mutable {
		scheduleTimeout(milli_);
	});
}
}
