#include "WorkScheduler.h"

namespace pio {
WorkScheduler::WorkScheduler() {
	thread_ = std::thread([this] () mutable {
		this->ThreadMain();
	});
	base_.waitUntilRunning();
}

WorkScheduler::~WorkScheduler() {
	base_.terminateLoopSoon();
	thread_.join();
}

folly::EventBase* WorkScheduler::GetEventBase() noexcept {
	return &base_;
}

void WorkScheduler::ThreadMain() {
	base_.loopForever();
}
}
