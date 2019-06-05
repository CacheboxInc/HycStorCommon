#include "TimePoint.h"

namespace hyc {

constexpr auto kTimePointMin = TimePoint::Clock::time_point::min();

TimePoint::TimePoint() : start_(kTimePointMin) {

}

void TimePoint::Start() noexcept {
	start_ = Clock::now();
}

void TimePoint::Clear() noexcept {
	start_ = kTimePointMin;
}

bool TimePoint::IsStarted() const noexcept {
	return start_ != kTimePointMin;
}

int64_t TimePoint::GetMicroSec() const noexcept {
	if (not IsStarted()) {
		return 0;
	}

	auto diff = Clock::now() - start_;
	return std::chrono::duration_cast<Duration>(diff).count();
}

}
