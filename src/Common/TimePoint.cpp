#include "TimePoint.h"

namespace hyc {

TimePoint::TimePoint() : start_(Clock::time_point::min()) {

}

void TimePoint::Start() noexcept {
	start_ = Clock::now();
}

void TimePoint::Clear() noexcept {
	start_ = Clock::time_point::min();
}

bool TimePoint::IsStarted() const noexcept {
	return start_ != Clock::time_point::min();
}

int64_t TimePoint::GetMicroSec() const noexcept {
	if (not IsStarted()) {
		return 0;
	}

	auto diff = Clock::now() - start_;
	return std::chrono::duration_cast<Duration>(diff).count();
}

}