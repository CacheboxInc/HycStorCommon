#pragma once

#include <chrono>

namespace hyc {

class TimePoint {
public:
	using Clock = std::chrono::steady_clock;
	using Duration = std::chrono::microseconds;

	TimePoint();
	void Start() noexcept;
	void Clear() noexcept;
	bool IsStarted() const noexcept;
	int64_t GetMicroSec() const noexcept;
private:
	Clock::time_point start_;
};

}