#pragma once

#include <memory>
#include <chrono>

namespace pio {
class Work {
public:
	using Clock = std::chrono::system_clock;
	using TimePoint = Clock::time_point;

	template <typename T>
	Work(std::shared_ptr<T> xp) :
			self_(std::make_unique<Model<T>>(std::move(xp))) {
	}

	template <typename T>
	Work(std::shared_ptr<T> xp, TimePoint run_at) :
			self_(std::make_unique<Model<T>>(std::move(xp))),
			run_at_(std::move(run_at)), delayed_(true) {
	}

	Work(Work&& x) noexcept = default;
	Work& operator = (Work&& x) noexcept = default;

	void DoWork() {
		self_->DoWork_();
		complete_ = true;
	}

	const TimePoint& RunAt() const noexcept {
		return run_at_;
	}

	bool IsDelayed() const noexcept {
		return delayed_;
	}

	void Cancel() noexcept {
		cancel_ = true;
	}

	bool IsCancelled() const noexcept {
		return cancel_;
	}

	bool IsComplete() const noexcept {
		return complete_;
	}

private:
	struct Concept {
		virtual ~Concept() = default;
		virtual void DoWork_() = 0;
	};

	template <typename T>
	struct Model : public Concept {
		Model(std::shared_ptr<T> xp) : userp_(std::move(xp)) {
		}

		void DoWork_() {
			userp_->DoWork();
		}

		std::shared_ptr<T> userp_;
	};

private:
	std::unique_ptr<Concept> self_;
	TimePoint run_at_{Clock::now()};
	bool delayed_{false};
	bool cancel_{false};
	bool complete_{false};
};
}