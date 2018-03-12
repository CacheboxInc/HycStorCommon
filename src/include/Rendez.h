#pragma once

#include <atomic>
#include <queue>

#include "SpinLock.h"

/* forward declaration */
namespace folly {
	namespace fibers {
		class Baton;
	}
}

namespace pio {
	class QLock;
}

namespace pio {
class Rendez {
public:
	Rendez();
	~Rendez();
	void TaskSleep(pio::QLock* qlockp = nullptr);
	bool TaskWakeUp();
	size_t TaskWakeUpAll();
	bool IsSleeping() const;
private:
	size_t WakeUp(size_t count);
private:
	struct {
		mutable SpinLock mutex_;
		std::queue<folly::fibers::Baton *> queue_;
	} waiters_;
};
}