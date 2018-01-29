#pragma once

namespace pio {
class SpinLock {
public:
	SpinLock();
	void lock();
	void unlock();
private:
	std::atomic_flag flag_{ATOMIC_FLAG_INIT};
};
}