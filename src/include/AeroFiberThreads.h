#pragma once

#include "ThreadPool.h"

namespace pio {
class AeroFiberThreads {
public:
	struct threadpool {
		std::once_flag initialized_;
		std::unique_ptr<ThreadPool> pool_;
	} threadpool_;

	int CreateInstance();
	void FreeInstance();
};
}
