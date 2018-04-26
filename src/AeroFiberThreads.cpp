#include "AeroFiberThreads.h"

namespace pio {
int AeroFiberThreads::CreateInstance() {
	auto cores = std::thread::hardware_concurrency();
        try {
                std::call_once(threadpool_.initialized_, [=] () mutable {
                        threadpool_.pool_ = std::make_unique<ThreadPool>(cores);
                        threadpool_.pool_->CreateThreads();
                });
        } catch (const std::exception& e) {
		threadpool_.pool_ = nullptr;
		return -ENOMEM;
	}
	
	return 0;
}

/* TBD : Fill it to destory the threadpool at time of deinit */
void AeroFiberThreads::FreeInstance() {
	return;
}

}
