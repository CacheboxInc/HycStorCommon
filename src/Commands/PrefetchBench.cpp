#include <cassert>

#include <memory>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <folly/init/Init.h>

#include "CommonMacros.h"
#include "prefetch/ghb.h"

constexpr int kIndexes = 32;
constexpr int kHistory = 1024;
constexpr int kLookback = 8;
constexpr int kPrefetchDepth = 8;
constexpr int kContext = 1;
constexpr size_t kNOps = 1 << 20ul;

static ghb_params_t GetDefaultGhbParams() {
	return {kIndexes, kHistory, kLookback, kPrefetchDepth};
}

using GhbPtr = std::unique_ptr<ghb_t, void (*) (ghb_t*)>;
static void DestroyGhb(ghb_t* ghbp) {
	::ghb_finalize(ghbp);
	delete ghbp;
}

static GhbPtr NewGhb() {
	ghb_params_t param = GetDefaultGhbParams();
	ghb_t* ghbp = new ghb_t;
	::ghb_init(ghbp, &param);
	return GhbPtr(ghbp, DestroyGhb);
}

static void TestGHB(bool create_handle, ghb_t* ghbp) {
	auto ghb = NewGhb();
	if (create_handle) {
		ghbp = ghb.get();
	}
	log_assert(ghbp);

	uint64_t prefetch[kPrefetchDepth];
	for (size_t lba = 0, count = 0; count < kNOps; ++count, lba += 512) {
		auto nprefetch = ghb_update_and_query(ghbp, kContext, lba, prefetch);
		(void) nprefetch;
	}
}

DEFINE_int32(nthreads, 1, "Number of threads");
DEFINE_bool(use_same_handle, false, "Turning this ON will cause segment fault");

int main(int argc, char* argv[]) {
	folly::init(&argc, &argv, true);

	ghb_t ghb;
	if (FLAGS_use_same_handle) {
		ghb_params_t param = GetDefaultGhbParams();
		::ghb_init(&ghb, &param);
	}

	std::vector<std::thread> threads;
	for (int32_t count = 0; count < FLAGS_nthreads; ++count) {
		threads.emplace_back(std::thread([&] () {
			TestGHB(not FLAGS_use_same_handle, &ghb);
		}));
	}

	for (auto& thread : threads) {
		thread.join();
	}

	if (FLAGS_use_same_handle) {
		::ghb_finalize(&ghb);
	}
	return 0;
}
