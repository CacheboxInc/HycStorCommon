#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include <mutex>

#include <cstdint>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

#include "IDs.h"
#include "DaemonCommon.h"

namespace pio {

class FlushInstance {
public:
	FlushInstance(VmID vmid_);
	~FlushInstance();

	uint64_t flushed_blocks_{0};
	uint64_t moved_blocks_{0};
	int StartFlush(const VmID& vmid);
private:
	VmID vmid_;
};
}
