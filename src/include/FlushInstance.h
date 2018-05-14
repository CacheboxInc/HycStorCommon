#pragma once

#include <cstdint>

#include "gen-cpp2/MetaData_types.h"

namespace pio {

class FlushInstance {
public:
	FlushInstance(::ondisk::VmID vmid_);
	~FlushInstance();

	uint64_t flushed_blocks_{0};
	uint64_t moved_blocks_{0};
	int StartFlush(const ::ondisk::VmID& vmid);
private:
	::ondisk::VmID vmid_;
};
}
