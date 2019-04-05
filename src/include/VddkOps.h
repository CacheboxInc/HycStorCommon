#pragma once

#include <vector>

#include <folly/futures/Future.h>

namespace vddk {
class VddkFile;
}

namespace pio {
class RequestBlock;

class VddkTarget {
public:
	folly::Future<int> VddkWrite(vddk::VddkFile* filep,
		const std::vector<RequestBlock*>& process);
};
}
