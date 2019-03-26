#pragma once

#include <vector>

#include <folly/futures/Future.h>

namespace pio {
class VddkFile;
class RequestBlock;

class VddkTarget {
public:
	VddkTarget() noexcept;
	~VddkTarget() noexcept;

	folly::Future<int> VddkWrite(VddkFile* filep,
		const std::vector<RequestBlock*>& process);
};
}
