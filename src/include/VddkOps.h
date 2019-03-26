#pragma once

#include <vector>

#include <folly/futures/Future.h>

namespace pio {
class ArmVddkFile;
class RequestBlock;

class VddkTarget {
public:
	VddkTarget() noexcept;
	~VddkTarget() noexcept;

	folly::Future<int> VddkWrite(ArmVddkFile* filep,
		const std::vector<RequestBlock*>& process);
};
}
