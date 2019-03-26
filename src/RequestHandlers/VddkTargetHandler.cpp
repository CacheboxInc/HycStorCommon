#include <vector>
#include <string>

#include <folly/futures/Future.h>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "DaemonCommon.h"
#include "Vmdk.h"
#include "Request.h"
#include "RequestHandler.h"
#include "VddkTargetHandler.h"
#include "VmdkConfig.h"

#include "VddkLib.h"
#include "VddkOps.h"

using namespace ::ondisk;

namespace pio {

VddkTargetLibHandler::VddkTargetLibHandler() noexcept :
		RequestHandler(VddkTargetLibHandler::kName, nullptr),
		vddk_file_(std::make_unique<VddkFile>()),
		target_(std::make_unique<VddkTarget>()) {
}

VddkTargetLibHandler::~VddkTargetLibHandler() {

}

folly::Future<int> VddkTargetLibHandler::Read(ActiveVmdk*, Request*,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	failed.clear();
	for (auto blockp : process) {
		blockp->SetResult(0, RequestStatus::kMiss);
	}

	std::copy(process.begin(), process.end(), std::back_inserter(failed));
	return 0;
}

folly::Future<int> VddkTargetLibHandler::Write(ActiveVmdk*, Request*,
		CheckPointID, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	failed.clear();
	return target_->VddkWrite(vddk_file_.get(), process)
	.then([&process, &failed] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			for (auto blockp : process) {
				blockp->SetResult(rc, RequestStatus::kFailed);
			}
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
		}
		return rc;
	});
}

folly::Future<int> VddkTargetLibHandler::ReadPopulate(ActiveVmdk *,
		Request *, const std::vector<RequestBlock*>&,
		std::vector<RequestBlock *>&) {
	log_assert(0);
	return 0;
}

folly::Future<int> VddkTargetLibHandler::BulkWrite(ActiveVmdk*,
		::ondisk::CheckPointID,
		const std::vector<std::unique_ptr<Request>>&,
		const std::vector<RequestBlock*>&,
		std::vector<RequestBlock*>&) {
	log_assert(0);
	return 0;
}

folly::Future<int> VddkTargetLibHandler::BulkRead(ActiveVmdk*,
		const std::vector<std::unique_ptr<Request>>&,
		const std::vector<RequestBlock*>&,
		std::vector<RequestBlock*>&) {
	log_assert(0);
	return 0;
}

folly::Future<int> VddkTargetLibHandler::BulkReadPopulate(ActiveVmdk*,
		const std::vector<std::unique_ptr<Request>>&,
		const std::vector<RequestBlock*>&,
		std::vector<RequestBlock*>&) {
	log_assert(0);
	return 0;
}
}
