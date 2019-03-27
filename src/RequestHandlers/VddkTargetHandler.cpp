#include <vector>
#include <string>
#include <sstream>

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

VddkTargetHandler::VddkTargetHandler(ActiveVmdk*,
			VCenter* connp,
			const std::string& path):
			RequestHandler(VddkTargetHandler::kName, nullptr),
			vddk_file_(std::make_unique<VddkFile>(connp, path)),
			target_(std::make_unique<VddkTarget>()) {
	int rc = vddk_file_->Open();
	if (pio_unlikely(rc < 0)) {
		std::ostringstream os;
		os << "VddkTarget: VDDK open failed "
			<< " path " << path;
		LOG(ERROR) << os.str();
		throw std::runtime_error(os.str());
	}
}

VddkTargetHandler::~VddkTargetHandler() {
	vddk_file_ = nullptr;
}

folly::Future<int> VddkTargetHandler::Read(ActiveVmdk*, Request*,
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

folly::Future<int> VddkTargetHandler::Write(ActiveVmdk*, Request*,
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

folly::Future<int> VddkTargetHandler::ReadPopulate(ActiveVmdk *,
		Request *, const std::vector<RequestBlock*>&,
		std::vector<RequestBlock *>&) {
	log_assert(0);
	return 0;
}

folly::Future<int> VddkTargetHandler::BulkWrite(ActiveVmdk*,
		::ondisk::CheckPointID,
		const std::vector<std::unique_ptr<Request>>&,
		const std::vector<RequestBlock*>&,
		std::vector<RequestBlock*>&) {
	log_assert(0);
	return 0;
}

folly::Future<int> VddkTargetHandler::BulkRead(ActiveVmdk*,
		const std::vector<std::unique_ptr<Request>>&,
		const std::vector<RequestBlock*>&,
		std::vector<RequestBlock*>&) {
	log_assert(0);
	return 0;
}

folly::Future<int> VddkTargetHandler::BulkReadPopulate(ActiveVmdk*,
		const std::vector<std::unique_ptr<Request>>&,
		const std::vector<RequestBlock*>&,
		std::vector<RequestBlock*>&) {
	log_assert(0);
	return 0;
}
}
