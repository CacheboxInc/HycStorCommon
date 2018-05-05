#include <cerrno>
#include <iterator>
#include <vector>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "Vmdk.h"
#include "CleanHandler.h"
#include "DaemonTgtInterface.h"

namespace pio {
CleanHandler::CleanHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	aero_obj_ = std::make_unique<AeroSpike>();
}

CleanHandler::~CleanHandler() {

}

folly::Future<int> CleanHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	auto aero_conn = pio::GetAeroConn(vmdkp);
	if (pio_unlikely(aero_conn == nullptr)) {
		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	return aero_obj_->AeroReadCmdProcess(vmdkp, reqp, process,
			failed, kAsNamespaceCacheClean, aero_conn)
	.then([this, vmdkp, reqp, &process, &failed] (int rc)
			mutable -> folly::Future<int> {
		if (pio_likely(rc != 0)) {
			return rc;
		}

		if (nextp_ != nullptr) {
			return nextp_->Read(vmdkp, reqp, process, failed);
		}

		/* No layer below, move all the miss into failed list so
		 * Miss layer can process it
		 */
		for (auto blockp : process) {
			if (pio_likely(blockp->IsReadHit())) {
				blockp->SetResult(0, RequestStatus::kSuccess);
			} else {
				failed.emplace_back(blockp);
			}
		}
		return 0;
	});
}

folly::Future<int> CleanHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> CleanHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(),
				std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

}
