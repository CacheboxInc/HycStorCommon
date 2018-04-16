#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "Vmdk.h"
#include "CleanHandler.h"

namespace pio {
CleanHandler::CleanHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
}

CleanHandler::~CleanHandler() {

}

folly::Future<int> CleanHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	/* Get Aerospike connection corresponding to the given cluster ID */
	auto aero_conn = vmdkp->GetAeroConnection();
	if (aero_conn != nullptr) {

		/* TBD : Read should come with ckpt ID, for now
		 * assuming that checkpoint ID is 0
		 */

		CheckPointID ckpt = 0;
		aero_obj_->AeroReadCmdProcess(vmdkp, reqp, ckpt, process,
			failed, kAsNamespaceCacheClean);
	}

	/* 
	 * If this is the last layer (No SUCCESS layer below) then create failed
	 * list here. All misses will be moved into failed list
	 */

	if (aero_conn != nullptr && nextp_ == nullptr) {
		for (auto blockp : process) {
			if (pio_likely(blockp->IsReadHit())) {
				blockp->SetResult(0, RequestStatus::kSuccess);
			} else {
				failed.emplace_back(blockp);
			}
		}
		return 0;
	} else {
		return nextp_->Read(vmdkp, reqp, process, failed);
	}
}

folly::Future<int> CleanHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
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
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

}
