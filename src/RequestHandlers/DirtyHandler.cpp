#include <cerrno>

#include <iterator>
#include <vector>

#include "VmdkConfig.h"
#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "Vmdk.h"
#include "DirtyHandler.h"

namespace pio {
DirtyHandler::DirtyHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	aero_obj_ = std::make_unique<AeroSpike>();
}

DirtyHandler::~DirtyHandler() {

}

folly::Future<int> DirtyHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	/* Get connection corresponding to the given cluster ID */
	auto aero_conn = vmdkp->GetAeroConnection();
	if (aero_conn != nullptr) {
		/* TBD : Read should also come with ckpt ID, For now
		 * assuming that checkpoint ID is 0 */
		CheckPointID ckpt = 0;
		aero_obj_->AeroReadCmdProcess(vmdkp, reqp, ckpt, process,
				failed, kAsNamespaceCacheDirty);
	}

	return nextp_->Read(vmdkp, reqp, process, failed);
}

folly::Future<int> DirtyHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	/* Get connection corresponding to the given cluster ID */
	auto aero_conn = vmdkp->GetAeroConnection();
	if (aero_conn != nullptr) {
		int rc = aero_obj_->AeroWriteCmdProcess(vmdkp, reqp, ckpt,
				process, failed, kAsNamespaceCacheDirty);

		/* Try to Remove corresponding entry from CLEAN namespace */
		if (!rc) {
			std::cout << "Attempting Aero Del" << std::endl;
			aero_obj_->AeroDelCmdProcess(vmdkp, reqp, ckpt, process,
				failed, kAsNamespaceCacheClean);
		}
	}
	return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> DirtyHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
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
