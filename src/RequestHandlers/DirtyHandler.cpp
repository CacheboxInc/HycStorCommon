#include <cerrno>

#include <iterator>
#include <vector>

#include "VmdkConfig.h"
#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "DaemonTgtInterface.h"
#include "Vmdk.h"
#include "VmConfig.h"
#include "DirtyHandler.h"

namespace pio {
DirtyHandler::DirtyHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	aero_obj_ = std::make_unique<AeroSpike>();
}

DirtyHandler::~DirtyHandler() {

}

folly::Future<int> DirtyHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	/* Get connection corresponding to the given cluster ID */
	auto aero_conn = pio::GetAeroConn(vmdkp);
	if (pio_unlikely(aero_conn == nullptr)) {
		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	/* TBD : Read should also come with ckpt ID, For now
	 * assuming that checkpoint ID is 0 */
	CheckPointID ckpt = 0;
	(void) ckpt;
	return aero_obj_->AeroReadCmdProcess(vmdkp, reqp, ckpt, process, failed,
		kAsNamespaceCacheDirty, aero_conn)
	.then([this, vmdkp, reqp, &process, &failed] (int rc) mutable
			-> folly::Future<int> {
		if (pio_likely(rc != 0)) {
			return rc;
		}
		return nextp_->Read(vmdkp, reqp, process, failed);
	});
}

folly::Future<int> DirtyHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(),
				std::back_inserter(failed));
		return -ENODEV;
	}

	auto aero_conn = pio::GetAeroConn(vmdkp);
	if (pio_unlikely(aero_conn == nullptr)) {
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	}

	return aero_obj_->AeroWriteCmdProcess(vmdkp, reqp, ckpt, process, failed,
		kAsNamespaceCacheDirty, aero_conn)
	.then([this, vmdkp, reqp, &process, &failed, ckpt, aero_conn] (int rc)
			mutable -> folly::Future<int> {
		if (pio_likely(rc != 0)) {
			return rc;
		}

		return aero_obj_->AeroDelCmdProcess(vmdkp, reqp, ckpt, process, failed,
			kAsNamespaceCacheClean, aero_conn)
		.then([&process, &failed]  (int rc) mutable {
			if (pio_likely(rc != 0)) {
				return rc;
			}

			failed.clear();
			for (auto blockp : process) {
				blockp->SetResult(0, RequestStatus::kSuccess);
			}

			return 0;
		});
	});
}

folly::Future<int> DirtyHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
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