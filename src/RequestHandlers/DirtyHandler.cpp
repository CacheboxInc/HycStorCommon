#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "VmdkConfig.h"
#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtInterface.h"
#include "Vmdk.h"
#include "VmConfig.h"
#include "DirtyHandler.h"

using namespace ::ondisk;

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

	return aero_obj_->AeroReadCmdProcess(vmdkp, reqp, process, failed,
		kAsNamespaceCacheDirty, aero_conn)
	.then([this, vmdkp, reqp, &process, &failed] (int rc) mutable
			-> folly::Future<int> {
		if (pio_unlikely(rc != 0)) {
			return rc;
		}

		/*
		 * For Flush IO, reads don't need to go to CLEAN layer,
		 * return from here.
		 */

		if (reqp->IsFlushReq()) {
			auto rc = 0;
			for (auto blockp : process) {
				if (pio_unlikely(blockp->IsReadHit())) {
					blockp->SetResult(0, RequestStatus::kSuccess);
				} else {
					blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
					failed.emplace_back(blockp);
					rc = 1;
				}
			}
			return rc;
		} else {
			return nextp_->Read(vmdkp, reqp, process, failed);
		}
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
		if (pio_unlikely(rc != 0)) {
			return rc;
		}

		return aero_obj_->AeroDelCmdProcess(vmdkp, reqp, ckpt, process, failed,
			kAsNamespaceCacheClean, aero_conn)
		.then([&process, &failed]  (int rc) mutable {
			if (pio_unlikely(rc != 0)) {
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

folly::Future<int> DirtyHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
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
		/* TBD : May want to treat this as error */
		failed.clear();
		for (auto blockp : process) {
			blockp->SetResult(0, RequestStatus::kSuccess);
		}
		return 0;
	}

	/* Read record from DIRTY Namespace */
	return aero_obj_->AeroReadCmdProcess(vmdkp, reqp, process, failed,
		kAsNamespaceCacheDirty, aero_conn)
	.then([this, vmdkp, reqp, &process, &failed, aero_conn] (int rc) mutable
			-> folly::Future<int> {
		if (pio_unlikely(rc)) {
			return rc;
		}

		/* Write record into CLEAN namespace */
		return aero_obj_->AeroWriteCmdProcess(vmdkp, reqp, reqp->GetFlushCkptID(),
			process, failed, kAsNamespaceCacheClean, aero_conn)
		.then([this, vmdkp, reqp, &process, &failed, aero_conn] (int rc) mutable
			-> folly::Future<int> {
			if (pio_unlikely(rc)) {
				return rc;
			}

			/* Delete record from DIRTY namespace */
			return aero_obj_->AeroDelCmdProcess(vmdkp, reqp, reqp->GetFlushCkptID(),
				process, failed, kAsNamespaceCacheDirty, aero_conn)
			.then([&process, &failed]  (int rc) mutable {
				if (pio_unlikely(rc)) {
					return rc;
				}

				failed.clear();
				for (auto blockp : process) {
					blockp->SetResult(0, RequestStatus::kSuccess);
				}
				return 0;
			});
		});
	});
}
}
