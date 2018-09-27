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
DirtyHandler::DirtyHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) : RequestHandler(nullptr) {
	aero_obj_ = std::make_unique<AeroSpike>();
	aero_conn_ = pio::GetAeroConn(vmdkp);
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

	if (pio_unlikely(aero_conn_ == nullptr)) {
		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	return aero_obj_->AeroReadCmdProcess(vmdkp, process, failed,
		kAsNamespaceCacheDirty, aero_conn_)
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

	if (pio_unlikely(aero_conn_ == nullptr)) {
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	}

	return aero_obj_->AeroWriteCmdProcess(vmdkp, ckpt, process, failed,
		kAsNamespaceCacheDirty, aero_conn_)
	.then([this, vmdkp, reqp, &process, &failed, ckpt, connect = this->aero_conn_] (int rc)
			mutable -> folly::Future<int> {
		if (pio_unlikely(rc != 0)) {
			return rc;
		}

		if (pio_unlikely(!vmdkp->CleanupOnWrite())) {
			failed.clear();
			for (auto blockp : process) {
				blockp->SetResult(0, RequestStatus::kSuccess);
			}
			return 0;
		}

		return aero_obj_->AeroDelCmdProcess(vmdkp, ckpt, process, failed,
			kAsNamespaceCacheClean, connect)
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

	//LOG(ERROR) << __func__ << "Calling ReadPopulate";
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

	if (pio_unlikely(aero_conn_ == nullptr)) {
		/* TBD : May want to treat this as error */
		failed.clear();
		for (auto blockp : process) {
			blockp->SetResult(0, RequestStatus::kSuccess);
		}
		return 0;
	}

	/* Read record from DIRTY Namespace */
	return aero_obj_->AeroReadCmdProcess(vmdkp, process, failed,
		kAsNamespaceCacheDirty, aero_conn_)
	.then([this, vmdkp, reqp, &process, &failed, connect = this->aero_conn_] (int rc) mutable
			-> folly::Future<int> {
		if (pio_unlikely(rc)) {
			return rc;
		}

		/* Write record into CLEAN namespace */
		return aero_obj_->AeroWriteCmdProcess(vmdkp, reqp->GetFlushCkptID(),
			process, failed, kAsNamespaceCacheClean, connect)
		.then([this, vmdkp, reqp, &process, &failed, connect] (int rc) mutable
			-> folly::Future<int> {
			if (pio_unlikely(rc)) {
				return rc;
			}

			/* Delete record from DIRTY namespace */
			return aero_obj_->AeroDelCmdProcess(vmdkp, reqp->GetFlushCkptID(),
				process, failed, kAsNamespaceCacheDirty, connect)
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

folly::Future<int> DirtyHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {

	/* TODO: needs to be implemented */
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(),
				std::back_inserter(failed));
		return -ENODEV;
	}

	if (pio_unlikely(aero_conn_ == nullptr)) {
		return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
	}

	return aero_obj_->AeroWriteCmdProcess(vmdkp, ckpt, process, failed,
		kAsNamespaceCacheDirty, aero_conn_)
	.then([this, vmdkp, &process, &failed, ckpt, connect = this->aero_conn_] (int rc)
			mutable -> folly::Future<int> {
		if (pio_unlikely(rc != 0)) {
			return rc;
		}

		if (pio_unlikely(!vmdkp->CleanupOnWrite())) {
			failed.clear();
			for (auto blockp : process) {
				blockp->SetResult(0, RequestStatus::kSuccess);
			}
			return 0;
		}

		return aero_obj_->AeroDelCmdProcess(vmdkp, ckpt, process, failed,
			kAsNamespaceCacheClean, connect)
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
}
