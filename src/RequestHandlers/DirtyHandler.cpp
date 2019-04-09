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

#if 0
#define INJECT_MOVE_FAILURE 1
#endif
using namespace ::ondisk;

namespace pio {
DirtyHandler::DirtyHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig*) :
		RequestHandler(DirtyHandler::kName, nullptr),
		set_(vmdkp->GetVM()->GetJsonConfig()->GetTargetName()) {
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

		auto vmdkid = vmdkp->GetID();
		if (reqp->IsFlushReq()) {
			auto rc = 0;
			for (auto blockp : process) {
				if (pio_unlikely(blockp->IsReadHit())) {
					blockp->SetResult(0, RequestStatus::kSuccess);
				} else {
					LOG(ERROR) << __func__ << "Record not found ::" << vmdkid << ":"
						<< blockp->GetReadCheckPointId() << ":"
						<< blockp->GetAlignedOffset();
					blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
					failed.emplace_back(blockp);
					rc = -EIO;
				}
			}
			return rc;
		} else {
			return nextp_->Read(vmdkp, reqp, process, failed);
		}
	});
}

folly::Future<int> DirtyHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	if (pio_unlikely(aero_conn_ == nullptr)) {
		return nextp_->BulkRead(vmdkp, requests, process, failed);
	}

	return aero_obj_->AeroReadCmdProcess(vmdkp, process, failed,
		kAsNamespaceCacheDirty, aero_conn_)
	.then([this, vmdkp, &requests, &process, &failed] (int rc) mutable
			-> folly::Future<int> {
		if (pio_unlikely(rc != 0 or not failed.empty())) {
			return rc;
		}

		log_assert(not requests.empty());
		auto flush = (*requests.begin())->IsFlushReq();
#ifndef NDEBUG
		for (const auto& req : requests) {
			log_assert(flush == req->IsFlushReq());
		}
#endif

		if (not flush) {
			return nextp_->BulkRead(vmdkp, requests, process, failed);
		}

		/*
		 * For Flush IO, reads don't need to go to CLEAN layer,
		 * return from here.
		 */
		const int error = -ENOMEM;
		rc = 0;
		for (auto blockp : process) {
			if (pio_unlikely(blockp->IsReadHit())) {
				blockp->SetResult(0, RequestStatus::kSuccess);
			} else {
				blockp->SetResult(error, RequestStatus::kFailed);
				failed.emplace_back(blockp);
				rc = error;
			}
		}
		return rc;
	});
}

folly::Future<int> DirtyHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return nextp_->BulkReadPopulate(vmdkp, requests, process, failed);
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

	vmdkp->stats_->cache_writes_ += process.size();

	if (pio_unlikely(aero_conn_ == nullptr)) {
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
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
			kAsNamespaceCacheClean, set_, connect)
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

/* TBD : Pass the checkpoint ID from top */
folly::Future<int> DirtyHandler::BulkMove(ActiveVmdk *vmdkp,
		::ondisk::CheckPointID ckpt_id,
		const std::vector<std::unique_ptr<Request>>&,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {

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

#ifdef INJECT_MOVE_FAILURE
	static std::atomic<uint32_t> count=0;
#endif

	/* Read record from DIRTY Namespace */
	return aero_obj_->AeroReadCmdProcess(vmdkp, process, failed,
		kAsNamespaceCacheDirty, aero_conn_)
	.then([this, vmdkp, &process, &failed,
			connect = this->aero_conn_, ckpt_id] (int rc) mutable
			-> folly::Future<int> {

#ifdef INJECT_MOVE_FAILURE
		if (count++ > 3) {
			LOG(ERROR) << __func__ << "Injecting Move ERROR, count::" << count;
			count = 0;
			return -EIO;
		}
#endif
		if (pio_unlikely(rc)) {
			LOG(ERROR) << __func__ << "Reading from DIRTY namespace failed, error code::" << rc;
			return rc;
		}

		/* We should not be seeing any Miss */
		for (auto blockp : process) {
			if (pio_unlikely(blockp->IsReadHit())) {
				blockp->SetResult(0, RequestStatus::kSuccess);
			} else {
				blockp->SetResult(-EIO, RequestStatus::kFailed);
				failed.emplace_back(blockp);
				LOG(ERROR) << __func__ << "Record not found ::"
					<< vmdkp->GetID() << ":"
					<< blockp->GetReadCheckPointId() << ":"
					<< blockp->GetAlignedOffset();
				rc = -EIO;
			}
		}

		if (pio_unlikely(rc)) {
			LOG(ERROR) << __func__ << "Some of the records has not "
				" found in DIRTY namespace, error code::" << rc;
			return rc;
		}

		/* Write record into CLEAN namespace */
		return aero_obj_->AeroWriteCmdProcess(vmdkp, ckpt_id,
			process, failed, kAsNamespaceCacheClean, connect)
		.then([this, vmdkp, &process, &failed, connect, ckpt_id] (int rc) mutable
			-> folly::Future<int> {
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__ << "Writing in CLEAN namespace failed, error code::" << rc;
				return rc;
			}

			/* Delete record from DIRTY namespace */
			return aero_obj_->AeroDelCmdProcess(vmdkp, ckpt_id,
				process, failed, kAsNamespaceCacheDirty, set_, connect)
			.then([&process, &failed]  (int rc) mutable {
				if (pio_unlikely(rc)) {
					LOG(ERROR) << __func__ << "Delete from DIRTY namespace failed, error code::" << rc;
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

#ifdef INJECT_MOVE_FAILURE
	static std::atomic<uint32_t> count=0;
#endif

	/* Read record from DIRTY Namespace */
	return aero_obj_->AeroReadCmdProcess(vmdkp, process, failed,
		kAsNamespaceCacheDirty, aero_conn_)
	.then([this, vmdkp, reqp, &process, &failed, connect = this->aero_conn_] (int rc) mutable
			-> folly::Future<int> {

#ifdef INJECT_MOVE_FAILURE
		if (count++ > 3) {
			LOG(ERROR) << __func__ << "Injecting Move ERROR, count::" << count;
			count = 0;
			return -EIO;
		}
#endif
		if (pio_unlikely(rc)) {
			LOG(ERROR) << __func__ << "Reading from DIRTY namespace failed, error code::" << rc;
			return rc;
		}

		/* We should not be seeing any Miss */
		for (auto blockp : process) {
			if (pio_unlikely(blockp->IsReadHit())) {
				blockp->SetResult(0, RequestStatus::kSuccess);
			} else {
				blockp->SetResult(-EIO, RequestStatus::kFailed);
				failed.emplace_back(blockp);
				LOG(ERROR) << __func__ << "Record not found ::"
					<< vmdkp->GetID() << ":"
					<< blockp->GetReadCheckPointId() << ":"
					<< blockp->GetAlignedOffset();
				rc = -EIO;
			}
		}

		if (pio_unlikely(rc)) {
			LOG(ERROR) << __func__ << "Some of the records has not "
				" found in DIRTY namespace, error code::" << rc;
			return rc;
		}

		/* Write record into CLEAN namespace */
		return aero_obj_->AeroWriteCmdProcess(vmdkp, reqp->GetFlushCkptID(),
			process, failed, kAsNamespaceCacheClean, connect)
		.then([this, vmdkp, reqp, &process, &failed, connect] (int rc) mutable
			-> folly::Future<int> {
			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__ << "Writing in CLEAN namespace failed, error code::" << rc;
				return rc;
			}

			/* Delete record from DIRTY namespace */
			return aero_obj_->AeroDelCmdProcess(vmdkp, reqp->GetFlushCkptID(),
				process, failed, kAsNamespaceCacheDirty, set_, connect)
			.then([&process, &failed]  (int rc) mutable {
				if (pio_unlikely(rc)) {
					LOG(ERROR) << __func__ << "Delete from DIRTY namespace failed, error code::" << rc;
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

	vmdkp->stats_->cache_writes_ += process.size();
	return aero_obj_->AeroWriteCmdProcess(vmdkp, ckpt, process, failed,
		kAsNamespaceCacheDirty, aero_conn_)
	.then([this, vmdkp, &process, &failed, ckpt, connect = this->aero_conn_] (int rc)
			mutable -> folly::Future<int> {
		if (pio_unlikely(rc != 0)) {
			for (auto blockp : process) {
				blockp->SetResult(-EIO, RequestStatus::kFailed);
				failed.emplace_back(blockp);
				LOG(ERROR) << __func__ << "Record failed ::"
						<< vmdkp->GetID() << ":"
						<< blockp->GetReadCheckPointId() << ":"
						<< blockp->GetAlignedOffset();
			}
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
			kAsNamespaceCacheClean, set_, connect)
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

folly::Future<int> DirtyHandler::Delete(ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const std::pair<BlockID, BlockID> range) {
	if (pio_unlikely(not aero_conn_)) {
		return -ENODEV;
	}

	return aero_obj_->Delete(aero_conn_.get(), vmdkp, ckpt_id, range,
		kAsNamespaceCacheDirty, set_);
}

}
