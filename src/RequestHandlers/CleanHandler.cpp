#include <cerrno>
#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "Vmdk.h"
#include "CleanHandler.h"
#include "DaemonTgtInterface.h"

using namespace ::ondisk;

namespace pio {
CleanHandler::CleanHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig*) :
		RequestHandler(CleanHandler::kName, nullptr),
		set_(vmdkp->GetVM()->GetJsonConfig()->GetTargetName()) {
	aero_obj_ = std::make_unique<AeroSpike>();
	aero_conn_ = pio::GetAeroConn(vmdkp);
}

CleanHandler::~CleanHandler() {

}

folly::Future<int> CleanHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {

	failed.clear();
	if (pio_unlikely(aero_conn_ == nullptr)) {
		if (pio_unlikely(not nextp_)) {

			/* If aerospike is not configured then don't treat
			 * this as error, there may be a Lower layer which can
			 * handle read misses
			 */

			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return 0;
		} else {
			return nextp_->Read(vmdkp, reqp, process, failed);
		}
	}

	return aero_obj_->AeroReadCmdProcess(vmdkp, process,
			failed, kAsNamespaceCacheClean, aero_conn_)
	.then([this, vmdkp, reqp, &process, &failed] (int rc)
			mutable -> folly::Future<int> {
		if (pio_likely(rc != 0)) {
			return rc < 0 ? rc : -rc;
		}

		if (pio_unlikely(nextp_)) {
			/* Must be SUCCESS or ERROR layer */
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

folly::Future<int> CleanHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	if (pio_unlikely(aero_conn_ == nullptr)) {
		if (pio_unlikely(not nextp_)) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}
		return nextp_->BulkRead(vmdkp, requests, process, failed);
	}

	return aero_obj_->AeroReadCmdProcess(vmdkp, process, failed,
		kAsNamespaceCacheClean, aero_conn_)
	.then([&process, &failed] (int rc) mutable
			-> folly::Future<int> {
		if (pio_unlikely(rc != 0)) {
			return rc < 0 ? rc : -rc;
		}

		failed.clear();
		for (auto blockp : process) {
			if (blockp->IsReadHit()) {
				blockp->SetResult(0, RequestStatus::kSuccess);
			} else {
				if (not blockp->IsReadMissed()) {
					rc = blockp->GetResult();
				}
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
	if (pio_unlikely(aero_conn_ == nullptr)) {
		if (pio_unlikely(not nextp_)) {

			/* If aerospike is not configured then don't treat
			 * this as error, there may be Lower layer which can
			 * handle writes
			 */

			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return 0;
		} else {
			return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
		}
	}

	return aero_obj_->AeroWriteCmdProcess(vmdkp, ckpt, process, failed,
		kAsNamespaceCacheClean, aero_conn_, false)
	.then([&process, &failed] (int rc) mutable {
		if (pio_unlikely(rc != 0)) {
			LOG(ERROR) << __func__ << "Returning AeroWriteCmdProcess ERROR";
			return rc;
		}
		failed.clear();
		for (auto blockp : process) {
			blockp->SetResult(0, RequestStatus::kSuccess);
		}
		return 0;
	});
}

folly::Future<int> CleanHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	if (pio_unlikely(not aero_conn_)) {
		if (pio_unlikely(not nextp_)) {
			return 0;
		} else {
			return nextp_->BulkReadPopulate(vmdkp, requests, process, failed);
		}
	}

	return aero_obj_->AeroWriteCmdProcess(vmdkp, 0, process, failed,
		kAsNamespaceCacheClean, aero_conn_, false)
	.then([&process, &failed] (int rc) mutable {
		if (pio_unlikely(rc or not failed.empty())) {
			return rc < 0 ? rc : -rc;
		}

		for (auto blockp : process) {
			blockp->SetResult(0, RequestStatus::kSuccess);
		}
		return 0;
	});
}

folly::Future<int> CleanHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {

	failed.clear();
	if (pio_unlikely(aero_conn_ == nullptr)) {
		if (pio_unlikely(not nextp_)) {

			/* If aerospike is not configured then don't treat
			 * this as error, ignore the read populate.
			 */

			return 0;
		} else {
			return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
		}
	}

	return aero_obj_->AeroWriteCmdProcess(vmdkp, 0, process, failed,
		kAsNamespaceCacheClean, aero_conn_, false)
	.then([&process, &failed] (int rc) mutable {
		if (pio_unlikely(rc != 0)) {
			LOG(ERROR) << __func__ << "Returning AeroWriteCmdProcess ERROR";
			return rc;
		}
		failed.clear();
		for (auto blockp : process) {
			blockp->SetResult(0, RequestStatus::kSuccess);
		}
		return 0;
	});
}

folly::Future<int> CleanHandler::Move(ActiveVmdk *, Request *,
		const std::vector<RequestBlock*>&,
		std::vector<RequestBlock *>&) {
	/* Should not be here */
	log_assert(0);
	return 0;
}

folly::Future<int> CleanHandler::BulkMove(ActiveVmdk *vmdkp,
		::ondisk::CheckPointID ckpt_id,
		const std::vector<std::unique_ptr<Request>>& reqs,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {

	log_assert(ckpt_id > 0);
	failed.clear();
	if (pio_unlikely(aero_conn_ == nullptr)) {
		/* TBD : May want to treat this as error */
		for (auto blockp : process) {
			blockp->SetResult(0, RequestStatus::kSuccess);
		}
		return 0;
	}

#ifdef INJECT_MOVE_FAILURE
	static std::atomic<uint32_t> count=0;
#endif

	/* Check for MergeContext flag, it will be set for all request
	 * so just checking the first one is sufficient */

	::ondisk::CheckPointID
		move_write_ckpt_id = MetaData_constants::kInvalidCheckPointID(),
		move_read_ckpt_id = MetaData_constants::kInvalidCheckPointID();
	for (auto& reqp : reqs) {
		if (pio_unlikely(!reqp->GetMergeContext())) {
			LOG(ERROR) << __func__ << "MergeContext flag is not set, we should not be here";
			log_assert(0);
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -EINVAL;
		}
		move_write_ckpt_id = reqp->GetMoveWriteCkptID();
		move_read_ckpt_id = reqp->GetMoveReadCkptID();
	}

	for (auto blockp: process) {
		if (blockp->GetReadCheckPointId() != move_read_ckpt_id) {
			LOG(ERROR) << __func__ << "Invalid checkpoint IDs, in blockp:"
				<< blockp->GetReadCheckPointId()
				<< ", move_read_ckpt_id:" << move_read_ckpt_id;
			log_assert(0);
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -EINVAL;
		} else {
			VLOG(5) << __func__ << "offset:" << blockp->GetAlignedOffset()
				<< ", checkpoint IDs in blockp:"
				<< blockp->GetReadCheckPointId()
				<< ", move_read_ckpt_id:" << move_read_ckpt_id
				<< ", move_write_ckpt_id:" << move_write_ckpt_id;
		}
	}

	/* Read record from CLEAN Namespace */
	return aero_obj_->AeroReadCmdProcess(vmdkp, process, failed,
		kAsNamespaceCacheClean, aero_conn_)
	.then([this, vmdkp, &process, &failed,
			connect = this->aero_conn_,
			move_write_ckpt_id,
			move_read_ckpt_id] (int rc) mutable
			-> folly::Future<int> {

#ifdef INJECT_MOVE_FAILURE
		if (count++ > 3) {
			LOG(ERROR) << __func__ << "Injecting Move ERROR, count::" << count;
			count = 0;
			return -EIO;
		}
#endif
		if (pio_unlikely(rc)) {
			LOG(ERROR) << __func__ << "Read failed, error code::" << rc;
			return rc;
		}

		auto process_blocks = std::make_unique<std::vector<RequestBlock *>>();
		/* Ignore read Misses, TTL eviction case */
		for (auto blockp : process) {
			if (pio_unlikely(blockp->IsReadHit())) {
				blockp->SetResult(0, RequestStatus::kSuccess);
				process_blocks->emplace_back(blockp);
			}
		}

		if (pio_unlikely(rc)) {
			LOG(ERROR) << __func__ << "Some of the records has not "
				" found , error code::" << rc;
			return rc;
		}

		failed.clear();
		/* Write record into CLEAN namespace */
		return aero_obj_->AeroWriteCmdProcess(vmdkp, move_write_ckpt_id,
			*process_blocks, failed, kAsNamespaceCacheClean, connect, true)
		.then([this, vmdkp, &process, process_blocks = std::move(process_blocks),
		&failed, connect, move_read_ckpt_id] (int rc) mutable
			-> folly::Future<int> {

			if (pio_unlikely(rc)) {
				LOG(ERROR) << __func__ << "Writing in CLEAN namespace failed, error code::" << rc;
				return rc;
			}

			/* Delete record from DIRTY namespace */
			return aero_obj_->AeroDelCmdProcess(vmdkp, move_read_ckpt_id,
				process, failed, kAsNamespaceCacheClean, set_, connect)
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

folly::Future<int> CleanHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {

	failed.clear();
	if (pio_unlikely(aero_conn_ == nullptr)) {
		if (pio_unlikely(not nextp_)) {

			/* If aerospike is not configured then don't treat
			 * this as error, there may be Lower layer which can
			 * handle writes
			 */

			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return 0;
		} else {
			return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
		}
	}

	return aero_obj_->AeroWriteCmdProcess(vmdkp, ckpt, process, failed,
		kAsNamespaceCacheClean, aero_conn_, false)
	.then([&process, &failed] (int rc) mutable {
		if (pio_unlikely(rc != 0)) {
			LOG(ERROR) << __func__ << "Returning AeroWriteCmdProcess ERROR";
			return rc;
		}
		failed.clear();
		for (auto blockp : process) {
			blockp->SetResult(0, RequestStatus::kSuccess);
		}
		return 0;
	});
}

}
