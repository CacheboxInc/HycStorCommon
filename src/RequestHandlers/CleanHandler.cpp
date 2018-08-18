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
		const config::VmdkConfig* configp) : RequestHandler(nullptr) {
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

	return aero_obj_->AeroReadCmdProcess(vmdkp, reqp, process,
			failed, kAsNamespaceCacheClean, aero_conn_)
	.then([this, vmdkp, reqp, &process, &failed] (int rc)
			mutable -> folly::Future<int> {
		if (pio_likely(rc != 0)) {
			return rc;
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

	return aero_obj_->AeroWriteCmdProcess(vmdkp, reqp, 0, process, failed,
		kAsNamespaceCacheClean, aero_conn_)
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

	return aero_obj_->AeroWriteCmdProcess(vmdkp, reqp, 0, process, failed,
		kAsNamespaceCacheClean, aero_conn_)
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

folly::Future<int> CleanHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	/* Should not be here */
	log_assert(0);
	return 0;
}
}
