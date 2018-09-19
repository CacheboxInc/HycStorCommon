#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "MultiTargetHandler.h"
#include "CacheTargetHandler.h"
#include "NetworkTargetHandler.h"
#include "FileTargetHandler.h"
#include "Vmdk.h"
#include "VmdkConfig.h"
#include "Request.h"
#include "DaemonUtils.h"

using namespace ::ondisk;

namespace pio {
MultiTargetHandler::MultiTargetHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) : RequestHandler(nullptr) {
	InitializeTargetHandlers(vmdkp, configp);
}

void MultiTargetHandler::InitializeTargetHandlers(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) {
	auto cache_target = std::make_unique<CacheTargetHandler>(vmdkp, configp);
	targets_.push_back(std::move(cache_target));

	if (configp->IsFileTargetEnabled()) {
		auto file_target = std::make_unique<FileTargetHandler>(configp);
		targets_.push_back(std::move(file_target));
	}

	if (configp->IsNetworkTargetEnabled()) {
		auto network_target = std::make_unique<NetworkTargetHandler>(configp);
		targets_.push_back(std::move(network_target));
	}
}

MultiTargetHandler::~MultiTargetHandler() {
}

folly::Future<int> MultiTargetHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());
	return targets_[0]->Read(vmdkp, reqp, process, failed)
	.then([this, vmdkp, reqp, &process, &failed] (int rc) mutable -> folly::Future<int> {

		/* Read from CacheLayer complete */
		if (pio_unlikely(rc != 0)) {
			/* Failed Return error, miss is not a failed case*/
			LOG(ERROR) << __func__ << "Error in reading from Cache Layer";
			return rc;
		}

		/* No read miss to process, return from here */
		if (failed.size() == 0) {
			return 0;
		}

		if (pio_unlikely(targets_.size() <= 1)) {
			LOG(ERROR) << __func__ << "No Target handler registered";
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		/* Handle Read Miss */
		auto read_missed = std::make_unique<std::remove_reference<decltype(failed)>::type>();
		read_missed->swap(failed);
		failed.clear();

		/* Read from next StorageLayer - probably Network or File */
		return targets_[1]->Read(vmdkp, reqp, *read_missed, failed)
		.then([this, vmdkp, reqp, read_missed = std::move(read_missed), &failed] (int rc)
				mutable -> folly::Future<int> {
			if (pio_unlikely(rc != 0)) {
				LOG(ERROR) << __func__ << "Reading from TargetHandler layer for read populate failed";
				return rc;
			}

			log_assert(failed.empty());

			/* now read populate */
			return targets_[0]->ReadPopulate(vmdkp, reqp, *read_missed, failed)
			.then([read_missed = std::move(read_missed)]
					(int rc) -> folly::Future<int> {
				if (rc) {
					LOG(ERROR) << __func__ << "Cache (Read) populate failed";
				}
				return rc;
			});
		});
	});
}

folly::Future<int> MultiTargetHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());
	return targets_[0]->Write(vmdkp, reqp, ckpt, process, failed)
	.then([this, vmdkp, reqp, &process, &failed] (int rc) mutable -> folly::Future<int> {

		/* Write from CacheLayer complete */
		if (pio_unlikely(rc != 0)) {
			/* Failed Return error, miss is not a failed case*/
			LOG(ERROR) << __func__ << "Error in reading from Cache Layer";
			return rc;
		}

		/* No write miss to process, return from here */
		if (failed.size() == 0) {
			return 0;
		}

		if (pio_unlikely(targets_.size() <= 1)) {
			LOG(ERROR) << __func__ << "No Target handler registered";
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		/* Handle Write Miss */
		auto write_missed = std::make_unique<std::remove_reference<decltype(failed)>::type>();
		write_missed->swap(failed);
		failed.clear();

		/* Read from next StorageLayer - probably Network or File */
		return targets_[1]->Write(vmdkp, reqp, 0, *write_missed, failed)
		.then([write_missed = std::move(write_missed), &failed] (int rc)
					mutable -> folly::Future<int> {
			if (pio_unlikely(rc != 0)) {
				LOG(ERROR) << __func__ << "Writing on TargetHandler layer failed";
				return rc;
			}

			log_assert(failed.empty());
			return 0;
		});
	});
}

folly::Future<int> MultiTargetHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());
	return targets_[0]->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> MultiTargetHandler::Flush(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());

	/* Read from DIRTY NAMESPACE only */
	return targets_[0]->Read(vmdkp, reqp, process, failed)
	.then([this, vmdkp, reqp, &process, &failed] (int rc) mutable -> folly::Future<int> {
		/* Read from CacheLayer complete */
		if(pio_unlikely(rc != 0)) {
			return rc;
		}

		if(pio_unlikely(failed.size())) {

			/* TBD: - failure, do we need to move all Request
			 * blocks in failed vector. Get the error code
			 * from layers below
			 */

			return -EIO;
		}

		if (pio_unlikely(targets_.size() <= 1)) {
			LOG(ERROR) << __func__ << "No Target handler registered";
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		/* Write on prem, Next layer is target handler layer */
		return targets_[1]->Write(vmdkp, reqp, 0, process, failed)
		.then([&failed] (int rc) mutable -> folly::Future<int> {
			if (pio_unlikely(rc != 0)) {
				LOG(ERROR) << __func__ << "In future context on prem write error";
				log_assert(not failed.empty());
				return rc;
			}

			log_assert(failed.empty());
			return 0;
		});
	});
}

folly::Future<int> MultiTargetHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());
	return targets_[0]->Move(vmdkp, reqp, process, failed);
}
}
