#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "MultiTargetHandler.h"
#include "CacheTargetHandler.h"
#include "FileTargetHandler.h"
#include "FileCacheHandler.h"
#include "RamCacheHandler.h"
#include "ErrorHandler.h"
#include "SuccessHandler.h"
#include "Vmdk.h"
#include "VmdkConfig.h"
#include "Request.h"
#include "DaemonUtils.h"

#ifdef USE_NEP
#include "NetworkTargetHandler.h"
#endif

using namespace ::ondisk;

namespace pio {
MultiTargetHandler::MultiTargetHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) : RequestHandler(nullptr) {
	InitializeTargetHandlers(vmdkp, configp);
}

void MultiTargetHandler::InitializeTargetHandlers(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) {
	if (configp->ErrorHandlerEnabled()) {
		auto error = std::make_unique<ErrorHandler>(configp);
		targets_.emplace_back(std::move(error));
	}

	if (configp->IsSuccessHandlerEnabled()) {
		auto success = std::make_unique<SuccessHandler>(configp);
		targets_.emplace_back(std::move(success));
	}

	if (configp->IsRamCacheEnabled()) {
		auto ram_cache = std::make_unique<RamCacheHandler>(configp);
		targets_.emplace_back(std::move(ram_cache));
	}

	if (configp->IsFileCacheEnabled()) {
		auto file_target = std::make_unique<FileCacheHandler>(configp);
		targets_.push_back(std::move(file_target));
	}

	auto cache_target = std::make_unique<CacheTargetHandler>(vmdkp, configp);
	targets_.push_back(std::move(cache_target));

#ifdef USE_NEP
	if (configp->IsNetworkTargetEnabled()) {
		auto network_target = std::make_unique<NetworkTargetHandler>(configp);
		targets_.push_back(std::move(network_target));
	}
#endif
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
	return targets_[0]->Write(vmdkp, reqp, ckpt, process, failed);
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

folly::Future<int> MultiTargetHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return targets_[0]->BulkWrite(vmdkp, ckpt, requests, process, failed);
}

folly::Future<int> MultiTargetHandler::BulkReadComplete(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process, std::vector<RequestBlock*>& failed) {
	if (failed.empty()) {
		return 0;
	}

	if (pio_unlikely(targets_.size() <= 1)) {
		return -ENODEV;
	}

	auto missed = std::make_unique<std::remove_reference_t<decltype(failed)>>();
	if (pio_unlikely(not missed)) {
		LOG(ERROR) << "Failed to allocate memory for missed vector";
		return -ENOMEM;
	}
	missed->swap(failed);

	return targets_[1]->BulkRead(vmdkp, requests, *missed, failed)
	.then([this, vmdkp, &requests, &failed, missed = std::move(missed)]
			(int rc) mutable -> folly::Future<int> {
		if (pio_unlikely(rc)) {
			return rc < 0 ? rc : -rc;
		}
		if (pio_unlikely(not failed.empty())) {
			LOG(ERROR) << "Few requests failed on second target";
			return -ENODEV;
		}

		return targets_[0]->BulkReadPopulate(vmdkp, requests, *missed, failed)
		.then([missed = std::move(missed)] (int rc) mutable -> folly::Future<int> {
			return rc;
		});
	});
}


folly::Future<int> MultiTargetHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return targets_[0]->BulkRead(vmdkp, requests, process, failed)
	.then([this, vmdkp, &requests, &process, &failed] (int rc) mutable
			-> folly::Future<int> {
		if (pio_unlikely(rc)) {
			return rc < 0 ? rc : -rc;
		}

		return BulkReadComplete(vmdkp, requests, process, failed);
	});
}

folly::Future<int> MultiTargetHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return targets_[0]->BulkReadPopulate(vmdkp, requests, process, failed);		
}

}
