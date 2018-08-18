#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "CacheHandler.h"
#include "LockHandler.h"
#include "UnalignedHandler.h"
#include "CompressHandler.h"
#include "EncryptHandler.h"
#include "DirtyHandler.h"
#include "CleanHandler.h"
#include "RamCacheHandler.h"
#include "ErrorHandler.h"
#include "SuccessHandler.h"
#include "VmdkConfig.h"
#include "Request.h"
#include "DaemonUtils.h"

using namespace ::ondisk;

namespace pio {
CacheHandler::CacheHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) : RequestHandler(nullptr) {
	InitializeRequestHandlers(vmdkp, configp);
}

void CacheHandler::InitializeRequestHandlers(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) {
	auto lock = std::make_unique<LockHandler>();
	auto unalingned = std::make_unique<UnalignedHandler>();
	auto compress = std::make_unique<CompressHandler>(configp);
	auto ram_cache = std::make_unique<RamCacheHandler>(configp);
	auto encrypt = std::make_unique<EncryptHandler>(configp);
	auto dirty = std::make_unique<DirtyHandler>(vmdkp, configp);
	auto clean = std::make_unique<CleanHandler>(vmdkp, configp);

	headp_ = std::move(lock);
	headp_->RegisterNextRequestHandler(std::move(unalingned));
	headp_->RegisterNextRequestHandler(std::move(compress));
	headp_->RegisterNextRequestHandler(std::move(ram_cache));
	headp_->RegisterNextRequestHandler(std::move(encrypt));
	headp_->RegisterNextRequestHandler(std::move(dirty));
	headp_->RegisterNextRequestHandler(std::move(clean));

	if (configp->ErrorHandlerEnabled()) {
		auto error = std::make_unique<ErrorHandler>(configp);
		headp_->RegisterNextRequestHandler(std::move(error));
	}

	if (configp->IsSuccessHandlerEnabled()) {
		auto success = std::make_unique<SuccessHandler>(configp);
		headp_->RegisterNextRequestHandler(std::move(success));
	}
}

CacheHandler::~CacheHandler() {

}

folly::Future<int> CacheHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	reqp->active_level++;
	return headp_->Read(vmdkp, reqp, process, failed)
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

		if (pio_unlikely(not nextp_)) {
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
		return nextp_->Read(vmdkp, reqp, *read_missed, failed)
		.then([this, vmdkp, reqp, read_missed = std::move(read_missed), failed] (int rc)
				mutable -> folly::Future<int> {
			if (pio_unlikely(rc != 0)) {
				LOG(ERROR) << __func__ << "Reading from TargetHandler layer for read populate failed";
				return rc;
			}

			log_assert(failed.empty());
			/* Follow up operations is ReadModify, don't populate the cache */
			if (reqp->active_level > 1) {
				return 0;
			}

			/* now read populate */
			return this->ReadPopulate(vmdkp, reqp, *read_missed, failed)
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

folly::Future<int> CacheHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	reqp->active_level++;
	return headp_->Write(vmdkp, reqp, ckpt, process, failed)
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

		if (pio_unlikely(not nextp_)) {
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
		return nextp_->Write(vmdkp, reqp, 0, *write_missed, failed)
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

folly::Future<int> CacheHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	reqp->active_level++;
	return headp_->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> CacheHandler::Flush(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	reqp->active_level++;
	/* Read from DIRTY NAMESPACE only */
	return headp_->Read(vmdkp, reqp, process, failed)
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

		/* Write on prem, Next layer is target handler layer */
		return nextp_->Write(vmdkp, reqp, 0, process, failed)
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

folly::Future<int> CacheHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->Move(vmdkp, reqp, process, failed);
}
}
