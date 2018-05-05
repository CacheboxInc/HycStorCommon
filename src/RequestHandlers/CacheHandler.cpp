#include <cerrno>

#include <iterator>
#include <vector>

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

namespace pio {
CacheHandler::CacheHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	InitializeRequestHandlers(configp);
}

void CacheHandler::InitializeRequestHandlers(const config::VmdkConfig* configp) {
	auto lock = std::make_unique<LockHandler>();
	auto unalingned = std::make_unique<UnalignedHandler>();
	auto compress = std::make_unique<CompressHandler>(configp);
	auto ram_cache = std::make_unique<RamCacheHandler>(configp);
	auto encrypt = std::make_unique<EncryptHandler>(configp);
	auto dirty = std::make_unique<DirtyHandler>(configp);
	auto clean = std::make_unique<CleanHandler>(configp);

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
	return headp_->Read(vmdkp, reqp, process, failed)
	.then([&] (int rc) mutable -> folly::Future<int> {
		/* Read from CacheLayer complete */
		if (pio_likely(rc == 0)) {
			/* Success */
			log_assert(failed.empty());
			return 0;
		}

		log_assert(not failed.empty());
		if (pio_unlikely(not reqp->IsAllReadMissed(failed))) {
			/* failure */
			return rc;
		}

		/* Read Miss */
		auto read_missed = std::make_unique<
			std::remove_reference<decltype(failed)>::type>();
		read_missed->swap(failed);
		failed.clear();

		/* Read from next StorageLayer - probably Network or File */
		return nextp_->Read(vmdkp, reqp, *read_missed, failed)
		.then([this, &failed, vmdkp, reqp,
				read_missed = std::move(read_missed)] (int rc)
				mutable -> folly::Future<int> {
			if (pio_unlikely(rc != 0)) {
				log_assert(not failed.empty());
				return rc;
			}
			log_assert(failed.empty());

			/* now read populate */
			return this->ReadPopulate(vmdkp, reqp, *read_missed, failed)
			.then([read_missed = std::move(read_missed)] (int rc)
					-> folly::Future<int> {
				return rc;
			});
		});
	});
}

folly::Future<int> CacheHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> CacheHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> CacheHandler::Flush(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	/* Read from DIRTY NAMESPACE only */
	return headp_->Read(vmdkp, reqp, process, failed)
	.then([&] (int rc) mutable -> folly::Future<int> {
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

		return 0;
	});
}

}
