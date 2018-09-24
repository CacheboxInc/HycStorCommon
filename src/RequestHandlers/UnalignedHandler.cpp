#include <vector>
#include <string>

#include <folly/futures/Future.h>

#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "RangeLock.h"
#include "Request.h"
#include "RequestHandler.h"
#include "UnalignedHandler.h"
#include "Vmdk.h"

using namespace ::ondisk;

namespace pio {

UnalignedHandler::UnalignedHandler() : RequestHandler(nullptr) {

}

UnalignedHandler::~UnalignedHandler() {

}

folly::Future<int> UnalignedHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not nextp_)) {
		return 0;
	} else if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}
	return nextp_->Read(vmdkp, reqp, process, failed);
}

void UnalignedHandler::ReadModify(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process) {
	log_assert(not process.empty());

	for (auto& blockp : process) {
		auto count = blockp->GetRequestBufferCount();
		log_assert(count >= 2);

		size_t gap = blockp->GetOffset() - blockp->GetAlignedOffset();
		auto destp = blockp->GetRequestBufferAtBack();
		log_assert(destp->Size() == vmdkp->BlockSize());
		//auto srcp  = blockp->GetRequestBufferAt(count - 2);
		auto srcp  = blockp->GetRequestBufferAt(0);
		log_assert(gap + srcp->Size() <= destp->Size());
		log_assert(srcp->Size() < vmdkp->BlockSize());

		auto dp = destp->Payload() + gap;
		::memcpy(dp, srcp->Payload(), srcp->Size());
	}
}

folly::Future<int> UnalignedHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not nextp_)) {
		return 0;
	} else if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	if (not reqp->HasUnalignedIO()) {
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	}

	auto read_blocks = std::make_unique<std::vector<RequestBlock *>>();
	try {
		read_blocks->reserve(2);
	} catch (const std::bad_alloc& e) {
		return -ENOMEM;
	}

	auto blockp = process.front();
	log_assert(blockp != nullptr);
	if (blockp->IsPartial()) {
		read_blocks->emplace_back(blockp);
	}
	if (process.size() >= 2) {
		blockp = process.back();
		log_assert(blockp != nullptr);
		if (blockp->IsPartial()) {
			read_blocks->emplace_back(blockp);
		}
	}
	log_assert(not read_blocks->empty());

	/* Set the right checkpoint ID before attempting to Read */
	auto ckpts = std::make_pair(MetaData_constants::kInvalidCheckPointID() + 1,
		ckpt);
	vmdkp->SetReadCheckPointId(*read_blocks, ckpts);

	return this->Read(vmdkp, reqp, *read_blocks, failed)
	.then([this, vmdkp, reqp, &process, &failed, ckpt,
			read_blocks = std::move(read_blocks)] (int rc) mutable {

		/* Read from CacheLayer complete */
		if (pio_unlikely(rc != 0)) {
			/* Failed Return error, miss is not a failed case*/
			LOG(ERROR) << __func__ << "Error in reading from Cache Layer";
			return folly::makeFuture(rc);
		}

		/* No read miss to process, Proceed with merged write */
		if (failed.size() == 0) {
			this->ReadModify(vmdkp, reqp, *read_blocks);
			return this->nextp_->Write(vmdkp, reqp, ckpt, process, failed);
		}

		auto read_missed = std::make_unique<std::remove_reference<decltype(failed)>::type>();
		read_missed->swap(failed);

		/* Read from next StorageLayer - probably Network or File */
		failed.clear();
		return vmdkp->headp_->Read(vmdkp, reqp, *read_missed, failed)
		.then([this, vmdkp, reqp, read_missed = std::move(read_missed),
			&process, &failed, read_blocks = std::move(read_blocks),
			ckpt] (int rc) mutable -> folly::Future<int> {

			if (pio_unlikely(rc != 0)) {
				LOG(ERROR) << __func__ << "Reading from TargetHandler"
					" layer for read populate failed";
				return rc;
			}

			log_assert(failed.empty());
			this->ReadModify(vmdkp, reqp, *read_blocks);
			return this->nextp_->Write(vmdkp, reqp, ckpt, process, failed);
		});
});
}

folly::Future<int> UnalignedHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not nextp_)) {
		return 0;
	} else if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	#if 0
	auto blockp = process.front();
	log_assert(blockp != nullptr and not blockp->IsPartial());
	if (process.size() > 2) {
		blockp = process.back();
		log_assert(blockp != nullptr and not blockp->IsPartial());
	}
	#endif

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> UnalignedHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
#ifndef NDEBUG
	for (const auto blockp : process) {
		log_assert(not blockp->IsPartial());
	}
#endif
	return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
}

}
