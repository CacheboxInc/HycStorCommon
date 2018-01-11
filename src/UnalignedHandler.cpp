#include <vector>
#include <string>

#include <folly/futures/Future.h>

#include "IDs.h"
#include "RangeLock.h"
#include "Request.h"
#include "RequestHandler.h"
#include "UnalignedHandler.h"

namespace pio {

UnalignedHandler::UnalignedHandler() : RequestHandler(nullptr) {

}

UnalignedHandler::~UnalignedHandler() {

}

folly::Future<int> UnalignedHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
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
		auto srcp  = blockp->GetRequestBufferAt(count - 2);
		log_assert(gap + srcp->Size() <= destp->Size());

		auto dp = destp->Payload() + gap;
		::memcpy(dp, srcp->Payload(), srcp->Size());
	}
}

folly::Future<int> UnalignedHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not nextp_)) {
		return 0;
	} else if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	std::vector<RequestBlock *> read_blocks;
	try {
		read_blocks.reserve(2);
	} catch (const std::bad_alloc& e) {
		return -ENOMEM;
	}

	auto blockp = process.front();
	log_assert(blockp != nullptr);
	if (blockp->IsPartial()) {
		read_blocks.emplace_back(blockp);
	}
	if (process.size() > 2) {
		blockp = process.back();
		log_assert(blockp != nullptr);
		if (blockp->IsPartial()) {
			read_blocks.emplace_back(blockp);
		}
	}

	if (read_blocks.empty()) {
		return nextp_->Write(vmdkp, reqp, process, failed);
	}

	return this->Read(vmdkp, reqp, read_blocks, failed)
	.then([this, vmdkp, reqp, &process, &failed,
			read_blocks = std::move(read_blocks)] (int rc) mutable {
		if (pio_unlikely(not failed.empty() || rc < 0)) {
			return folly::makeFuture(rc);
		}
		this->ReadModify(vmdkp, reqp, read_blocks);
		return this->nextp_->Write(vmdkp, reqp, process, failed);
	});
}

}