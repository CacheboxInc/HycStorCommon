#include <vector>
#include <string>

#include <folly/futures/Future.h>

#include "IDs.h"
#include "Common.h"
#include "Vmdk.h"
#include "Request.h"
#include "RequestHandler.h"
#include "RamCache.h"
#include "RamCacheHandler.h"

namespace pio {

RamCacheHandler::RamCacheHandler() : RequestHandler(nullptr) {

}

RamCacheHandler::~RamCacheHandler() {

}

folly::Future<int> RamCacheHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	for (auto blockp : process) {
		auto destp   = NewRequestBuffer(vmdkp->BlockSize());
		auto payload = destp->Payload();
		blockp->PushRequestBuffer(std::move(destp));

		cache_.Read(vmdkp, payload, blockp->GetAlignedOffset());
	}
	return 0;
}

folly::Future<int> RamCacheHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	failed.clear();

	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		log_assert(srcp->Size() == vmdkp->BlockSize());

		cache_.Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset());
	}

	return 0;
}

folly::Future<int> RamCacheHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	failed.clear();

	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		log_assert(srcp->Size() == vmdkp->BlockSize());

		cache_.Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset());
	}

	return 0;
}
}