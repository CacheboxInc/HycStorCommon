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
#include "VmdkConfig.h"

namespace pio {

RamCacheHandler::RamCacheHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr), cache_(std::make_unique<RamCache>()) {
	enabled_ = configp->IsRamCacheEnabled();
	if (enabled_) {
		memory_mb_ = configp->GetRamCacheMemoryLimit();
		if (memory_mb_ <= 0) {
			VLOG(1) << "RamCache is enabled. However memory limit is 0.";
			enabled_ = false;
		}
	}
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

		cache_->Read(vmdkp, payload, blockp->GetAlignedOffset());
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

		cache_->Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset());
	}

	return 0;
}

folly::Future<int> RamCacheHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	failed.clear();

	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		log_assert(srcp->Size() == vmdkp->BlockSize());

		cache_->Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset());
	}

	return 0;
}
}