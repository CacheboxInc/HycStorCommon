#include <vector>
#include <string>

#include <folly/futures/Future.h>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "IDs.h"
#include "DaemonCommon.h"
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

	if (pio_likely(not enabled_)) {
		/* RamCache is disabled */
		if (pio_unlikely(not nextp_)) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	failed.clear();
	std::vector<RequestBlock*> missed;
	for (auto blockp : process) {
		auto destp   = NewRequestBuffer(vmdkp->BlockSize());
		auto payload = destp->Payload();

		auto rc = cache_->Read(vmdkp, payload, blockp->GetAlignedOffset());
		if (rc == false) {
			missed.emplace_back(blockp);
			continue;
		}

		blockp->PushRequestBuffer(std::move(destp));
	}

	if (missed.empty()) {
		return 0;
	}

	if (pio_unlikely(not nextp_)) {
		failed.reserve(missed.size());
		std::copy(missed.begin(), missed.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->Read(vmdkp, reqp, missed, failed)
	.then([&, missed = std::move(missed)] (int rc) mutable {
		if (pio_unlikely(not failed.empty() || rc < 0)) {
			return rc;
		}

		/* TODO: Store data in RamCache */
		return 0;
	});
}

folly::Future<int> RamCacheHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	if (pio_likely(not enabled_)) {
		/* RamCache is disabled */
		if (pio_unlikely(not nextp_)) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	}

	failed.clear();
	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		log_assert(srcp->Size() == vmdkp->BlockSize());

		cache_->Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset());
	}

	if (pio_unlikely(not nextp_)) {
		return 0;
	}

	return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> RamCacheHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	if (pio_likely(not enabled_)) {
		/* RamCache is disabled */
		if (pio_unlikely(not nextp_)) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
	}

	failed.clear();
	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		log_assert(srcp->Size() == vmdkp->BlockSize());

		cache_->Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset());
	}

	if (pio_unlikely(not nextp_)) {
		return 0;
	}

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}
}