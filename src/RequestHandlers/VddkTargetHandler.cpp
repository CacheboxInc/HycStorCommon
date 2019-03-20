#include <vector>
#include <string>

#include <folly/futures/Future.h>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "DaemonCommon.h"
#include "Vmdk.h"
#include "Request.h"
#include "RequestHandler.h"
#include "VddkTargetLib.h"
#include "VddkTargetHandler.h"
#include "VmdkConfig.h"

using namespace ::ondisk;

namespace pio {

VddkTargetLibHandler::VddkTargetLibHandler(const config::VmdkConfig* configp) :
		RequestHandler(VddkTargetLibHandler::kName, nullptr),
		target_(std::make_unique<VddkTargetLib>()) {
	enabled_ = configp->IsVddkTargetLibEnabled();
}

VddkTargetLibHandler::~VddkTargetLibHandler() {

}

folly::Future<int> VddkTargetLibHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	if (pio_likely(not enabled_)) {
		/* VddkTargetLib is disabled */
		if (pio_unlikely(not nextp_)) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	failed.clear();
	auto missed = std::make_unique<std::vector<RequestBlock*>>();
	for (auto blockp : process) {
		auto [destp, found] = target_->Read(vmdkp, blockp->GetAlignedOffset());
		if (not found || not destp) {
			blockp->SetResult(0, RequestStatus::kMiss);
			missed->emplace_back(blockp);
			continue;
		}

		blockp->PushRequestBuffer(std::move(destp));
	}

	if (missed->empty()) {
		return 0;
	}

	if (pio_unlikely(not nextp_)) {
		failed.reserve(missed->size());
		std::copy(missed->begin(), missed->end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->Read(vmdkp, reqp, *missed, failed)
	.then([&, missed = std::move(missed)] (int rc) mutable {
		if (pio_unlikely(not failed.empty() || rc < 0)) {
			return rc;
		}

		/* TODO: Store data in VddkTargetLib */
		return 0;
	});
}

folly::Future<int> VddkTargetLibHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	if (pio_likely(not enabled_)) {
		/* VddkTargetLib is disabled */
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
		target_->Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset(),
			srcp->PayloadSize());
	}

	if (pio_likely(not nextp_)) {
		return 0;
	}

	return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> VddkTargetLibHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	if (pio_likely(not enabled_)) {
		/* VddkTargetLib is disabled */
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
		//log_assert(srcp->Size() == vmdkp->BlockSize());

		target_->Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset(),
			srcp->PayloadSize());
	}

	if (pio_unlikely(not nextp_)) {
		return 0;
	}

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> VddkTargetLibHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {

	if (pio_likely(not enabled_)) {
		if (pio_unlikely(not nextp_)) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
	}

	failed.clear();
	for (const auto blockp : process) {
		const auto srcp = blockp->GetRequestBufferAtBack();
		//log_assert(srcp->Size() == vmdkp->BlockSize());
		target_->Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset(),
			srcp->PayloadSize());
	}

	if (pio_unlikely(not nextp_)) {
		return 0;
	}

	return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
}

folly::Future<int> VddkTargetLibHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	if (pio_likely(not enabled_)) {
		if (pio_unlikely(not nextp_)) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		return nextp_->BulkRead(vmdkp, requests, process, failed);
	}

	auto missed = std::make_unique<std::vector<RequestBlock*>>();
	for (auto blockp : process) {
		auto [destp, found] = target_->Read(vmdkp, blockp->GetAlignedOffset());
		if (not found || not destp) {
			blockp->SetResult(0, RequestStatus::kMiss);
			missed->emplace_back(blockp);
			continue;
		}

		blockp->PushRequestBuffer(std::move(destp));
	}

	if (missed->empty()) {
		return 0;
	}

	if (pio_unlikely(not nextp_)) {
		failed.reserve(missed->size());
		std::copy(missed->begin(), missed->end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->BulkRead(vmdkp, requests, *missed, failed)
	.then([missed = std::move(missed)] (int rc) mutable {
		return rc;
	});
}

folly::Future<int> VddkTargetLibHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	failed.clear();
	if (pio_likely(not enabled_)) {
		if (pio_unlikely(not nextp_)) {
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}
		return nextp_->BulkReadPopulate(vmdkp, requests, process, failed);
	}

	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		target_->Write(vmdkp, srcp->Payload(), blockp->GetAlignedOffset(),
			srcp->PayloadSize());
	}

	if (pio_unlikely(not nextp_)) {
		return 0;
	}

	return nextp_->BulkReadPopulate(vmdkp, requests, process, failed);
}

folly::Future<int> VddkTargetLibHandler::Delete(ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const std::pair<::ondisk::BlockID, ::ondisk::BlockID> range) {
	if (pio_likely(not enabled_)) {
		if (pio_unlikely(not nextp_)) {
			return 0;
		}
		return nextp_->Delete(vmdkp, ckpt_id, range);
	}

	auto r = iter::Range(range.first, range.second+1);
	std::vector<VddkTargetLib::Key> keys;
	std::transform(r.begin(), r.end(), std::back_inserter(keys),
		[shift = vmdkp->BlockShift()] (BlockID block) {
			return block << shift;
		});
	target_->Erase(keys.begin(), keys.end());
	if (pio_unlikely(not nextp_)) {
		return 0;
	}
	return nextp_->Delete(vmdkp, ckpt_id, range);
}

const VddkTargetLib* VddkTargetLibHandler::Cache() const noexcept {
	return target_.get();
}
}
