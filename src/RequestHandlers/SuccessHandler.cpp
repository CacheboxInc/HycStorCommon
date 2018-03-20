#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "Request.h"
#include "VmdkConfig.h"
#include "SuccessHandler.h"
#include "Vmdk.h"

namespace pio {
SuccessHandler::SuccessHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	enabled_ = configp->IsSuccessHandlerEnabled();
}

SuccessHandler::~SuccessHandler() {

}

folly::Future<int> SuccessHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	for (auto blockp : process) {
		auto destp = NewRequestBuffer(vmdkp->BlockSize());
		if (pio_unlikely(not destp)) {
			blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
			failed.emplace_back(blockp);
			return -ENOMEM;
		}

		::memset(destp->Payload(), 0, destp->Size());
		blockp->PushRequestBuffer(std::move(destp));
		blockp->SetResult(0, RequestStatus::kSuccess);
	}
	return 0;
}

folly::Future<int> SuccessHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	}

	failed.clear();
	for (auto blockp : process) {
		blockp->SetResult(0, RequestStatus::kSuccess);
	}
	return 0;
}

folly::Future<int> SuccessHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
	}

	failed.clear();
	for (auto blockp : process) {
		blockp->SetResult(0, RequestStatus::kSuccess);
	}
	return 0;
}
}
