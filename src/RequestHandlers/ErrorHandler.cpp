#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "Request.h"
#include "VmdkConfig.h"
#include "ErrorHandler.h"
#include "Vmdk.h"

namespace pio {
ErrorHandler::ErrorHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	enabled_ = configp->ErrorHandlerEnabled();
	if (enabled_) {
		throw_ = configp->ErrorHandlerShouldThrow();
		if (not throw_) {
			error_no_ = configp->ErrorHandlerReturnValue();
			if (error_no_ > 0) {
				error_no_ = -error_no_;
			}
		}

		frequency_ = configp->ErrorHandlerFrequency();
	}
}

ErrorHandler::~ErrorHandler() {

}

bool ErrorHandler::FailOperation() {
	auto total = total_ios_.fetch_add(std::memory_order_relaxed);
	return not frequency_ or total % frequency_ == 0;
}

folly::Future<int> ErrorHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	failed.clear();
	if (not FailOperation()) {
		/* return success */
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

	if (throw_) {
		throw std::bad_alloc();
	}

	for (auto blockp : process) {
		blockp->SetResult(error_no_, RequestStatus::kFailed);
		failed.emplace_back(blockp);
	}

	return error_no_;
}

folly::Future<int> ErrorHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	}

	failed.clear();
	if (not FailOperation()) {
		for (auto blockp : process) {
			blockp->SetResult(0, RequestStatus::kSuccess);
		}
		return 0;
	}

	if (throw_) {
		throw std::bad_alloc();
	}

	for (auto blockp : process) {
		blockp->SetResult(error_no_, RequestStatus::kFailed);
		failed.emplace_back(blockp);
	}
	return error_no_;
}

folly::Future<int> ErrorHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
	}

	failed.clear();
	if (not FailOperation()) {
		for (auto blockp : process) {
			blockp->SetResult(0, RequestStatus::kSuccess);
		}
		return 0;
	}

	if (throw_) {
		throw std::bad_alloc();
	}

	failed.clear();
	for (auto blockp : process) {
		blockp->SetResult(error_no_, RequestStatus::kFailed);
		failed.emplace_back(blockp);
	}
	return error_no_;
}

}
