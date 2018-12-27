#include <cerrno>

#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "Vmdk.h"
#include "BlockTraceHandler.h"
#include "Analyzer.h"

using namespace ::ondisk;

namespace pio {

BlockTraceHandler::BlockTraceHandler(ActiveVmdk* vmdkp) :
		RequestHandler(nullptr), analyzerp_(vmdkp->GetVM()->GetAnalyzer()) {
	handle_ = analyzerp_->RegisterVmdk(vmdkp->GetID());
}

BlockTraceHandler::~BlockTraceHandler() {
	handle_ = 0;
}

folly::Future<int> BlockTraceHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	++pending_.read_;
	return nextp_->Read(vmdkp, reqp, process, failed)
	.then([this, reqp] (int rc) mutable {
		auto qd = --pending_.read_ + 1;
		auto l = reqp->GetLatency();
		auto o = reqp->GetOffset();
		auto s = reqp->GetTransferLength() >> kSectorShift;

		auto status = analyzerp_->Read(handle_, l, o, s, qd);
		log_assert(status);
		return rc;
	});
}

folly::Future<int> BlockTraceHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	++pending_.write_;
	return nextp_->Write(vmdkp, reqp, ckpt, process, failed)
	.then([this, reqp] (int rc) mutable {
		auto qd = --pending_.write_ + 1;
		auto l = reqp->GetLatency();
		auto o = reqp->GetOffset();
		auto s = reqp->GetTransferLength() >> kSectorShift;

		auto status = analyzerp_->Write(handle_, l, o, s, qd);
		log_assert(status);
		return rc;
	});
}

folly::Future<int> BlockTraceHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> BlockTraceHandler::Flush(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->Flush(vmdkp, reqp, process, failed);
}

folly::Future<int> BlockTraceHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	pending_.write_ += requests.size();
	return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed)
	.then([this, &requests] (int rc) mutable {
		auto qd = pending_.write_.load();
		pending_.write_ -= requests.size();
		for (const auto& reqp : requests) {
			auto l = reqp->GetLatency();
			auto o = reqp->GetOffset();
			auto s = reqp->GetTransferLength() >> kSectorShift;
			auto status = analyzerp_->Write(handle_, l, o, s, qd);
			log_assert(status);
		}
		return rc;
	});
}

folly::Future<int> BlockTraceHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	pending_.read_ += requests.size();
	return nextp_->BulkRead(vmdkp, requests, process, failed)
	.then([this, &requests] (int rc) mutable {
		auto qd = pending_.read_.load();
		pending_.read_ -= requests.size();
		for (const auto& reqp : requests) {
			auto l = reqp->GetLatency();
			auto o = reqp->GetOffset();
			auto s = reqp->GetTransferLength() >> kSectorShift;
			auto status = analyzerp_->Read(handle_, l, o, s, qd);
			log_assert(status);
		}
		return rc;
	});
}

folly::Future<int> BlockTraceHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return nextp_->BulkReadPopulate(vmdkp, requests, process, failed);
}
}
