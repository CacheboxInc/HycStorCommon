#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "VmdkConfig.h"
#include "CompressHandler.h"
#include "Request.h"
#include "Vmdk.h"
#include "hyc_compress.h"

using namespace ::ondisk;
using namespace pio::hyc;

namespace pio {
CompressHandler::CompressHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	enabled_ = configp->IsCompressionEnabled();
	if (not enabled_) {
		return;
	}

	algorithm_ = configp->GetCompressionType();

	//TODO: Must be replaced with vmid and vmdkid 
	uint32_t srcid = 0, destid = 0;
	hyc_disk_uuid_t uuid = {srcid, destid};

	auto compress_type = get_compress_type(algorithm_.c_str());
	log_assert(compress_type != HYC_COMPRESS_UNKNOWN);

	auto min_compress_ratio = configp->GetMinCompressRatio();
	ctxp_ = hyc_compress_ctx_init(uuid, compress_type,
		min_compress_ratio, enabled_);
	log_assert(ctxp_ != nullptr);
}

CompressHandler::~CompressHandler() {
	if (not enabled_) {
		return;
	}
	log_assert(ctxp_ != nullptr);
	hyc_compress_ctx_dinit(ctxp_);
}

std::pair<std::unique_ptr<RequestBuffer>, int32_t>
CompressHandler::RequestBlockReadComplete(ActiveVmdk* vmdkp,
		RequestBlock* blockp) {
	auto dest_bufsz = vmdkp->BlockSize();
	auto destp = pio::NewRequestBuffer(dest_bufsz);
	if (pio_unlikely(not destp)) {
		return std::make_pair(nullptr, -ENOMEM);
	}

	int32_t error = 0;
	auto srcp = blockp->GetRequestBufferAtBack();
	auto rc = hyc_uncompress(ctxp_, srcp->Payload(), srcp->Size(),
		destp->Payload(), &dest_bufsz);
	if (pio_unlikely(rc != HYC_COMPRESS_SUCCESS)) {
		LOG(ERROR) << "Uncompress failed";
		error = -ENOMEM;
	}

	return std::make_pair(std::move(destp), error);
}

folly::Future<int> CompressHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	if (pio_unlikely(not enabled_)) {
		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	return nextp_->Read(vmdkp, reqp, process, failed)
	.then([this, vmdkp, reqp, &process, &failed] (int rc) mutable {
		if (pio_unlikely(rc != 0)) {
			return rc;
		} else if (pio_unlikely(not failed.empty())) {
			return reqp->GetResult();
		}

		int error = 0;
		for (auto blockp : process) {
			auto [destp, rc] = RequestBlockReadComplete(vmdkp, blockp);
			if (pio_unlikely(rc < 0)) {
				failed.emplace_back(blockp);
				error = rc;
				continue;
			}
			log_assert(destp);
			blockp->PushRequestBuffer(std::move(destp));
		}

		return error;
	});
}

int CompressHandler::ProcessWrite(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	int error = 0;
	auto max_bufsz = hyc_compress_get_maxlen(ctxp_, vmdkp->BlockSize());
	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();

		auto dest_bufsz = max_bufsz;
		std::unique_ptr<char[]> dst_uptr(new char[dest_bufsz]);
		if (pio_unlikely(not dst_uptr)) {
			error = -ENOMEM;
			break;
		}
		auto dest_bufp = dst_uptr.get();

		auto rc = hyc_compress(ctxp_, srcp->Payload(), srcp->Size(),
			dest_bufp, &dest_bufsz);
		if (pio_unlikely(rc != HYC_COMPRESS_SUCCESS)) {
			error = -ENOMEM;
			break;
		}

		//TODO: avoid extra mem alloc and copy
		//1. allocate NewRequestBuffer() with maxsize
		//2. Compress
		//3. Call SetPayloadSize(cmpsd_sz);
		auto dstp = pio::NewRequestBuffer(dest_bufsz);
		if (pio_unlikely(not dstp)) {
			error = -ENOMEM;
			break;
		}
		std::memcpy(dstp->Payload(), dest_bufp, dest_bufsz);
		blockp->PushRequestBuffer(std::move(dstp));
	}
	return error;
}

folly::Future<int> CompressHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	if (pio_unlikely(not enabled_)) {
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	}

	int error = ProcessWrite(vmdkp, process, failed);
	if (pio_unlikely(error)) {
		failed.clear();
		for (auto blockp : process) {
			blockp->SetResult(error, RequestStatus::kFailed);
			failed.emplace_back(blockp);
		}
		return error;
	}

	return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> CompressHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> CompressHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	if (pio_unlikely(not enabled_)) {
		return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
	}

	int error = ProcessWrite(vmdkp, process, failed);
	if (pio_unlikely(error)) {
		failed.clear();
		for (auto blockp : process) {
			blockp->SetResult(error, RequestStatus::kFailed);
			failed.emplace_back(blockp);
		}
		return error;
	}

	return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
}
}
