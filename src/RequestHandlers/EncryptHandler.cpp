#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "VmdkConfig.h"
#include "Request.h"
#include "EncryptHandler.h"
#include "hyc_encrypt.h"

using namespace ::ondisk;
using namespace pio::hyc;

namespace pio {
EncryptHandler::EncryptHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	enabled_ = configp->IsEncryptionEnabled();
	if (not enabled_) {
		return;
	}

	algorithm_ = configp->GetEncryptionType();
	//TODO: Must be replaced with vmid and vmdkid 
	uint32_t srcid = 0, destid = 0;
	hyc_disk_uuid_t uuid = {srcid, destid};

	auto encrypt_type = get_encrypt_type(algorithm_.c_str());
	log_assert(encrypt_type != HYC_ENCRYPT_UNKNOWN);

	ctxp_ = hyc_encrypt_ctx_init(uuid, encrypt_type);
	log_assert(ctxp_ != nullptr);
}

EncryptHandler::~EncryptHandler() {
	if (not enabled_) {
		return;
	}
	log_assert(ctxp_ != nullptr);
	hyc_encrypt_ctx_dinit(ctxp_);
}

int EncryptHandler::ReadComplete(const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	int32_t error = 0;
	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();

		auto dest_bufsz = hyc_encrypt_plain_bufsz(ctxp_, srcp->Payload(),
			srcp->PayloadSize());
		auto destp = pio::NewRequestBuffer(dest_bufsz);
		if (pio_unlikely(not destp)) {
			failed.emplace_back(blockp);
			error = -ENOMEM;
			continue;
		}

		auto rc = hyc_decrypt(ctxp_, srcp->Payload(), srcp->PayloadSize(),
			destp->Payload(), &dest_bufsz);
		if (pio_unlikely(rc != HYC_ENCRYPT_SUCCESS)) {
			failed.emplace_back(blockp);
			error = -ENOMEM ;
			continue;
		}

		blockp->PushRequestBuffer(std::move(destp));
	}
	return error;
}

folly::Future<int> EncryptHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
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
	.then([this, reqp, &process, &failed] (int rc) mutable {
		if (pio_unlikely(rc != 0)) {
			return rc;
		} else if (pio_unlikely(not failed.empty())) {
			return reqp->GetResult();
		}

		return ReadComplete(process, failed);
	});
}

int EncryptHandler::ProcessWrite(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	int error = 0;
	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();

		auto dest_bufsz = hyc_encrypt_get_maxlen(ctxp_, srcp->PayloadSize());
		std::unique_ptr<char[]> dst_uptr(new char[dest_bufsz]);
		if (pio_unlikely(not dst_uptr)) {
			error = -ENOMEM;
			break;
		}
		auto dest_bufp = dst_uptr.get();

		auto rc = hyc_encrypt(ctxp_, srcp->Payload(), srcp->PayloadSize(),
			dest_bufp, &dest_bufsz);
		if (pio_unlikely(rc != HYC_ENCRYPT_SUCCESS)) {
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

folly::Future<int> EncryptHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
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

folly::Future<int> EncryptHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed)
	.then([this, &process, &failed] (int rc) mutable {
		if (pio_unlikely(rc or not failed.empty())) {
			return rc < 0 ? rc : -rc;
		}
		return ReadComplete(process, failed);
	});
}

folly::Future<int> EncryptHandler::BulkWrite(ActiveVmdk* vmdkp,
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

	int rc = ProcessWrite(vmdkp, process, failed);
	if (pio_unlikely(rc)) {
		failed.clear();
		for (auto blockp : process) {
			blockp->SetResult(rc, RequestStatus::kFailed);
			failed.emplace_back(blockp);
		}
		return rc;
	}

	return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
}

folly::Future<int> EncryptHandler::BulkRead(ActiveVmdk* vmdkp,
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
		return nextp_->BulkRead(vmdkp, requests, process, failed);
	}

	return nextp_->BulkRead(vmdkp, requests, process, failed)
	.then([this, &process, &failed] (int rc) mutable {
		if (pio_unlikely(rc or not failed.empty())) {
			return rc < 0 ? rc : -rc;
		}

		return ReadComplete(process, failed);
	});
}

folly::Future<int> EncryptHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return nextp_->BulkReadPopulate(vmdkp, requests, process, failed)
	.then([this, &process, &failed] (int rc) mutable {
		if (pio_unlikely(rc or not failed.empty())) {
			return rc < 0 ? rc : -rc;
		}

		return ReadComplete(process, failed);
	});
}
}
