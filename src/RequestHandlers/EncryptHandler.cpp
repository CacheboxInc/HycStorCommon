#include <cerrno>

#include <iterator>
#include <vector>

#include "VmdkConfig.h"
#include "EncryptHandler.h"

namespace pio {
EncryptHandler::EncryptHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	enabled_ = configp->IsEncryptionEnabled();
	if (not enabled_) {
		return;
	}

	key_ = configp->GetEncryptionKey();
	if (key_.empty()) {
		enabled_ = false;
	} else {
		enabled_ = configp->IsEncryptionEnabled();
	}
}

EncryptHandler::~EncryptHandler() {

}

folly::Future<int> EncryptHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	if (not enabled_) {
		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	/* TODO: encrypt data here and forward request down */
	return nextp_->Read(vmdkp, reqp, process, failed);
}

folly::Future<int> EncryptHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	if (not enabled_) {
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	}

	/* TODO: encrypt data here and forward request down */
	return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> EncryptHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	if (not enabled_) {
		return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
	}

	/* TODO: encrypt data here and forward request down */
	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

}
