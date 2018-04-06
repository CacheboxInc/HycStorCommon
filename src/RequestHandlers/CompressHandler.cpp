#include <cerrno>

#include <iterator>
#include <vector>

#include "VmdkConfig.h"
#include "CompressHandler.h"

namespace pio {
CompressHandler::CompressHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	enabled_ = configp->IsCompressionEnabled();
	if (enabled_) {
		algorithm_ = configp->GetCompressionType();
		level_ = configp->GetCompressionLevel();
	}
}

CompressHandler::~CompressHandler() {

}

folly::Future<int> CompressHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
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

	/* TODO: write compression code */
	return nextp_->Read(vmdkp, reqp, process, failed);
}

folly::Future<int> CompressHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
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

	/* TODO: write compression code */
	return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> CompressHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	if (pio_unlikely(not enabled_)) {
		return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
	}

	/* TODO: write compression code */
	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

}