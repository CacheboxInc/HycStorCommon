#include <cerrno>

#include <iterator>
#include <vector>

#include "CompressHandler.h"

namespace pio {
CompressHandler::CompressHandler() : RequestHandler(nullptr) {
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

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

}
