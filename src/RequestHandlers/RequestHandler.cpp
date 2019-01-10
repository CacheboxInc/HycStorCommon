#include <memory>

#include <cassert>

#include "IDs.h"
#include "Request.h"
#include "RequestHandler.h"

namespace pio {

RequestHandler::RequestHandler(void *udatap) : udatap_(udatap) {

}

RequestHandler::~RequestHandler() {

}

void RequestHandler::RegisterNextRequestHandler(
		std::unique_ptr<RequestHandler> handlerp) {
	if (nextp_ == nullptr) {
		nextp_ = std::move(handlerp);
		return;
	}

	nextp_->RegisterNextRequestHandler(std::move(handlerp));
}

folly::Future<int> RequestHandler::Flush(ActiveVmdk *vmdkp, Request *reqp,
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

folly::Future<int> RequestHandler::BulkFlush(ActiveVmdk *vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->BulkFlush(vmdkp, requests, process, failed);
}

folly::Future<int> RequestHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->Move(vmdkp, reqp, process, failed);
}

int RequestHandler::Cleanup(ActiveVmdk *vmdkp) {
	if (pio_unlikely(not nextp_)) {
		return 0;
	}
	return nextp_->Cleanup(vmdkp);
}

}
