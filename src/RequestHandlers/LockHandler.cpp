#include <vector>
#include <string>

#include <folly/futures/Future.h>

#include "IDs.h"
#include "Request.h"
#include "RequestHandler.h"
#include "LockHandler.h"

namespace pio {

LockHandler::LockHandler() : RequestHandler(nullptr) {

}

LockHandler::~LockHandler() {

}

folly::Future<int> LockHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	auto[start, end] = reqp->Blocks();

	RangeLock::LockGuard g(&range_lock_, start, end);
	return g.Lock()
	.then([g = std::move(g), vmdkp, reqp, &process, &failed, this] () mutable {
		if (not nextp_) {
			return folly::makeFuture(0);
		}
		return nextp_->Read(vmdkp, reqp, process, failed);
	});
}

folly::Future<int> LockHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	auto[start, end] = reqp->Blocks();

	RangeLock::LockGuard g(&range_lock_, start, end);
	return g.Lock()
	.then([g = std::move(g), vmdkp, reqp, &process, &failed, this, ckpt]
			() mutable {
		if (not nextp_) {
			return folly::makeFuture(0);
		}
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	});
}

folly::Future<int> LockHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	auto[start, end] = reqp->Blocks();

	RangeLock::LockGuard g(&range_lock_, start, end);
	return g.Lock()
	.then([g = std::move(g), vmdkp, reqp, &process, &failed, this, ckpt]
			() mutable {
		if (not nextp_) {
			return folly::makeFuture(0);
		}
		return nextp_->ReadPopulate(vmdkp, reqp, ckpt, process, failed);
	});
}

}