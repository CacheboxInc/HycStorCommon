#include <vector>
#include <string>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "Request.h"
#include "RequestHandler.h"
#include "RangeLock.h"
#include "LockHandler.h"

using namespace ::ondisk;

namespace pio {

using Guard = RangeLock::LockGuard;

LockHandler::LockHandler() : RequestHandler(nullptr),
		range_lock_(std::make_unique<RangeLock::RangeLock>()) {
}

LockHandler::~LockHandler() {

}

folly::Future<int> LockHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	auto[start, end] = reqp->Blocks();

	auto g = std::make_unique<Guard>(range_lock_.get(), start, end);
	return g->Lock()
	.then([g = std::move(g), vmdkp, reqp, &process, &failed, this] () mutable {
		log_assert(g->IsLocked());
		if (not nextp_) {
			return folly::makeFuture(0);
		}
		return nextp_->Read(vmdkp, reqp, process, failed);
	});
}

folly::Future<int> LockHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	auto[start, end] = reqp->Blocks();

	auto g = std::make_unique<Guard>(range_lock_.get(), start, end);
	return g->Lock()
	.then([g = std::move(g), vmdkp, reqp, &process, &failed, this, ckpt] () mutable {
		log_assert(g->IsLocked());
		if (not nextp_) {
			return folly::makeFuture(0);
		}
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	});
}

folly::Future<int> LockHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	auto[start, end] = reqp->Blocks();

	auto g = std::make_unique<Guard>(range_lock_.get(), start, end);
	return g->Lock()
	.then([g = std::move(g), vmdkp, reqp, &process, &failed, this] () mutable {
		log_assert(g->IsLocked());
		if (not nextp_) {
			return folly::makeFuture(0);
		}
		return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
	});
}

folly::Future<int> LockHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	auto[start, end] = reqp->Blocks();

	auto g = std::make_unique<Guard>(range_lock_.get(), start, end);
	return g->Lock()
	.then([g = std::move(g), vmdkp, reqp, &process, &failed, this] () mutable {
		log_assert(g->IsLocked());
		if (pio_unlikely(not nextp_)) {
			return folly::makeFuture(0);
		}
		return nextp_->Move(vmdkp, reqp, process, failed);
	});
}
}
