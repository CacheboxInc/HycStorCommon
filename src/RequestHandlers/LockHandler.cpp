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
		return nextp_->Read(vmdkp, reqp, process, failed)
		.then([g = std::move(g)] (int rc) mutable {
			return rc;
		});
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
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed)
		.then([g = std::move(g)] (int rc) mutable {
			return rc;
		});
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
		return nextp_->ReadPopulate(vmdkp, reqp, process, failed)
		.then([g = std::move(g)] (int rc) mutable {
			return rc;
		});
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
		return nextp_->Move(vmdkp, reqp, process, failed)
		.then([g = std::move(g)] (int rc) mutable {
			return rc;
		});
	});
}

folly::Future<int> LockHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	auto g = std::make_unique<Guard>(range_lock_.get(), Ranges(requests));
	return g->Lock()
	.then([this, g = std::move(g), vmdkp, ckpt, &requests, &process, &failed]
			(int rc) mutable -> folly::Future<int> {
		if (pio_unlikely(not g->IsLocked() || rc < 0)) {
			return rc ? rc : -1;
		} else if (pio_unlikely(not nextp_)) {
			return 0;
		}

		return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed)
		.then([g = std::move(g)] (int rc) {
			return rc;
		});
	});
}

std::vector<pio::RangeLock::range_t> LockHandler::Ranges(
		const std::vector<std::unique_ptr<Request>>& requests) {
	std::vector<pio::RangeLock::range_t> ranges;
	ranges.reserve(requests.size());
	for (const auto& request : requests) {
		ranges.emplace_back(request->Blocks());
	}
	return ranges;
}

folly::Future<int> LockHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	auto g = std::make_unique<Guard>(range_lock_.get(), Ranges(requests));
	return g->Lock()
	.then([this, g = std::move(g), vmdkp, &requests, &process, &failed]
			(int rc) mutable -> folly::Future<int> {
		if (pio_unlikely(not g->IsLocked() || rc < 0)) {
			return rc ? rc : -1;
		} else if (pio_unlikely(not nextp_)) {
			return 0;
		}

		return nextp_->BulkRead(vmdkp, requests, process, failed)
		.then([g = std::move(g)] (int rc) {
			return rc;
		});
	});
}

folly::Future<int> LockHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	auto g = std::make_unique<Guard>(range_lock_.get(), Ranges(requests));
	return g->Lock()
	.then([this, g = std::move(g), vmdkp, &requests, &process, &failed]
			(int rc) mutable -> folly::Future<int> {
		if (pio_unlikely(not g->IsLocked() || rc < 0)) {
			return rc ? rc : -1;
		} else if (pio_unlikely(not nextp_)) {
			return 0;
		}

		return nextp_->BulkReadPopulate(vmdkp, requests, process, failed)
		.then([g = std::move(g)] (int rc) {
			return rc;
		});
	});
}
}
