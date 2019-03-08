#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "CacheTargetHandler.h"
#include "NetworkTargetHandler.h"
#include "VddkTargetHandler.h"
#include "VmdkConfig.h"
#include "Request.h"
#include "DaemonUtils.h"

using namespace ::ondisk;

namespace pio {
VddkTargetHandler::VddkTargetHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) :
		RequestHandler(VddkTargetHandler::kName, nullptr) {
	InitializeRequestHandlers(CacheTargertHandler::kName, NetworkTargetHandler::kName);
}

RequestHandler* VddkTargetHandler::GetRequestHandler(const char* namep)
		noexcept {
	if (std::strncmp(VddkTargetHandler::kName, namep, std::strlen(namep)) == 0) {
		return this;
	}
	if (pio_unlikely(not headp_)) {
		return nullptr;
	}
	return headp_->GetRequestHandler(namep);
}


void VddkTargetHandler::InitializeRequestHandlers(const char* first,
		const char* second) {
	auto cache = std::make_unique<GetRequestHandler>(first);
	auto net = std::make_unique<GetRequestHandler>(second);

	headp_ = std::move(cache);
	headp_->RegisterNextRequestHandler(std::move(net));
}

VddkTargetHandler::~VddkTargetHandler() {

}

folly::Future<int> VddkTargetHandler::Delete(ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const std::pair<BlockID, BlockID> range) {
	return headp_->Delete(vmdkp, ckpt_id, range);
}

folly::Future<int> VddkTargetHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->Read(vmdkp, reqp, process, failed);
}

folly::Future<int> VddkTargetHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> VddkTargetHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> VddkTargetHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->Move(vmdkp, reqp, process, failed);
}

folly::Future<int> VddkTargetHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return headp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
}

folly::Future<int> VddkTargetHandler::BulkMove(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return headp_->BulkMove(vmdkp, ckpt, requests, process, failed);
}

folly::Future<int> VddkTargetHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return headp_->BulkRead(vmdkp, requests, process, failed);
}

folly::Future<int> VddkTargetHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return headp_->BulkReadPopulate(vmdkp, requests, process, failed);
}
}
