#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "CacheTargetHandler.h"
#include "DirtyHandler.h"
#include "CleanHandler.h"
#include "VmdkConfig.h"
#include "Request.h"
#include "DaemonUtils.h"

using namespace ::ondisk;

namespace pio {
CacheTargetHandler::CacheTargetHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) :
		RequestHandler(CacheTargetHandler::kName, nullptr) {
	InitializeRequestHandlers(vmdkp, configp);
}

RequestHandler* CacheTargetHandler::GetRequestHandler(const char* namep)
		noexcept {
	if (std::strncmp(CacheTargetHandler::kName, namep, std::strlen(namep)) == 0) {
		return this;
	}
	if (pio_unlikely(not headp_)) {
		return nullptr;
	}
	return headp_->GetRequestHandler(namep);
}


void CacheTargetHandler::InitializeRequestHandlers(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) {
	auto dirty = std::make_unique<DirtyHandler>(vmdkp, configp);
	auto clean = std::make_unique<CleanHandler>(vmdkp, configp);

	headp_ = std::move(dirty);
	headp_->RegisterNextRequestHandler(std::move(clean));
}

CacheTargetHandler::~CacheTargetHandler() {

}

folly::Future<int> CacheTargetHandler::Delete(ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const std::pair<BlockID, BlockID> range) {
	return headp_->Delete(vmdkp, ckpt_id, range);
}

folly::Future<int> CacheTargetHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->Read(vmdkp, reqp, process, failed);
}

folly::Future<int> CacheTargetHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> CacheTargetHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> CacheTargetHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->Move(vmdkp, reqp, process, failed);
}

folly::Future<int> CacheTargetHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return headp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
}

folly::Future<int> CacheTargetHandler::BulkMove(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return headp_->BulkMove(vmdkp, ckpt, requests, process, failed);
}

folly::Future<int> CacheTargetHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return headp_->BulkRead(vmdkp, requests, process, failed);
}

folly::Future<int> CacheTargetHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return headp_->BulkReadPopulate(vmdkp, requests, process, failed);
}
}
