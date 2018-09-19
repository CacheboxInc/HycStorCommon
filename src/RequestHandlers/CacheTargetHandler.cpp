#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "CacheTargetHandler.h"
#include "DirtyHandler.h"
#include "CleanHandler.h"
#include "RamCacheHandler.h"
#include "ErrorHandler.h"
#include "SuccessHandler.h"
#include "VmdkConfig.h"
#include "Request.h"
#include "DaemonUtils.h"

using namespace ::ondisk;

namespace pio {
CacheTargetHandler::CacheTargetHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) : RequestHandler(nullptr) {
	InitializeRequestHandlers(vmdkp, configp);
}

void CacheTargetHandler::InitializeRequestHandlers(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) {
	auto ram_cache = std::make_unique<RamCacheHandler>(configp);
	auto dirty = std::make_unique<DirtyHandler>(vmdkp, configp);
	auto clean = std::make_unique<CleanHandler>(vmdkp, configp);

	headp_ = std::move(ram_cache);
	headp_->RegisterNextRequestHandler(std::move(dirty));
	headp_->RegisterNextRequestHandler(std::move(clean));

	if (configp->ErrorHandlerEnabled()) {
		auto error = std::make_unique<ErrorHandler>(configp);
		headp_->RegisterNextRequestHandler(std::move(error));
	}

	if (configp->IsSuccessHandlerEnabled()) {
		auto success = std::make_unique<SuccessHandler>(configp);
		headp_->RegisterNextRequestHandler(std::move(success));
	}
}

CacheTargetHandler::~CacheTargetHandler() {

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

folly::Future<int> CacheTargetHandler::Flush(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(0);
}

folly::Future<int> CacheTargetHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(headp_);
	return headp_->Move(vmdkp, reqp, process, failed);
}
}
