#include <cerrno>

#include <iterator>
#include <vector>

#include "CacheHandler.h"
#include "LockHandler.h"
#include "UnalignedHandler.h"
#include "CompressHandler.h"
#include "EncryptHandler.h"
#include "DirtyHandler.h"
#include "CleanHandler.h"

namespace pio {
CacheHandler::CacheHandler() : RequestHandler(nullptr) {
	InitializeRequestHandlers();
}

void CacheHandler::InitializeRequestHandlers() {
	auto lock = std::make_unique<LockHandler>();
	auto unalingned = std::make_unique<UnalignedHandler>();
	auto compress = std::make_unique<CompressHandler>();
	auto encrypt = std::make_unique<EncryptHandler>();
	auto dirty = std::make_unique<DirtyHandler>();
	auto clean = std::make_unique<CleanHandler>();

	headp_ = std::move(lock);
	headp_->RegisterNextRequestHandler(std::move(unalingned));
	headp_->RegisterNextRequestHandler(std::move(compress));
	headp_->RegisterNextRequestHandler(std::move(encrypt));
	headp_->RegisterNextRequestHandler(std::move(dirty));
	headp_->RegisterNextRequestHandler(std::move(clean));
}

CacheHandler::~CacheHandler() {

}

folly::Future<int> CacheHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	assert(headp_);
	return headp_->Read(vmdkp, reqp, process, failed);
}

folly::Future<int> CacheHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	assert(headp_);
	return headp_->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> CacheHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	assert(headp_);
	return headp_->ReadPopulate(vmdkp, reqp, ckpt, process, failed);
}

}