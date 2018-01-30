#pragma once

#include "RequestHandler.h"

namespace pio {
/* forward declaration for pimpl */
namespace config {
	class VmdkConfig;
}

class CacheHandler : public RequestHandler {
public:
	CacheHandler(const config::VmdkConfig* configp);
	~CacheHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
private:
	void InitializeRequestHandlers(const config::VmdkConfig* configp);
private:
	std::unique_ptr<RequestHandler> headp_;
};
}