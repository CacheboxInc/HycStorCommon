#pragma once

#include "RequestHandler.h"
#include "AeroOps.h"

namespace pio {
class CleanHandler : public RequestHandler {
public:
	CleanHandler(const config::VmdkConfig* configp);
	~CleanHandler();
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
	std::unique_ptr<RequestHandler> headp_;
	std::unique_ptr<AeroSpike> aero_obj_{nullptr};
};
}
