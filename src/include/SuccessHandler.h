#pragma once

#include "RequestHandler.h"

namespace pio {
class SuccessHandler : public RequestHandler {
public:
	SuccessHandler(const config::VmdkConfig* configp);
	~SuccessHandler();
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
	bool enabled_{false};
};
}
