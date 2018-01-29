#pragma once

#include "RequestHandler.h"
#include "JsonConfig.h"

namespace pio {
class CompressHandler : public RequestHandler {
public:
	CompressHandler(config::JsonConfig* configp);
	~CompressHandler();
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
};
}