#pragma once

#include "RamCache.h"

namespace pio {

class RamCacheHandler : public RequestHandler {
public:
	RamCacheHandler();
	virtual ~RamCacheHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
private:
	RamCache cache_;
};

}