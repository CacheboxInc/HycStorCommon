#pragma once

#include <folly/futures/Future.h>

#include "Common.h"

namespace pio {
class RequestHandler {
public:
	RequestHandler(void *udatap);
	virtual ~RequestHandler();

	void RegisterNextRequestHandler(std::unique_ptr<RequestHandler> handlerp);

	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) = 0;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) = 0;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) = 0;
protected:
	void *udatap_;
	std::unique_ptr<RequestHandler> nextp_;
};

class NetworkHandler : public RequestHandler {
public:
	NetworkHandler();
	~NetworkHandler();
};

}