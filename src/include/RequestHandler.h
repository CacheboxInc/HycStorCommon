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
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) = 0;
protected:
	void *udatap_;
	std::unique_ptr<RequestHandler> nextp_;
};

class BlockTraceHandler : public RequestHandler {
public:
	BlockTraceHandler();
	~BlockTraceHandler();
};

class CompressionHandler : public RequestHandler {
public:
	CompressionHandler();
	~CompressionHandler();
};

class EncryptionHandler : public RequestHandler {
public:
	EncryptionHandler();
	~EncryptionHandler();
};

class CacheHandler : public RequestHandler {
public:
	CacheHandler();
	~CacheHandler();
};

class ReadAheadHandler : public RequestHandler {
public:
	ReadAheadHandler();
	~ReadAheadHandler();
};

class NetworkHandler : public RequestHandler {
public:
	NetworkHandler();
	~NetworkHandler();
};

}