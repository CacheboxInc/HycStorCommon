#pragma once

#include "RequestHandler.h"

namespace pio {
class ErrorHandler : public RequestHandler {
public:
	ErrorHandler(const config::VmdkConfig* configp);
	~ErrorHandler();
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
	bool FailOperation();
private:
	bool enabled_{false};
	bool throw_{false};
	int  error_no_;
	uint64_t frequency_{0};
	std::atomic<uint64_t> total_ios_{0};
};
}
