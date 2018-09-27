#pragma once

#include "RequestHandler.h"

namespace pio {

namespace hyc {
struct hyc_compress_ctx_;
}

class CompressHandler : public RequestHandler {
public:
	CompressHandler(const config::VmdkConfig* configp);
	~CompressHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
private:
	std::pair<std::unique_ptr<RequestBuffer>, int32_t>
	RequestBlockReadComplete(ActiveVmdk* vmdkp, RequestBlock* blockp);
private:
	bool enabled_{false};
	std::string algorithm_{"snappy"};
	hyc::hyc_compress_ctx_* ctxp_{nullptr};
};
}
