#pragma once

#include "RequestHandler.h"
#include "HycEncrypt.hpp"

namespace pio {
namespace hyc {
	struct hyc_encrypt_ctx_;
}

class EncryptHandler : public RequestHandler {
public:
	static constexpr char kName[] = "EncryptHandler";
	EncryptHandler(const ActiveVmdk* vmdkp, const config::VmdkConfig* configp);
	~EncryptHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
private:
	int ProcessWrite(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed);
	int ReadComplete(const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed);
private:
	bool enabled_{false};
	std::string algorithm_{"aes256-gcm"};
	std::vector<uint64_t> keyids_;
	//hyc::hyc_encrypt_ctx_* ctxp_{nullptr};
	pio::hyc::HycEncrypt *ctxp_{nullptr};
};
}
