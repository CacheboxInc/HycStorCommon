#pragma once

#include "RequestHandler.h"

namespace pio {
/* forward declaration for pimpl */
namespace config {
	class VmdkConfig;
}

class CacheTargetHandler : public RequestHandler {
public:
	static constexpr char kName[] = "CacheTarget";

	CacheTargetHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp);
	~CacheTargetHandler();
	virtual RequestHandler* GetRequestHandler(const char* namep) noexcept override;
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
	virtual folly::Future<int> Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> BulkMove(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> Delete(ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		std::pair<::ondisk::BlockID, ::ondisk::BlockID> range) override;
private:
	void InitializeRequestHandlers(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp);
private:
	std::unique_ptr<RequestHandler> headp_;
};
}
