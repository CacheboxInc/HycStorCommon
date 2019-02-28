#pragma once

#include "RequestHandler.h"

namespace pio {
/* forward declaration for pimpl */
namespace config {
	class VmdkConfig;
}

class MultiTargetHandler : public RequestHandler {
public:
	static constexpr char kName[] = "MultiTarget";
	MultiTargetHandler(const ActiveVmdk* vmdkp, const config::VmdkConfig* configp);
	~MultiTargetHandler();
	virtual RequestHandler* GetRequestHandler(const char* namep) noexcept override;
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	folly::Future<int> BulkWrite(ActiveVmdk* vmdkp,
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
	virtual folly::Future<int> Flush(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> BulkFlush(ActiveVmdk *vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	folly::Future<int> BulkMove(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> Delete(ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		std::pair<::ondisk::BlockID, ::ondisk::BlockID> range) override;
private:
	void InitializeTargetHandlers(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp);
	folly::Future<int> BulkReadComplete(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed);
	void RecursiveDelete(
		ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const std::pair<::ondisk::BlockID, ::ondisk::BlockID> range,
		std::vector<std::unique_ptr<pio::RequestHandler>>::iterator cur,
		std::vector<std::unique_ptr<pio::RequestHandler>>::iterator end,
		folly::Promise<int>&& promise);

private:
	std::vector<std::unique_ptr<RequestHandler>> targets_;
};
}
