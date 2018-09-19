#pragma once

#include "RequestHandler.h"

namespace pio {
/* forward declaration for pimpl */
namespace config {
	class VmdkConfig;
}

class MultiTargetHandler : public RequestHandler {
public:
	MultiTargetHandler(const ActiveVmdk* vmdkp, const config::VmdkConfig* configp);
	~MultiTargetHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Flush(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
private:
	void InitializeTargetHandlers(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp);
private:
	std::vector<std::unique_ptr<RequestHandler>> targets_;
};
}
