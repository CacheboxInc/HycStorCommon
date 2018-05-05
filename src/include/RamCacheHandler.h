#pragma once

namespace pio {

/* forward declaration for pimpl */
class RamCache;

class RamCacheHandler : public RequestHandler {
public:
	RamCacheHandler(const config::VmdkConfig* configp);
	virtual ~RamCacheHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
private:
	std::unique_ptr<RamCache> cache_;
	bool enabled_{false};
	uint16_t memory_mb_{0};
};

}
