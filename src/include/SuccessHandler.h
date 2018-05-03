#pragma once

#include <random>
#include <memory>
#include "RequestHandler.h"

namespace pio {

/* Forward declaration for Pimpl */
class BackGroundWorkers;
class Work;

class SuccessHandler : public RequestHandler {
public:
	SuccessHandler(const config::VmdkConfig* configp);
	~SuccessHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;

	int ReadNow(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed);

	int WriteNow(ActiveVmdk *vmdkp, Request *reqp, CheckPointID ckpt,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed);
private:
	folly::Future<int> WriteDelayed(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed);
	folly::Future<int> ReadDelayed(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed);
private:
	bool enabled_{false};
	int32_t delay_{0};
	std::random_device rd_;
	std::mt19937 gen_;
	std::uniform_int_distribution<int32_t> distr_;

	struct {
		std::unique_ptr<pio::BackGroundWorkers> scheduler_;

		mutable std::mutex mutex_;
		std::vector<std::shared_ptr<Work>> scheduled_;
	} work_;
};
}