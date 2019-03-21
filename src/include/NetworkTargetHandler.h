#pragma once

#include "RequestHandler.h"
#include "Request.h"
#include "Common.hpp"
#include "IO.hpp"
#include "IOSession.hpp"
#include "Target.hpp"

#include <folly/fibers/Fiber.h>
#include <folly/fibers/FiberManager.h>
#include <folly/fibers/GenericBaton.h>
#include <folly/io/async/EventBase.h>

namespace pio {
using namespace hyc;
class NetworkTargetHandler : public RequestHandler, public IORequestor {
public:
	static constexpr char kName[] = "NetworkHandler";
	NetworkTargetHandler(const config::VmdkConfig* configp);
	~NetworkTargetHandler();

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
	virtual int Cleanup(ActiveVmdk *vmdkp) override;

	virtual int IOProcessed(IOSession *session, std::shared_ptr<IO> io) override;

	virtual int RegisterIOProcessor(IOProcessor *processor, bool preferred, int srcid, int destid) override;
	virtual int UnRegisterIOProcessor() override;
	int64_t GetSnapID(ActiveVmdk* vmdkp, const uint64_t& ckpt_id);

	int Open();

private:
	IOSession *io_session_{nullptr};
	Target *target_{nullptr};
	int srcid_;
	int destid_;
	std::string vm_id_;
	std::string vmdk_id_;
};
}
