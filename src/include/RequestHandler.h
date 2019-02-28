#pragma once

#include <folly/futures/Future.h>

#include "gen-cpp2/MetaData_types.h"
#include "DaemonCommon.h"
#include "DaemonUtils.h"

namespace pio {
class RequestHandler {
public:
	RequestHandler(const char* namep, void *udatap);
	virtual ~RequestHandler();
	virtual RequestHandler* GetRequestHandler(const char* namep) noexcept;

	void RegisterNextRequestHandler(std::unique_ptr<RequestHandler> handlerp);

	virtual std::string Name() const;
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) = 0;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request* reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) = 0;
	virtual folly::Future<int> BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) = 0;
	virtual folly::Future<int> BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) = 0;
	virtual folly::Future<int> BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) = 0;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) = 0;
	virtual folly::Future<int> Flush(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed);
	virtual folly::Future<int> BulkFlush(ActiveVmdk *vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed);
	virtual folly::Future<int> Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed);
	virtual folly::Future<int> BulkMove(ActiveVmdk *vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed);
	virtual int Cleanup(ActiveVmdk *vmdkp);

	virtual folly::Future<int> Delete(ActiveVmdk* vmdkp,
			const ::ondisk::CheckPointID ckpt_id,
			const std::pair<::ondisk::BlockID, ::ondisk::BlockID> range) {
		if (not nextp_) {
			return 0;
		}
		return nextp_->Delete(vmdkp, ckpt_id, range);
	}

protected:
	const char* namep_{};
	void *udatap_{};
	std::unique_ptr<RequestHandler> nextp_;
};
}
