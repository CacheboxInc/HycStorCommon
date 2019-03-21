#pragma once

#include "RequestHandler.h"
#include <libaio.h>
#include <thread>
#include <atomic>
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include "Vmdk.h"

namespace pio {

class FileTargetHandler : public RequestHandler {
public:
	static constexpr char kName[] = "FileTarget";
	FileTargetHandler(const config::VmdkConfig* configp);
	virtual ~FileTargetHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
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
	const std::string& GetFileCachePath() const;
	void GatherEvents();
	int32_t Getfd(ActiveVmdk* vmdkp, const int32_t& snap_id);
	int32_t Getfd_v1(const int32_t& snap_id);
	int CreateNewDeltaContext(int64_t snap_id);

public:
	bool enabled_{false};
	std::string file_path_;
	int fd_{-1}; /* File fd */
	bool create_file_{false};
	std::string delta_file_path_;
	uint64_t file_size_{0};

#ifdef FILETARGET_ASYNC
	/* AIO related */
	int afd_{-1}; /* Aync processing fd */
	int ep_fd_{-1}; /* Epoll fd */
	io_context_t ctx_;
	std::atomic_bool thread_running_{true};
	std::thread thread_;
	std::atomic<uint32_t> pending_io_{0};
#endif
};

#ifdef FILETARGET_ASYNC
struct AIORequestBlock;
typedef void (*event_handler_t)(int fd, int events, void *data);
using req_block_type = std::shared_ptr<AIORequestBlock>;

enum class ReqType {
	OP_READ,
	OP_WRITE,
};
struct AIORequest{
	int cnt_{0};
	struct iocb *iocbs_{NULL};
	std::shared_ptr<folly::Promise<int>> promise_{nullptr};
	AIORequest(int cnt, ReqType type);
	~AIORequest();
	int n_pending_{0};
	int status_{0};
	std::vector<req_block_type> reqblocks_list;
	ReqType req_type_;
};

struct AIORequestBlock{
	AIORequest *req_{nullptr};
	RequestBlock *blockp_{nullptr};
	std::unique_ptr<RequestBuffer> destp_;
};

struct event_data {
	event_handler_t handler;
	void *data{nullptr};
	int fd{-1};
};
#endif

}
