#include <memory>
#include <iostream>
#include <thread>
#include <mutex>
#include <string>

#include <cassert>
#include <cstdint>

#include <sys/eventfd.h>
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "Utils.h"
#include "TgtTypes.h"
#include "gen-cpp2/StorRpc.h"


namespace hyc {
using namespace apache::thrift;
using namespace apache::thrift::async;
using namespace hyc_thrift;

static constexpr int32_t kServerPort = 9876;
static const std::string kServerIp = "127.0.0.1";

struct Request {
	enum class Type {
		kRead,
		kWrite,
		kWriteSame,
	};

	RequestID id;
	Type type;
	const void* privatep;
	char* bufferp;
	int32_t buf_sz;
	int32_t xfer_sz;
	int64_t offset;
	int32_t result;
};

std::ostream& operator << (std::ostream& os, const Request::Type type) {
	switch (type) {
	case Request::Type::kRead:
		os << "read";
		break;
	case Request::Type::kWrite:
		os << "write";
		break;
	case Request::Type::kWriteSame:
		os << "writesame";
		break;
	}
	return os;
}

std::ostream& operator << (std::ostream& os, const Request& request) {
	os << "ID " << request.id
		<< " type " << request.type
		<< " privatep " << request.privatep
		<< " bufferp " << request.bufferp
		<< " buf_sz " << request.buf_sz
		<< " xfer_sz " << request.xfer_sz
		<< " offset " << request.offset;
	return os;
}

class RpcConnection {
public:
	RpcConnection(RpcConnectHandle handle);
	~RpcConnection();
	void Connect();
	int OpenVmdk(std::string vmid, std::string vmdkid, int eventfd);
	int CloseVmdk();
	uint32_t GetCompleteRequests(RequestResult* resultsp,
		uint32_t nresults, bool *has_morep);
	RequestID ScheduleRead(const void* privatep, char* bufferp, int32_t buf_sz,
		int64_t offset);
	RequestID ScheduleWrite(const void* privatep, char* bufferp, int32_t buf_sz,
		int64_t offset);
	RequestID ScheduleWriteSame(const void* privatep, char* bufferp,
		int32_t buf_sz, int32_t write_sz, int64_t offset);

	friend std::ostream& operator << (std::ostream& os, const RpcConnection& rpc);
private:
	uint64_t PendingOperations() const;
	Request* NewRequest(Request::Type type, const void* privatep,
		char* bufferp, size_t buf_sz, size_t xfer_sz, int64_t offset);
	void RequestComplete(Request* reqp);

private:
	RpcConnectHandle rpc_handle_{kInvalidRpcHandle};
	std::string vmid_;
	std::string vmdkid_;
	VmdkHandle vmdk_handle_{kInvalidVmdkHandle};
	int eventfd_{-1};

	struct {
		std::atomic<uint64_t> pending_{0};

		std::mutex mutex_;
		std::unordered_map<RequestID, std::unique_ptr<Request>> scheduled_;
		std::vector<std::unique_ptr<Request>> complete_;
	} requests_;

	std::atomic<RequestID> requestid_{0};

	std::unique_ptr<StorRpcAsyncClient> client_;
	std::unique_ptr<folly::EventBase> base_;
	std::unique_ptr<std::thread> runner_;
};

std::ostream& operator << (std::ostream& os, const RpcConnection& rpc) {
	os << "VmID " << rpc.vmid_
		<< " VmdkID " << rpc.vmdkid_
		<< " VmdkHandle " << rpc.vmdk_handle_
		<< " eventfd " << rpc.eventfd_
		<< " pending " << rpc.requests_.pending_
		<< " requestid " << rpc.requestid_;
	return os;
}

RpcConnection::RpcConnection(RpcConnectHandle handle) : rpc_handle_(handle) {
}

RpcConnection::~RpcConnection() {
	assert(PendingOperations() == 0);
	if (base_) {
		base_->terminateLoopSoon();
	}

	if (runner_) {
		runner_->join();
	}
	runner_ = nullptr;
	base_ = nullptr;
}

void RpcConnection::Connect() {
	runner_ = std::make_unique<std::thread>([this] () mutable {
		this->base_ = std::make_unique<folly::EventBase>();

		this->client_ = std::make_unique<StorRpcAsyncClient>(
			HeaderClientChannel::newChannel(
				async::TAsyncSocket::newSocket(this->base_.get(),
					{kServerIp, kServerPort})));

		auto chan = dynamic_cast<HeaderClientChannel*>(client_->getHeaderChannel());

		this->base_->loopForever();

		chan->closeNow();
	});
}

int RpcConnection::OpenVmdk(std::string vmid, std::string vmdkid, int eventfd) {
	auto vmdk_handle_ = client_->sync_OpenVmdk(vmid, vmdkid);
	if (vmdk_handle_ == kInvalidVmHandle) {
		return -ENODEV;
	}
	this->vmid_ = std::move(vmid);
	this->vmdkid_ = std::move(vmdkid);
	this->eventfd_ = eventfd;
	return 0;
}

int RpcConnection::CloseVmdk() {
	if (PendingOperations() != 0) {
		return -EBUSY;
	}

	/* TODO: stop accepting new IOs */
	auto rc = client_->sync_CloseVmdk(vmdk_handle_);
	if (rc < 0) {
		return rc;
	}
	vmdk_handle_ = kInvalidVmdkHandle;
	return 0;
}

uint64_t RpcConnection::PendingOperations() const {
	return requests_.pending_;
}

Request* RpcConnection::NewRequest(Request::Type type, const void* privatep,
		char* bufferp, size_t buf_sz, size_t xfer_sz, int64_t offset) {
	auto request = std::make_unique<Request>();
	if (hyc_unlikely(not request)) {
		return nullptr;
	}

	request->id = ++requestid_;
	request->type = type;
	request->privatep = privatep;
	request->bufferp = bufferp;
	request->buf_sz = buf_sz;
	request->xfer_sz = xfer_sz;
	request->offset = offset;

	auto reqp = request.get();
	std::lock_guard<std::mutex> lock(requests_.mutex_);
	requests_.scheduled_.emplace(request->id, std::move(request));
	return reqp;
}

void RpcConnection::RequestComplete(Request* reqp) {
	std::lock_guard<std::mutex> lock(requests_.mutex_);
	auto it = requests_.scheduled_.find(reqp->id);
	assert(hyc_unlikely(it == requests_.scheduled_.end()));

	auto request = std::move(it->second);
	requests_.scheduled_.erase(it);
	requests_.complete_.emplace_back(std::move(request));

	if (hyc_likely(eventfd_ >= 0)) {
		auto rc = ::eventfd_write(eventfd_, 1);
		if (hyc_unlikely(rc < 0)) {
			LOG(ERROR) << "eventfd_write write failed RPC " << *this
				<< " Request " << *reqp;
		}
		(void) rc;
	}
}

uint32_t RpcConnection::GetCompleteRequests(RequestResult* resultsp,
		uint32_t nresults, bool *has_morep) {
	std::vector<std::unique_ptr<Request>> dst;

	{
		dst.reserve(nresults);
		std::lock_guard<std::mutex> lock(requests_.mutex_);
		*has_morep = requests_.complete_.size() > nresults;
		hyc::MoveLastElements(dst, requests_.complete_, nresults);
	}

	auto i = 0;
	RequestResult* resultp = resultsp;
	for (const auto& reqp : dst) {
		resultp->privatep   = reqp->privatep;
		resultp->request_id = reqp->id;
		resultp->result     = reqp->result;

		++i;
		++resultp;
	}

	return dst.size();
}

RequestID RpcConnection::ScheduleRead(const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset) {
	auto reqp = NewRequest(Request::Type::kRead, privatep, bufferp, buf_sz,
		buf_sz, offset);
	if (hyc_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	base_->runInEventBaseThread([this, reqp, buf_sz, offset] () mutable {
		client_->future_Read(vmdk_handle_, reqp->id, buf_sz, offset)
		.then([this, reqp] (const ReadResult& result) mutable {
			reqp->result = result.get_result();
			if (hyc_likely(reqp->result == 0)) {
				assert((uint32_t)reqp->buf_sz == result.get_data().size());
				::memcpy(reqp->bufferp, result.get_data().data(), reqp->buf_sz);
			}
			RequestComplete(reqp);
		})
		.onError([this, reqp] (const std::exception& e) mutable {
			reqp->result = -EIO;
			RequestComplete(reqp);
		});
	});
	return reqp->id;
}

RequestID RpcConnection::ScheduleWrite(const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset) {
	auto reqp = NewRequest(Request::Type::kWrite, privatep, bufferp, buf_sz,
		buf_sz, offset);
	if (hyc_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	base_->runInEventBaseThread([this, reqp, bufferp, buf_sz, offset] () mutable {
		std::string data(bufferp, buf_sz);
		assert(hyc_likely(data.size() == (uint32_t)buf_sz));
		client_->future_Write(vmdk_handle_, reqp->id, data, buf_sz, offset)
		.then([this, reqp] (const WriteResult& result) mutable {
			reqp->result = result.get_result();
			RequestComplete(reqp);
		})
		.onError([this, reqp] (const std::exception& e) mutable {
			reqp->result = -EIO;
			RequestComplete(reqp);
		});
	});
	return reqp->id;
}

RequestID RpcConnection::ScheduleWriteSame(const void* privatep, char* bufferp,
		int32_t buf_sz, int32_t write_sz, int64_t offset) {
	auto reqp = NewRequest(Request::Type::kWrite, privatep, bufferp, buf_sz,
		write_sz, offset);
	if (hyc_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	base_->runInEventBaseThread([this, reqp, bufferp, buf_sz, write_sz, offset]
			() mutable {
		std::string data(bufferp, buf_sz);
		assert(hyc_likely(data.size() == (uint32_t)buf_sz));
		client_->future_WriteSame(vmdk_handle_, reqp->id, data, buf_sz,
			write_sz, offset)
		.then([this, reqp] (const WriteResult& result) mutable {
			reqp->result = result.get_result();
			RequestComplete(reqp);
		})
		.onError([this, reqp] (const std::exception& e) mutable {
			reqp->result = -EIO;
			RequestComplete(reqp);
		});
	});
	return reqp->id;
}

/*
 * GLOBAL DATA STRUCTURES
 * ======================
 */
struct {
	std::mutex mutex_;
	std::unordered_map<RpcConnectHandle, std::unique_ptr<RpcConnection>> map_;
	std::atomic<RpcConnectHandle> handle_{0};
} g_connections_;

RpcConnectHandle RpcServerConnect() {
	auto handle = ++g_connections_.handle_;
	auto rpc = std::make_unique<RpcConnection>(handle);
	rpc->Connect();

	std::lock_guard<std::mutex> lock(g_connections_.mutex_);
	g_connections_.map_.emplace(handle, std::move(rpc));
	return handle;
}

RpcConnection* FindRpcConnection(RpcConnectHandle handle) {
	std::lock_guard<std::mutex> lock(g_connections_.mutex_);
	auto it = g_connections_.map_.find(handle);
	if (hyc_unlikely(it == g_connections_.map_.end())) {
		return nullptr;
	}

	return it->second.get();
}

int RpcOpenVmdk(RpcConnectHandle handle, const char* vmid, const char* vmdkid,
		int eventfd) {
	auto rpc = FindRpcConnection(handle);
	if (hyc_unlikely(rpc == nullptr)) {
		return -ENODEV;
	}

	return rpc->OpenVmdk(vmid, vmdkid, eventfd);
}

int RpcCloseVmdk(RpcConnectHandle handle) {
	auto rpc = FindRpcConnection(handle);
	if (hyc_unlikely(rpc == nullptr)) {
		return -ENODEV;
	}

	auto rc = rpc->CloseVmdk();
	if (rc < 0) {
		return rc;
	}

	std::lock_guard<std::mutex> lock(g_connections_.mutex_);
	auto it = g_connections_.map_.find(handle);
	assert(hyc_unlikely(it != g_connections_.map_.end()));
	g_connections_.map_.erase(it);
	return 0;
}

RequestID RpcScheduleRead(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	auto rpc = FindRpcConnection(handle);
	if (hyc_unlikely(rpc == nullptr)) {
		return kInvalidRequestID;
	}

	return rpc->ScheduleRead(privatep, bufferp, buf_sz, offset);
}

uint32_t RpcGetCompleteRequests(RpcConnectHandle handle, RequestResult* resultsp,
		uint32_t nresults, bool *has_morep) {
	auto rpc = FindRpcConnection(handle);
	if (hyc_unlikely(rpc == nullptr)) {
		*has_morep = false;
		return 0;
	}

	return rpc->GetCompleteRequests(resultsp, nresults, has_morep);
}

RequestID RpcScheduleWrite(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	auto rpc = FindRpcConnection(handle);
	if (hyc_unlikely(rpc == nullptr)) {
		return kInvalidRequestID;
	}

	return rpc->ScheduleWrite(privatep, bufferp, buf_sz, offset);
}

RequestID RpcScheduleWriteSame(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset) {
	auto rpc = FindRpcConnection(handle);
	if (hyc_unlikely(rpc == nullptr)) {
		return kInvalidRequestID;
	}

	return rpc->ScheduleWriteSame(privatep, bufferp, buf_sz, write_sz, offset);
}

} // namespace hyc

RpcConnectHandle HycStorRpcServerConnect() {
	try  {
		return hyc::RpcServerConnect();
	} catch (std::exception& e) {
		return kInvalidRpcHandle;
	}
}

int HycOpenVmdk(RpcConnectHandle handle, const char* vmid, const char* vmdkid,
		int eventfd) {
	assert(vmid != nullptr and vmdkid != nullptr and eventfd >= 0);
	try {
		return hyc::RpcOpenVmdk(handle, vmid, vmdkid, eventfd);
	} catch (std::exception& e) {
		return -ENODEV;
	}
}

int HycCloseVmdk(RpcConnectHandle handle) {
	try {
		return hyc::RpcCloseVmdk(handle);
	} catch (std::exception& e) {
		return -ENODEV;
	}
}

RequestID HycScheduleRead(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	try {
		return hyc::RpcScheduleRead(handle, privatep, bufferp, buf_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}

uint32_t HycGetCompleteRequests(RpcConnectHandle handle, RequestResult *resultsp,
		uint32_t nresults, bool *has_morep) {
	try {
		return hyc::RpcGetCompleteRequests(handle, resultsp, nresults,
			has_morep);
	} catch (const std::exception& e) {
		*has_morep = true;
		return 0;
	}
}

RequestID HycScheduleWrite(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	try {
		return hyc::RpcScheduleWrite(handle, privatep, bufferp, buf_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}

RequestID HycScheduleWriteSame(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset) {
	try {
		return hyc::RpcScheduleWriteSame(handle, privatep, bufferp, buf_sz,
			write_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}