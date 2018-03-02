#include "gen-cpp2/StorageRpc.h"

#include <memory>
#include <iostream>
#include <thread>
#include <mutex>
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

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
};

class RpcConnection {
public:
	RpcConnection(RpcConnectHandle handle);
private:
	RpcConnectHandle rpc_handle_{kInvalidRpcHandle};

	struct {
		std::string vmid_;
		std::string vmdkid_;
		VmHandle vm_handle_{kInvalidVmHandle};
		VmdkHandle vmdk_handle_{kInvalidVmdkHandle};
		int eventfd_{-1};
	};

	struct {
		std::atomic<uint64_t> pending_{0};

		std::mutex mutex_;
		std::unordered_map<RequestID, std::unique_ptr<Request>> scheduled_;
		std::vector<std::unique_ptr<Request>> complete_;
	} requests_;

	std::atomic<RequestID> requestid_{0};

	std::unique_ptr<folly::EventBase> base_;
	std::unique_ptr<std::thread> runner_;
};

RpcConnection::RpcConnection(StorRpcConnectHandle handle) : rpc_handle_(handle) {
}

RpcConnection::~RpcConnection() {
	assert(pending_ == 0);
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

		this->client_ = std::make_unique<CalculatorAsyncClient>(
			HeaderClientChannel::newChannel(
				async::TAsyncSocket::newSocket(this->base.get(),
					{kServerIp, kServerPort})));

		auto chan = dynamic_cast<HeaderClientChannel*>(client->getHeaderChannel());

		this->base.loopForever();

		chan->closeNow();
	});
}

int RpcConnection::OpenVmdk(std::string vmid, std::string vmdkid, int eventfd) {
	vm_handle_ = client_->sync_GetVmHandle(vmid_);
	if (vm_handle_ == kInvalidVmHandle) {
		return -ENODEV;
	}

	vmdk_handle_ = client_->sync_GetVmdkHandle(vm_handle_, vmdkid_);
	if (vmdk_handle_ == kInvalidVmHandle) {
		return -ENODEV;
	}

	this->vmid_ = std::move(vmid);
	this->vmdkid_ = std::move(vmdkid);
	this->eventfd_ = eventfd;
	return 0;
}

uint64_t RpcConnect::PendingOperations() const {
	return pending_;
}

Request* RpcConnect::NewRequest(Request::Type type, const void* privatep,
		char* bufferp, size_t buf_sz, size_t xfer_sz, int64_t offset) {
	auto request = std::make_unique<Request>();
	if (pio_unlikely(not request)) {
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

void RpcConnect::RequestComplete(Request* reqp) {
	std::lock_guard<std::mutex> lock(requests_.mutex_);
	auto it = requests_.scheduled_.find(reqp->id);
	assert(pio_unlikely(it == requests_.scheduled_.end()));

	auto request = std::move(it.second);
	requests_.scheduled_.erase(it);
	requests_.complete_.emplace(std::move(request));

	::eventfd_write(eventfd_, 1);
}

uint32_t RpcConnect::GetCompleteRequests(RequestResult* resultsp,
		uint32_t nresults, bool *has_morep) {
	std::vector<std::unique_ptr<Request>> dst;

	{
		dst.reserve(nresults);
		std::lock_guard<std::mutex> lock(requests_.mutex_);
		*has_morep = requests_.complete_.size() > nresults;
		pio::MoveLastElements(dst, requests_.complete_, nresults);
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

RequestID RpcConnect::ScheduleRead(const void* privatep, char* bufferp,
		int32_t length, int64_t offset) {
	auto reqp = NewRequest(Request::Type::kRead, privatep, bufferp, length,
		length, offset);
	if (pio_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	base_->runInEventBaseThread([this, reqp] () mutable {
		client_->future_Read(vmdk_handle_, reqp->id, length, offset)
		.then([this, reqp] (std::unique_ptr<ReadResult> result) mutable {
			reqp->result = result->result;
			if (pio_likely(reqp->result == 0)) {
				assert(reqp->buf_sz == result.data.size());
				::memcpy(reqp->bufferp, result->data.data(), reqp->buf_sz);
			}
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
	g_connections_.map_.insert(handle, std::move(rpc));
	return handle;
}

RpcConnection* FindRpcConnection(RpcConnectHandle handle) {
	std::lock_guard<std::mutex> lock(g_connections_.mutex_);
	auto it = g_connections_.map_.find(handle);
	if (pio_unlikely(it == g_connections_.map_.end())) {
		return nullptr;
	}

	return it.second.get();
}

int RpcOpenVmdk(RpcConnectHandle handle, const char* vmid, const char* vmdkid,
		int eventfd) {
	auto rpc = FindRpcConnection(handle);
	if (pio_unlikely(rpc == nullptr)) {
		return -ENODEV;
	}

	return rpc->OpenVmdk(vmid, vmdkid, eventfd);
}

int RpcCloseVmdk(RpcConnectHandle handle) {
	std::lock_guard<std::mutex> lock(g_connections_.mutex_);
	auto it = g_connections_.map_.find(handle);
	if (pio_unlikely(it == g_connections_.map_.end())) {
		return -ENODEV;
	}

	auto rpc = it.second.get();
	if (rpc->PendingOperations()) {
		return -EBUSY;
	}

	g_connections_.map_.erase(it);
	return 0;
}

RequestID RpcScheduleRead(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	auto rpc = FindRpcConnection(handle);
	if (pio_unlikely(rpc == nullptr)) {
		return kInvalidRequestID;
	}

	return rpc->ScheduleRead(privatep, bufferp, buf_sz, offset);
}

uint32_t RpcGetCompleteRequests(RpcConnection handle, RequestResult *resultsp,
		uint32_t nresults, bool *has_morep) {
	auto rpc = FindRpcConnection(handle);
	if (pio_unlikely(rpc == nullptr)) {
		*has_morep = false;
		return 0;
	}

	return rpc->GetCompleteRequests(resultsp, nresults, has_morep);
}

} // namespace hyc

RpcConnectHandle HycStorRpcServerConnect() {
	try  {
		return hyc::RpcServerConnect();
	} catch (std::exception& e) {
		return kInvalidRpcHandle
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

RequestID HycScheduleRead(const void* privatep, char* bufferp, int32_t buf_sz,
		int64_t offset) {
	try {
		return hyc::RpcScheduleRead(privatep, bufferp, buf_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}

uint32_t HycGetCompleteRequests(RpcConnection handle, RequestResult *resultsp,
		uint32_t nresults, bool *has_morep) {
	try {
		return hyc::RpcGetCompleteRequests(handle, resultsp, nresults,
			has_morep);
	} catch (const std::exception& e) {
		*has_morep = true;
		return 0;
	}
}