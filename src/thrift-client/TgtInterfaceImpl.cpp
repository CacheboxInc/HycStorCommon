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
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <folly/init/Init.h>

#include "Utils.h"
#include "TgtTypes.h"
#include "gen-cpp2/StorRpc.h"
#include "TgtInterface.h"
#include "TimePoint.h"
#include "SpinLock.h"

static std::string StordIp = "127.0.0.1";
static uint16_t StordPort = 9876;

namespace hyc {
using namespace apache::thrift;
using namespace apache::thrift::async;
using namespace hyc_thrift;
using namespace folly;

class ReschedulingTimeout : public AsyncTimeout {
public:
	ReschedulingTimeout(EventBase* basep, uint32_t milli) :
			AsyncTimeout(basep), milli_(milli) {
	}

	~ReschedulingTimeout() {
		Cancel();
	}

	void timeoutExpired() noexcept override {
		if (func_()) {
			ScheduleTimeout(func_);
		}
	}

	void ScheduleTimeout(std::function<bool (void)> func) {
		func_ = func;
		scheduleTimeout(milli_);
	}

	void Cancel() {
		if (not isScheduled()) {
			return;
		}
		this->cancelTimeout();
	}

private:
	uint32_t milli_{0};
	std::function<bool (void)> func_;
};

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

	TimePoint timer;
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

class StordConnection {
public:
	/* list of deleted functions */
	StordConnection(const StordConnection& rhs) = delete;
	StordConnection(const StordConnection&& rhs) = delete;
	StordConnection& operator = (const StordConnection& rhs) = delete;
	StordConnection& operator = (const StordConnection&& rhs) = delete;

public:
	~StordConnection();
	int32_t Connect(const std::string& ip, const uint16_t port);
	folly::EventBase* GetEventBase() const noexcept;
	StorRpcAsyncClient* GetRpcClient() const noexcept;

private:
	int32_t Disconnect();
	void SetPingTimeout();
private:
	std::unique_ptr<StorRpcAsyncClient> client_;
	std::unique_ptr<folly::EventBase> base_;
	std::unique_ptr<std::thread> runner_;
};

folly::EventBase* StordConnection::GetEventBase() const {
	return base_.get();
}

StorRpcAsyncClient* StordConnection::GetRpcClient() const {
	return client_.get();
}

StordConnection::~StordConnection() {
	assert(not PendingOperations());
	Disconnect();
}

int32_t StordConnection::Disconnect() {
	if (PendingOperations()) {
		return -EBUSY;
	}

	if (base_) {
		base_->terminateLoopSoon();
	}

	if (runner_) {
		runner_->join();
	}
	return 0;
}

StordConnection::Connect(const std::string& ip,
		const uint16_t port) {
	std::mutex m;
	std::condition_variable cv;
	bool started = false;
	int32_t result = 0;

	runner_ = std::make_unique<std::thread>(
			[this, &m, &cv, &started, &result] () mutable {
		try {
			::sleep(1);
			std::this_thread::yield();

			auto base = std::make_unique<folly::EventBase>();
			auto client = std::make_unique<StorRpcAsyncClient>(
				HeaderClientChannel::newChannel(
					async::TAsyncSocket::newSocket(base.get(),
						{ip, port})));

			{
				/*
				 * ping stord
				 * - ping sends a response string back to client.
				 * If the server is not connected or connection isrefused, this
				 * throws AsyncSocketException .
				 */
				std::string pong;
				client->sync_Ping(pong);
			}

			auto chan =
				dynamic_cast<HeaderClientChannel*>(client->getHeaderChannel());

			this->base_ = std::move(base);
			this->client_ = std::move(client);
			SetPingTimeout();

			{
				/* notify main thread of success */
				result = 0;
				started = true;
				std::unique_lock<std::mutex> lk(m);
				cv.notify_all();
			}

			VLOG(1) << *this <<  " EventBase looping forever";
			this->base_->loopForever();
			VLOG(1) << *this << " EventBase loop stopped";

			chan->closeNow();
			base = std::move(this->base_);
			client = std::move(this->client_);
		} catch (const folly::AsyncSocketException& e) {
			/* notify main thread of failure */
			LOG(ERROR) << "Failed to connect with stord "
				<< e.getType() << "," << e.getErrno() << " ";
			started = true;
			result = -1;
			std::unique_lock<std::mutex> lk(m);
			cv.notify_all();
		} catch (const apache::thrift::transport::TTransportException& e) {
			LOG(ERROR) << "Failed to connect with stord";
			started = true;
			result = -1;
			std::unique_lock<std::mutex> lk(m);
			cv.notify_all();
		} catch (const std::exception& e) {
			LOG(ERROR) << "Received exception " << e.what();
			started = true;
			result = -1;
			std::unique_lock<std::mutex> lk(m);
			cv.notify_all();
		}
	});

	std::unique_lock<std::mutex> lk(m);
	cv.wait(lk, [&started] { return started; });

	if (result < 0) {
		return result;
	}

	/* ensure that EventBase loop is started */
	this->base_->waitUntilRunning();
	VLOG(1) << *this << " Connection Result " << result;
	return 0;
}

void StordConnection::SetPingTimeout() {
	assert(base_ != nullptr);
	std::chrono::seconds s(ping_.timeout_secs_);
	auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(s).count();
	ping_.timeout_ = std::make_unique<ReschedulingTimeout>(base_.get(), ms);
	ping_.timeout_->ScheduleTimeout([this] () {
		auto fut = client_->future_Ping()
		.then([] (const std::string& result) {
			return 0;
		})
		.onError([] (const std::exception& e) {
			return -ENODEV;
		});
		return true;
	});
}

class StordRpc {
public:
	enum class SchedulePolicy {
		kRoundRobin,
		kCurrentCpu,
		kLeastUsed,
	};

	StordRpc(std::string ip, uint16_t port, uint16_t nthreads,
		SchedulePolicy policy);
	~StordRpc();
	int32_t Connect();
	StordConnection* GetStordConnection() const noexcept;
	void SetPolicy(SchedulePolicy policy);
private:
	StordConnection* GetStordConnectionRR() const noexcept;
	StordConnection* GetStordConnectionCpu() const noexcept;

private:
	const std::string ip_;
	const uint16_t port_{0};
	const uint16_t nthreads_{0};
	SchedulePolicy policy_;
	std::vector<std::unique_ptr<StordConnection>> connections_;

	struct {
		SchedulePolicy policy_;
		mutable std::mutex mutex_;
		mutable uint16_t last_cpu_{0};
	} policy_;
};

StordRpc::StordRpc(std::string ip, uint16_t port, uint16_t nthreads,
		StordRpc::SchedulePolicy policy) : ip_(std::move(ip)),
		port_(port), nthreads_(nthreads), policy_.policy_(policy) {
}

StordRpc::~StordRpc() {
	connections_.clear();
}

StordRpc::Connect() {
	connections_.reserve(nthreads_);
	for (auto i = 0u; i < nthreads_; ++i) {
		auto connect = std::mmake_unique<StordConnection>(i);
		auto rc = connect->Connect();
		assert(rc == 0);
		connections_.emplace_back(std::move(connect));
	}
}

void StordRpc::SetPolicy(SchedulePolicy policy) {
	policy_.policy_ = policy;
}

folly::EventBase* StordRpc::GetStordConnectionCpu() const {
	auto cpu = GetCurCpuCore();
	policy_.last_cpu_ = cpu;
	return connections_[cpu].get();
}

folly::EventBase* StordRpc::GetStordConnectionRR() const {
	std::lock_guard<std::mutex> lock(policy_.mutex_);
	auto cpu = (++last_cpu_) % nthreads_;
	last_cpu_ = cpu;
	return connections_[cpu].get();
}

StordConnection* StordRpc::GetStordConnection() const {
	switch (policy_.policy_) {
	case StordRpc::SchedulePolicy::kRoundRobin:
		return GetStordConnectionRR();
	case StordRpc::SchedulePolicy::kCurrentCpu:
		return GetStordConnectionCpu();
	case StordRpc::SchedulePolicy::kLeastUsed:
		return GetStordConnectionRR();
	}
}

struct VmdkStats {
	std::atomic<int64_t> read_requests_{0};
	std::atomic<int64_t> read_failed_{0};
	std::atomic<int64_t> read_bytes_{0};
	std::atomic<int64_t> read_latency_{0};

	std::atomic<int64_t> write_requests_{0};
	std::atomic<int64_t> write_failed_{0};
	std::atomic<int64_t> write_same_requests_{0};
	std::atomic<int64_t> write_same_failed_{0};
	std::atomic<int64_t> write_bytes_{0};
	std::atomic<int64_t> write_latency_{0};

	std::atomic<int64_t> pending_{0};
};

class StordVmdk {
public:
	int32_t OpenVmdk(StordConnection* connectp, std::string vmid,
		std::string vmdkid, int eventfd);
	int32_t CloseVmdk(StordConnection* connectp);
	uint32_t GetCompleteRequests(RequestResult* resultsp, uint32_t nresults,
		bool *has_morep);
	RequestID ScheduleRead(StordConnection* connectp, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset);
	RequestID ScheduleWrite(StordConnection* connectp, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset);
	RequestID ScheduleWriteSame(StordConnection* connectp, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset);

	friend std::ostream& operator << (std::ostream& os,
		const RpcConnection& rpc);
private:
	uint64_t PendingOperations() const;
	Request* NewRequest(Request::Type type, const void* privatep,
		char* bufferp, size_t buf_sz, size_t xfer_sz, int64_t offset);
	void RequestComplete(Request* reqp);
	void UpdateStats(Request* reqp);
	void PostRequestCompletion() const;
	void ReadDataCopy(Request* reqp, const ReadResult& result);
private:
	std::string vmid_;
	std::string vmdkid_;
	VmdkHandle vmdk_handle_{kInvalidVmdkHandle};
	int eventfd_{-1};

	struct {
		mutable SpinLock mutex_;
		std::unordered_map<RequestID, std::unique_ptr<Request>> scheduled_;
		std::vector<std::unique_ptr<Request>> complete_;
	} requests_;

	VmdkStats stats_;

	std::atomic<RequestID> requestid_{0};
};

std::ostream& operator << (std::ostream& os, const StordVmdk& vmdk) {
	os << " VmID " << vmdk.vmid_
		<< " VmdkID " << vmdk.vmdkid_
		<< " VmdkHandle " << vmdk.vmdk_handle_
		<< " eventfd " << vmdk.eventfd_
		<< " pending " << vmdk.stats_.total_pending_
		<< " requestid " << vmdk.requestid_;
	return os;
}

int32_t StordVmdk::OpenVmdk(StordConnection* connectp, std::string vmid,
		std::string vmdkid, int eventfd) {
	if (vmdk_handle_ != kInvalidVmdkHandle) {
		/* already open */
		return -EBUSY;
	}

	auto rpcp = connectp->GetRpcClient();
	auto vmdk_handle = rpcp->future_OpenVmdk(vmid, vmdkid).get();
	if (vmdk_handle == kInvalidVmHandle) {
		return -ENODEV;
	}
	vmid_ = std::move(vmid);
	vmdkid_ = std::move(vmdkid);
	eventfd_ = eventfd;
	vmdk_handle_ = vmdk_handle;
	return 0;
}

int StordVmdk::CloseVmdk(StordConnection* connectp) {
	if (PendingOperations() != 0) {
		return -EBUSY;
	}

	auto rpcp = connectp->GetRpcClient();
	auto rc = rpcp->future_CloseVmdk(vmdk_handle_).get();
	if (rc < 0) {
		return rc;
	}
	vmdk_handle_ = kInvalidVmdkHandle;
	return 0;
}

uint64_t StordVmdk::PendingOperations() const {
	return stats_.pending_;
}

Request* StordVmdk::NewRequest(Request::Type type, const void* privatep,
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
	std::lock_guard<SpinLock> lock(requests_.mutex_);
	requests_.scheduled_.emplace(request->id, std::move(request));
	return reqp;
}

void StordVmdk::UpdateStats(Request* reqp) {
	--stats_.pending_;
	auto latency = reqp->timer.GetMicroSec();
	switch (reqp->type) {
	case Request::Type::kRead:
		++stats_.read_requests_;
		if (hyc_unlikely(reqp->result)) {
			++stats_.read_failed_;
		} else {
			stats_.read_latency_ += latency;
			stats_.read_bytes_ += reqp->xfer_sz;
		}
		break;
	case Request::Type::kWrite:
		++stats_.write_requests_;
		if (hyc_unlikely(reqp->result)) {
			++stats_.write_failed_;
		} else {
			stats_.write_latency_ += latency;
			stats_.write_bytes_ += reqp->xfer_sz;
		}
		break;
	case Request::Type::kWriteSame:
		++stats_.write_same_requests_;
		if (hyc_unlikely(reqp->result)) {
			++stats_.write_same_failed_;
		} else {
			stats_.write_latency_ += latency;
			stats_.write_bytes_ += reqp->xfer_sz;
		}
		break;
	}
}

void StordVmdk::RequestComplete(Request* reqp) {
	UpdateStats(reqp);
	std::lock_guard<SpinLock> lock(requests_.mutex_);
	auto it = requests_.scheduled_.find(reqp->id);
	assert(hyc_unlikely(it != requests_.scheduled_.end()));

	auto request = std::move(it->second);
	requests_.scheduled_.erase(it);
	requests_.complete_.emplace_back(std::move(request));
	if (requests_.scheduled_.empty()) {
		auto rc = PostRequestCompletion();
		(void) rc;
	}
}

int StordVmdk::PostRequestCompletion() const {
	if (hyc_likely(not requests_.complete_.empty() and eventfd_ >= 0)) {
		auto rc = ::eventfd_write(eventfd_, requests_.complete_.size());
		if (hyc_unlikely(rc < 0)) {
			LOG(ERROR) << "eventfd_write write failed RPC " << *this;
			return rc;
		}
	}
	return 0;
}

uint32_t StordVmdk::GetCompleteRequests(RequestResult* resultsp,
		uint32_t nresults, bool *has_morep) {
	*has_morep = false;
	std::lock_guard<SpinLock> lock(requests_.mutex_);
	auto tocopy = std::min(requests_.complete_.size(), static_cast<size_t>(nresults));
	if (not tocopy) {
		return 0;
	}

	auto eit = requests_.complete_.end();
	auto sit = std::prev(eit, tocopy);
	for (auto resultp = resultsp; sit != eit; ++sit, ++resultp) {
		resultp->privatep   = sit->get()->privatep;
		resultp->request_id = sit->get()->id;
		resultp->result     = sit->get()->result;
	}
	requests_.complete_.erase(std::prev(eit, tocopy), eit);
	*has_morep = not requests_.complete_.empty();
	return tocopy;
}

void StordVmdk::ReadDataCopy(Request* reqp, const ReadResult& result) {
	assert(hyc_likely(result.data->computeChainDataLength() ==
		static_cast<size_t>(reqp->buf_sz)));

	auto bufp = reqp->bufferp;
	auto const* p = result.data.get();
	auto e = result.data->countChainElements();
	for (auto c = 0u; c < e; ++c, p = p->next()) {
		auto l = p->length();
		if (not l) {
			continue;
		}
		::memcpy(bufp, p->data(), l);
		bufp += l;
	}
}

RequestID StordVmdk::ScheduleRead(StordConnection* connectp,
		const void* privatep, char* bufferp, int32_t buf_sz, int64_t offset) {
	assert(hyc_likely(vmdk_handle_ != kInvalidVmdkHandle));
	auto reqp = NewRequest(Request::Type::kRead, privatep, bufferp, buf_sz,
		buf_sz, offset);
	if (hyc_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	++stats_.pending_;

	auto basep = connectp->GetEventBase();
	auto clientp = connectp->GetRpcClient();
	basep->runInEventBaseThread([this, clientp, reqp, buf_sz, offset] () mutable {
		clientp->future_Read(vmdk_handle_, reqp->id, buf_sz, offset)
		.then([this, reqp] (const ReadResult& result) mutable {
			reqp->result = result.get_result();
			if (hyc_likely(reqp->result == 0)) {
				ReadDataCopy(reqp, result);
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

RequestID RpcConnection::ScheduleWrite(StordConnection* connectp,
		const void* privatep, char* bufferp, int32_t buf_sz, int64_t offset) {
	assert(hyc_likely(vmdk_handle_ != kInvalidVmdkHandle));
	auto reqp = NewRequest(Request::Type::kWrite, privatep, bufferp, buf_sz,
		buf_sz, offset);
	if (hyc_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	++stats_.pending_;

	auto basep = connectp->GetEventBase();
	auto clientp = connectp->GetRpcClient();
	basep->runInEventBaseThread(
			[this, clientp, reqp, bufferp, buf_sz, offset] () mutable {
		auto data = std::make_unique<folly::IOBuf>(folly::IOBuf::WRAP_BUFFER,
			bufferp, buf_sz);
		clientp->future_Write(vmdk_handle_, reqp->id, data, buf_sz, offset)
		.then([this, reqp, data = std::move(data)]
				(const WriteResult& result) mutable {
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

RequestID RpcConnection::ScheduleWriteSame(StordConnection* connectp,
		const void* privatep, char* bufferp, int32_t buf_sz, int32_t write_sz,
		int64_t offset) {
	assert(hyc_likely(vmdk_handle_ != kInvalidVmdkHandle));
	auto reqp = NewRequest(Request::Type::kWrite, privatep, bufferp, buf_sz,
		write_sz, offset);
	if (hyc_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	++stats_.pending_;

	auto basep = connectp->GetEventBase();
	auto clientp = connectp->GetRpcClient();
	basep->runInEventBaseThread([this, clientp, reqp, bufferp, buf_sz, write_sz,
			offset] () mutable {
		auto data = std::make_unique<folly::IOBuf>(folly::IOBuf::WRAP_BUFFER,
			bufferp, buf_sz);
		clientp->future_WriteSame(vmdk_handle_, reqp->id, data, buf_sz,
			write_sz, offset)
		.then([this, reqp, data = std::move(data)]
				(const WriteResult& result) mutable {
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
	SpinLock mutex_;
	std::unordered_map<RpcConnectHandle, std::unique_ptr<RpcConnection>> map_;
	std::atomic<RpcConnectHandle> handle_{0};
} g_connections_;

class TgtData {
	std::unique_ptr<StordRpc> stord_rpc_;
	struct {
		std::mutex mutex_
		std::unordered_map<VmdkHandle, std::unique_ptr<StordVmdk>> map_;
	} vmdk_;
} g_tgt_data;


RpcConnectHandle RpcServerConnect(uint32_t ping_secs = 30) {

	auto handle = ++g_connections_.handle_;
	auto rpc = std::make_unique<RpcConnection>(handle, ping_secs);
	auto rc = rpc->Connect();
	if (rc < 0) {
		return kInvalidRpcHandle;
	}

	std::lock_guard<SpinLock> lock(g_connections_.mutex_);
	g_connections_.map_.emplace(handle, std::move(rpc));
	return handle;
}

RpcConnection* FindRpcConnection(RpcConnectHandle handle) {
	std::lock_guard<SpinLock> lock(g_connections_.mutex_);
	auto it = g_connections_.map_.find(handle);
	if (hyc_unlikely(it == g_connections_.map_.end())) {
		return nullptr;
	}

	return it->second.get();
}

int32_t RpcServerDisconnect(RpcConnectHandle handle) {
	auto rpc = FindRpcConnection(handle);
	if (hyc_unlikely(rpc == nullptr)) {
		return -ENODEV;
	}

	std::lock_guard<SpinLock> lock(g_connections_.mutex_);
	auto it = g_connections_.map_.find(handle);
	assert(hyc_unlikely(it != g_connections_.map_.end()));
	g_connections_.map_.erase(it);
	return 0;
}

int32_t RpcOpenVmdk(RpcConnectHandle handle, const char* vmid, const char* vmdkid,
		int eventfd) {
	auto rpc = FindRpcConnection(handle);
	if (hyc_unlikely(rpc == nullptr)) {
		return -ENODEV;
	}

	return rpc->OpenVmdk(vmid, vmdkid, eventfd);
}

int32_t RpcCloseVmdk(RpcConnectHandle handle) {
	auto rpc = FindRpcConnection(handle);
	if (hyc_unlikely(rpc == nullptr)) {
		return -ENODEV;
	}

	auto rc = rpc->CloseVmdk();
	if (rc < 0) {
		return rc;
	}

	return RpcServerDisconnect(handle);
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

void HycStorInitialize(int argc, char *argv[], char *stord_ip,
		uint16_t stord_port) {
	FLAGS_v = 2;
	folly::init(&argc, &argv);

	StordIp.assign(stord_ip);
	StordPort = stord_port;
}

RpcConnectHandle HycStorRpcServerConnect() {
	try  {
		return hyc::RpcServerConnect();
	} catch (std::exception& e) {
		return kInvalidRpcHandle;
	}
}

RpcConnectHandle HycStorRpcServerConnectTest(uint32_t ping_secs) {
	try {
		return hyc::RpcServerConnect(ping_secs);
	} catch (std::exception& e) {
		return kInvalidRpcHandle;
	}
}

int32_t HycStorRpcServerDisconnect(RpcConnectHandle handle) {
	try {
		return hyc::RpcServerDisconnect(handle);
	} catch (std::exception& e) {
		return -1;
	}
}

int32_t HycOpenVmdk(RpcConnectHandle handle, const char* vmid, const char* vmdkid,
		int eventfd) {
	assert(vmid != nullptr and vmdkid != nullptr);
	try {
		return hyc::RpcOpenVmdk(handle, vmid, vmdkid, eventfd);
	} catch (std::exception& e) {
		return -ENODEV;
	}
}

int32_t HycCloseVmdk(RpcConnectHandle handle) {
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
