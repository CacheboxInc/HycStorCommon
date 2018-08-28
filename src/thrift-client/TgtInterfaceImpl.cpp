#include <memory>
#include <iostream>
#include <thread>
#include <mutex>
#include <string>
#include <algorithm>
#include <atomic>

#include <cstdint>
#include <cassert>

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
#include "Common.h"

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
	StordConnection(std::string ip, uint16_t port, uint16_t cpu, uint32_t ping);
	~StordConnection();
	int32_t Connect();
	inline folly::EventBase* GetEventBase() const noexcept;
	inline StorRpcAsyncClient* GetRpcClient() const noexcept;

private:
	int32_t Disconnect();
	void SetPingTimeout();
	static void SetThreadAffinity(uint16_t cpu, std::thread* threadp);
	uint64_t PendingOperations() const noexcept;
private:
	const std::string ip_;
	const uint16_t port_;
	const uint16_t cpu_;

	struct {
		uint32_t timeout_secs_{30};
		std::unique_ptr<ReschedulingTimeout> timeout_;
	} ping_;

	struct {
		std::atomic<uint64_t> pending_{0};
	} requests_;

	std::unique_ptr<StorRpcAsyncClient> client_;
	StorRpcAsyncClient* clientp_;
	std::unique_ptr<folly::EventBase> base_;
	folly::EventBase* basep_;
	std::unique_ptr<std::thread> runner_;
};

StordConnection::StordConnection(std::string ip, uint16_t port, uint16_t cpu,
		uint32_t ping) : ip_(std::move(ip)), port_(port), cpu_(cpu),
		ping_{ping, nullptr} {
}

folly::EventBase* StordConnection::GetEventBase() const noexcept {
	return basep_;
}

StorRpcAsyncClient* StordConnection::GetRpcClient() const noexcept {
	return clientp_;
}

StordConnection::~StordConnection() {
	log_assert(not PendingOperations());
	Disconnect();
}

uint64_t StordConnection::PendingOperations() const noexcept {
	return requests_.pending_.load();
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

int StordConnection::Connect() {
	std::mutex m;
	std::condition_variable cv;
	bool started = false;
	int32_t result = 0;

	runner_ = std::make_unique<std::thread>([this, &m, &cv, &started, &result]
			() mutable {
		try {
			::sleep(1);
			std::this_thread::yield();

			auto base = std::make_unique<folly::EventBase>();
			auto client = std::make_unique<StorRpcAsyncClient>(
				HeaderClientChannel::newChannel(
					async::TAsyncSocket::newSocket(base.get(),
						{ip_, port_})));

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

			this->basep_ = base.get();
			this->base_ = std::move(base);
			this->clientp_ = client.get();
			this->client_ = std::move(client);
			SetPingTimeout();

			{
				/* notify main thread of success */
				result = 0;
				started = true;
				std::unique_lock<std::mutex> lk(m);
				cv.notify_all();
			}

			VLOG(1) << " EventBase looping forever";
			this->base_->loopForever();
			VLOG(1) << " EventBase loop stopped";

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


	SetThreadAffinity(cpu_, runner_.get());

	std::unique_lock<std::mutex> lk(m);
	cv.wait(lk, [&started] { return started; });

	if (result < 0) {
		return result;
	}

	/* ensure that EventBase loop is started */
	this->base_->waitUntilRunning();
	VLOG(1) << " Connection Result " << result;
	return 0;
}

void StordConnection::SetThreadAffinity(uint16_t cpu, std::thread* threadp) {
	auto handle = threadp->native_handle();
	cpu_set_t set;
	CPU_ZERO(&set);
	CPU_SET(cpu, &set);
	auto rc = pthread_setaffinity_np(handle, sizeof(set), &set);
	if (hyc_unlikely(rc < 0)) {
		LOG(ERROR) << "Failed to bind thread " << handle << " to CPU " << cpu;
	} else {
		LOG(ERROR) << "Thread " << handle << " bound to CPU " << cpu;
	}
}

void StordConnection::SetPingTimeout() {
	log_assert(base_ != nullptr);
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
		kStatic,
	};

	StordRpc(std::string ip, uint16_t port, uint16_t nthreads,
		SchedulePolicy policy);
	~StordRpc();
	int32_t Connect(uint32_t ping_secs = 30);
	StordConnection* GetStordConnection() const noexcept;
	void SetPolicy(SchedulePolicy policy);
private:
	inline StordConnection* GetStordConnectionRR() const noexcept;
	inline StordConnection* GetStordConnectionCpu() const noexcept;

private:
	const std::string ip_;
	const uint16_t port_{0};
	const uint16_t nthreads_{0};
	struct {
		std::vector<std::unique_ptr<StordConnection>> uptrs_;
		std::vector<StordConnection* > ptrs_;
	} connections_;

	struct {
		SchedulePolicy policy_{SchedulePolicy::kRoundRobin};
		mutable std::atomic<uint64_t> last_cpu_{0};
	} policy_;
};

StordRpc::StordRpc(std::string ip, uint16_t port, uint16_t nthreads,
		StordRpc::SchedulePolicy policy) : ip_(std::move(ip)),
		port_(port), nthreads_(nthreads), policy_{policy} {
}

StordRpc::~StordRpc() {
	connections_.ptrs_.clear();
	connections_.uptrs_.clear();
}

int32_t StordRpc::Connect(uint32_t ping_secs) {
	const auto cores = os::NumberOfCpus();
	connections_.uptrs_.reserve(nthreads_);
	connections_.ptrs_.reserve(nthreads_);
	for (auto i = 0u; i < nthreads_; ++i) {
		auto cpu = (cores-1) - (i % cores);
		auto c = std::make_unique<StordConnection>(ip_, port_, cpu, ping_secs);
		auto rc = c->Connect();
		if (hyc_unlikely(rc < 0)) {
			return rc;
		}
		connections_.ptrs_.emplace_back(c.get());
		connections_.uptrs_.emplace_back(std::move(c));
	}
	return 0;
}

void StordRpc::SetPolicy(SchedulePolicy policy) {
	policy_.policy_ = policy;
}

StordConnection* StordRpc::GetStordConnectionCpu() const noexcept {
	auto cpu = os::GetCurCpuCore() % this->nthreads_;
	return connections_.ptrs_[cpu];
}

StordConnection* StordRpc::GetStordConnectionRR() const noexcept {
	const auto cpu = policy_.last_cpu_.fetch_add(1u, std::memory_order_relaxed);
	return connections_.ptrs_[cpu % nthreads_];
}

StordConnection* StordRpc::GetStordConnection() const noexcept {
	switch (policy_.policy_) {
	case StordRpc::SchedulePolicy::kRoundRobin:
		return GetStordConnectionRR();;
	case StordRpc::SchedulePolicy::kLeastUsed:
		return GetStordConnectionRR();
	case StordRpc::SchedulePolicy::kCurrentCpu:
		return GetStordConnectionCpu();
	case StordRpc::SchedulePolicy::kStatic:
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
	StordVmdk(const StordVmdk& rhs) = delete;
	StordVmdk(const StordVmdk&& rhs) = delete;
	StordVmdk& operator = (const StordVmdk& rhs) = delete;
	StordVmdk& operator = (const StordVmdk&& rhs) = delete;
public:
	StordVmdk(std::string vmid, std::string vmdkid, int eventfd);
	~StordVmdk();
	void SetStordConnection(StordConnection* connectp) noexcept;
	int32_t OpenVmdk();
	int32_t CloseVmdk();
	RequestID ScheduleRead(const void* privatep, char* bufferp, int32_t buf_sz,
		int64_t offset);
	RequestID ScheduleWrite(const void* privatep, char* bufferp, int32_t buf_sz,
		int64_t offset);
	RequestID ScheduleWriteSame(const void* privatep, char* bufferp,
		int32_t buf_sz, int32_t write_sz, int64_t offset);

	uint32_t GetCompleteRequests(RequestResult* resultsp, uint32_t nresults,
		bool *has_morep);

	VmdkHandle GetHandle() const noexcept;
	const std::string& GetVmdkId() const noexcept;

	friend std::ostream& operator << (std::ostream& os, const StordVmdk& vmdk);
private:
	uint64_t PendingOperations() const noexcept;
	std::pair<Request*, bool> NewRequest(Request::Type type,
		const void* privatep, char* bufferp, size_t buf_sz, size_t xfer_sz,
		int64_t offset);
	void RequestComplete(Request* reqp);
	void UpdateStats(Request* reqp);
	int PostRequestCompletion() const;
	void ReadDataCopy(Request* reqp, const ReadResult& result);

	void ScheduleMore(folly::EventBase* basep, StorRpcAsyncClient* clientp);
	void ScheduleNow(folly::EventBase* basep, StorRpcAsyncClient* clientp);
	void ScheduleRead(folly::EventBase* basep, StorRpcAsyncClient* clientp,
		Request* reqp);
	void ScheduleWrite(folly::EventBase* basep, StorRpcAsyncClient* clientp,
		Request* reqp);
	void ScheduleWriteSame(folly::EventBase* basep, StorRpcAsyncClient* clientp,
		Request* reqp);
private:
	std::string vmid_;
	std::string vmdkid_;
	VmdkHandle vmdk_handle_{kInvalidVmdkHandle};
	int eventfd_{-1};
	StordConnection* connectp_{nullptr};

	struct {
		mutable std::mutex mutex_;
		std::unordered_map<RequestID, std::unique_ptr<Request>> scheduled_;
		std::vector<Request*> pending_;
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
		<< " pending " << vmdk.stats_.pending_
		<< " requestid " << vmdk.requestid_;
	return os;
}

StordVmdk::StordVmdk(std::string vmid, std::string vmdkid, int eventfd) :
		vmid_(std::move(vmid)), vmdkid_(std::move(vmdkid)),
		eventfd_(eventfd) {
}

StordVmdk::~StordVmdk() {
	CloseVmdk();
}

VmdkHandle StordVmdk::GetHandle() const noexcept {
	return vmdk_handle_;
}

const std::string& StordVmdk::GetVmdkId() const noexcept {
	return vmdkid_;
}

void StordVmdk::SetStordConnection(StordConnection* connectp) noexcept {
	connectp_ = connectp;
}

int32_t StordVmdk::OpenVmdk() {
	if (hyc_unlikely(vmdk_handle_ != kInvalidVmdkHandle)) {
		/* already open */
		return -EBUSY;
	}

	auto vmdk_handle = connectp_->GetRpcClient()
		->future_OpenVmdk(vmid_, vmdkid_)
		.get();
	if (hyc_unlikely(vmdk_handle == kInvalidVmHandle)) {
		return -ENODEV;
	}
	vmdk_handle_ = vmdk_handle;
	return 0;
}

int StordVmdk::CloseVmdk() {
	if (vmdk_handle_ == kInvalidVmdkHandle) {
		return 0;
	}

	if (PendingOperations() != 0) {
		return -EBUSY;
	}

	auto rc = connectp_->GetRpcClient()
		->future_CloseVmdk(vmdk_handle_)
		.get();
	if (hyc_unlikely(rc < 0)) {
		return rc;
	}
	vmdk_handle_ = kInvalidVmdkHandle;
	return 0;
}

uint64_t StordVmdk::PendingOperations() const noexcept {
	return stats_.pending_;
}

std::pair<Request*, bool> StordVmdk::NewRequest(Request::Type type,
		const void* privatep, char* bufferp, size_t buf_sz, size_t xfer_sz,
		int64_t offset) {
	auto request = std::make_unique<Request>();
	if (hyc_unlikely(not request)) {
		return std::make_pair(nullptr, false);
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
	auto empty = requests_.scheduled_.empty();
	requests_.scheduled_.emplace(request->id, std::move(request));
	requests_.pending_.emplace_back(reqp);
	++stats_.pending_;
	return std::make_pair(reqp, empty);
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
	bool post = false;
	{
		std::lock_guard<std::mutex> lock(requests_.mutex_);
		auto it = requests_.scheduled_.find(reqp->id);
		log_assert(it != requests_.scheduled_.end());
		requests_.complete_.emplace_back(std::move(it->second));
		requests_.scheduled_.erase(it);
		post = requests_.scheduled_.empty();
	}

	if (post) {
		auto rc = PostRequestCompletion();
		(void) rc;
	}
}

int StordVmdk::PostRequestCompletion() const {
	if (hyc_likely(eventfd_ >= 0)) {
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
	std::lock_guard<std::mutex> lock(requests_.mutex_);
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
	log_assert(result.data->computeChainDataLength() ==
		static_cast<size_t>(reqp->buf_sz));

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

void StordVmdk::ScheduleWriteSame(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, Request* reqp) {
	auto data = std::make_unique<folly::IOBuf>(folly::IOBuf::WRAP_BUFFER,
		reqp->bufferp, reqp->buf_sz);
	clientp->future_WriteSame(vmdk_handle_, reqp->id, data, reqp->buf_sz,
		reqp->xfer_sz, reqp->offset)
	.then([this, reqp, data = std::move(data), basep, clientp]
			(const WriteResult& result) mutable {
		reqp->result = result.get_result();
		RequestComplete(reqp);
		ScheduleMore(basep, clientp);
	})
	.onError([this, reqp, basep, clientp] (const std::exception& e) mutable {
		reqp->result = -EIO;
		RequestComplete(reqp);
		ScheduleMore(basep, clientp);
	});
}

void StordVmdk::ScheduleWrite(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, Request* reqp) {
	log_assert(reqp && basep->isInEventBaseThread());

	auto data = std::make_unique<folly::IOBuf>(folly::IOBuf::WRAP_BUFFER,
		reqp->bufferp, reqp->buf_sz);
	clientp->future_Write(vmdk_handle_, reqp->id, data, reqp->buf_sz, reqp->offset)
	.then([this, reqp, data = std::move(data), basep, clientp]
			(const WriteResult& result) mutable {
		reqp->result = result.get_result();
		RequestComplete(reqp);
		ScheduleMore(basep, clientp);
	})
	.onError([this, reqp, basep, clientp] (const std::exception& e) mutable {
		reqp->result = -EIO;
		RequestComplete(reqp);
		ScheduleMore(basep, clientp);
	});
}

void StordVmdk::ScheduleRead(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, Request* reqp) {
	log_assert(reqp && basep->isInEventBaseThread());

	clientp->future_Read(vmdk_handle_, reqp->id, reqp->buf_sz, reqp->offset)
	.then([this, reqp, basep, clientp] (const ReadResult& result) mutable {
		reqp->result = result.get_result();
		if (hyc_likely(reqp->result == 0)) {
			ReadDataCopy(reqp, result);
		}
		RequestComplete(reqp);
		ScheduleMore(basep, clientp);
	})
	.onError([this, reqp, basep, clientp] (const std::exception& e) mutable {
		reqp->result = -EIO;
		RequestComplete(reqp);
		ScheduleMore(basep, clientp);
	});
}

void StordVmdk::ScheduleNow(folly::EventBase* basep, StorRpcAsyncClient* clientp) {
	auto GetPending = [this] (std::vector<Request*>& pending) mutable {
		pending.clear();
		std::lock_guard<std::mutex> lock(requests_.mutex_);
		requests_.pending_.swap(pending);
		requests_.pending_.reserve(32);
	};

	std::vector<Request*> pending;
	GetPending(pending);

	if (pending.empty()) {
		return;
	}

	basep->runInEventBaseThread([this, clientp, basep,
			pending = std::move(pending)] () {
		for (auto reqp : pending) {
			switch (reqp->type) {
			case Request::Type::kRead:
				ScheduleRead(basep, clientp, reqp);
				break;
			case Request::Type::kWrite:
				ScheduleWrite(basep, clientp, reqp);
				break;
			case Request::Type::kWriteSame:
				ScheduleWriteSame(basep, clientp, reqp);
				break;
			}
		}
	});
}

void StordVmdk::ScheduleMore(folly::EventBase* basep,
		StorRpcAsyncClient* clientp) {
	ScheduleNow(basep, clientp);
}

RequestID StordVmdk::ScheduleRead(const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset) {
	log_assert(vmdk_handle_ != kInvalidVmdkHandle);
	auto [reqp, now] = NewRequest(Request::Type::kRead, privatep, bufferp, buf_sz,
		buf_sz, offset);
	if (hyc_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	if (now) {
		auto basep = connectp_->GetEventBase();
		auto clientp = connectp_->GetRpcClient();
		ScheduleNow(basep, clientp);
	}
	return reqp->id;
}

RequestID StordVmdk::ScheduleWrite(const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset) {
	log_assert(vmdk_handle_ != kInvalidVmdkHandle);
	auto [reqp, now] = NewRequest(Request::Type::kWrite, privatep, bufferp, buf_sz,
		buf_sz, offset);
	if (hyc_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	if (now) {
		auto basep = connectp_->GetEventBase();
		auto clientp = connectp_->GetRpcClient();
		ScheduleNow(basep, clientp);
	}

	return reqp->id;
}

RequestID StordVmdk::ScheduleWriteSame(const void* privatep, char* bufferp,
		int32_t buf_sz, int32_t write_sz, int64_t offset) {
	log_assert(vmdk_handle_ != kInvalidVmdkHandle);
	auto [reqp, now] = NewRequest(Request::Type::kWrite, privatep, bufferp, buf_sz,
		write_sz, offset);
	if (hyc_unlikely(not reqp)) {
		return kInvalidRequestID;
	}

	if (now) {
		auto basep = connectp_->GetEventBase();
		auto clientp = connectp_->GetRpcClient();
		ScheduleNow(basep, clientp);
	}

	return reqp->id;
}

class Stord {
public:
	~Stord();
	int32_t Connect(uint32_t ping_secs = 30);
	int32_t Disconnect(bool force = false);
	int32_t OpenVmdk(const char* vmid, const char* vmdkid, int eventfd,
		VmdkHandle* handlep);
	int32_t CloseVmdk(VmdkHandle handle);
	RequestID VmdkRead(VmdkHandle handle, const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset);
	uint32_t VmdkGetCompleteRequest(VmdkHandle handle, RequestResult* resultsp,
		uint32_t nresults, bool *has_morep);
	RequestID VmdkWrite(VmdkHandle handle, const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset);
	RequestID VmdkWriteSame(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset);
private:
	StordVmdk* FindVmdk(VmdkHandle handle);
	StordVmdk* FindVmdk(const std::string& vmdkid);
private:
	struct {
		std::unique_ptr<StordRpc> rpc_;
		StordRpc* rpcp_{nullptr};
	} stord_;
	struct {
		mutable std::mutex mutex_;
		std::unordered_map<std::string, std::unique_ptr<StordVmdk>> ids_;
		std::unordered_map<VmdkHandle, StordVmdk*> handles_;
	} vmdk_;
};

Stord::~Stord() {
	auto rc = Disconnect();
	if (rc < 0) {
		rc = Disconnect(true);
		log_assert(rc == 0);
	}
}

int32_t Stord::Connect(uint32_t ping_secs) {
	auto cores = std::max(os::NumberOfCpus()/2, 1u);
	auto rpc = std::make_unique<StordRpc>(StordIp, StordPort, cores,
		StordRpc::SchedulePolicy::kRoundRobin);
	if (hyc_unlikely(not rpc)) {
		return -ENOMEM;
	}

	auto rc = rpc->Connect(ping_secs);
	if (hyc_unlikely(rc < 0)) {
		return rc;
	}

	/* TODO: use pthread_once */
	rpc->SetPolicy(StordRpc::SchedulePolicy::kRoundRobin);
	stord_.rpcp_ = rpc.get();
	stord_.rpc_ = std::move(rpc);
	return 0;
}

int32_t Stord::Disconnect(bool force) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	if (not vmdk_.ids_.empty()) {
		LOG(ERROR) << "StordDisconnect called with active connections";
		if (not force) {
			return -EBUSY;
		}
	}
	vmdk_.handles_.clear();
	stord_.rpc_ = nullptr;
	return 0;
}

StordVmdk* Stord::FindVmdk(VmdkHandle handle) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	auto it = vmdk_.handles_.find(handle);
	if (hyc_unlikely(it == vmdk_.handles_.end())) {
		return nullptr;
	}

	return it->second;
}

StordVmdk* Stord::FindVmdk(const std::string& vmdkid) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	auto it = vmdk_.ids_.find(vmdkid);
	if (hyc_unlikely(it == vmdk_.ids_.end())) {
		return nullptr;
	}

	return it->second.get();
}

int32_t Stord::OpenVmdk(const char* vmid, const char* vmdkid, int eventfd,
		VmdkHandle* handlep) {
	*handlep = kInvalidVmdkHandle;
	auto vmdkp = FindVmdk(vmdkid);
	if (hyc_unlikely(vmdkp)) {
		return -EEXIST;
	}

	auto vmdk = std::make_unique<StordVmdk>(vmid, vmdkid, eventfd);
	if (hyc_unlikely(not vmdk)) {
		return -ENOMEM;
	}
	vmdkp = vmdk.get();

	auto rpcp = stord_.rpcp_;
	log_assert(rpcp);
	vmdkp->SetStordConnection(rpcp->GetStordConnection());

	auto rc = vmdk->OpenVmdk();
	if (hyc_unlikely(rc < 0)) {
		return rc;
	}

	*handlep = vmdkp->GetHandle();
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	vmdk_.ids_.emplace(vmdkid, std::move(vmdk));
	try {
		vmdk_.handles_.emplace(*handlep, vmdkp);
	} catch (...) {
		auto it = vmdk_.ids_.find(vmdkp->GetVmdkId());
		vmdk_.ids_.erase(it);
		throw;
	}
	return 0;
}

int32_t Stord::CloseVmdk(VmdkHandle handle) {
	std::unique_lock<std::mutex> lock(vmdk_.mutex_);
	auto hit = vmdk_.handles_.find(handle);
	if (hyc_unlikely(hit == vmdk_.handles_.end())) {
		return -ENODEV;
	}

	auto vmdkp = hit->second;
	const auto& id = vmdkp->GetVmdkId();

	auto iit = vmdk_.ids_.find(id);
	log_assert(iit != vmdk_.ids_.end());

	auto vmdk = std::move(iit->second);
	try {
		vmdk_.ids_.erase(iit);
	} catch (...) {
		iit->second = std::move(vmdk);
		throw;
	}
	vmdk_.handles_.erase(hit);
	lock.unlock();

	vmdk->CloseVmdk();
	return 0;
}

RequestID Stord::VmdkRead(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	auto vmdkp = FindVmdk(handle);
	if (hyc_unlikely(not vmdkp)) {
		return kInvalidRequestID;
	}

	return vmdkp->ScheduleRead(privatep, bufferp, buf_sz, offset);
}

uint32_t Stord::VmdkGetCompleteRequest(VmdkHandle handle,
		RequestResult* resultsp, uint32_t nresults, bool *has_morep) {
	auto vmdkp = FindVmdk(handle);
	if (hyc_unlikely(not vmdkp)) {
		*has_morep = false;
		return 0;
	}

	return vmdkp->GetCompleteRequests(resultsp, nresults, has_morep);
}

RequestID Stord::VmdkWrite(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	auto vmdkp = FindVmdk(handle);
	if (hyc_unlikely(not vmdkp)) {
		return kInvalidRequestID;
	}

	return vmdkp->ScheduleWrite(privatep, bufferp, buf_sz, offset);
}

RequestID Stord::VmdkWriteSame(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset) {
	auto vmdkp = FindVmdk(handle);
	if (hyc_unlikely(not vmdkp)) {
		return kInvalidRequestID;
	}

	return vmdkp->ScheduleWriteSame(privatep, bufferp, buf_sz, write_sz, offset);
}

} // namespace hyc

/*
 * GLOBAL DATA STRUCTURES
 * ======================
 */
static hyc::Stord g_stord;

void HycStorInitialize(int argc, char *argv[], char *stord_ip,
		uint16_t stord_port) {
	FLAGS_v = 2;
	folly::init(&argc, &argv);

	StordIp.assign(stord_ip);
	StordPort = stord_port;
}

int32_t HycStorRpcServerConnect(void) {
	try  {
		return g_stord.Connect();
	} catch (std::exception& e) {
		return -ENODEV;
	}
}

int32_t HycStorRpcServerConnectTest(uint32_t ping_secs) {
	try {
		return g_stord.Connect(ping_secs);
	} catch (std::exception& e) {
		return -ENOMEM;
	}
}

int32_t HycStorRpcServerDisconnect(void) {
	try {
		return g_stord.Disconnect();
	} catch (std::exception& e) {
		return -1;
	}
}

int32_t HycOpenVmdk(const char* vmid, const char* vmdkid, int eventfd,
		VmdkHandle* handlep) {
	log_assert(vmid != nullptr and vmdkid != nullptr and handlep);
	try {
		return g_stord.OpenVmdk(vmid, vmdkid, eventfd, handlep);
	} catch (std::exception& e) {
		*handlep = kInvalidVmdkHandle;
		return -ENODEV;
	}
}

int32_t HycCloseVmdk(VmdkHandle handle) {
	try {
		return g_stord.CloseVmdk(handle);
	} catch (std::exception& e) {
		return -ENODEV;
	}
}

RequestID HycScheduleRead(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	try {
		return g_stord.VmdkRead(handle, privatep, bufferp, buf_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}

uint32_t HycGetCompleteRequests(VmdkHandle handle, RequestResult *resultsp,
		uint32_t nresults, bool *has_morep) {
	try {
		return g_stord.VmdkGetCompleteRequest(handle, resultsp, nresults,
			has_morep);
	} catch (const std::exception& e) {
		*has_morep = true;
		return 0;
	}
}

RequestID HycScheduleWrite(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	try {
		return g_stord.VmdkWrite(handle, privatep, bufferp, buf_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}

RequestID HycScheduleWriteSame(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset) {
	try {
		return g_stord.VmdkWriteSame(handle, privatep, bufferp, buf_sz,
			write_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}
