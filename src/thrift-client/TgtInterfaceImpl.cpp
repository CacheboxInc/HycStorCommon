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

using namespace std::chrono_literals;
static size_t kExpectedWanLatency = std::chrono::microseconds(20ms).count();
static size_t kMaxBatchSize = 100 * 32; //100 VMDK equivalent
static size_t kMinBatchSize = 1;
static size_t kIdealLatency = (kExpectedWanLatency * 80) / 100; //80% of max
static size_t kBatchIncrValue = 4;
static size_t kBatchDecrPercent = 25;
static bool kAdaptiveBatching = true;
static uint32_t kSystemLoadFactor = 6; //system load influence in batch size determination
static size_t kLogging = 0;

namespace hyc {
using namespace apache::thrift;
using namespace apache::thrift::async;
using namespace hyc_thrift;
using namespace folly;

class StordVmdk;
class StordConnection;

std::ostream& operator << (std::ostream& os, const StordVmdk& vmdk);

template <typename T, uint64_t N>
class MovingAverage {
public:
	MovingAverage() = default;
	~MovingAverage() = default;

	MovingAverage(const MovingAverage& rhs) = delete;
	MovingAverage(MovingAverage&& rhs) = delete;
	MovingAverage& operator ==(const MovingAverage& rhs) = delete;
	MovingAverage& operator ==(MovingAverage&& rhs) = delete;

	T Add(T sample) {
		std::lock_guard<std::mutex> lock(mutex_);
		if (nsamples_ < N) {
			samples_[nsamples_++] = sample;
			total_ += sample;
		} else {
			T& oldest = samples_[nsamples_++ % N];
			total_ -= oldest;
			total_ += sample;
			oldest = sample;
		}
		return Average();
	}

	T Average() const noexcept {
		auto div = std::min(nsamples_, N);
		if (not div) {
			return 0;
		}
		return total_ / div;
	}

	void Reset() {
		std::lock_guard<std::mutex> lock(mutex_);
		total_ = 0;
		nsamples_ = 0;
	}

	uint64_t GetSamples() const { return nsamples_; }
	uint64_t GetMaxSamples() const { return N; }

private:
	std::mutex mutex_;
	T samples_[N];
	T total_{0};
	uint64_t nsamples_{0};
};

class ReschedulingTimeout : public AsyncTimeout {
public:
	ReschedulingTimeout(EventBase* basep, uint32_t milli) :
			AsyncTimeout(basep), milli_(milli) {
	}

	~ReschedulingTimeout() {
		Cancel();
	}

	void timeoutExpired() noexcept override {
		if (not cancel_ && func_()) {
			ScheduleTimeout(func_);
		}
	}

	void ScheduleTimeout(std::function<bool (void)> func) {
		if (cancel_) {
			return;
		}
		func_ = func;
		scheduleTimeout(milli_);
	}

	void Cancel() {
		cancel_ = true;
		if (not isScheduled()) {
			return;
		}
		this->cancelTimeout();
	}

private:
	uint32_t milli_{0};
	bool cancel_{false};
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
	size_t batch_size;

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
		<< " offset " << request.offset
		<< " batch_size " << request.batch_size;
	return os;
}

class SchedulePending : public EventBase::LoopCallback {
public:
	SchedulePending(StordConnection* connectp);
	void runLoopCallback() noexcept override;
private:
	StordConnection* connectp_{nullptr};
};

SchedulePending::SchedulePending(StordConnection* connectp) :
		connectp_(connectp) {
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
	void RegisterVmdk(StordVmdk* vmdkp);
	void UnregisterVmdk(StordVmdk* vmdkp);
	void GetRegisteredVmdks(std::vector<StordVmdk*>& vmdks) const noexcept;

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

	struct {
		mutable std::mutex mutex_;
		std::vector<StordVmdk *> vmdks_;
	} registered_;

	SchedulePending sched_pending_;

	std::unique_ptr<StorRpcAsyncClient> client_;
	StorRpcAsyncClient* clientp_;
	std::unique_ptr<folly::EventBase> base_;
	folly::EventBase* basep_;
	std::unique_ptr<std::thread> runner_;
};

StordConnection::StordConnection(std::string ip, uint16_t port, uint16_t cpu,
		uint32_t ping) : ip_(std::move(ip)), port_(port), cpu_(cpu),
		ping_{ping, nullptr}, sched_pending_(this) {
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
		base_->runInEventBaseThreadAndWait([this] () {
			ping_.timeout_ = nullptr;
			sched_pending_.~SchedulePending();
		});
		base_->terminateLoopSoon();
	}

	if (runner_) {
		runner_->join();
	}
	return 0;
}

void StordConnection::RegisterVmdk(StordVmdk* vmdkp) {
	std::lock_guard<std::mutex> l(registered_.mutex_);
	registered_.vmdks_.emplace_back(vmdkp);
}

void StordConnection::GetRegisteredVmdks(std::vector<StordVmdk*>& vmdks) const noexcept {
	std::lock_guard<std::mutex> l(registered_.mutex_);
	vmdks.reserve(registered_.vmdks_.size());
	std::copy(registered_.vmdks_.begin(), registered_.vmdks_.end(),
		std::back_inserter(vmdks));
}

void StordConnection::UnregisterVmdk(StordVmdk* vmdkp) {
	std::lock_guard<std::mutex> l(registered_.mutex_);
	auto it = std::find(registered_.vmdks_.begin(),
		registered_.vmdks_.end(), vmdkp);
	log_assert(it != registered_.vmdks_.end());
	registered_.vmdks_.erase(it);
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
			this->base_->runBeforeLoop(&this->sched_pending_);
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
#if 0
	cpu_set_t set;
	CPU_ZERO(&set);
	CPU_SET(cpu, &set);
	auto rc = pthread_setaffinity_np(handle, sizeof(set), &set);
	if (hyc_unlikely(rc < 0)) {
		LOG(ERROR) << "Failed to bind thread " << handle << " to CPU " << cpu;
	} else {
		LOG(ERROR) << "Thread " << handle << " bound to CPU " << cpu;
	}
#endif

	std::string name("StordClient");
	name += std::to_string(cpu);
	auto rc = pthread_setname_np(handle, name.c_str());
	if (hyc_unlikely(rc < 0)) {
		LOG(ERROR) << "Setting thread name failed";
	}
}

void StordConnection::SetPingTimeout() {
	log_assert(base_ != nullptr);
	std::chrono::seconds s(ping_.timeout_secs_);
	auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(s).count();
	ping_.timeout_ = std::make_unique<ReschedulingTimeout>(base_.get(), ms);
	ping_.timeout_->ScheduleTimeout([this] () {
		std::vector<StordVmdk*> vmdks;
		GetRegisteredVmdks(vmdks);
		if (not vmdks.empty()) {
			for (StordVmdk* vmdkp : vmdks) {
				LOG(ERROR) << *vmdkp;
			}
		}

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
	default:
		return nullptr;
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
	std::atomic<int64_t> rpc_requests_scheduled_{0};

	std::atomic<int64_t> batchsize_decr_{0};
	std::atomic<int64_t> batchsize_incr_{0};
	std::atomic<int64_t> batchsize_same_{0};
	std::atomic<int64_t> need_schedule_count_{0};
	MovingAverage<uint64_t, 128> avg_batchsize_{};
};

struct StordStats {
	std::atomic<uint64_t> pending_{0};
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

	::hyc_thrift::VmdkHandle GetHandle() const noexcept;
	const std::string& GetVmdkId() const noexcept;
	void ScheduleMore(folly::EventBase* basep, StorRpcAsyncClient* clientp);
	const VmdkStats& GetVmdkStats() const noexcept;
	const StordStats& GetStordStats() const noexcept;

	friend std::ostream& operator << (std::ostream& os, const StordVmdk& vmdk);
private:
	void ScheduleNow(folly::EventBase* basep, StorRpcAsyncClient* clientp);
	int64_t RpcRequestScheduledCount() const noexcept;
	uint64_t PendingOperations() const noexcept;
	std::pair<Request*, bool> NewRequest(Request::Type type,
		const void* privatep, char* bufferp, size_t buf_sz, size_t xfer_sz,
		int64_t offset);
	void UpdateStats(Request* reqp);
	void UpdateBatchSize(Request* reqp);
	int PostRequestCompletion() const;
	void ReadDataCopy(Request* reqp, const ReadResult& result);

	void ScheduleRead(folly::EventBase* basep, StorRpcAsyncClient* clientp,
		Request* reqp);
	void ScheduleWrite(folly::EventBase* basep, StorRpcAsyncClient* clientp,
		Request* reqp);
	void ScheduleWriteSame(folly::EventBase* basep, StorRpcAsyncClient* clientp,
		Request* reqp);
	void ScheduleBulkWrite(folly::EventBase* basep,
		StorRpcAsyncClient* clientp,
		std::unique_ptr<std::vector<::hyc_thrift::WriteRequest>> reqs);
	void ScheduleBulkRead(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, std::vector<Request*> requests,
		std::unique_ptr<std::vector<::hyc_thrift::ReadRequest>> thrift_requests);
private:
	void RequestComplete(RequestID id, int32_t result);
	template <typename T = Request>
	void RequestComplete(T* reqp);
	template <typename T, typename... ErrNo>
	void RequestComplete(const std::vector<T>& requests, ErrNo&&... no);
	void BulkReadComplete(const std::vector<Request*>& requests,
		const std::vector<::hyc_thrift::ReadResult>& results);

private:
	std::string vmid_;
	std::string vmdkid_;
	::hyc_thrift::VmdkHandle vmdk_handle_{kInvalidVmdkHandle};
	int eventfd_{-1};
	StordConnection* connectp_{nullptr};

	struct {
		mutable std::mutex mutex_;
		std::unordered_map<RequestID, std::unique_ptr<Request>> scheduled_;
		std::vector<Request*> pending_;
		std::vector<std::unique_ptr<Request>> complete_;
	} requests_;

	MovingAverage<uint64_t, 128> latency_avg_{};
	MovingAverage<uint64_t, 128> bulk_depth_avg_{};
	size_t batch_size_{1};
	bool scheduled_early_{false};
	bool need_schedule_{false};
	VmdkStats stats_;

	static StordStats stord_stats_;

	std::atomic<RequestID> requestid_{0};
};

StordStats StordVmdk::stord_stats_;

void SchedulePending::runLoopCallback() noexcept {
	auto basep = connectp_->GetEventBase();
	basep->runBeforeLoop(this);

	std::vector<StordVmdk*> vmdks;
	connectp_->GetRegisteredVmdks(vmdks);
	if (vmdks.empty()) {
		return;
	}

	for (auto& vmdkp : vmdks) {
		auto clientp = connectp_->GetRpcClient();
		vmdkp->ScheduleMore(basep, clientp);
	}
}

std::ostream& operator << (std::ostream& os, const StordVmdk& vmdk) {
	os << " VmID " << vmdk.vmid_
		<< " VmdkID " << vmdk.vmdkid_
		<< " VmdkHandle " << vmdk.vmdk_handle_
		<< " eventfd " << vmdk.eventfd_
		<< " pending " << vmdk.stats_.pending_
		<< " batchsize_decr " << vmdk.stats_.batchsize_decr_
		<< " batchsize_incr " << vmdk.stats_.batchsize_incr_
		<< " batchsize_same " << vmdk.stats_.batchsize_same_
		<< " need_schedule_count " << vmdk.stats_.need_schedule_count_
		<< " avg_batchsize " << vmdk.stats_.avg_batchsize_.Average()
		<< " requestid " << vmdk.requestid_
		<< " latency avg " << vmdk.latency_avg_.Average()
		<< " Bulk IODepth avg " << vmdk.bulk_depth_avg_.Average()
		;
	return os;
}

StordVmdk::StordVmdk(std::string vmid, std::string vmdkid, int eventfd) :
		vmid_(std::move(vmid)), vmdkid_(std::move(vmdkid)),
		eventfd_(eventfd) {
}

StordVmdk::~StordVmdk() {
	CloseVmdk();
}

::hyc_thrift::VmdkHandle StordVmdk::GetHandle() const noexcept {
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

	folly::Promise<hyc_thrift::VmdkHandle> promise;
	connectp_->GetEventBase()->runInEventBaseThread([&] () mutable {
		auto clientp = connectp_->GetRpcClient();
		clientp->future_OpenVmdk(vmid_, vmdkid_)
		.then([&] (const folly::Try<::hyc_thrift::VmdkHandle>& tri) mutable {
			if (hyc_unlikely(tri.hasException())) {
				promise.setValue(kInvalidVmdkHandle);
				return;
			}
			auto vmdk_handle = tri.value();
			if (hyc_unlikely(vmdk_handle == kInvalidVmdkHandle)) {
				promise.setValue(kInvalidVmdkHandle);
				return;
			}
			vmdk_handle_ = vmdk_handle;
			connectp_->RegisterVmdk(this);
			promise.setValue(vmdk_handle);
		});
	});

	auto future = promise.getFuture();
	auto vmdk_handle = future.get();
	if (hyc_unlikely(vmdk_handle == kInvalidVmHandle)) {
		LOG(ERROR) << "Open VMDK Failed" << *this;
		return -ENODEV;
	}
	return 0;
}

int StordVmdk::CloseVmdk() {
	if (vmdk_handle_ == kInvalidVmdkHandle) {
		return 0;
	}

	if (PendingOperations() != 0) {
		LOG(ERROR) << "Close VMDK Failed" << *this;
		return -EBUSY;
	}

	folly::Promise<int> promise;
	connectp_->GetEventBase()->runInEventBaseThread([&] () mutable {
		auto clientp = connectp_->GetRpcClient();
		clientp->future_CloseVmdk(this->vmdk_handle_)
		.then([&] (const folly::Try<int>& tri) mutable {
			if (hyc_unlikely(tri.hasException())) {
				promise.setValue(-1);
				return;
			}
			auto rc = tri.value();
			if (hyc_unlikely(rc < 0)) {
				promise.setValue(rc);
				return;
			}
			this->connectp_->UnregisterVmdk(this);
			this->vmdk_handle_ = kInvalidVmdkHandle;
			promise.setValue(rc);
			return;
		});
	});

	auto future = promise.getFuture();
	auto rc = future.get();
	if (hyc_unlikely(rc < 0)) {
		LOG(ERROR) << "Close VMDK Failed" << *this;
		return rc;
	}
	return 0;
}

uint64_t StordVmdk::PendingOperations() const noexcept {
	return stats_.pending_;
}

int64_t StordVmdk::RpcRequestScheduledCount() const noexcept {
	return stats_.rpc_requests_scheduled_;
}

std::pair<Request*, bool> StordVmdk::NewRequest(Request::Type type,
		const void* privatep, char* bufferp, size_t buf_sz, size_t xfer_sz,
		int64_t offset) {
	auto request = std::make_unique<Request>();
	if (hyc_unlikely(not request)) {
		return std::make_pair(nullptr, false);
	}

	++stats_.pending_;
	++stord_stats_.pending_;

	request->id = ++requestid_;
	request->type = type;
	request->privatep = privatep;
	request->bufferp = bufferp;
	request->buf_sz = buf_sz;
	request->xfer_sz = xfer_sz;
	request->offset = offset;
	request->batch_size = batch_size_;

	auto reqp = request.get();
	std::lock_guard<std::mutex> lock(requests_.mutex_);
	bool schedule_now = requests_.scheduled_.empty();
	requests_.scheduled_.emplace(request->id, std::move(request));
	requests_.pending_.emplace_back(reqp);
	if (kAdaptiveBatching) {
		//scheduled early is set if there is atleast one already scheduled IO
		//and we are attempting to send more due to pending IOs size >= batch_size.
		//scheduled early indicates that current batch sie is insufficient to
		//absorb the application parallelism and need a change. IO callback
		//will look at it and if latency permits, will increase the batch size
		if (not schedule_now && requests_.pending_.size() >= batch_size_) {
			scheduled_early_ = true;
			schedule_now = true;
		}
	} else if (not schedule_now) {
		schedule_now = latency_avg_.Average() > kExpectedWanLatency;
	}

	return std::make_pair(reqp, schedule_now);
}

void StordVmdk::UpdateStats(Request* reqp) {
	--stats_.pending_;
	--stord_stats_.pending_;
	log_assert(stats_.rpc_requests_scheduled_ >= 0);
	auto latency = reqp->timer.GetMicroSec();
	if (not kAdaptiveBatching || reqp->batch_size == batch_size_) {
		latency_avg_.Add(latency);
	}
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

template <typename R,
	bool result = std::is_integral_v<decltype(((R*)nullptr)->result)>>
constexpr bool HasResultHelper(int) {
	return result;
}

template <typename R>
constexpr bool HasResultHelper(...) {
	return false;
}

template <typename R>
constexpr bool HasResult() {
	return HasResultHelper<R>(0);
}

template<typename T, typename... Args>
constexpr T GetErrNo(T arg1, Args... args) {
	static_assert(sizeof...(args) == 0);
	return arg1;
}

void StordVmdk::UpdateBatchSize(Request* reqp) {
	//don't update, if the batch size has already been updated
	//or if there are not enough latency samples
	if ((reqp->batch_size != batch_size_) ||
			(latency_avg_.GetSamples() < latency_avg_.GetMaxSamples())) {
		return;
	}

	bool batch_changed = true;
	size_t system_load = stord_stats_.pending_ >> kSystemLoadFactor;
	if (latency_avg_.Average() > (kExpectedWanLatency + system_load)) {

		//reduce the batch size, since we have hit limit for latency
		batch_size_ -= (batch_size_ * kBatchDecrPercent) / 100;
		if (batch_size_ < kMinBatchSize) {
			kLogging && LOG(ERROR) << "Resetting batch size " << batch_size_ <<
				" to minimum " << kMinBatchSize;
			batch_size_ = kMinBatchSize;
		}

		kLogging && LOG(ERROR) << "Reduced batch size to " << batch_size_ <<
			" avg_latency " << latency_avg_.Average();
		//new smaller batch_size might have caused pending ios
		//size to be more than new batch size. Schedule all such IOs
		if (requests_.pending_.size() >= batch_size_) {
			kLogging && LOG(ERROR) << "Setting need_schedule_ due to reduced batch size"
			       << batch_size_;
			need_schedule_ = true;
			++stats_.need_schedule_count_;
		}
		++stats_.batchsize_decr_;
	} else if ((latency_avg_.Average() < kIdealLatency) && scheduled_early_) {
		//application has more parallelism(scheduled_early), increase batch size
		batch_size_ += kBatchIncrValue;
		//don't go above a high threashold.
		//excessive batching can also destabilize the system
		if (batch_size_ > kMaxBatchSize) {
			kLogging && LOG(ERROR) << "Resetting batch size " << batch_size_ <<
				" to maximum " << kMaxBatchSize;
			batch_size_ = kMaxBatchSize;
		}
		kLogging && LOG(ERROR) << "Increased batch size to " << batch_size_ <<
			" avg_latency " << latency_avg_.Average();
		++stats_.batchsize_incr_;
	} else {
		batch_changed = false;
		++stats_.batchsize_same_;
	}
	//Reset scheduled_early, now that we have seen it
	scheduled_early_ = false;

	//next batch change decision should consider only the new IOs
	//using new batch_size
	if (batch_changed) {
		latency_avg_.Reset();
		stats_.avg_batchsize_.Add(batch_size_);
	}
}

void StordVmdk::RequestComplete(RequestID id, int32_t result) {
	auto it = requests_.scheduled_.find(id);
	log_assert(it != requests_.scheduled_.end());
	if (result) {
		VLOG(5) << "reqid " << id << " has nonzero res: " << result;
	}

	auto req = std::move(it->second);
	auto reqp = req.get();
	reqp->result = result;
	UpdateStats(reqp);

	if (kAdaptiveBatching) {
		UpdateBatchSize(reqp);
	}

	requests_.scheduled_.erase(it);
	requests_.complete_.emplace_back(std::move(req));
}

template <>
void StordVmdk::RequestComplete(Request* reqp) {
	bool post = false;
	{
		std::lock_guard<std::mutex> lock(requests_.mutex_);
		RequestComplete(reqp->id, reqp->result);
		post = requests_.scheduled_.empty() or
			requests_.complete_.size() >= bulk_depth_avg_.Average();
	}

	if (post) {
		auto rc = PostRequestCompletion();
		(void) rc;
	}
}

template <typename T, typename... ErrNo>
void StordVmdk::RequestComplete(const std::vector<T>& requests, ErrNo&&... no) {
	std::unique_lock<std::mutex> lock(requests_.mutex_);
	for (const auto& req : requests) {
		if constexpr (HasResult<T>()) {
			RequestComplete(req.reqid, req.result);
		} else if constexpr (sizeof...(no) == 1)  {
			RequestComplete(req.reqid, GetErrNo(std::forward<ErrNo>(no)...));
		} else {
			static_assert(not std::is_same<T, T>::value,
					"Not sure what to set result");
		}
	}
	bool post = requests_.scheduled_.empty();
	lock.unlock();

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
	.then([this, reqp, data = std::move(data)]
			(const WriteResult& result) mutable {
		reqp->result = result.get_result();
		RequestComplete(reqp);
		--stats_.rpc_requests_scheduled_;
	})
	.onError([this, reqp] (const std::exception& e) mutable {
		reqp->result = -EIO;
		RequestComplete(reqp);
		--stats_.rpc_requests_scheduled_;
	});
	++stats_.rpc_requests_scheduled_;
}

void StordVmdk::ScheduleWrite(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, Request* reqp) {
	log_assert(reqp && basep->isInEventBaseThread());

	auto data = std::make_unique<folly::IOBuf>(folly::IOBuf::WRAP_BUFFER,
		reqp->bufferp, reqp->buf_sz);
	clientp->future_Write(vmdk_handle_, reqp->id, data, reqp->buf_sz, reqp->offset)
	.then([this, reqp, data = std::move(data)]
			(const WriteResult& result) mutable {
		reqp->result = result.get_result();
		RequestComplete(reqp);
		--stats_.rpc_requests_scheduled_;
	})
	.onError([this, reqp] (const std::exception& e) mutable {
		reqp->result = -EIO;
		RequestComplete(reqp);
		--stats_.rpc_requests_scheduled_;
	});
	++stats_.rpc_requests_scheduled_;
}

void StordVmdk::ScheduleBulkWrite(folly::EventBase* basep,
		StorRpcAsyncClient* clientp,
		std::unique_ptr<std::vector<::hyc_thrift::WriteRequest>> reqs) {
	log_assert(basep->isInEventBaseThread());
	clientp->future_BulkWrite(vmdk_handle_, *reqs.get())
	.then([this, reqs = std::move(reqs)]
			(const folly::Try<std::vector<::hyc_thrift::WriteResult>>& trie)
			mutable {
		if (hyc_unlikely(trie.hasException())) {
			LOG(ERROR) << __func__ << " STORD sent exception";
			RequestComplete(*reqs, -ENOMEM);
			--stats_.rpc_requests_scheduled_;
			return;
		}

		const auto& results = trie.value();
		RequestComplete(results);
		--stats_.rpc_requests_scheduled_;
	});
	++stats_.rpc_requests_scheduled_;
}

void StordVmdk::ScheduleRead(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, Request* reqp) {
	log_assert(reqp && basep->isInEventBaseThread());

	clientp->future_Read(vmdk_handle_, reqp->id, reqp->buf_sz, reqp->offset)
	.then([this, reqp] (const ReadResult& result) mutable {
		reqp->result = result.get_result();
		if (hyc_likely(reqp->result == 0)) {
			ReadDataCopy(reqp, result);
		}
		RequestComplete(reqp);
		--stats_.rpc_requests_scheduled_;
	})
	.onError([this, reqp] (const std::exception& e) mutable {
		reqp->result = -EIO;
		RequestComplete(reqp);
		--stats_.rpc_requests_scheduled_;
	});
	++stats_.rpc_requests_scheduled_;
}

void StordVmdk::BulkReadComplete(const std::vector<Request*>& requests,
		const std::vector<::hyc_thrift::ReadResult>& results) {
	auto RequestFind = [&requests] (RequestID id) -> Request* {
		for (auto reqp : requests) {
			if (reqp->id == id) {
				return reqp;
			}
		}
		return nullptr;
	};
	for (const auto& result : results) {
		auto reqp = RequestFind(result.reqid);
		reqp->result = result.result;
		if (hyc_likely(result.result == 0)) {
			log_assert(reqp != nullptr);
			ReadDataCopy(reqp, result);
		}
	}

	for (auto reqp : requests) {
		RequestComplete(reqp);
	}
}

void StordVmdk::ScheduleBulkRead(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, std::vector<Request*> requests,
		std::unique_ptr<std::vector<::hyc_thrift::ReadRequest>> thrift_requests) {
	log_assert(basep->isInEventBaseThread());

	clientp->future_BulkRead(vmdk_handle_, *thrift_requests)
	.then([this, thrift_requests = std::move(thrift_requests),
			requests = std::move(requests)]
			(const folly::Try<std::vector<::hyc_thrift::ReadResult>>& trie)
			mutable {
		if (hyc_unlikely(trie.hasException())) {
			LOG(ERROR) << __func__ << " STORD sent exception";
			RequestComplete(*thrift_requests, -ENOMEM);
			--stats_.rpc_requests_scheduled_;
			return;
		}

		const auto& result = trie.value();
		BulkReadComplete(requests, result);
		--stats_.rpc_requests_scheduled_;
	});
	++stats_.rpc_requests_scheduled_;
}

void StordVmdk::ScheduleNow(folly::EventBase* basep, StorRpcAsyncClient* clientp) {
	auto GetPending = [this] (std::vector<Request*>& pending) mutable {
		pending.clear();
		std::lock_guard<std::mutex> lock(requests_.mutex_);
		requests_.pending_.swap(pending);
		requests_.pending_.reserve(32);
		if (not pending.empty()) {
			bulk_depth_avg_.Add(pending.size());
		}
	};

	std::vector<Request*> pending;
	GetPending(pending);

	if (pending.empty()) {
		return;
	}

	basep->runInEventBaseThread([this, clientp, basep,
			pending = std::move(pending)] () {
		std::unique_ptr<std::vector<::hyc_thrift::WriteRequest>> write;
		std::unique_ptr<std::vector<::hyc_thrift::ReadRequest>> read;
		std::vector<Request*> read_requests;
		uint32_t nwrites = 0;
		uint32_t nreads = 0;

		for (auto reqp : pending) {
			reqp->timer.Start();
			switch (reqp->type) {
			case Request::Type::kRead:
				if (nreads++ == 0) {
					read = std::make_unique<std::vector<::hyc_thrift::ReadRequest>>();
				}
				read->emplace_back(apache::thrift::FragileConstructor(),
					reqp->id, reqp->buf_sz, reqp->offset);
				read_requests.emplace_back(reqp);
				break;
			case Request::Type::kWrite: {
				if (nwrites++ == 0) {
					write = std::make_unique<std::vector<::hyc_thrift::WriteRequest>>();
				}
				auto data = std::make_unique<folly::IOBuf>
					(folly::IOBuf::WRAP_BUFFER, reqp->bufferp, reqp->buf_sz);
				write->emplace_back(apache::thrift::FragileConstructor(),
					reqp->id, std::move(data), reqp->buf_sz, reqp->offset);
				break;
			}
			case Request::Type::kWriteSame:
				ScheduleWriteSame(basep, clientp, reqp);
				break;
			}
		}

		if (write and not write->empty()) {
			ScheduleBulkWrite(basep, clientp, std::move(write));
		}
		if (read and not read->empty()) {
			ScheduleBulkRead(basep, clientp, std::move(read_requests), std::move(read));
		}
	});
}

void StordVmdk::ScheduleMore(folly::EventBase* basep,
		StorRpcAsyncClient* clientp) {

	if (not RpcRequestScheduledCount() || need_schedule_) {
		need_schedule_ = false;
		ScheduleNow(basep, clientp);
	}
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

const VmdkStats& StordVmdk::GetVmdkStats() const noexcept {
	return stats_;
}

const StordStats& StordVmdk::GetStordStats() const noexcept {
	return stord_stats_;
}


class Stord {
public:
	~Stord();
	int32_t Connect(uint32_t ping_secs = 30);
	int32_t Disconnect(bool force = false);
	int32_t OpenVmdk(const char* vmid, const char* vmdkid, int eventfd,
		StordVmdk** vmdkpp);
	int32_t CloseVmdk(StordVmdk* vmdkp);
	RequestID VmdkRead(StordVmdk* vmdkp, const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset);
	uint32_t VmdkGetCompleteRequest(StordVmdk* vmdkp, RequestResult* resultsp,
		uint32_t nresults, bool *has_morep);
	RequestID VmdkWrite(StordVmdk* vmdkp, const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset);
	RequestID VmdkWriteSame(StordVmdk* vmdkp, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset);
	StordVmdk* FindVmdk(const std::string& vmdkid);
private:
	StordVmdk* FindVmdk(::hyc_thrift::VmdkHandle handle);
private:
	struct {
		std::unique_ptr<StordRpc> rpc_;
		StordRpc* rpcp_{nullptr};
	} stord_;
	struct {
		mutable std::mutex mutex_;
		std::unordered_map<std::string, std::unique_ptr<StordVmdk>> ids_;
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
	auto cores = std::min(os::NumberOfCpus()/2, 1u);
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
	stord_.rpc_ = nullptr;
	return 0;
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
		StordVmdk** vmdkpp) {
	*vmdkpp = nullptr;
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

	*vmdkpp = vmdkp;
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	vmdk_.ids_.emplace(vmdkid, std::move(vmdk));
	return 0;
}

int32_t Stord::CloseVmdk(StordVmdk* vmdkp) {
	const auto& id = vmdkp->GetVmdkId();

	std::unique_lock<std::mutex> lock(vmdk_.mutex_);
	auto it = vmdk_.ids_.find(id);
	if (hyc_unlikely(it == vmdk_.ids_.end())) {
		return -ENODEV;
	}

	auto vmdk = std::move(it->second);
	vmdk_.ids_.erase(it);
	lock.unlock();

	return vmdk->CloseVmdk();
}

RequestID Stord::VmdkRead(StordVmdk* vmdkp, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	return vmdkp->ScheduleRead(privatep, bufferp, buf_sz, offset);
}

uint32_t Stord::VmdkGetCompleteRequest(StordVmdk* vmdkp,
		RequestResult* resultsp, uint32_t nresults, bool *has_morep) {
	return vmdkp->GetCompleteRequests(resultsp, nresults, has_morep);
}

RequestID Stord::VmdkWrite(StordVmdk* vmdkp, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	return vmdkp->ScheduleWrite(privatep, bufferp, buf_sz, offset);
}

RequestID Stord::VmdkWriteSame(StordVmdk* vmdkp, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset) {
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
	*handlep = nullptr;
	try {
		::hyc::StordVmdk* vmdkp;
		auto rc = g_stord.OpenVmdk(vmid, vmdkid, eventfd, &vmdkp);
		if (hyc_unlikely(rc < 0)) {
			return rc;
		}
		*handlep = reinterpret_cast<VmdkHandle>(vmdkp);
		return rc;
	} catch (std::exception& e) {
		return -ENODEV;
	}
}

int32_t HycCloseVmdk(VmdkHandle handle) {
	try {
		auto vmdkp = reinterpret_cast<::hyc::StordVmdk*>(handle);
		return g_stord.CloseVmdk(vmdkp);
	} catch (std::exception& e) {
		return -ENODEV;
	}
}

RequestID HycScheduleRead(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	try {
		auto vmdkp = reinterpret_cast<::hyc::StordVmdk*>(handle);
		return g_stord.VmdkRead(vmdkp, privatep, bufferp, buf_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}

uint32_t HycGetCompleteRequests(VmdkHandle handle, RequestResult *resultsp,
		uint32_t nresults, bool *has_morep) {
	try {
		auto vmdkp = reinterpret_cast<::hyc::StordVmdk*>(handle);
		return g_stord.VmdkGetCompleteRequest(vmdkp, resultsp, nresults,
			has_morep);
	} catch (const std::exception& e) {
		*has_morep = true;
		return 0;
	}
}

RequestID HycScheduleWrite(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	try {
		auto vmdkp = reinterpret_cast<::hyc::StordVmdk*>(handle);
		return g_stord.VmdkWrite(vmdkp, privatep, bufferp, buf_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}

RequestID HycScheduleWriteSame(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset) {
	try {
		auto vmdkp = reinterpret_cast<::hyc::StordVmdk*>(handle);
		return g_stord.VmdkWriteSame(vmdkp, privatep, bufferp,
			buf_sz, write_sz, offset);
	} catch(std::exception& e) {
		return kInvalidRequestID;
	}
}

void HycDumpVmdk(VmdkHandle handle) {
	try {
		auto vmdkp = reinterpret_cast<::hyc::StordVmdk*>(handle);
		LOG(ERROR) << *vmdkp;
	} catch (std::exception& e) {
		LOG(ERROR) << "Invalid VMDK " << handle;
	}

}

void HycSetBatchingAttributes(uint32_t adaptive_batch, uint32_t wan_latency,
		uint32_t batch_incr_val, uint32_t batch_decr_pct,
		uint32_t system_load_factor, uint32_t debug_log) {
	LOG(ERROR) << "Changing adaptive batching from "
		<< kAdaptiveBatching << " to " << adaptive_batch;
	LOG(ERROR) << "Changing expected WAN latency from "
		<< kExpectedWanLatency << " to " << wan_latency
		<< " (all units in micro-seconds)";
	LOG(ERROR) << "Changing kBatchIncrValue from "
		<< kBatchIncrValue << " to " << batch_incr_val;
	LOG(ERROR) << "Changing kBatchDecrPercent from "
		<< kBatchDecrPercent << " to " << batch_decr_pct;
	LOG(ERROR) << "Changing kSystemLoadFactor from "
		<< kSystemLoadFactor << " to " << system_load_factor;
	LOG(ERROR) << "Changing kLogging from "
		<< kLogging << " to " << debug_log;

	//Assumption is that system is quiesced, when these
	//parameters are being set. No IOs should be going on.
	kExpectedWanLatency = wan_latency;
	kAdaptiveBatching = adaptive_batch ? true : false;
	kBatchIncrValue = batch_incr_val;
	kBatchDecrPercent = batch_decr_pct;
	kSystemLoadFactor = system_load_factor;
	kLogging = debug_log;
}


int HycGetVmdkStats(const char* vmdkid, vmdk_stats_t *vmdk_stats) {
	::hyc::StordVmdk *vmdkp = g_stord.FindVmdk(std::string(vmdkid));
	if (!vmdkp) {
		return -EINVAL;
	}

	const ::hyc::VmdkStats& stats = vmdkp->GetVmdkStats();
	vmdk_stats->stord_stats_pending = vmdkp->GetStordStats().pending_;
	vmdk_stats->batchsize_decr = stats.batchsize_decr_;
	vmdk_stats->batchsize_incr = stats.batchsize_incr_;
	vmdk_stats->batchsize_same = stats.batchsize_same_;
	vmdk_stats->need_schedule_count = stats.need_schedule_count_;
	vmdk_stats->avg_batchsize = stats.avg_batchsize_.Average();

	return 0;
}
