#include <memory>
#include <iostream>
#include <thread>
#include <mutex>
#include <string>
#include <algorithm>
#include <atomic>

#include <cstdint>
#include <cassert>
#include <cerrno>

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
#include "Serialize.h"
#include "Request.h"

static std::string StordIp = "127.0.0.1";
static uint16_t StordPort = 9876;

using namespace std::chrono_literals;
static size_t kExpectedWanLatency = std::chrono::microseconds(20ms).count();
static size_t kMaxBatchSize = 32; //tgt limit of outstanding IOs
static size_t kMinBatchSize = 4;
static size_t kMaxBatchSizeJump = kMaxBatchSize >> 1;
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
				LOG(INFO) << *vmdkp;
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

	std::atomic<int64_t> truncate_requests_{0};
	std::atomic<int64_t> truncate_failed_{0};
	std::atomic<int64_t> truncate_latency_{0};

	std::atomic<int64_t> sync_requests_;
	std::atomic<int64_t> sync_ongoing_writes_;
	std::atomic<int64_t> sync_hold_new_writes_;

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
	StordVmdk(std::string vmid, std::string vmdkid, uint64_t lun_size,
		uint32_t lun_blk_size, int eventfd);
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
	RequestID ScheduleTruncate(const void* privatep, char* bufferp,
		int32_t buf_sz);
	int32_t AbortScheduledRequest(const void* privatep);
	int32_t AbortRequest(const void* privatep);
	RequestID ScheduleSyncCache(const void* privatep, uint64_t offset,
		uint64_t length);
	uint32_t GetCompleteRequests(RequestResult* resultsp, uint32_t nresults,
		bool *has_morep);

	::hyc_thrift::VmdkHandle GetHandle() const noexcept;
	const std::string& GetVmdkId() const noexcept;
	void ScheduleMore(folly::EventBase* basep, StorRpcAsyncClient* clientp);

	const VmdkStats& GetVmdkStats() const noexcept;

	friend std::ostream& operator << (std::ostream& os, const StordVmdk& vmdk);

	mutable std::mutex stats_mutex_;

private:
	void ScheduleNow(folly::EventBase* basep, StorRpcAsyncClient* clientp);
	int64_t RpcRequestScheduledCount() const noexcept;
	uint64_t PendingOperations() const noexcept;
	bool PrepareRequest(std::unique_ptr<SyncRequest> request);
	bool PrepareRequest(std::unique_ptr<Request> request);
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
	folly::Future<int> ScheduleTruncate(StorRpcAsyncClient* clientp,
		RequestID reqid, std::vector<TruncateReq>&& requests);
	void ScheduleTruncate(folly::EventBase* basep, StorRpcAsyncClient* clientp,
		Request* reqp);

private:
	bool SyncRequestComplete(RequestID id, int32_t result);
	bool RequestComplete(RequestID id, int32_t result);
	template <typename T>
	void RequestComplete(T* reqp);
	template <typename T, typename... ErrNo>
	void RequestComplete(const std::vector<T>& requests, ErrNo&&... no);
	void BulkReadComplete(const std::vector<Request*>& requests,
		const std::vector<::hyc_thrift::ReadResult>& results);

private:
	std::string vmid_;
	std::string vmdkid_;
	::hyc_thrift::VmdkHandle vmdk_handle_{kInvalidVmdkHandle};
	const uint64_t lun_size_{};
	const uint32_t lun_blk_shift_{};
	int eventfd_{-1};
	StordConnection* connectp_{nullptr};

	struct {
		mutable std::mutex mutex_;
		std::unordered_map<RequestID, std::unique_ptr<Request>> scheduled_;
		std::vector<Request*> rpc_pending_;
		std::vector<std::unique_ptr<RequestBase>> complete_;
		std::unordered_map<RequestID, std::unique_ptr<SyncRequest>> sync_pending_;
	} requests_;

	mutable std::mutex send_rpc_mutex_;
	MovingAverage<uint64_t, 128> latency_avg_{};
	MovingAverage<uint64_t, 128> bulk_depth_avg_{};
	size_t batch_size_{1};
	size_t batch_size_jump_{0};
	int32_t batch_dir_{1};
	uint64_t bottom_latency_{0};
	bool batch_hit_bottom_{false};
	bool scheduled_early_{false};
	bool need_schedule_{false};
	VmdkStats stats_;

	static StordStats stord_stats_;
	static MovingAverage<uint64_t, 128> stord_load_avg_;

	std::atomic<RequestID> requestid_{0};
};

StordStats StordVmdk::stord_stats_;
MovingAverage<uint64_t, 128> StordVmdk::stord_load_avg_;

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
		<< " Lun Size " << vmdk.lun_size_
		<< " Lun BlkShift " << vmdk.lun_blk_shift_
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

StordVmdk::StordVmdk(std::string vmid, std::string vmdkid, uint64_t lun_size,
		uint32_t lun_blk_shift, int eventfd) :
		vmid_(std::move(vmid)),
		vmdkid_(std::move(vmdkid)),
		lun_size_(lun_size),
		lun_blk_shift_(lun_blk_shift),
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
	std::lock_guard<std::mutex> lock(stats_mutex_);

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

/* Prepare a sync request for completion */
bool StordVmdk::PrepareRequest(std::unique_ptr<SyncRequest> request) {
	bool prepared = true;
	auto nreqp = request.get();

	if (nreqp->type != Request::Type::kSync) {
		return prepared;
	}

	std::lock_guard<std::mutex> lock(requests_.mutex_);
	for (auto& req_map : requests_.scheduled_) {
		auto req_ptr = req_map.second.get();
		if ((req_ptr->type == Request::Type::kWrite or
			req_ptr->type == Request::Type::kWriteSame) &&
			req_ptr->IsOverlapped(nreqp->offset, nreqp->length)) {
				req_ptr->sync_req = nreqp;
				++nreqp->count;
				++stats_.sync_ongoing_writes_;
				/* Do not complete a sync request as overlapping writes on
				   sync are not completed */
				prepared = false;
		}
	}
	requests_.sync_pending_.emplace(nreqp->id, std::move(request));

	return prepared;
}

/* Prepare a request for RPC */
bool StordVmdk::PrepareRequest(std::unique_ptr<Request> request) {
	bool prepared = true;
	auto nreqp = request.get();

	++stats_.pending_;
	std::lock_guard<std::mutex> lock(requests_.mutex_);

	/* Effective for new writes overlapping on sync */
	bool overlapped_write = false;
	if ((nreqp->type == Request::Type::kWrite or
		nreqp->type == Request::Type::kWriteSame) &&
		requests_.sync_pending_.size()) {
		for (auto& sync_req : requests_.sync_pending_) {
			SyncRequest *syncp = sync_req.second.get();
			if (nreqp->IsOverlapped(syncp->offset, syncp->length)) {
				syncp->write_pending.emplace_back(nreqp);
				overlapped_write = true;
				prepared = false;
				++stats_.sync_hold_new_writes_;
			}
		}
	}


	bool scheduled_list_empty = requests_.scheduled_.empty();
	requests_.scheduled_.emplace(request->id, std::move(request));

	/* Do RPC only if write is not overlapping on sync */
	if (!overlapped_write) {
		requests_.rpc_pending_.emplace_back(nreqp);
	}

	if (prepared) {
	        prepared = false;
		if (kAdaptiveBatching) {  
			//scheduled early is set if there is atleast one already scheduled IO
			//and we are attempting to send more due to pending IOs size >= batch_size.
			//scheduled early indicates that current batch sie is insufficient to
			//absorb the application parallelism and need a change. IO callback
			//will look at it and if latency permits, will increase the batch size
			if (not scheduled_list_empty && requests_.rpc_pending_.size() >= batch_size_) {
				scheduled_early_ = true;
				prepared = true;
			}
		} else if (not scheduled_list_empty) {
			prepared = latency_avg_.Average() > kExpectedWanLatency;
		}
	}
	return prepared | scheduled_list_empty;
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
			stats_.read_bytes_ += reqp->length;
		}
		break;
	case Request::Type::kWrite:
		++stats_.write_requests_;
		if (hyc_unlikely(reqp->result)) {
			++stats_.write_failed_;
		} else {
			stats_.write_latency_ += latency;
			stats_.write_bytes_ += reqp->length;
		}
		break;
	case Request::Type::kWriteSame:
		++stats_.write_same_requests_;
		if (hyc_unlikely(reqp->result)) {
			++stats_.write_same_failed_;
		} else {
			stats_.write_latency_ += latency;
			stats_.write_bytes_ += reqp->length;
		}
		break;
	case Request::Type::kTruncate:
		++stats_.truncate_requests_;
		if (hyc_unlikely(reqp->result)) {
			++stats_.truncate_failed_;
		} else {
			stats_.truncate_latency_ += latency;
		}
	case Request::Type::kSync:
		/* No stats requires for sync as it will not fails in thrift */
		break;
	}
}

std::ostream& operator << (std::ostream& os, const RequestBase::Type type) {
	switch (type) {
	case RequestBase::Type::kRead:
		os << "read";
		break;
	case RequestBase::Type::kWrite:
		os << "write";
		break;
	case RequestBase::Type::kWriteSame:
		os << "writesame";
		break;
	case RequestBase::Type::kTruncate:
		os << "truncate";
		break;
	case RequestBase::Type::kSync:
		os << "sync";
		break;
	}
	return os;
}

std::ostream& operator << (std::ostream& os, const SyncRequest& request) {
	os << "ID " << request.id
		<< " type " << request.type
		<< " privatep " << request.privatep
		<< " offset " << request.offset
		<< " length " << request.length;
	return os;
}

std::ostream& operator << (std::ostream& os, const Request& request) {
	os << "ID " << request.id
		<< " type " << request.type
		<< " privatep " << request.privatep
		<< " bufferp " << request.bufferp
		<< " buf_sz " << request.buf_sz
		<< " offset " << request.offset
		<< " length " << request.length;
	return os;
}

RequestBase::RequestBase(RequestID id, Type t, const void* privatep,
	uint64_t length, int64_t offset, size_t batch_size) : id(id), type(t),
	privatep(privatep), length(length), offset(offset), 
	batch_size(batch_size) {
}

RequestBase::~RequestBase() {
}

const RequestBase::Type& RequestBase::GetType() const noexcept {
	return type;
}

bool RequestBase::IsOverlapped(uint64_t req_offset,
	uint64_t req_length) const noexcept {
	return not((offset + length-1) < req_offset or
		(req_offset + req_length-1) < offset);
}

Request::Request(RequestID id, Type t, const void* privatep, char *bufferp,
	int32_t buf_sz, uint64_t length, int64_t offset, size_t batch_size) : 
	RequestBase(id, t, privatep, length, offset, batch_size), 
	bufferp(bufferp), buf_sz(buf_sz), sync_req(NULL) {
}

Request::~Request() {
}

SyncRequest::SyncRequest(RequestID id, Type t, const void* privatep,
	uint64_t length, int64_t offset, size_t batch_size) :
	RequestBase(id, t, privatep, length, offset, batch_size), count(0) {
}

SyncRequest::~SyncRequest() {
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

bool StordVmdk::SyncRequestComplete(RequestID id, int32_t result) {
	bool pending_ios = false;

	std::unique_lock<std::mutex> lock(requests_.mutex_);
	auto sync_it = requests_.sync_pending_.find(id);
	log_assert(sync_it != requests_.sync_pending_.end());
	if (result) {
		VLOG(5) << "reqid " << id << " has nonzero res: " << result;
	}

	auto sync_req = std::move(sync_it->second);
	requests_.sync_pending_.erase(sync_it);

	for (auto write_req : sync_req->write_pending) {
		pending_ios = true;
		Request *reqp = reinterpret_cast<Request *>(write_req);
		requests_.rpc_pending_.emplace_back(reqp);
		--stats_.sync_hold_new_writes_;
	}

	--stats_.pending_;
	++stats_.sync_requests_;
	requests_.complete_.emplace_back(std::move(sync_req));

	lock.unlock();
	if (pending_ios) {
		auto basep = connectp_->GetEventBase();
		auto clientp = connectp_->GetRpcClient();
		ScheduleNow(basep, clientp);
	}

	return true;
}

void StordVmdk::UpdateBatchSize(Request* reqp) {
	//don't update, if the batch size has already been updated
	//or if there are not enough latency samples
	if ((reqp->batch_size != batch_size_) ||
			(latency_avg_.GetSamples() < latency_avg_.GetMaxSamples())) {
		return;
	}

	bool batch_changed = true;
	uint64_t avg_latency = latency_avg_.Average();
	size_t system_load = stord_load_avg_.Average();

	//If batch size has hit the bottom, adjust the batch_size_jump.
	//batch_size_jump is the amount that the batch size will be
	//moved up, the next time it hits the bottom.
	//Up or down is determined by the batch_dir. Batch_dir is positive
	//if moving up is giving the latency benefit, else it is negative.
	if (batch_hit_bottom_) {
		batch_hit_bottom_ = false;
		if (bottom_latency_) {
			if (avg_latency < bottom_latency_) {
				batch_size_jump_ += batch_dir_;
				kLogging && LOG(ERROR) << "New batch size jump positive " << batch_size_jump_;
			} else {
				batch_dir_ *= -1;
				batch_size_jump_ += batch_dir_;
				kLogging && LOG(ERROR) << "New batch size jump negative " << batch_size_jump_;
			}
		}
		if (batch_size_jump_ == 0) {
			batch_size_jump_ = 1;
		} else if (batch_size_jump_ >= kMaxBatchSizeJump) {
			batch_size_jump_ = kMaxBatchSizeJump;
		}
		bottom_latency_ = avg_latency;
	}

	if (avg_latency > (kExpectedWanLatency + system_load)) {
		//reduce the batch size, since we have hit limit for latency
		batch_size_ -= (batch_size_ * kBatchDecrPercent) / 100;
		if (batch_size_ <= kMinBatchSize) {
			kLogging && LOG(ERROR) << "Resetting batch size " << batch_size_ <<
				" to minimum " << kMinBatchSize;
			batch_size_ = kMinBatchSize + batch_size_jump_;
			batch_hit_bottom_ = true;
		}

		kLogging && LOG(ERROR) << "Reduced batch size to " << batch_size_ <<
			" avg_latency " << avg_latency;
		//new smaller batch_size might have caused pending ios
		//size to be more than new batch size. Schedule all such IOs
		if (requests_.rpc_pending_.size() >= batch_size_) {
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
		stord_load_avg_.Add(stord_stats_.pending_ >> kSystemLoadFactor);
		latency_avg_.Reset();
		stats_.avg_batchsize_.Add(batch_size_);
	}
}

bool StordVmdk::RequestComplete(RequestID id, int32_t result) {
	std::unique_lock<std::mutex> lock(requests_.mutex_);
	bool post = false;
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

	/* Special handling for sync completion */
	if ((reqp->type == RequestBase::Type::kWrite ||
		reqp->type == RequestBase::Type::kWriteSame) && reqp->sync_req) {
		SyncRequest *sync_reqp = reinterpret_cast<SyncRequest *>(reqp->sync_req);

		--stats_.sync_ongoing_writes_;
		--sync_reqp->count;
		if (!sync_reqp->count) {
			sync_reqp->result = 0;
			lock.unlock();
			/* Complete sync request */
			RequestComplete(sync_reqp);
			lock.lock();
		}
		return false;
	}

	post = requests_.scheduled_.empty() or
		requests_.complete_.size() >= bulk_depth_avg_.Average();

	return post;
}

template <typename T>
void StordVmdk::RequestComplete(T* reqp) {
	bool post = false;

	if (reqp->type == RequestBase::Type::kSync) {
		post = SyncRequestComplete(reqp->id, reqp->result);
	} else {
		post = RequestComplete(reqp->id, reqp->result);
	}

	if (post) {
		auto rc = PostRequestCompletion();
		(void) rc;
	}
}

template <typename T, typename... ErrNo>
void StordVmdk::RequestComplete(const std::vector<T>& requests, ErrNo&&... no) {
	bool post = false;

	for (const auto& req : requests) {
		if constexpr (HasResult<T>()) {
			post = RequestComplete(req.reqid, req.result);
		} else if constexpr (sizeof...(no) == 1)  {
			post = RequestComplete(req.reqid, GetErrNo(std::forward<ErrNo>(no)...));
		} else {
			static_assert(not std::is_same<T, T>::value,
					"Not sure what to set result");
		}
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

int32_t StordVmdk::AbortRequest(const void* privatep) {
	std::lock_guard<std::mutex> lock(send_rpc_mutex_);
	{
		std::lock_guard<std::mutex> lock(requests_.mutex_);

		for (auto reqp : requests_.rpc_pending_) {
			std::lock_guard<std::mutex> lock(reqp->mutex_);
			if (reqp->privatep == privatep) {
				reqp->privatep = nullptr;
				break;
			}
		}

		auto sit = requests_.complete_.begin();
		for (auto& reqp : requests_.complete_) {
			std::lock_guard<std::mutex> lock(reqp->mutex_);
			if (reqp->privatep == privatep) {
				reqp->privatep = nullptr;
				requests_.complete_.erase(sit);
				return 0;
			}
			sit++;
		}

		for (auto& req_map : requests_.scheduled_) {
			auto reqp = req_map.second.get();
			{
				std::lock_guard<std::mutex> lock(reqp->mutex_);
				if (reqp->privatep == privatep) {
					reqp->privatep = nullptr;
					return 0;
				}
			}
		}
	}
	return 0;
}


uint32_t StordVmdk::GetCompleteRequests(RequestResult* resultsp,
		uint32_t nresults, bool *has_morep) {
	RequestBase *reqp;
	*has_morep = false;
	std::lock_guard<std::mutex> lock(requests_.mutex_);
	auto tocopy = std::min(requests_.complete_.size(), static_cast<size_t>(nresults));
	if (not tocopy) {
		return 0;
	}

	auto eit = requests_.complete_.end();
	auto sit = std::prev(eit, tocopy);
	for (auto resultp = resultsp; sit != eit; ++sit) {
		reqp = sit->get();
		{
			std::lock_guard<std::mutex> lock(reqp->mutex_);
			if (reqp->privatep != nullptr) {
				resultp->privatep   = reqp->privatep;
				resultp->request_id = reqp->id;
				resultp->result     = reqp->result;
				++resultp;
			}
		}
	}
	requests_.complete_.erase(std::prev(eit, tocopy), eit);
	*has_morep = not requests_.complete_.empty();
	return tocopy;
}

void StordVmdk::ReadDataCopy(Request* reqp, const ReadResult& result) {
	std::lock_guard<std::mutex> lock(reqp->mutex_);
	if (reqp->privatep == nullptr) {
		return;
	}
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
	++stats_.rpc_requests_scheduled_;
	clientp->future_WriteSame(vmdk_handle_, reqp->id, data, reqp->buf_sz,
		reqp->length, reqp->offset)
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
}

void StordVmdk::ScheduleWrite(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, Request* reqp) {
	log_assert(reqp && basep->isInEventBaseThread());

	auto data = std::make_unique<folly::IOBuf>(folly::IOBuf::WRAP_BUFFER,
		reqp->bufferp, reqp->buf_sz);
	++stats_.rpc_requests_scheduled_;
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
}

void StordVmdk::ScheduleBulkWrite(folly::EventBase* basep,
		StorRpcAsyncClient* clientp,
		std::unique_ptr<std::vector<::hyc_thrift::WriteRequest>> reqs) {
	log_assert(basep->isInEventBaseThread());
	++stats_.rpc_requests_scheduled_;
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
}

void StordVmdk::ScheduleRead(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, Request* reqp) {
	log_assert(reqp && basep->isInEventBaseThread());

	++stats_.rpc_requests_scheduled_;
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

	++stats_.rpc_requests_scheduled_;
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
}

folly::Future<int> StordVmdk::ScheduleTruncate(StorRpcAsyncClient* clientp,
		RequestID reqid, std::vector<TruncateReq>&& requests) {
	++stats_.rpc_requests_scheduled_;
	return clientp->future_Truncate(vmdk_handle_, reqid, std::forward<std::vector<TruncateReq>>(requests))
	.then([this] (const TruncateResult& result) {
		--stats_.rpc_requests_scheduled_;
		return result.get_result();
	})
	.onError([this] (const std::exception& e) {
		--stats_.rpc_requests_scheduled_;
		return -EIO;
	});
}

void StordVmdk::ScheduleTruncate(folly::EventBase* basep,
		StorRpcAsyncClient* clientp, Request* reqp) {
	log_assert(basep->isInEventBaseThread());

	std::vector<folly::Future<int>> futures;
	std::vector<::hyc_thrift::TruncateReq> truncate;

	auto length = reqp->buf_sz;
	char* bufp = reqp->bufferp;
	for (; length >= 16; length -= 16, bufp += 16) {
		uint64_t offset;
		uint32_t len;

		offset = BigEndian::DeserializeInt<decltype(offset)>
			(reinterpret_cast<const uint8_t*>(&bufp[0])) << lun_blk_shift_;
		len = BigEndian::DeserializeInt<decltype(len)>
			(reinterpret_cast<const uint8_t*>(&bufp[8])) << lun_blk_shift_;
		if (offset + len > lun_size_) {
			LOG(ERROR) << "Truncate beyond EOD "
				<< offset << ' '
				<< len << ' '
				<< lun_size_ << ' '
				<< length;
			break;
		} else if (len <= 0) {
			continue;
		}

		truncate.emplace_back(apache::thrift::FragileConstructor(), offset, len);
		if (truncate.size() > 1024) {
			futures.emplace_back(
				ScheduleTruncate(clientp, reqp->id, std::move(truncate))
			);
		}
	}

	if (not truncate.empty()) {
		futures.emplace_back(
			ScheduleTruncate(clientp, reqp->id, std::move(truncate))
		);
	}

	if (futures.empty()) {
		reqp->result = 0;
		RequestComplete(reqp);
		return;
	}

	folly::collectAll(std::move(futures))
	.then([this, reqp] (const folly::Try<std::vector<folly::Try<int>>>& tries) {
		if (hyc_unlikely(tries.hasException())) {
			reqp->result = -EIO;
			RequestComplete(reqp);
			return;
		}

		auto vec = std::move(tries.value());
		for (const auto& trie : vec) {
			if (hyc_unlikely(trie.hasException())) {
				reqp->result = -EIO;
			} else {
				reqp->result = trie.value();
			}
		}

		RequestComplete(reqp);
	});
}

void StordVmdk::ScheduleNow(folly::EventBase* basep, StorRpcAsyncClient* clientp) {
	auto GetPending = [this] (std::vector<Request*>& pending) mutable {
		pending.clear();
		std::lock_guard<std::mutex> lock(requests_.mutex_);
		requests_.rpc_pending_.swap(pending);
		requests_.rpc_pending_.reserve(32);
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

		std::lock_guard<std::mutex> lock(send_rpc_mutex_);
		for (auto reqp : pending) {
			std::lock_guard<std::mutex> lock(reqp->mutex_);
			if (reqp->privatep == nullptr) {
				continue;
			}

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
			case Request::Type::kTruncate:
				ScheduleTruncate(basep, clientp, reqp);
				break;
			case Request::Type::kSync:
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
	auto req = std::make_unique<Request>(++requestid_, Request::Type::kRead,
		privatep, bufferp, buf_sz, buf_sz, offset, batch_size_);
	if (hyc_unlikely(not req)) {
		return kInvalidRequestID;
	}

	auto reqp = req.get();
	if (PrepareRequest(std::move(req))) {
		auto basep = connectp_->GetEventBase();
		auto clientp = connectp_->GetRpcClient();
		ScheduleNow(basep, clientp);
	}
	return reqp->id;
}

int32_t StordVmdk::AbortScheduledRequest(const void* privatep) {
	int32_t rc;

	log_assert(vmdk_handle_ != kInvalidVmdkHandle);

	rc = AbortRequest(privatep);
	return rc;
}

RequestID StordVmdk::ScheduleWrite(const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset) {
	log_assert(vmdk_handle_ != kInvalidVmdkHandle);
	auto req = std::make_unique<Request>(++requestid_, Request::Type::kWrite,
		privatep, bufferp, buf_sz, buf_sz, offset, batch_size_);
	if (hyc_unlikely(not req)) {
		return kInvalidRequestID;
	}

	auto reqp = req.get();
	if (PrepareRequest(std::move(req))) {
		auto basep = connectp_->GetEventBase();
		auto clientp = connectp_->GetRpcClient();
		ScheduleNow(basep, clientp);
	}

	return reqp->id;
}

RequestID StordVmdk::ScheduleWriteSame(const void* privatep, char* bufferp,
		int32_t buf_sz, int32_t write_sz, int64_t offset) {
	log_assert(vmdk_handle_ != kInvalidVmdkHandle);
	auto req = std::make_unique<Request>(++requestid_, Request::Type::kWrite,
		privatep, bufferp, buf_sz, write_sz, offset, batch_size_);
	if (hyc_unlikely(not req)) {
		return kInvalidRequestID;
	}

	auto reqp = req.get();
	if (PrepareRequest(std::move(req))) {
		auto basep = connectp_->GetEventBase();
		auto clientp = connectp_->GetRpcClient();
		ScheduleNow(basep, clientp);
	}

	return reqp->id;
}

RequestID StordVmdk::ScheduleTruncate(const void* privatep, char* bufferp,
		int32_t buf_sz) {
	if (hyc_unlikely(vmdk_handle_ == kInvalidVmdkHandle)) {
		return kInvalidRequestID;
	}

	auto req = std::make_unique<Request>(++requestid_, Request::Type::kTruncate,
		privatep, bufferp, buf_sz, buf_sz, 0, batch_size_);
	if (hyc_unlikely(not req)) {
		return kInvalidRequestID;
	}

	auto reqp = req.get();
	if (PrepareRequest(std::move(req))) {
		auto basep = connectp_->GetEventBase();
		auto clientp = connectp_->GetRpcClient();
		ScheduleNow(basep, clientp);
	}
	return reqp->id;
}

RequestID StordVmdk::ScheduleSyncCache(const void* privatep, uint64_t offset,
		uint64_t length) {
	if (hyc_unlikely(vmdk_handle_ == kInvalidVmdkHandle)) {
		return kInvalidRequestID;
	}

	auto sync_req = std::make_unique<SyncRequest>(++requestid_,
		Request::Type::kSync, privatep, length, offset, batch_size_);
	if (hyc_unlikely(not sync_req)) {
		return kInvalidRequestID;
	}

	sync_req->count = 0;
	sync_req->result = 0;

	++stats_.pending_;
	auto sync_reqp = sync_req.get();
	if (PrepareRequest(std::move(sync_req))) {
		RequestComplete(sync_reqp);
	}

	return sync_reqp->id;
}

const VmdkStats& StordVmdk::GetVmdkStats() const noexcept {
	return stats_;
}

class Stord {
public:
	~Stord();
	int32_t Connect(uint32_t ping_secs = 30);
	int32_t Disconnect(bool force = false);
	int32_t OpenVmdk(const char* vmid, const char* vmdkid, uint64_t lun_size,
		uint32_t lun_blk_shift,  int eventfd, StordVmdk** vmdkpp);
	int32_t CloseVmdk(StordVmdk* vmdkp);
	RequestID VmdkRead(StordVmdk* vmdkp, const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset);
	uint32_t VmdkGetCompleteRequest(StordVmdk* vmdkp, RequestResult* resultsp,
		uint32_t nresults, bool *has_morep);
	RequestID VmdkWrite(StordVmdk* vmdkp, const void* privatep, char* bufferp,
		int32_t buf_sz, int64_t offset);
	RequestID VmdkWriteSame(StordVmdk* vmdkp, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset);
	RequestID VmdkTruncate(StordVmdk* vmdkp, const void* privatep,
		char* bufferp, int32_t buf_sz);
	int32_t AbortVmdkOp(StordVmdk* vmdkp, const void* privatep);
	RequestID VmdkSyncCache(StordVmdk* vmdkp, const void* privatep,
		uint64_t offset, uint64_t length);
	StordVmdk* FindVmdk(::hyc_thrift::VmdkHandle handle);
	StordVmdk* FindVmdk(const std::string& vmdkid);
	std::vector<StordVmdk*> GetVmdkList();
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
	if (cores <= 0) {
		cores = 1;
	}
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

std::vector<StordVmdk*> Stord::GetVmdkList() {
	std::vector<StordVmdk*> vmdks;
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (auto& it : vmdk_.ids_) {
		vmdks.push_back(it.second.get());
	}
	return vmdks;
}

StordVmdk* Stord::FindVmdk(const std::string& vmdkid) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	auto it = vmdk_.ids_.find(vmdkid);
	if (hyc_unlikely(it == vmdk_.ids_.end())) {
		return nullptr;
	}

	return it->second.get();
}

int32_t Stord::OpenVmdk(const char* vmid, const char* vmdkid, uint64_t lun_size,
		uint32_t lun_blk_shift, int eventfd, StordVmdk** vmdkpp) {
	*vmdkpp = nullptr;
	auto vmdkp = FindVmdk(vmdkid);
	if (hyc_unlikely(vmdkp)) {
		return -EEXIST;
	}

	auto vmdk = std::make_unique<StordVmdk>(vmid, vmdkid, lun_size,
			lun_blk_shift, eventfd);
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

int32_t Stord::AbortVmdkOp(StordVmdk* vmdkp, const void* privatep) {
	return vmdkp->AbortScheduledRequest(privatep);
}

RequestID Stord::VmdkWrite(StordVmdk* vmdkp, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset) {
	return vmdkp->ScheduleWrite(privatep, bufferp, buf_sz, offset);
}

RequestID Stord::VmdkWriteSame(StordVmdk* vmdkp, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset) {
	return vmdkp->ScheduleWriteSame(privatep, bufferp, buf_sz, write_sz, offset);
}

RequestID Stord::VmdkTruncate(StordVmdk* vmdkp, const void* privatep,
		char* bufferp, int32_t buf_sz) {
	return vmdkp->ScheduleTruncate(privatep, bufferp, buf_sz);
}

RequestID Stord::VmdkSyncCache(StordVmdk* vmdkp, const void* privatep,
		uint64_t offset, uint64_t length) {
	return vmdkp->ScheduleSyncCache(privatep, offset, length);
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

int32_t HycOpenVmdk(const char* vmid, const char* vmdkid, uint64_t lun_size,
		uint32_t lun_blk_shift, int eventfd, VmdkHandle* handlep) {
	log_assert(vmid != nullptr and vmdkid != nullptr and handlep);
	*handlep = nullptr;
	try {
		::hyc::StordVmdk* vmdkp;
		auto rc = g_stord.OpenVmdk(vmid, vmdkid, lun_size, lun_blk_shift, eventfd, &vmdkp);
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

int32_t HycScheduleAbort(VmdkHandle handle, const void* privatep) {
	try {
		auto vmdkp = reinterpret_cast<::hyc::StordVmdk*>(handle);
		return g_stord.AbortVmdkOp(vmdkp, privatep);
	} catch(std::exception& e) {
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

RequestID HycScheduleTruncate(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz) {
	try {
		auto vmdkp = reinterpret_cast<::hyc::StordVmdk*>(handle);
		return g_stord.VmdkTruncate(vmdkp, privatep, bufferp, buf_sz);
	} catch (std::exception& e) {
		return kInvalidRequestID;
	}
}

/* Usually sync come on complete disk */
RequestID HycScheduleSyncCache(VmdkHandle handle, const void* privatep,
	uint64_t offset, uint64_t length) {
	try {
		auto vmdkp = reinterpret_cast<::hyc::StordVmdk*>(handle);
		return g_stord.VmdkSyncCache(vmdkp, privatep, offset, length);
	} catch (std::exception& e) {
		return kInvalidRequestID;
	}
}

int HycGetVmdkStats(const char* vmdkid, vmdk_stats_t *vmdk_stats) {
	::hyc::StordVmdk *vmdkp = g_stord.FindVmdk(std::string(vmdkid));
	if (!vmdkp) {
		return -EINVAL;
	}

	const ::hyc::VmdkStats& stats = vmdkp->GetVmdkStats();

	vmdk_stats->read_requests = stats.read_requests_;
	vmdk_stats->read_failed = stats.read_failed_;
	vmdk_stats->read_bytes = stats.read_bytes_;
	vmdk_stats->read_latency = stats.read_latency_;

	vmdk_stats->write_requests = stats.write_requests_;
	vmdk_stats->write_failed = stats.write_failed_;
	vmdk_stats->write_same_requests = stats.write_same_requests_;
	vmdk_stats->write_same_failed = stats.write_same_failed_;
	vmdk_stats->write_bytes = stats.write_bytes_;
	vmdk_stats->write_latency = stats.write_latency_;

	vmdk_stats->truncate_requests = stats.truncate_requests_;
	vmdk_stats->truncate_failed = stats.truncate_failed_;
	vmdk_stats->truncate_latency = stats.truncate_latency_;

	vmdk_stats->sync_requests = stats.sync_requests_;
	vmdk_stats->sync_ongoing_writes = stats.sync_ongoing_writes_;
	vmdk_stats->sync_hold_new_writes = stats.sync_hold_new_writes_;

	vmdk_stats->pending = stats.pending_;
	vmdk_stats->rpc_requests_scheduled = stats.rpc_requests_scheduled_;

	return 0;
}

int HycGetComponentStats(component_stats_t *g_stats)
{
	std::vector<::hyc::StordVmdk*> vmdks = g_stord.GetVmdkList();
	if (not vmdks.size()) {
		return -EINVAL;
	}

	for (unsigned i=0; i<vmdks.size(); i++) {
		if (not vmdks[i]) {
			LOG(INFO) << "vmdk object got deleted in between";
			continue;
		}
		std::unique_lock<std::mutex> lock(vmdks[i]->stats_mutex_);
		const ::hyc::VmdkStats& stats = vmdks[i]->GetVmdkStats();
		g_stats->vmdk_stats.read_requests += stats.read_requests_;
		g_stats->vmdk_stats.read_failed += stats.read_failed_;
		g_stats->vmdk_stats.read_bytes += stats.read_bytes_;
		g_stats->vmdk_stats.read_latency += stats.read_latency_;
		g_stats->vmdk_stats.write_requests += stats.write_requests_;
		g_stats->vmdk_stats.write_failed += stats.write_failed_;
		g_stats->vmdk_stats.write_same_requests += stats.write_same_requests_;
		g_stats->vmdk_stats.write_same_failed += stats.write_same_failed_;
		g_stats->vmdk_stats.write_bytes += stats.write_bytes_;
		g_stats->vmdk_stats.write_latency += stats.write_latency_;
		g_stats->vmdk_stats.truncate_requests += stats.truncate_requests_;
		g_stats->vmdk_stats.truncate_failed += stats.truncate_failed_;
		g_stats->vmdk_stats.truncate_latency += stats.truncate_latency_;
		g_stats->vmdk_stats.sync_requests += stats.sync_requests_;
		g_stats->vmdk_stats.pending += stats.pending_;
		g_stats->vmdk_stats.rpc_requests_scheduled += stats.rpc_requests_scheduled_;
		lock.unlock();
	}
	return 0;
}

