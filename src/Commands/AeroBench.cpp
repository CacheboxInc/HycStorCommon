#include <string>
#include <random>
#include <atomic>
#include <thread>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <iostream>

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream_buffer.hpp>

#include <glog/logging.h>
#include <folly/futures/Future.h>
#include <folly/init/Init.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_event.h>

#include "CommonMacros.h"
#include "TimePoint.h"

using namespace std::chrono_literals;
namespace bio = boost::iostreams;
using ostring_buf = bio::stream_buffer<bio::back_insert_device<std::string>>;

static constexpr char kAsBin[] = "bin";

struct RequestBuffer {
public:
	RequestBuffer(size_t size) : size_(size) {
	}

	char* Payload() noexcept {
		return payload_;
	}

	size_t PayloadSize() const noexcept {
		return size_;
	}
private:
	const size_t size_{0};
	char payload_[0];
};

using RequestBufferPtr = std::unique_ptr<RequestBuffer, void (*) (RequestBuffer*)>;

void DestroyRequestBuffer(RequestBuffer* bufp) {
	if (pio_unlikely(not bufp)) {
		return;
	}
	bufp->~RequestBuffer();
	std::free(bufp);
}

RequestBufferPtr NewRequestBuffer(size_t size) {
	size_t sz = size + sizeof(RequestBuffer);
	void* datap = std::malloc(sz);
	if (pio_unlikely(not datap)) {
		return RequestBufferPtr(nullptr, DestroyRequestBuffer);
	}
	RequestBuffer* bufp = new (datap) RequestBuffer(size);
	return RequestBufferPtr(bufp, DestroyRequestBuffer);
}

class KeyGen {
public:
	KeyGen(int64_t start, int64_t end, bool random) : start_(start), end_(end),
			rd_(), eng_(rd_()), distr_(start_, end_), random_(random) {
	}

	int64_t Next() {
		std::lock_guard<std::mutex> lock(key_.mutex_);
		if (random_) {
			key_.last_ = distr_(eng_);
		} else {
			key_.last_ = (key_.last_ + 1) % end_;
		}
		return key_.last_;
	}

	std::pair<int64_t, int64_t> Range() const noexcept {
		return {start_, end_};
	}
private:
	int64_t start_{};
	int64_t end_{};
	std::random_device rd_;
	std::mt19937 eng_;
	std::uniform_int_distribution<int64_t> distr_;

	bool random_{false};
	struct {
		std::mutex mutex_;
		int64_t last_{0};
	} key_;
};

class AeroSpikeConnection {
public:
	AeroSpikeConnection(std::string& ips, int32_t port) :
			ips_(std::forward<std::string>(ips)), port_(port) {
	}

	void InitializeDefaultConfig(as_config& cfg) {
		as_config_init(&cfg);
		cfg.max_conns_per_node = 3000;
		cfg.async_max_conns_per_node = 3000;
		cfg.pipe_max_conns_per_node = 3000;

		cfg.policies.write.key = AS_POLICY_KEY_SEND;
		cfg.policies.read.key = AS_POLICY_KEY_SEND;
		cfg.policies.operate.key = AS_POLICY_KEY_SEND;
		cfg.policies.remove.key = AS_POLICY_KEY_SEND;
		cfg.policies.apply.key = AS_POLICY_KEY_SEND;

		/* Timeout setting */
		cfg.policies.write.base.total_timeout = 5000;// 5secs
		cfg.policies.write.base.max_retries = 0;
		cfg.policies.write.base.sleep_between_retries = 300; // 300ms 

		cfg.policies.read.base.total_timeout = 5000; // 5secs
		cfg.policies.read.base.max_retries = 10;
		cfg.policies.read.base.sleep_between_retries = 300; // 300ms 

		cfg.policies.operate.base.total_timeout = 5000; // 5secs
		cfg.policies.operate.base.max_retries = 10;
		cfg.policies.operate.base.sleep_between_retries = 300; // 300ms 

		cfg.policies.remove.base.total_timeout = 5000; // 5secs
		cfg.policies.remove.base.max_retries = 0;
		cfg.policies.remove.base.sleep_between_retries = 300; // 300ms 

		cfg.policies.batch.base.total_timeout = 5000; // 5secs
		cfg.policies.batch.base.max_retries = 10;
		cfg.policies.batch.base.sleep_between_retries = 300; // 300ms 

		cfg.policies.apply.base.total_timeout = 5000; // 5secs
		cfg.policies.apply.base.max_retries = 0;
		cfg.policies.apply.base.sleep_between_retries = 300; // 300ms 

		cfg.policies.scan.base.total_timeout = 5000; // 5secs
		cfg.policies.scan.base.max_retries = 0;
		cfg.policies.scan.base.sleep_between_retries = 300; // 300ms 
	}

	int CreateEventLoops() {
		as_error err;
		as_policy_event policy;
		as_policy_event_init(&policy);
		policy.max_commands_in_process = kMaxCommandsInProgress;
		auto status = as_create_event_loops(&err, &policy, kAeroEventLoops,
			nullptr);
		return status == AEROSPIKE_OK;
	}

	void Connect() {
		auto rc = CreateEventLoops();
		log_assert(rc == 0);

		as_error err;
		as_status status;
		as_config config;
		InitializeDefaultConfig(config);

		auto c = std::count(ips_.begin(), ips_.end(), ',') + 1;
		if (c == 1) {
			as_config_add_host(&config, ips_.c_str(), port_);
		} else {
			as_config_add_hosts(&config, ips_.c_str(), port_);
		}

		aerospike_init(&as_, &config);

		status = aerospike_connect(&as_, &err);
		log_assert(status == AEROSPIKE_OK);

		as_monitor_init(&as_mon_);
	}

	~AeroSpikeConnection() {
		as_error err;
		as_monitor_destroy(&as_mon_);
		aerospike_close(&as_, &err);
		aerospike_destroy(&as_);
		as_event_close_loops();
	}
public:
	std::string ips_;
	int32_t port_;
	aerospike as_;
	as_monitor as_mon_;
public:
	static constexpr uint16_t kAeroEventLoops = 4;
	static constexpr uint16_t kMaxCommandsInProgress = 800 / kAeroEventLoops;
};

struct WriteBatch;
struct WriteRecord {
	WriteBatch* batchp_{};
	const std::string& ns_;
	const std::string& set_;
	std::string key_;
	RequestBufferPtr buffer_;

	struct {
		as_key key_;
		as_record record_;
	} as_;

	struct {
		as_status status_;
	} result_;

	WriteRecord(WriteBatch* batchp, const std::string& ns,
			const std::string& set, std::string&& key, RequestBufferPtr buffer)
			: batchp_(batchp), ns_(ns), set_(set), key_(std::move(key)),
			buffer_(std::move(buffer)) {
	}

	~WriteRecord() {
		as_record_destroy(&as_.record_);
		as_key_destroy(&as_.key_);
	}

	int Initialize() {
		as_key_init(&as_.key_, ns_.c_str(), set_.c_str(), key_.c_str());
		as_record_init(&as_.record_, 1);
		auto s = as_record_set_raw(&as_.record_, kAsBin,
			(const uint8_t*) buffer_->Payload(), buffer_->PayloadSize());
		if (s == false) {
			LOG(ERROR) << "WriteRecord initialization failed";
			return -ENOMEM;
		}
		return 0;
	}
	int WriteComplete() const noexcept {
		switch (result_.status_) {
		default:
			LOG(ERROR) << "Key write failed " << result_.status_;
			return -EIO;
		case AEROSPIKE_OK:
			return 0;
		case AEROSPIKE_ERR_ASYNC_CONNECTION:
		case AEROSPIKE_ERR_TIMEOUT:
		case AEROSPIKE_ERR_RECORD_BUSY:
		case AEROSPIKE_ERR_DEVICE_OVERLOAD:
		case AEROSPIKE_ERR_CLUSTER:
		case AEROSPIKE_ERR_SERVER:
			LOG(ERROR) << "key write failed. Retrying.";
			return -EAGAIN;
		}
	}
};

struct WriteBatch {
	WriteBatch(AeroSpikeConnection* connectp, const std::string& ns,
			const std::string& set, const std::string& key_prefix,
			const size_t record_size, const size_t nrecords) :
			connectp_(connectp), ns_(ns), set_(set), key_prefix_(key_prefix),
			record_size_(record_size), nrecords_(nrecords) {
	}

	~WriteBatch() {
		WriteRecord* recordp = records_;
		for (size_t i = 0; i < nrecords_; ++i, ++recordp) {
			recordp->~WriteRecord();
		}
	}

	int Initialize(std::vector<int64_t>::iterator& it,
			std::vector<int64_t>::iterator& eit) {
		WriteRecord* recordp = records_;
		for (size_t i = 0; i < nrecords_; ++i, ++recordp, ++it) {
			log_assert(it != eit);
			std::string key;
			key.reserve(key_prefix_.size() + 10);
			ostring_buf buf(key);
			std::ostream os(&buf);
			os << key_prefix_ << ':' << *it << std::flush;

			auto buffer = NewRequestBuffer(record_size_);
			if (pio_unlikely(not buffer)) {
				LOG(ERROR) << "Memory allocation failed";
				return -ENOMEM;
			}
			std::memset(buffer->Payload(), 0, buffer->PayloadSize());
			WriteRecord* recp = new (recordp) WriteRecord(this, ns_, set_,
				std::move(key), std::move(buffer));
			log_assert(recp == recordp);
			auto rc = recp->Initialize();
			if (pio_unlikely(rc < 0)) {
				return rc;
			}
		}
		return 0;
	}

	void WriteListener(WriteRecord* recordp, as_error* errp) {
		recordp->result_.status_ = AEROSPIKE_OK;
		if (pio_unlikely(errp)) {
			recordp->result_.status_ = errp->code;
			as_monitor_notify(&connectp_->as_mon_);
		}

		bool complete = false;
		{
			std::lock_guard<std::mutex> lock(submit_.mutex_);
			complete = submit_.nsent_ == ++submit_.ncomplete_ and
				submit_.submited_;
		}
		if (complete) {
			result_.promise_.setValue(0);
		}
	}

	static void WriteListener(as_error* errp, void* datap, as_event_loop* lp) {
		auto recordp = reinterpret_cast<WriteRecord*>(datap);
		auto batchp = recordp->batchp_;
		batchp->WriteListener(recordp, errp);
	}

	void WritePipeListener(WriteRecord* recordp, as_event_loop* lp) {
		as_pipe_listener pipep = WriteBatch::WritePipeListener;

		std::unique_lock<std::mutex> lock(submit_.mutex_);
		auto sent = ++submit_.nsent_;
		if (sent == nrecords_) {
			submit_.submited_ = true;
			pipep = nullptr;
		}
		lock.unlock();

		bool complete{false};
		int rc = 0;
		recordp = records_ + sent - 1;
		as_key* kp = &recordp->as_.key_;
		as_record* rp = &recordp->as_.record_;
		as_error err;
		auto s = aerospike_key_put_async(&connectp_->as_, &err, nullptr, kp,
			rp, WriteListener, reinterpret_cast<void*>(recordp), lp, pipep);
		if (pio_unlikely(s != AEROSPIKE_OK)) {
			LOG(ERROR) << "Key put failed";
			std::lock_guard<std::mutex> lock(submit_.mutex_);
			submit_.submited_ = true;
			--submit_.nsent_;
			if (submit_.nsent_ == submit_.ncomplete_) {
				complete = true;
			}
			rc = -EIO;
		}

		if (pio_unlikely(complete)) {
			result_.promise_.setValue(rc);
		}
	}

	static void WritePipeListener(void *datap, as_event_loop *lp) {
		auto recordp = reinterpret_cast<WriteRecord*>(datap);
		auto batchp = recordp->batchp_;
		batchp->WritePipeListener(recordp, lp);
	}

	int WriteComplete() {
		WriteRecord* recordp = records_;
		int res = 0;
		for (size_t i = 0; i < nrecords_; ++i, ++recordp) {
			auto rc = recordp->WriteComplete();
			if (pio_unlikely(rc < 0)) {
				res = rc;
			}
		}
		return res;
	}

	folly::Future<int> SubmitInternal() {
		auto lp = as_event_loop_get();
		log_assert(lp);

		as_pipe_listener pipep{};
		if (nrecords_ == 1) {
			submit_.submited_ = true;
		} else {
			submit_.submited_ = false;
			pipep = WriteBatch::WritePipeListener;
		}
		submit_.nsent_ = 1;

		WriteRecord* recordp = records_;
		as_key* kp = &recordp->as_.key_;
		as_record* rp = &recordp->as_.record_;
		as_error err;

		as_status s = aerospike_key_put_async(&connectp_->as_, &err, nullptr, kp,
			rp, WriteBatch::WriteListener, reinterpret_cast<void*>(recordp), lp,
			pipep);
		if (pio_unlikely(s != AEROSPIKE_OK)) {
			LOG(ERROR) << "Key Put failed";
			submit_.nsent_ = 0;
			recordp->result_.status_ = s;
			return -EIO;
		}

		log_assert(not result_.promise_.isFulfilled());
		return result_.promise_.getFuture()
		.then([this] (int rc) mutable {
			if (pio_unlikely(rc < 0)) {
				return rc;
			}
			return WriteComplete();
		});
	}

	folly::Future<int> Submit() {
		return SubmitInternal()
		.then([this] (int rc) mutable -> folly::Future<int> {
			if (pio_unlikely(rc < 0)) {
				if (pio_likely(rc == -EAGAIN and ++result_.retry_count_ < 5)) {
					ReinitializeBatch();
					return Submit();
				}
			}
			return rc;
		});
	}

	void ReinitializeBatch() {
		log_assert(submit_.submited_ == true and
			submit_.nsent_ == submit_.ncomplete_);
		submit_.nsent_ = 0;
		submit_.ncomplete_ = 0;
		submit_.submited_ = false;

		log_assert(result_.promise_.isFulfilled() == true);
		result_.promise_ = folly::Promise<int>();
	}

public:
	AeroSpikeConnection* connectp_;
	const std::string& ns_;
	const std::string& set_;
	const std::string& key_prefix_;
	const size_t record_size_{};

	struct {
		mutable std::mutex mutex_;
		size_t nsent_{0};
		size_t ncomplete_{0};
		bool submited_{false};
	} submit_;

	struct {
		folly::Promise<int> promise_;
		int retry_count_{0};
	} result_;

	/* Following struct should always be last member */
	struct {
		const size_t nrecords_{0};
		struct WriteRecord records_[0];
	};
};

using WriteBatchPtr = std::unique_ptr<WriteBatch, void (*) (WriteBatch *)>;

void DestroyWriteBatch(WriteBatch* batchp) {
	if (pio_unlikely(not batchp)) {
		return;
	}
	batchp->~WriteBatch();
	std::free(batchp);
}

WriteBatchPtr NewWriteBatch(AeroSpikeConnection* cp, const std::string& ns,
		const std::string& set, const std::string& prefix,
		const size_t record_size, std::vector<int64_t>::iterator& it,
		std::vector<int64_t>::iterator& eit) {
	size_t records = std::distance(it, eit);
	size_t size = sizeof(WriteBatch) + records * sizeof(WriteRecord);
	void* datap = std::malloc(size);
	if (pio_unlikely(not datap)) {
		LOG(ERROR) << "memory allocation failed";
		return WriteBatchPtr(nullptr, DestroyWriteBatch);
	}
	WriteBatch* batchp = new (datap) WriteBatch(cp, ns, set, prefix,
		record_size, records);
	batchp->Initialize(it, eit);
	return WriteBatchPtr(batchp, DestroyWriteBatch);
}

struct ReadBatch;
struct ReadRecord {
	ReadBatch* batchp_{};
	const std::string& ns_;
	const std::string& set_;
	const std::string key_;
	RequestBufferPtr buffer_;

	struct {
		as_batch_read_record* recordp_{};
	} as_;

	struct {
		as_status status_{AEROSPIKE_OK};
	} result_;

	ReadRecord(ReadBatch* batchp, const std::string& ns,
			const std::string& set, std::string&& key,
			RequestBufferPtr buffer) : batchp_(batchp), ns_(ns), set_(set),
			key_(std::forward<std::string>(key)), buffer_(std::move(buffer)) {
	}

	~ReadRecord() {

	}

	int Initialize(as_batch_read_record* recordp) {
		recordp->read_all_bins = true;

		auto kp = as_key_init(&recordp->key, ns_.c_str(), set_.c_str(),
			key_.c_str());
		if (pio_unlikely(not kp)) {
			LOG(ERROR) << "memory allocation failed";
			return -ENOMEM;
		}
		log_assert(kp == &recordp->key);
		as_.recordp_ = recordp;
		return 0;
	}

	int CopyData() noexcept {
		as_bytes* bp = as_record_get_bytes(&as_.recordp_->record, kAsBin);
		if (pio_unlikely(not bp)) {
			LOG(ERROR) << "failed to get bin";
			return -ENOMEM;
		}

		auto rc = as_bytes_copy(bp, 0, (uint8_t *) buffer_->Payload(),
			(uint32_t) buffer_->PayloadSize());
		if (pio_unlikely(rc != buffer_->PayloadSize())) {
			LOG(ERROR) << "failed to copy data after read";
			return -ENOMEM;
		}
		return 0;
	}

	int ReadComplete() noexcept {
		switch (result_.status_) {
		default:
			LOG(ERROR) << "Read failed with error" << result_.status_;
			return -EIO;
		case AEROSPIKE_ERR_RECORD_NOT_FOUND:
			LOG(ERROR) << "Record not found";
			log_assert(0);
			return 0;
		case AEROSPIKE_OK:
			return CopyData();
		}
	}
};

struct ReadBatch {
public:
	~ReadBatch() {
		ReadRecord* recordp = records_;
		for (size_t i = 0; i < nrecords_; ++i, ++recordp) {
			recordp->~ReadRecord();
		}
		as_batch_read_destroy(as_.recordsp_);
	}

	ReadBatch(AeroSpikeConnection* cp, const std::string& ns,
			const std::string& set, const std::string& prefix,
			const size_t record_size, const size_t records) :
			connectp_(cp), ns_(ns), set_(set), key_prefix_(prefix),
			record_size_(record_size), nrecords_(records) {
	}

	int Initialize(std::vector<int64_t>::iterator& it,
			std::vector<int64_t>::iterator& eit) {
		log_assert(nrecords_ == (size_t) std::distance(it, eit));
		as_.recordsp_ = as_batch_read_create(nrecords_);
		if (pio_unlikely(not as_.recordsp_)) {
			LOG(ERROR) << "Creating Read Batch Failed";
			return -ENOMEM;
		}

		ReadRecord* recordp = records_;
		for (size_t i = 0; i < nrecords_; ++i, ++recordp, ++it) {
			log_assert(it != eit);
			std::string key;
			key.reserve(key_prefix_.size() + 10);
			ostring_buf buf(key);
			std::ostream os(&buf);
			os << key_prefix_ << ':' << *it << std::flush;

			auto buffer = NewRequestBuffer(record_size_);
			if (pio_unlikely(not buffer)) {
				LOG(ERROR) << "Memory allocation failed";
				return -ENOMEM;
			}
			std::memset(buffer->Payload(), 0, buffer->PayloadSize());

			auto as_recp = as_batch_read_reserve(as_.recordsp_);
			if (pio_unlikely(not as_recp)) {
				LOG(ERROR) << "Fatal Error: Read batch reserve failed";
				return -ENOMEM;
			}

			auto recp = new (recordp) ReadRecord(this, ns_, set_,
				std::move(key), std::move(buffer));
			log_assert(recp == recordp);
			auto rc = recp->Initialize(as_recp);
			if (pio_unlikely(rc < 0)) {
				return rc;
			}
		}
		return 0;
	}

	void ReadListener(as_error* errp) {
		if (pio_unlikely(errp)) {
			as_.result_ = errp->code;
		}
		return result_.promise_.setValue(0);
	}

	static void ReadListener(as_error* errp, as_batch_read_records* recordsp,
			void* datap, as_event_loop* lp) {
		ReadBatch* batchp = reinterpret_cast<ReadBatch*>(datap);
		batchp->ReadListener(errp);
	}

	int ReadComplete() {
		ReadRecord* recordp = records_;
		int res = 0;
		for (size_t i = 0; i < nrecords_; ++i, ++recordp) {
			auto rc = recordp->ReadComplete();
			if (pio_unlikely(rc < 0)) {
				res = rc;
			}
		}
		return res;
	}

	folly::Future<int> SubmitInternal() {
		auto lp = as_event_loop_get();
		log_assert(lp);

		as_error err;
		auto s = aerospike_batch_read_async(&connectp_->as_, &err, nullptr,
			as_.recordsp_, ReadBatch::ReadListener, this, lp);
		if (pio_unlikely(s != AEROSPIKE_OK)) {
			LOG(ERROR) << "Submiting AS Batch Read failed";
			return -EIO;
		}
		return result_.promise_.getFuture()
		.then([this] (int rc) mutable -> folly::Future<int> {
			if (pio_unlikely(rc < 0)) {
				return rc;
			}
			return ReadComplete();
		});
	}

	folly::Future<int> Submit() {
		auto ReinitializeBatch = [this] () mutable {
			result_.promise_ = folly::Promise<int>();
		};

		return SubmitInternal()
		.then([this, &ReinitializeBatch] (int rc) mutable -> folly::Future<int> {
			if (pio_unlikely(rc < 0)) {
				if (pio_likely(rc == -EAGAIN and ++result_.retry_count_ < 5)) {
					LOG(ERROR) << "Read failed. Retrying";
					ReinitializeBatch();
					return Submit();
				}
			}
			return rc;
		});
	}
public:
	AeroSpikeConnection* connectp_;
	const std::string& ns_;
	const std::string& set_;
	const std::string& key_prefix_;
	const size_t record_size_{};

	struct {
		as_batch_read_records* recordsp_{};
		as_status result_;
	} as_;

	struct {
		folly::Promise<int> promise_;
		int retry_count_{};
	} result_;

	/* Following struct should always be last member */
	struct {
		const size_t nrecords_{};
		struct ReadRecord records_[0];
	};
};

using ReadBatchPtr = std::unique_ptr<ReadBatch, void (*) (ReadBatch*)>;

void DestroyReadBatch(ReadBatch* batchp) {
	if (pio_unlikely(not batchp)) {
		return;
	}
	batchp->~ReadBatch();
	std::free(batchp);
}

ReadBatchPtr NewReadBatch(AeroSpikeConnection* cp, const std::string& ns,
		const std::string& set, const std::string& prefix,
		const size_t record_size, std::vector<int64_t>::iterator& it,
		std::vector<int64_t>::iterator& eit) {
	const size_t records = std::distance(it, eit);
	size_t size = sizeof(ReadBatch) + records * sizeof(ReadRecord);
	void* datap = std::malloc(size);
	if (pio_unlikely(not datap)) {
		LOG(ERROR) << "memory allocation failed";
		return ReadBatchPtr(nullptr, DestroyReadBatch);
	}
	ReadBatch* batchp = new (datap) ReadBatch(cp, ns, set, prefix, record_size,
		records);
	batchp->Initialize(it, eit);
	return ReadBatchPtr(batchp, DestroyReadBatch);
}

class AeroSpike {
public:
	AeroSpike(AeroSpikeConnection* connectp) : connectp_(connectp) {
	}

	folly::Future<int> Read(const std::string& ns, const std::string& set,
			const std::string& key_prefix, size_t block_size,
			std::vector<int64_t>::iterator it,
			std::vector<int64_t>::iterator eit) {
		auto batch = NewReadBatch(connectp_, ns, set, key_prefix, block_size,
			it, eit);
		auto batchp = batch.get();
		return batchp->Submit()
		.then([batch = std::move(batch)] (int rc) {
			return rc;
		});
	}

	folly::Future<int> Write(const std::string& ns, const std::string& set,
			const std::string& key_prefix, size_t block_size,
			std::vector<int64_t>::iterator it,
			std::vector<int64_t>::iterator eit) {
		auto batch = NewWriteBatch(connectp_, ns, set, key_prefix, block_size,
			it, eit);
		auto batchp = batch.get();
		return batchp->Submit()
		.then([batch = std::move(batch)] (int rc) mutable {
			if (pio_unlikely(rc < 0)) {
				return rc;
			}
			return rc;
		});
	}
private:
	AeroSpikeConnection* connectp_{};
};

const static std::string kNameSpace = "DIRTY";
const static std::string kSetName = "VMDK";
const static std::string kKeyPrefix = "VMDK";

class Benchmark {
public:
	Benchmark(AeroSpike* asp, int64_t nobjects, int64_t run_time,
			int32_t write_percent, int32_t block_size, int32_t iodepth,
			int32_t batch_size, bool random) : asp_(asp), nobjects_(nobjects),
			write_percent_(write_percent), iodepth_(iodepth),
			bs_(block_size), nblocks_(batch_size), random_(random),
			key_gen_(0, nobjects, random_) {
		run_time_.seconds_ = run_time;
	}

	bool ShouldScheduleWrite() {
		bool write{true};
		if (stats_.total_ios_ == 0) {
			if (write_percent_ >= 50) {
				write = true;
			} else {
				write = false;
			}
		} else {
			int32_t wp = 100 * stats_.total_writes_.load() / stats_.total_ios_.load();
			write = wp <= write_percent_;
		}
		return write;
	}

	folly::Future<int> SubmitWrite() {
		++stats_.total_writes_;
		++stats_.total_ios_;
		++stats_.pending_ios_;
		std::vector<int64_t> blocks;
		blocks.reserve(nblocks_);
		for (auto i = 0; i < nblocks_; ++i) {
			blocks.emplace_back(key_gen_.Next());
		}
		return asp_->Write(kNameSpace, kSetName, kKeyPrefix, bs_,
			blocks.begin(), blocks.end())
		.then([this, blocks = std::move(blocks)] (int rc) mutable {
			--stats_.pending_ios_;
			return rc;
		});
	}

	folly::Future<int> SubmitRead() {
		++stats_.total_reads_;
		++stats_.total_ios_;
		++stats_.pending_ios_;
		std::vector<int64_t> blocks;
		blocks.reserve(nblocks_);
		for (auto i = 0; i < nblocks_; ++i) {
			blocks.emplace_back(key_gen_.Next());
		}
		return asp_->Read(kNameSpace, kSetName, kKeyPrefix, bs_, blocks.begin(),
			blocks.end())
		.then([this, blocks = std::move(blocks)] (int rc)  mutable {
			--stats_.pending_ios_;
			return rc;
		});
	}

	folly::Future<int> SubmitIO() {
		if (pio_unlikely(stop_)) {
			return 0;
		}
		if (ShouldScheduleWrite()) {
			return SubmitWrite();
		}
		return SubmitRead();
	}

	folly::Future<int> ScheduleIO() {
		return SubmitIO()
		.then([this] (int rc) mutable {
			if (pio_unlikely(rc)) {
				failed_ = true;
				stop_= true;
				return 0;
			}
			if (pio_unlikely(stop_)) {
				if (stats_.pending_ios_ == 0) {
					SignalBenchmarkFinished();
				}
				return 0;
			}

			ScheduleIO();
			return 0;
		});
	}

	void Start() {
		start_time_.Start();
		for (int i = 0; i < iodepth_; ++i) {
			ScheduleIO();
		}
	}

	void SignalBenchmarkFinished() {
		std::unique_lock<std::mutex> lock(cond_.mutex_);
		cond_.time_.notify_all();
		cond_.pending_ios_.notify_all();
	}

	void Wait() {
		std::unique_lock<std::mutex> lock(cond_.mutex_);
		cond_.time_.wait_for(lock, run_time_.seconds_ * 1s);
		if (pio_likely(not stop_)) {
			stop_ = true;
			cond_.pending_ios_.wait(lock);
		}
	}

	void Layout() {
		const int kStep = 32;
		KeyGen gen(0, nobjects_, false);
		std::vector<int64_t> blocks(kStep, 0);
		for (int64_t start = 0, end = nobjects_; start <= end; start += blocks.size()) {
			std::iota(blocks.begin(), blocks.end(), start);
			auto f = asp_->Write(kNameSpace, kSetName, kKeyPrefix, bs_,
				blocks.begin(), blocks.end())
			.wait();
			log_assert(f.value() == 0);
		}
	}

	friend std::ostream& operator << (std::ostream& os, const Benchmark& b) {
		os << "RunTime:" << b.start_time_.GetMicroSec() << ','
			<< "#Objects:" << b.nobjects_ << ','
			<< "Random:" << b.random_ << ','
			<< "WritePercent:" << b.write_percent_ << ','
			<< "IODepth:" << b.iodepth_ << ','
			<< "BS:" << b.bs_ << ','
			<< "BatchSize:" << b.nblocks_ << ','
			<< "TotalIOs:" << b.stats_.total_ios_ << ','
			<< "TotalReads:" << b.stats_.total_reads_ << ','
			<< "TotalWrites:" << b.stats_.total_writes_;
		return os;
	}
private:
	AeroSpike* asp_{};
	const int64_t nobjects_{};
	struct {
		int64_t seconds_;
	} run_time_;
	const int32_t write_percent_{};
	const int32_t iodepth_{};
	const int32_t bs_{};
	const int32_t nblocks_{};
	const bool random_{};

	KeyGen key_gen_;
	hyc::TimePoint start_time_;

	struct {
		std::mutex mutex_;
		std::condition_variable time_;
		std::condition_variable pending_ios_;
	} cond_;

	struct {
		bool stop_{false};
		bool failed_{false};
	};

	struct {
		std::atomic<uint64_t> total_ios_{0};
		std::atomic<uint64_t> total_writes_{0};
		std::atomic<uint64_t> total_reads_{0};
		std::atomic<uint64_t> pending_ios_{0};
	} stats_;
};

DEFINE_int32(block_size, 4096, "Block Size");
DEFINE_int32(iodepth, 32, "IO Depth");
DEFINE_int32(batch_size, 8, "Batch size");
DEFINE_int64(objects, 1024 * 1024, "Number of objects");
DEFINE_bool(random, true, "Random workload");
DEFINE_int32(write_mix_percent, 100, "Percentage writes");
DEFINE_int64(runtime, 300, "Runtime in seconds");
DEFINE_string(ips, "127.0.0.1", "Comma Separated AeroSpike Cluster IPs");
DEFINE_int64(port, 8001, "AeroSpike server port");

static void StartBenchmark(Benchmark& benchmark) {
	benchmark.Layout();

	benchmark.Start();
	benchmark.Wait();
	LOG(ERROR) << benchmark;
	std::cout << benchmark << std::endl;
	return;
}

int main(int argc, char* argv[]) {
	folly::init(&argc, &argv, true);

	if (FLAGS_runtime < 60) {
		LOG(ERROR) << "Too small runtime";
		return -EINVAL;
	}

	AeroSpikeConnection connect(FLAGS_ips, FLAGS_port);
	connect.Connect();
	AeroSpike asp(&connect);

	Benchmark benchmark(&asp, FLAGS_objects, FLAGS_runtime,
		FLAGS_write_mix_percent, FLAGS_block_size, FLAGS_iodepth,
		FLAGS_batch_size, FLAGS_random);

	std::thread thread([&benchmark] () {
		LOG(INFO) << "Starting benchmark";
		StartBenchmark(benchmark);
	});
	thread.join();
	return 0;
}
