#include <memory>
#include <string>
#include <atomic>
#include <gtest/gtest.h>

#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

#include "gen-cpp2/StorRpc.h"
#include "TgtInterface.h"
#include "SharedMemory.h"

using namespace apache::thrift;
using namespace apache::thrift::async;
using namespace ::hyc_thrift;

static constexpr int32_t kServerPort = 9876;
static const std::string kServerIp = "127.0.0.1";
static const std::string kVmdkId = "vmdkid";

extern std::string StordIp;
extern uint16_t StordPort;

extern int32_t HycStorRpcServerConnectTest(uint32_t ping_secs);

class StorRpcSimpleImpl : public virtual StorRpcSvIf {
public:
	void async_tm_Ping(
			std::unique_ptr<HandlerCallback<std::unique_ptr<std::string>>> cb)
			override {
		std::string pong("pong");
		cb->result(std::move(pong));
		++nping_;
	}

	void async_tm_PushVmdkStats(
				std::unique_ptr<HandlerCallbackBase> cb,
				int32_t fd, std::unique_ptr<VmdkStats> stats
			) override {
		++nstats_;
	}

	void async_tm_OpenVm(std::unique_ptr<HandlerCallback<VmHandle>> cb,
			std::unique_ptr<std::string> vmid) override {
		cb->result(++vm_handle_);
		++nopen_vm_;
	}

	void async_tm_CloseVm(std::unique_ptr<HandlerCallback<void>> cb,
			VmHandle vm) override {
		cb->done();
		++nclose_vm_;
	}

	void async_tm_OpenVmdk(
			std::unique_ptr<HandlerCallback<std::unique_ptr<::hyc_thrift::OpenResult>>> cb,
			std::unique_ptr<std::string> vmid,
			std::unique_ptr<std::string> vmdkid, bool is_local,
			int64_t max_io_size) override {
		EXPECT_TRUE(is_local);
		EXPECT_EQ(nopen_vmdk_, 0);
		EXPECT_EQ(shm_.Create(*vmdkid, max_io_size), 0);
		OpenResult result;
		result.set_handle(++vmdk_handle_);
		result.set_shm_id(*vmdkid);
		result.set_fd(0);
		cb->result(std::move(result));
		++nopen_vmdk_;
	}

	void async_tm_CloseVmdk(
				std::unique_ptr<HandlerCallback<int32_t>> cb,
				int32_t fd
			) override {
		EXPECT_EQ(nclose_vmdk_, 0);
		EXPECT_TRUE(shm_.Destroy());
		cb->result(0);
		++nclose_vmdk_;
	}

	void async_tm_Read(
				std::unique_ptr<HandlerCallback<std::unique_ptr<ReadResult>>> cb,
				int32_t fd,
				ShmHandle shm,
				RequestID reqid,
				int32_t size,
				int64_t offset
			) override {
		EXPECT_NE(shm, 0);
		void* addrp = shm_.HandleToAddress(shm);
		::memset(addrp, 'A', size);
		auto read = std::make_unique<ReadResult>();
		read->set_reqid(reqid);
		read->set_result(0);
		cb->result(std::move(read));
		++nread_;
	}

	void async_tm_Write(
				std::unique_ptr<HandlerCallback<std::unique_ptr< WriteResult>>> cb,
				int32_t fd,
				::hyc_thrift::ShmHandle shm,
				::hyc_thrift::RequestID reqid,
				std::unique_ptr<IOBufPtr> data,
				int32_t size,
				int64_t offset) override {
		EXPECT_NE(shm, 0);
		EXPECT_FALSE(data);
		auto write = std::make_unique<WriteResult>();
		write->set_reqid(reqid);
		write->set_result(0);
		cb->result(std::move(write));
		++nwrite_;
	}

	void async_tm_WriteSame(
				std::unique_ptr<HandlerCallback<std::unique_ptr<WriteResult>>> cb,
				int32_t fd,
				::hyc_thrift::ShmHandle shm,
				::hyc_thrift::RequestID reqid,
				std::unique_ptr<::hyc_thrift::IOBufPtr> data,
				int32_t data_size,
				int32_t write_size,
				int64_t offset) override {
		EXPECT_NE(shm, 0);
		EXPECT_FALSE(data);
		auto write = std::make_unique<WriteResult>();
		write->set_reqid(reqid);
		write->set_result(0);
		cb->result(std::move(write));
		++nwrite_same_;
	}

	folly::Future<std::unique_ptr<std::vector<ReadResult>>> future_BulkRead(
				int fd,
				std::unique_ptr<std::vector<ReadRequest>> requests
			) override {
		auto results = std::make_unique<std::vector<ReadResult>>();
		for (const auto& req : *requests) {
			auto size = req.get_size();
			auto iobuf = folly::IOBuf::create(size);
			::memset(iobuf->writableTail(), 'A', size);
			iobuf->append(size);
			results->emplace_back(apache::thrift::FragileConstructor(),
				req.reqid, 0, std::move(iobuf));
			++nread_;
		}
		return std::move(results);
	}

	folly::Future<std::unique_ptr<std::vector<WriteResult>>> future_BulkWrite(
				int32_t fd,
				std::unique_ptr<std::vector<WriteRequest>> requests
			) override {
		auto results = std::make_unique<std::vector<WriteResult>>();
		for (const auto& req : *requests) {
			results->emplace_back(apache::thrift::FragileConstructor(),
				req.reqid, 0);
			++nwrite_;
		}
		return std::move(results);
	}

public:
	std::atomic<uint32_t> nping_{0};
	std::atomic<uint32_t> nopen_vm_{0};
	std::atomic<uint32_t> nclose_vm_{0};
	std::atomic<uint32_t> nopen_vmdk_{0};
	std::atomic<uint32_t> nclose_vmdk_{0};
	std::atomic<uint32_t> nread_{0};
	std::atomic<uint32_t> nwrite_{0};
	std::atomic<uint32_t> nwrite_same_{0};
	std::atomic<uint32_t> nstats_{0};
private:
	std::atomic<VmHandle> vm_handle_{0};
	std::atomic<::hyc_thrift::VmdkHandle> vmdk_handle_{0};
	hyc::SharedMemory shm_;
};

static std::shared_ptr<ScopedServerInterfaceThread> StartServer() {
	boost::interprocess::shared_memory_object::remove(kVmdkId.c_str());
	auto si = std::make_shared<StorRpcSimpleImpl>();
	auto ts = std::make_shared<ThriftServer>();
	ts->setInterface(si);
	ts->setAddress(kServerIp, kServerPort);
	ts->setNumIOWorkerThreads(1);
	return std::make_shared<ScopedServerInterfaceThread>(ts);
}

TEST(TgtInterfaceImplTest, NoServerConnectFails) {
	/* Connect fails without server */
	auto rc = HycStorRpcServerConnect();
	EXPECT_NE(rc, 0);
}

TEST(TgtInterfaceImplTest, ConnectDisconnect) {
	auto server = StartServer();
	std::vector<std::thread> threads;
	for (auto i = 0; i < 10; ++i) {
		threads.emplace_back(std::thread([] () mutable {
			for (auto i = 0; i < 10; ++i) {
				VLOG(1) << "Connecting " << std::endl;
				auto rc = HycStorRpcServerConnect();
				EXPECT_EQ(rc, 0);

				::sleep(1);
				VLOG(1) << " disconnecting" << std::endl;

				rc = HycStorRpcServerDisconnect();
				EXPECT_EQ(rc, 0);

				VLOG(1) << "Disconnected " << std::endl;
			}
		}));
	}

	for (auto& thread : threads) {
		thread.join();
	}
	threads.clear();
}

TEST(TgtInterfaceImplTest, Ping) {
	auto kSleep = 5;

	auto si = std::make_shared<StorRpcSimpleImpl>();
	auto ts = std::make_shared<ThriftServer>();
	ts->setInterface(si);
	ts->setAddress(kServerIp, kServerPort);
	ts->setNumIOWorkerThreads(1);
	auto server = std::make_shared<ScopedServerInterfaceThread>(ts);

	auto rc = HycStorRpcServerConnectTest(1);
	EXPECT_EQ(rc, 0);
	::sleep(kSleep);
	EXPECT_GE(si->nping_, kSleep);

	rc = HycStorRpcServerDisconnect();
	EXPECT_EQ(rc, 0);
}

TEST(TgtInterfaceImplTest, Read) {
	const int32_t kReadsPerThread = 4096;
	const int32_t kThreads = 10;
	std::string buf(4096, 'A');

	auto server = StartServer();

	auto rc = HycStorRpcServerConnect();
	EXPECT_EQ(rc, rc);

	::VmdkHandle handle = nullptr;
	rc = HycOpenVmdk("vmid", kVmdkId.c_str(), -1, 1ull<<30, 12, &handle);
	EXPECT_EQ(rc, 0);
	EXPECT_NE(handle, nullptr);

	std::mutex mutex;
	std::set<RequestID> scheduled;

	auto Schedule = [&] () {
		std::set<RequestID> ids;
		for (auto i = 0; i < kReadsPerThread; i++) {
			auto id = HycScheduleRead(handle, (void*) (0x1) , buf.data(), buf.size(), 0);
			EXPECT_NE(id, kInvalidRequestID);
			ids.insert(id);
		}
		EXPECT_EQ(ids.size(), kReadsPerThread);

		std::lock_guard<std::mutex> lock(mutex);
		auto s1 = scheduled.size();
		scheduled.insert(ids.begin(), ids.end());
		EXPECT_EQ(s1 + ids.size(), scheduled.size());
	};

	{
		std::vector<std::thread> threads;
		for (auto t = 0; t < kThreads; ++t) {
			threads.emplace_back(std::thread(Schedule));
		}

		for (auto& thread: threads) {
			thread.join();
		}
	}

	EXPECT_EQ(scheduled.size(), kReadsPerThread * kThreads);

	while (not scheduled.empty()) {
		RequestResult completions;
		bool more;

		auto c = HycGetCompleteRequests(handle, &completions, 1, &more);
		if (c == 1) {
			auto it = scheduled.find(completions.request_id);
			EXPECT_NE(it, scheduled.end());
			scheduled.erase(it);
			EXPECT_EQ(completions.result, 0);
		} else {
			EXPECT_FALSE(more);
		}
	}

	HycCloseVmdk(handle);
	HycStorRpcServerDisconnect();
}

TEST(TgtInterfaceImplTest, PingFailure) {
	class StorRpcError : public virtual StorRpcSvIf {
	public:
		void async_tm_Ping(
				std::unique_ptr<HandlerCallback<std::unique_ptr<std::string>>> cb)
				override {
			ServiceException e;
			e.message = "No memory";
			e.error_number = ENOMEM;
			cb->exception(e);
		}
	};

	auto si = std::make_shared<StorRpcError>();
	auto ts = std::make_shared<ThriftServer>();
	ts->setInterface(si);
	ts->setAddress(kServerIp, kServerPort);
	ts->setNumIOWorkerThreads(1);
	auto server = std::make_shared<ScopedServerInterfaceThread>(ts);

	auto rc = HycStorRpcServerConnect();
	(void) rc;
}
