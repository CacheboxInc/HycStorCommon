#include <memory>
#include <string>
#include <atomic>
#include <gtest/gtest.h>

#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

#include "gen-cpp2/StorRpc.h"
#include "TgtInterface.h"

using namespace apache::thrift;
using namespace apache::thrift::async;
using namespace ::hyc_thrift;

static constexpr int32_t kServerPort = 9876;
static const std::string kServerIp = "127.0.0.1";

extern RpcConnectHandle HycStorRpcServerConnectTest(uint32_t ping_secs);

class StorRpcSimpleImpl : public virtual StorRpcSvIf {
public:
	void async_tm_Ping(
			std::unique_ptr<HandlerCallback<std::unique_ptr<std::string>>> cb)
			override {
		VLOG(1) << "Received ping from client";
		std::string pong("pong");
		cb->result(std::move(pong));
		++nping_;
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

	void async_tm_OpenVmdk(std::unique_ptr<HandlerCallback<VmdkHandle>> cb,
			std::unique_ptr<std::string> vmid,
			std::unique_ptr<std::string> vmdkid) override {
		cb->result(++vmdk_handle_);
		++nopen_vmdk_;
	}

	void async_tm_CloseVmdk(std::unique_ptr<HandlerCallback<int32_t>> cb,
			VmdkHandle vmdk) override {
		cb->result(0);
		++nclose_vmdk_;
	}

	void async_tm_Read(
			std::unique_ptr<HandlerCallback<std::unique_ptr<ReadResult>>> cb,
			VmdkHandle vmdk, RequestId reqid, int32_t size, int64_t offset)
			override {
		std::string data('A', size);
		auto read = std::make_unique<ReadResult>();
		read->set_data(std::move(data));
		read->set_reqid(reqid);
		read->set_result(0);
		cb->result(std::move(read));
		++nread_;
	}

	void async_tm_Write(
			std::unique_ptr<HandlerCallback<std::unique_ptr<WriteResult>>> cb,
			VmdkHandle vmdk, RequestId reqid, std::unique_ptr<std::string> data,
			int32_t size, int64_t offset) override {
		auto write = std::make_unique<WriteResult>();
		write->set_reqid(reqid);
		write->set_result(0);
		cb->result(std::move(write));
		++nwrite_;
	}

	void async_tm_WriteSame(
			std::unique_ptr<HandlerCallback<std::unique_ptr<WriteResult>>> cb,
			VmdkHandle vmdk, RequestId reqid, std::unique_ptr<std::string> data,
			int32_t data_size, int32_t write_size, int64_t offset) override {
		auto write = std::make_unique<WriteResult>();
		write->set_reqid(reqid);
		write->set_result(0);
		cb->result(std::move(write));
		++nwrite_same_;
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
private:
	std::atomic<VmHandle> vm_handle_{0};
	std::atomic<VmdkHandle> vmdk_handle_{0};
};

static std::shared_ptr<ScopedServerInterfaceThread> StartServer() {
	auto si = std::make_shared<StorRpcSimpleImpl>();
	auto ts = std::make_shared<ThriftServer>();
	ts->setInterface(si);
	ts->setAddress(kServerIp, kServerPort);
	ts->setNumIOWorkerThreads(1);
	return std::make_shared<ScopedServerInterfaceThread>(ts);
}

TEST(TgtInterfaceImplTest, NoServerConnectFails) {
	/* Connect fails without server */
	auto rpc = HycStorRpcServerConnect();
	EXPECT_EQ(rpc, kInvalidRpcHandle);
}

TEST(TgtInterfaceImplTest, ConnectDisconnect) {
	auto server = StartServer();
	std::vector<std::thread> threads;
	for (auto i = 0; i < 5; ++i) {
		threads.emplace_back(std::thread([] () mutable {
			for (auto i = 0; i < 10; ++i) {
				VLOG(1) << "Connecting " << std::endl;
				auto rpc = HycStorRpcServerConnect();
				EXPECT_NE(rpc, kInvalidRpcHandle);

				VLOG(1) << "Recived rpc " << rpc
					<< " disconnecting" << std::endl;

				auto rc = HycStorRpcServerDisconnect(rpc);
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

	auto rpc = HycStorRpcServerConnectTest(1);
	::sleep(kSleep);
	EXPECT_GT(si->nping_, kSleep);

	auto rc = HycStorRpcServerDisconnect(rpc);
	EXPECT_EQ(rc, 0);
}