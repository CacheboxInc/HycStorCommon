#include <cstdint>
#include <cstddef>
#include <chrono>
#include <thread>
#include <memory>
#include <unistd.h>
#include <csignal>

#include <glog/logging.h>

#include <folly/init/Init.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

#include "gen-cpp2/StorRpc.h"
#include "DaemonTgtTypes.h"
#include "DaemonTgtInterface.h"
#include "Request.h"
#include "Vmdk.h"
#include "HycRestServer.h"

using namespace apache::thrift;
using namespace apache::thrift::async;
using namespace ::hyc_thrift;
using namespace pio;

static constexpr int32_t kServerPort = 9876;
static const std::string kServerIp = "127.0.0.1";

class StorRpcSvImpl : public virtual StorRpcSvIf {
public:
	void async_tm_Ping(
			std::unique_ptr<HandlerCallback<std::unique_ptr<std::string>>> cb)
			override {
		std::string pong("pong");
		cb->result(std::move(pong));
	}

	// stub
	void async_tm_PushVmdkStats(std::unique_ptr<HandlerCallbackBase> cb,
			VmdkHandle vmdk, std::unique_ptr<VmdkStats> stats) override {
		LOG(WARNING) << "read_requests " << stats->read_requests
			<< " read_bytes " << stats->read_bytes
			<< " read_latency " << stats->read_latency;
	}

	// stub
	void async_tm_OpenVm(std::unique_ptr<HandlerCallback<VmHandle>> cb,
			std::unique_ptr<std::string> vmid) override {
		cb->result(GetVmHandle(vmid.get()->c_str()));
	}

	// stub
	void async_tm_CloseVm(std::unique_ptr<HandlerCallback<void>> cb,
			VmHandle vm) override {
		cb->done();
	}

	// stub
	void async_tm_OpenVmdk(std::unique_ptr<HandlerCallback<VmdkHandle>> cb,
			std::unique_ptr<std::string> vmid,
			std::unique_ptr<std::string> vmdkid) override {
		cb->result(GetVmdkHandle(vmdkid.get()->c_str()));
	}

	// stub
	void async_tm_CloseVmdk(std::unique_ptr<HandlerCallback<int32_t>> cb,
			VmdkHandle vmdk) override {
		cb->result(0);
	}

	void async_tm_Read(
			std::unique_ptr<HandlerCallback<std::unique_ptr<ReadResult>>> cb,
			VmdkHandle vmdk, RequestId reqid, int32_t size, int64_t offset)
			override {
		auto vmdkp = VmdkFromVmdkHandle(vmdk);
		assert(vmdkp != nullptr);

		auto vmp = vmdkp->GetVM();
		assert(vmp != nullptr);

		std::string data;
		data.resize(size);
		auto reqp = std::make_unique<Request>(reqid, vmdkp, Request::Type::kRead,
			data.data(), size, size, offset);

		vmp->Read(vmdkp, reqp.get())
		.then([data = std::move(data), cb = std::move(cb),
				reqp = std::move(reqp)] (int rc) mutable {
			auto read = std::make_unique<ReadResult>();
			read->set_reqid(reqp->GetID());
			read->set_data(std::move(data));
			read->set_result(rc);
			cb->result(std::move(read));
		});
	}

	void async_tm_Write(
			std::unique_ptr<HandlerCallback<std::unique_ptr<WriteResult>>> cb,
			VmdkHandle vmdk, RequestId reqid, std::unique_ptr<std::string> data,
			int32_t size, int64_t offset) override {
		auto vmdkp = VmdkFromVmdkHandle(vmdk);
		assert(vmdkp != nullptr);

		auto vmp = vmdkp->GetVM();
		assert(vmp != nullptr);

		auto reqp = std::make_unique<Request>(reqid, vmdkp, Request::Type::kWrite,
			data->data(), size, size, offset);

		vmp->Write(vmdkp, reqp.get())
		.then([data = std::move(data), cb = std::move(cb),
				reqp = std::move(reqp)] (int rc) mutable {
			auto write = std::make_unique<WriteResult>();
			write->set_reqid(reqp->GetID());
			write->set_result(rc);
			cb->result(std::move(write));
		});
	}

	void async_tm_WriteSame(
			std::unique_ptr<HandlerCallback<std::unique_ptr<WriteResult>>> cb,
			VmdkHandle vmdk, RequestId reqid, std::unique_ptr<std::string> data,
			int32_t data_size, int32_t write_size, int64_t offset) override {
		auto write = std::make_unique<WriteResult>();
		write->set_reqid(reqid);
		write->set_result(0);
		cb->result(std::move(write));
	}

private:
	std::atomic<VmHandle> vm_handle_{0};
	std::atomic<VmdkHandle> vmdk_handle_{0};
};

std::shared_ptr<ThriftServer> thirft_server;

static void Usr1SignalHandler(int signal) {
	thirft_server->stopListening();
	thirft_server->stop();
}

int main(int argc, char* argv[])
{
	FLAGS_v = 2;
	folly::init(&argc, &argv, true);

#if 0
	auto pid = ::fork();
	if (pid < 0) {
		LOG(ERROR) << "Fork failed" << pid;
		exit(EXIT_FAILURE);
	}

	if (pid > 0) {
		LOG(INFO) << "Parent process exiting.";
		exit(EXIT_SUCCESS);
	}

	// Create a new SID for child process
	auto sid = ::setsid();
	if (sid < 0) {
		LOG(ERROR) << "Setsid for child process failed" << sid;
		exit(EXIT_FAILURE);
	}

	// Change current working dir to root
	if ((chdir("/")) < 0) {
		LOG(ERROR) << "Changing pwd to / failed";
		exit(EXIT_FAILURE);
	}

	// Close standard file descriptors
	::close(STDIN_FILENO);
	::close(STDOUT_FILENO);
	::close(STDERR_FILENO);

	LOG(INFO) << "stord daemon started successfully";
#else
	std::signal(SIGUSR1, Usr1SignalHandler);

#endif

	auto ret = HycRestServerStart();
	if (ret) {
		// TODO deinitializeLib
		LOG(ERROR) << "Rest server start failed " << ret;
		return ret;
	}

	LOG(INFO) << "Rest server started.";

	auto si = std::make_shared<StorRpcSvImpl>();
	thirft_server = std::make_shared<ThriftServer>();

	thirft_server->setInterface(si);
	thirft_server->setAddress(kServerIp, kServerPort);
	LOG(INFO) << "Starting Thrift Server";
	google::FlushLogFiles(google::INFO);
	google::FlushLogFiles(google::ERROR);

	thirft_server->serve();

	HycRestServerStop();

	return ret;
}