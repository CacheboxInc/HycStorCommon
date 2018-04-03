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
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "halib.h"
#include "VmdkFactory.h"
#include "Singleton.h"

using namespace apache::thrift;
using namespace apache::thrift::async;
using namespace ::hyc_thrift;
using namespace pio;

static constexpr int32_t kServerPort = 9876;
static const std::string kServerIp = "127.0.0.1";
static const std::string kNewVm = "new_vm";
static const std::string kNewVmdk = "new_vmdk";

class StorRpcSvImpl : public virtual StorRpcSvIf {
public:
	void async_tm_Ping(
			std::unique_ptr<HandlerCallback<std::unique_ptr<std::string>>> cb)
			override {
		std::string pong("pong");
		cb->result(std::move(pong));
	}

	void async_tm_PushVmdkStats(std::unique_ptr<HandlerCallbackBase> cb,
			VmdkHandle vmdk, std::unique_ptr<VmdkStats> stats) override {
		LOG(WARNING) << "read_requests " << stats->read_requests
			<< " read_bytes " << stats->read_bytes
			<< " read_latency " << stats->read_latency;
	}

	void async_tm_OpenVm(std::unique_ptr<HandlerCallback<VmHandle>> cb,
			std::unique_ptr<std::string> vmid) override {
		cb->result(GetVmHandle(*vmid.get()));
	}

	void async_tm_CloseVm(std::unique_ptr<HandlerCallback<void>> cb,
			VmHandle vm) override {
		cb->done();
	}

	void async_tm_OpenVmdk(std::unique_ptr<HandlerCallback<VmdkHandle>> cb,
			std::unique_ptr<std::string> vmid,
			std::unique_ptr<std::string> vmdkid) override {
		cb->result(GetVmdkHandle(*vmdkid.get()));
	}

	void async_tm_CloseVmdk(std::unique_ptr<HandlerCallback<int32_t>> cb,
			VmdkHandle vmdk) override {
		cb->result(0);
	}

	void async_tm_Read(
			std::unique_ptr<HandlerCallback<std::unique_ptr<ReadResult>>> cb,
			VmdkHandle vmdk, RequestId reqid, int32_t size, int64_t offset)
			override {
		auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdk);
		assert(pio_likely(p));
		auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
		assert(pio_likely(vmdkp));

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
		auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdk);
		assert(pio_likely(p));
		auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
		assert(pio_likely(vmdkp));

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

DEFINE_string(etcd_ip, "", "etcd_ip supplied by HA");
DEFINE_string(svc_label, "", "represents service label for the service");
DEFINE_string(stord_version, "", "protocol version of tgt");

static std::string exec(const std::string& cmd, int& status)
{
	std::array<char, 128> buffer;
	std::string result;

	LOG(INFO) << "Executing cmd: " << cmd;

	std::shared_ptr<FILE> filp(::popen(cmd.c_str(), "r"), [&] (FILE *filp) mutable {
		int ret = pclose(filp);
		status = WEXITSTATUS(ret);
	});

	if (!filp) {
		throw std::runtime_error("::open failed");
	}

	while (!feof(filp.get())) {
		if (fgets(buffer.data(), buffer.size(), filp.get()) != nullptr)
			result += buffer.data();
	}
	return result;
}

static struct {
	std::once_flag initialized_;
	std::unique_ptr<std::thread> thread_;
	std::condition_variable ha_hb_stop_cv_;
	bool stop_{false};
	std::mutex mutex_; // protects above cond var
	struct _ha_instance *ha_instance_;
} g_thread_;

enum StordSvcErr {
	STORD_ERR_TARGET_CREATE = 1,
	STORD_ERR_LUN_CREATE,
	STORD_ERR_INVALID_VM,
	STORD_ERR_INVALID_VMDK,
	STORD_ERR_INVALID_NO_DATA,
	STORD_ERR_INVALID_PARAM,
};

void HaHeartbeat(void *userp) {
	struct _ha_instance *ha = (struct _ha_instance *) userp;

	while(1) {
		std::unique_lock<std::mutex> lck(g_thread_.mutex_);
		if (g_thread_.ha_hb_stop_cv_.wait_for(lck, std::chrono::seconds(60),
			 [] { return g_thread_.stop_; })) {
			LOG(INFO) << " Stop HA heartbeat thread";
			break;
		} else {
			LOG(INFO) << "After 60 seconds wait, update health status to ha";
			ha_healthupdate(ha);
		}
	}
	LOG(INFO) << "HB thread exiting";
}

int StordHaStartCallback(const _ha_request *reqp, _ha_response *resp,
		void *userp) {
	try {
		log_assert(userp != nullptr);

		std::call_once(g_thread_.initialized_, [=] () mutable {
			g_thread_.thread_ =
			std::make_unique<std::thread>(HaHeartbeat, userp);
			});
		return HA_CALLBACK_CONTINUE;
	} catch (const std::exception& e) {
		return HA_CALLBACK_ERROR;
	}
}

int StordHaStopCallback(const _ha_request *reqp, _ha_response *resp,
		void *userp) {
	{
		std::lock_guard<std::mutex> lk(g_thread_.mutex_);
		g_thread_.stop_ = true;
		g_thread_.ha_hb_stop_cv_.notify_one();
	}
	g_thread_.thread_->join();
	return HA_CALLBACK_CONTINUE;
}

static void SetErrMsg(_ha_response *resp, StordSvcErr err,
		const std::string& msg) {
	auto err_msg = ha_get_error_message(g_thread_.ha_instance_,
		err, msg.c_str());
	ha_set_response_body(resp, HTTP_STATUS_ERR, err_msg,
		strlen(err_msg));
	::free(err_msg);
}

static int NewVm(const _ha_request *reqp, _ha_response *resp, void *userp ) {
	auto param_valuep = ha_parameter_get(reqp, "vm-id");

	if (param_valuep  == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vmid param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	auto data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
			"VM config invalid");
		return HA_CALLBACK_CONTINUE;
	}
	std::string req_data(data);
	LOG(INFO) << "VM Configuration " << req_data;
	::free(data);

	auto vm_handle = pio::NewVm(vmid, req_data);

	if (vm_handle == kInvalidVmHandle) {
		std::ostringstream es;
		es << "Adding new VM failed, VM config: " << req_data;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}
	pio::config::VmConfig config(req_data);

	uint32_t id;
	config.GetTargetId(id);
	std::string name = config.GetTargetName();

	std::ostringstream os;
	os << "tgtadm --lld iscsi --mode target --op new"
		<< " --tid=" << id
		<< " --targetname " << name;

	int failed = 0;
	auto result = exec(os.str(), failed);

	if (failed) {
		std::ostringstream es;
		es << os.str() << " Failed with, " << result;
		es << "tgt target create failed";
		SetErrMsg(resp, STORD_ERR_TARGET_CREATE, es.str());
		RemoveVm(vm_handle);
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Added VmID " << vmid << " VmHandle " << vm_handle;
	const auto res = std::to_string(vm_handle);

	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

static int NewVmdk(const _ha_request *reqp, _ha_response *resp, void *userp ) {
	auto param_valuep = ha_parameter_get(reqp, "vm-id");

	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);
	param_valuep = ha_parameter_get(reqp, "vmdk-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vmdk-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmdkid(param_valuep);

	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == kInvalidVmHandle) {
		std::ostringstream es;
		es << "Adding new VMDK failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	char *data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
			"VMDK config invalid");
		return HA_CALLBACK_CONTINUE;
	}
	std::string req_data(data);
	LOG(INFO) << "VM Configuration " << req_data;
	::free(data);

	auto vmdk_handle = pio::NewActiveVmdk(vm_handle, vmdkid, req_data);
	if (vmdk_handle == kInvalidVmdkHandle) {
		std::ostringstream es;
		es << "Adding new VMDK failed."
			<< " VmID = " << vmid
			<< " VmHandle = " << vm_handle
			<< " VmdkID = " << vmdkid;
		SetErrMsg(resp, STORD_ERR_INVALID_VMDK, es.str());
		return HA_CALLBACK_CONTINUE;
	}
	pio::config::VmdkConfig config(req_data);

	uint32_t tid;
	config.GetTargetId(tid);
	uint32_t lid;
	config.GetLunId(lid);

	std::string dev_path = config.GetDevPath();

	std::ostringstream os;
	os << "tgtadm --lld iscsi --mode logicalunit --op new"
		<< " --tid=" << tid
		<< " --lun=" << lid
		<< " -b " << dev_path
		<< " --bstype hyc"
		<< " --bsopts "
		<< "vmid=" << vmid
		<< ":"
		<< "vmdkid=" << vmdkid;

	int failed = 0;
	auto result = exec(os.str(), failed);
	if (failed) {
		std::ostringstream es;
		es << os.str() << " Failed with, " << result;
		RemoveVmdk(vmdk_handle);
		SetErrMsg(resp, STORD_ERR_LUN_CREATE, es.str());
		return HA_CALLBACK_CONTINUE;
	}
	LOG(INFO) << "Added VMDK VmID " << vmid
		<< " VmHandle " << vm_handle
		<< " VmdkID " << vmdkid
		<< " VmdkHandle " << vmdk_handle;

	const auto res = std::to_string(vmdk_handle);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
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
	auto size = sizeof(struct ha_handlers) + 2 * sizeof(struct ha_endpoint_handlers);

	auto handlers =
		std::unique_ptr<struct ha_handlers,
		void(*)(void *)>(reinterpret_cast<struct ha_handlers*>(::malloc(size)),
			::free);
	if (not handlers) {
		LOG(ERROR) << "Memory allocation for ha handlers failed";
		return -ENOMEM;
	}

	// new_vm handler
	handlers->ha_endpoints[0].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[0].ha_url_endpoint, kNewVm.c_str(),
		kNewVm.size() + 1);
	handlers->ha_endpoints[0].callback_function = NewVm;
	handlers->ha_endpoints[0].ha_user_data = NULL;
	handlers->ha_count = 1;

	// new_vmdk handler
	handlers->ha_endpoints[1].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[1].ha_url_endpoint, kNewVmdk.c_str(),
		strlen("new_vmdk") + 1);
	handlers->ha_endpoints[1].callback_function = NewVmdk;
	handlers->ha_endpoints[1].ha_user_data = NULL;
	handlers->ha_count += 1;

	g_thread_.ha_instance_ = ::ha_initialize(FLAGS_etcd_ip.c_str(),
			FLAGS_svc_label.c_str(), FLAGS_stord_version.c_str(),
			120, const_cast<const struct ha_handlers *> (handlers.get()),
			StordHaStartCallback, StordHaStopCallback);

	if (g_thread_.ha_instance_ == nullptr) {
		LOG(ERROR) << "ha_initialize failed";
		return -EINVAL;
	}

	InitStordLib();
	auto si = std::make_shared<StorRpcSvImpl>();
	thirft_server = std::make_shared<ThriftServer>();

	thirft_server->setInterface(si);
	thirft_server->setAddress(kServerIp, kServerPort);
	LOG(INFO) << "Starting Thrift Server";
	google::FlushLogFiles(google::INFO);
	google::FlushLogFiles(google::ERROR);

	thirft_server->serve();

	DeinitStordLib();

	return 0;
}
