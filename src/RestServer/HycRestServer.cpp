#include <sys/wait.h>
#include <thread>
#include <atomic>
#include <memory>
#include <restbed>
#include <array>

#include <iostream>
#include <glog/logging.h>

#include "HycRestServer.h"
#include "TgtInterface.h"
#include "VmConfig.h"
#include "VmdkConfig.h"

using namespace restbed;

namespace pio {

struct {
	std::once_flag initialized_;
	std::unique_ptr<std::thread> thread_;
} g_thread_;

std::unique_ptr<Service> service;

void HelloWorld(const std::shared_ptr<Session> session) {
	const auto& request = session->get_request();

	const auto body = "Hello, " + request->get_path_parameter("name");
	session->close(OK, body, {
		{"Content-Length", std::to_string(body.size()) }
	});
}

std::string exec(const std::string& cmd, int& status)
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

void NewVmRest(const std::shared_ptr<Session> session) {
	const auto& request = session->get_request();

	VmHandle vm_handle = kInvalidVmHandle;

	std::string req_data;
	auto vmid = request->get_path_parameter("id");
	auto len = request->get_header("Content-Length", 0);
	session->fetch(len, [&] (const std::shared_ptr<Session> session, const Bytes& body)  mutable {
		req_data.assign(body.begin(), body.end());
		LOG(INFO) << "VM Configuration " << req_data;
		vm_handle = ::NewVm(vmid.c_str(), req_data.c_str());
	});

	if (vm_handle == kInvalidVmHandle) {
		LOG(ERROR) << "Adding new VM failed."
			<< " VmID = " << vmid;
		session->close(400);
		return;
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
		LOG(ERROR) << os.str() << "\nFailed with, " << result;
		RemoveVm(vm_handle);
		session->close(400);
		return;
	}

	LOG(INFO) << "Added VmID " << vmid << " VmHandle " << vm_handle;
	const auto res = std::to_string(vm_handle);
	session->close(OK, res, {
		{"Content-Length", std::to_string(res.length()) }
	});
}

void NewVmdkRest(const std::shared_ptr<Session> session) {
	const auto& request = session->get_request();

	auto vmid = request->get_path_parameter("vmid");

	auto vm_handle = ::GetVmHandle(vmid.c_str());
	if (vm_handle == kInvalidVmHandle) {
		LOG(ERROR) << "Adding new VMDK failed."
			<< " Invalid VmID = " << vmid ;
		session->close(400);
		return;
	}

	VmdkHandle vmdk_handle = kInvalidVmdkHandle;

	auto vmdkid = request->get_path_parameter("vmdkid");
	auto len = request->get_header("Content-Length", 0);

	std::string req_data;

	session->fetch(len, [&] (const std::shared_ptr<Session> session, const Bytes& body) {
		req_data.assign(body.begin(), body.end());
		LOG(INFO) << "VMDK Configuration " << req_data;
		vmdk_handle = ::NewActiveVmdk(vm_handle, vmdkid.c_str(), req_data.c_str());
	});

	if (vmdk_handle == kInvalidVmdkHandle) {
		LOG(ERROR) << "Adding new VMDK failed."
			<< " VmID = " << vmid
			<< " VmHandle = " << vm_handle
			<< " VmdkID = " << vmdkid;
		session->close(400);
		return;
	}

	pio::config::VmdkConfig config(req_data);

	uint32_t tid;
	config.GetTargetId(tid);
	uint32_t lid;
	config.GetLunId(lid);

	std::string dev_path = config.GetDevPath();

	std::ostringstream os;

	os << "tgtadm  --lld iscsi --mode logicalunit --op new"
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
		LOG(ERROR) << os.str() << "\nFailed with, " << result;
		RemoveVmdk(vmdk_handle);
		session->close(400);
		return;
	}

	LOG(INFO) << "Added VMDK VmID " << vmid
		<< " VmHandle " << vm_handle
		<< " VmdkID " << vmdkid
		<< " VmdkHandle " << vmdk_handle;
	const auto res = std::to_string(vmdk_handle);
	session->close(OK, res, {
		{"Content-Length", std::to_string(res.length()) }
	});
}

void HycRestServerStart_() {
	auto hello = std::make_shared<Resource>();
	hello->set_path("/resource/{name: .*}" );
	hello->set_method_handler("GET", HelloWorld);

	auto new_vm = std::make_shared<Resource>();
	new_vm->set_path("/new_vm/{id: .*}");
	new_vm->set_method_handler("POST", NewVmRest);

	auto new_vmdk = std::make_shared<Resource>();
	new_vmdk->set_path("/vm/{vmid: .*}/new_vmdk/{vmdkid: .*}");
	new_vmdk->set_method_handler("POST", NewVmdkRest);

	service = std::make_unique<Service>();
	service->publish(hello);
	service->publish(new_vm);
	service->publish(new_vmdk);

	auto settings = std::make_shared<Settings>();
	settings->set_port(1984);
	settings->set_default_header("Connection", "close");
	service->start(settings);
}
} // pio namespace

int HycRestServerStart(void) {
	try {
		std::call_once(pio::g_thread_.initialized_, [=] () mutable {
			pio::g_thread_.thread_ =
				std::make_unique<std::thread>(pio::HycRestServerStart_);
		});
		return 0;
	} catch (const std::exception& e) {
		return -1;
	}
}

void HycRestServerStop(void) {
	if (pio::service) {
		pio::service->stop();
	}
	if (pio::g_thread_.thread_) {
		pio::g_thread_.thread_->join();
		pio::g_thread_.thread_.reset();
	}
	return;
}
