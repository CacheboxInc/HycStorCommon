#include <thread>
#include <atomic>
#include <memory>

#include <restbed>

#include <glog/logging.h>

#include "HycRestServer.h"
#include "TgtInterface.h"

using namespace restbed;

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

void NewVmRest(const std::shared_ptr<Session> session) {
	const auto& request = session->get_request();

	VmHandle vm_handle = kInvalidVmHandle;

	auto vmid = request->get_path_parameter("id");
	auto len = request->get_header("Content-Length", 0);
	session->fetch(len, [&] (const std::shared_ptr<Session> session, const Bytes& body) {
		std::string s(body.begin(), body.end());
		LOG(INFO) << "VM Configuration " << s;
		vm_handle = ::NewVm(vmid.c_str(), s.c_str());
	});

	if (vm_handle == kInvalidVmHandle) {
		LOG(ERROR) << "Adding new VM failed."
			<< " VmID = " << vmid;
		session->close(400);
		return;
	}

	LOG(INFO) << "Added VmID " << vmid << " VmHandle " << vm_handle;
	const auto res = std::to_string(vm_handle);
	session->close(OK, res, {
		{"Content-Length", std::to_string(sizeof(res)) }
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
	session->fetch(len, [&] (const std::shared_ptr<Session> session, const Bytes& body) {
		std::string s(body.begin(), body.end());
		LOG(INFO) << "VMDK Configuration " << s;
		vmdk_handle = ::NewActiveVmdk(vm_handle, vmdkid.c_str(), s.c_str());
	});

	if (vmdk_handle == kInvalidVmdkHandle) {
		LOG(ERROR) << "Adding new VMDK failed."
			<< " VmID = " << vmid
			<< " VmHandle = " << vm_handle
			<< " VmdkID = " << vmdkid;
		session->close(400);
		return;
	}

	LOG(INFO) << "Added VMDK VmID " << vmid
		<< " VmHandle " << vm_handle
		<< " VmdkID " << vmdkid
		<< " VmdkHandle " << vmdk_handle;
	const auto res = std::to_string(vmdk_handle);
	session->close(OK, res, {
		{"Content-Length", std::to_string(sizeof(res)) }
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

int HycRestServerStart() {
	try {
		std::call_once(g_thread_.initialized_, [=] () mutable {
			g_thread_.thread_ = std::make_unique<std::thread>(HycRestServerStart_);
		});
		return 0;
	} catch (const std::exception& e) {
		return -1;
	}
}

void HycRestServerStop() {
	if (service) {
		service->stop();
	}
	if (g_thread_.thread_) {
		g_thread_.thread_->join();
		g_thread_.thread_.reset();
	}
	return;
}