#include <thread>
#include <atomic>
#include <memory>

#include <restbed>
#include "HycRestServer.h"

using namespace std;
using namespace restbed;

struct {
	std::once_flag initialized_;
	std::unique_ptr<std::thread> thread_;
} g_thread_;

std::unique_ptr<Service> service;

void HelloWorld(const std::shared_ptr<Session> session) {
	const auto& request = session->get_request();

	const string body = "Hello, " + request->get_path_parameter("name");
	session->close(OK, body, {
		{ "Content-Length", ::to_string(body.size()) }
	});
}

void HycRestServerStart_() {
	auto resource = make_shared<Resource>();
	resource->set_path("/resource/{name: .*}" );
	resource->set_method_handler("GET", HelloWorld);

	auto settings = make_shared<Settings>();
	settings->set_port(1984);
	settings->set_default_header("Connection", "close");

	service = std::make_unique<Service>();
	service->publish(resource);
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
	}
	return;
}