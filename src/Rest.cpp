#include <folly/futures/Future.h>

#include "CommonMacros.h"
#include "Rest.h"
#include "halib.h"

namespace pio {
static void HaPostCallback(_ha_response* resp, void* datap) {
	auto promisep = reinterpret_cast<folly::Promise<RestResponse>*>(datap);
	RestResponse res;
	res.status = resp->status;
	promisep->setValue(std::move(res));
	delete promisep;
}

RestCall::RestCall(_ha_instance* instancep) : instancep_(instancep) {
}

folly::Future<RestResponse> RestCall::Post(const char* endpointp,
		const char* bodyp) {
	auto promisep = new folly::Promise<RestResponse>();
	auto future = promisep->getFuture();
	auto rc = ha_post_analyzer_stats(instancep_, endpointp, bodyp, nullptr,
		true, reinterpret_cast<void*>(promisep), HaPostCallback);
	if (pio_unlikely(rc != SUCCESS)) {
		RestResponse response;
		response.status = -errno;
		response.post_failed = true;
		LOG(ERROR) << "Failed to post Rest call. EndPoint = " << endpointp;
		promisep->setValue(std::move(response));
		delete promisep;
	}
	return future;
}
}
