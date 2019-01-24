#pragma once

#include <folly/futures/Future.h>

struct _ha_instance;

namespace pio {
struct EndPoint {
	constexpr static char kStats[] = "/stats/";
	constexpr static char kFingerPrint[] = "/fprints/";
	constexpr static char kStordCacheStats[] = "/hdm_stats?component=stord";
};

struct RestResponse {
	int status;
	bool post_failed{false};

	RestResponse() = default;
	RestResponse(RestResponse& rhs) = default;
	~RestResponse() = default;
	RestResponse(RestResponse&& rhs) : status(rhs.status),
			post_failed(rhs.post_failed) {
	}
};

class RestCall {
public:
	RestCall(_ha_instance* instancep);
	folly::Future<RestResponse> Post(const char* endpointp, const char* bodyp);
private:
	_ha_instance* instancep_{nullptr};
};
}
