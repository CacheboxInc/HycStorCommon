#pragma once

#include <string>

#include "IDs.h"
#include "JsonConfig.h"

namespace pio { namespace config {
class FlushConfig : public JsonConfig {
public:
	FlushConfig(const std::string& config);
	FlushConfig();

	void SetMoveAllowedStatus(bool status);
	bool GetMoveAllowedStatus(bool& status);
	bool GetFlushAllowedStatus(bool& status);
	bool GetMaxPendingReqsCnt(uint32_t& val);
	bool GetMaxReqSize(uint32_t& val);
public:
	static const std::string kMoveAllowed;
	static const std::string kFlushAllowed;
	static const std::string kMaxPendingReqCnt;
	static const std::string kMaxReqSize;
};
}}
