#include <string>
#include "FlushConfig.h"
namespace pio { namespace config {
const std::string FlushConfig::kMoveAllowed = "MoveAllowed";
const std::string FlushConfig::kFlushAllowed = "FlushAllowed";
const std::string FlushConfig::kMaxPendingReqCnt = "MaxPendingReqCnt";
const std::string FlushConfig::kMaxReqSize = "MaxReqSize";

FlushConfig::FlushConfig(const std::string& config) : JsonConfig(config) {
}

FlushConfig::FlushConfig() {
}

void FlushConfig::SetMoveAllowedStatus(bool status) {
	JsonConfig::SetKey(kMoveAllowed, status);
}

bool FlushConfig::GetMoveAllowedStatus(bool& status) {
	return JsonConfig::GetKey(kMoveAllowed, status);
}

bool FlushConfig::GetFlushAllowedStatus(bool& status) {
	return JsonConfig::GetKey(kFlushAllowed, status);
}

bool FlushConfig::GetMaxPendingReqsCnt(uint32_t& val) {
	return JsonConfig::GetKey(kMaxPendingReqCnt, val);
}

bool FlushConfig::GetMaxReqSize(uint32_t& val) {
	return JsonConfig::GetKey(kMaxReqSize, val);
}

}}
