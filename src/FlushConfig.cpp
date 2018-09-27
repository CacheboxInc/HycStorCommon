#include <string>
#include "FlushConfig.h"
namespace pio { namespace config {
const std::string FlushConfig::kMoveAllowed = "MoveAllowed";

FlushConfig::FlushConfig(const std::string& config) {
	if (config.size()) {
		JsonConfig(config);
	}
}

FlushConfig::FlushConfig() {}

void FlushConfig::SetMoveAllowedStatus(bool status) {
	JsonConfig::SetKey(kMoveAllowed, status);
}

bool FlushConfig::GetMoveAllowedStatus(bool& status) {
	return JsonConfig::GetKey(kMoveAllowed, status);
}

}}
