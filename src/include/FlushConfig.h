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

public:
	static const std::string kMoveAllowed;
};
}}
