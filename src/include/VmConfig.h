#pragma once

#include <string>

#include "IDs.h"
#include "JsonConfig.h"

namespace pio { namespace config {
class VmConfig : public JsonConfig {
public:
	VmConfig(const std::string& config);
	VmConfig();

	void SetVmId(const VmID& vmid);
	bool GetVmId(VmID& vmid) const;

public:
	static const std::string kVmID;
};
}}