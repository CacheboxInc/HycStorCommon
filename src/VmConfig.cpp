#include <string>

#include "VmConfig.h"

namespace pio { namespace config {

const std::string VmConfig::kVmID = "VmID";
const std::string VmConfig::kTargetID = "TargetID";
const std::string VmConfig::kTargetName = "TargetName";

VmConfig::VmConfig(const std::string& config) : JsonConfig(config) {
}

VmConfig::VmConfig() {
}

void VmConfig::SetVmId(const VmID& id) {
	JsonConfig::SetKey(kVmID, id);
}

bool VmConfig::GetVmId(VmID& id) const {
	return JsonConfig::GetKey(kVmID, id);
}

void VmConfig::SetTargetId(uint32_t target_id) {
	JsonConfig::SetKey(kTargetID, target_id);
}

bool VmConfig::GetTargetId(uint32_t& target_id) const {
	return JsonConfig::GetKey(kTargetID, target_id);
}

void VmConfig::SetTargetName(const std::string& target_name) {
	return JsonConfig::SetKey(kTargetName, target_name);
}

std::string VmConfig::GetTargetName() const {
	std::string name;
	auto rc = JsonConfig::GetKey(kTargetName, name);
	if (not rc) {
		name.clear();
	}
	return name;
}

}}