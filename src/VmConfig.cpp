#include <string>

#include "VmConfig.h"

namespace pio { namespace config {

const std::string VmConfig::kVmID = "VmID";

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

}}