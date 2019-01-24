#include <string>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "VmConfig.h"

using namespace ::hyc_thrift;
using namespace ::ondisk;

namespace pio { namespace config {

const std::string VmConfig::kVmID = "VmID";
const std::string VmConfig::kVmUUID = "VmUUID";
const std::string VmConfig::kTargetID = "TargetID";
const std::string VmConfig::kTargetName = "TargetName";
const std::string VmConfig::kAeroClusterID = "AeroClusterID";

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

void VmConfig::SetVmUUID(const VmUUID& uuid) {
	JsonConfig::SetKey(kVmUUID, uuid);
}

bool VmConfig::GetVmUUID(VmUUID& uuid) const {
	return JsonConfig::GetKey(kVmUUID, uuid);
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

void VmConfig::SetAeroClusterID(const AeroClusterID& cluster_id) {
	JsonConfig::SetKey(kAeroClusterID, cluster_id);
}

bool VmConfig::GetAeroClusterID(AeroClusterID& cluster_id) const {
	return JsonConfig::GetKey(kAeroClusterID, cluster_id);
}

}}
