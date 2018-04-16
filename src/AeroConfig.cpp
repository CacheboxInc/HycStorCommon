#include <string>
#include "AeroConfig.h"
namespace pio { namespace config {
const std::string AeroConfig::kIPs = "AeroClusterIPs";
const std::string AeroConfig::kPort = "AeroClusterPort";
const std::string AeroConfig::kID = "AeroClusterID";

AeroConfig::AeroConfig(const std::string& config) : JsonConfig(config) {}
AeroConfig::AeroConfig() {}

void AeroConfig::SetAeroPort(uint32_t port) {
	JsonConfig::SetKey(kPort, port);
}

bool AeroConfig::GetAeroPort(uint32_t& port) {
	return JsonConfig::GetKey(kPort, port);
}

void AeroConfig::SetAeroID(AeroClusterID id) {
	JsonConfig::SetKey(kID, id);
}

bool AeroConfig::GetAeroID(AeroClusterID& id) {
	return JsonConfig::GetKey(kID, id);
}

void AeroConfig::SetAeroIPs(const std::string& ips) {
	return JsonConfig::SetKey(kIPs, ips);
}

std::string AeroConfig::GetAeroIPs() {
	std::string name;
	auto rc = JsonConfig::GetKey(kIPs, name);
	if (not rc) {
		name.clear();
	}
	return name;
}

}}
