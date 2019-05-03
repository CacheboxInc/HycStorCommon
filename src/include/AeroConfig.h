#pragma once

#include <string>

#include "IDs.h"
#include "JsonConfig.h"
#define STORE_KEY_IN_BIN 1

namespace pio {
namespace config {
class AeroConfig : public JsonConfig {
public:
	AeroConfig(const std::string& config);
	AeroConfig();

	void SetAeroIPs(const std::string& ips);
	std::string GetAeroIPs();

	void SetAeroPort(const uint32_t port);
	bool GetAeroPort(uint32_t& port);

	void SetAeroID(const AeroClusterID id);
	bool GetAeroID(AeroClusterID& id);
	
public:
	static const std::string kIPs;
	static const std::string kPort;
	static const std::string kID;
};
}}
