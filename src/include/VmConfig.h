#pragma once

#include <string>

#include "gen-cpp2/MetaData_types.h"
#include "JsonConfig.h"
#include "IDs.h"

namespace pio { namespace config {
class VmConfig : public JsonConfig {
public:
	VmConfig(const std::string& config);
	VmConfig();

	void SetVmId(const ::ondisk::VmID& vmid);
	bool GetVmId(::ondisk::VmID& vmid) const;

	void SetTargetId(uint32_t target_id);
	bool GetTargetId(uint32_t& target_id) const;
	void SetTargetName(const std::string& target_name);

	void SetAeroClusterID(const AeroClusterID& cluster_id);
	bool GetAeroClusterID(AeroClusterID& cluster_id) const;

	std::string GetTargetName() const;

public:
	static const std::string kVmID;
	static const std::string kTargetID;
	static const std::string kTargetName;
	static const std::string kAeroClusterID;
};
}}
