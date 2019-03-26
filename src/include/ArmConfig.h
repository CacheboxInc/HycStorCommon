#pragma once

#include <string>
#include <map>
#include "include/JsonConfig.h"
#include "include/custom_struct.h"

namespace pio {

namespace vc_ns {

using vc_info = std::map<std::string, std::string>;

const std::string kVcIp = "VCENTER_IP";
const std::string kVcUser = "VCENTER_USER";
const std::string kVcPasswd = "VCENTER_PASSWD";
const std::string kVcFprint1 = "VCENTER_FINGERPRINT1";
const std::string kVcFprint256 = "VCENTER_FINGERPRINT256";

void SetVcConfig(std::string&&, vc_info&&);

} // namespace vc_ns.

namespace config {

namespace arm_config {

using vmdk_info = std::map<std::string, std::string>;
using vmdk_info_map = std::map<std::string, vmdk_info>;

} // namespace arm_config.

class ArmConfig : public JsonConfig {
public:
	ArmConfig(const std::string& config);
	ArmConfig();

	std::string GetCookie();
	int64_t GetCkptId();
	std::string GetMoId();
	std::string GetVcIp();
	std::string GetVcUser();
	std::string GetVcPasswd();
	std::string GetVcFprint1();
	std::string GetVcFprint256();
	arm_config::vmdk_info_map GetVmdkPathInfo();

	static const std::string kVmdkList;
	static const std::string kVmdkId;
	static const std::string kVmdkDStore;
	static const std::string kVmdkDir;
	static const std::string kVmdkFile;
	static const std::string kVcIp;
	static const std::string kCookie;
	static const std::string kCkptId;
	static const std::string kMoId;

private:
	std::mutex mtx_;
};
} // namespace config.
} // namespace pio.
