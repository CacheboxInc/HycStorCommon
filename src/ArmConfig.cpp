#include <string>
#include <mutex>
#include "ArmConfig.h"

namespace pio {
namespace vc_ns {

struct {
	std::mutex vc_mtx;
	std::map<std::string, vc_info> vc_map;
} g_vc_map;

vc_info* GetVcenterDetails(const std::string& vc_ip) {
	std::unique_lock<std::mutex> lock1(g_vc_map.vc_mtx);

	if (g_vc_map.vc_map.find(vc_ip) != g_vc_map.vc_map.end()) {
		return &g_vc_map.vc_map[vc_ip];
	}
	return nullptr;
}

std::string* GetVcInfo(const std::string& vc_ip,
		const std::string& key)  {
	vc_info* vcinfo = GetVcenterDetails(vc_ip);
	if (not vcinfo) {
		return nullptr;
	}
	return &(*vcinfo)[key];
}

void SetVcConfig(std::string&& vc_ip, vc_info&& vcinfo) {
	std::unique_lock<std::mutex> lock1(g_vc_map.vc_mtx);

	if (g_vc_map.vc_map.find(vc_ip) == g_vc_map.vc_map.end()) {
		g_vc_map.vc_map.insert(std::make_pair(std::move(vc_ip),
				std::move(vcinfo)));
	} else {
		g_vc_map.vc_map[vc_ip] = std::move(vcinfo);
	}
}

} // namespace vc_ns.

namespace config {

const std::string ArmConfig::kVmdkList = "VMDK_LIST";
const std::string ArmConfig::kVmdkId = "VMDK_ID";
const std::string ArmConfig::kVmdkPath = "VMDK_PATH";
const std::string ArmConfig::kVcIp = "VCENTER_IP";
const std::string ArmConfig::kCookie = "CKPT_COOKIE";
const std::string ArmConfig::kCkptId = "CKPT_ID";
const std::string ArmConfig::kMoId = "MOID";

ArmConfig::ArmConfig(const std::string& config) : JsonConfig(config) {
}

ArmConfig::ArmConfig() {
}

std::string ArmConfig::GetVcIp() {
	std::lock_guard<std::mutex> lock1(mtx_);

	return JsonConfig::GetValue<std::string>(kVcIp);
}

std::string ArmConfig::GetCookie() {
	std::lock_guard<std::mutex> lock1(mtx_);

	return JsonConfig::GetValue<std::string>(kCookie);
}

int64_t ArmConfig::GetCkptId() {
	std::lock_guard<std::mutex> lock1(mtx_);

	return JsonConfig::GetValue<int64_t>(kCkptId);
}

std::string ArmConfig::GetMoId() {
	std::lock_guard<std::mutex> lock1(mtx_);

	return JsonConfig::GetValue<std::string>(kMoId);
}


std::string ArmConfig::GetVcUser() {
	std::string vcip = GetVcIp();
	if (false) {
		return std::string();
	}

	std::lock_guard<std::mutex> lock1(mtx_);
	return *vc_ns::GetVcInfo(vcip, vc_ns::kVcUser);
}

std::string ArmConfig::GetVcPasswd() {
	std::string vcip = GetVcIp();
	if (false) {
		return std::string();
	}

	std::lock_guard<std::mutex> lock1(mtx_);
	return *vc_ns::GetVcInfo(vcip, vc_ns::kVcPasswd);
}

std::string ArmConfig::GetVcFprint1() {
	std::string vcip = GetVcIp();
	if (false) {
		return std::string();
	}

	std::lock_guard<std::mutex> lock1(mtx_);
	return *vc_ns::GetVcInfo(vcip, vc_ns::kVcFprint1);
}

std::string ArmConfig::GetVcFprint256() {
	std::string vcip = GetVcIp();
	if (false) {
		return std::string();
	}

	std::lock_guard<std::mutex> lock1(mtx_);
	return *vc_ns::GetVcInfo(vcip, vc_ns::kVcFprint256);
}

arm_config::vmdk_info_map
ArmConfig::GetVmdkPathInfo() {
	arm_config::vmdk_info_map vmdks;

	std::lock_guard<std::mutex> lock1(mtx_);

	auto& config = GetJsonRoot();
	auto& vmdklist = config.get_child(kVmdkList);
	for (auto& row : vmdklist) {
		/* TODO: Error handling for json data. */
		auto vmdk_id = row.second.get(kVmdkId, "");
		arm_config::vmdk_info vmdk_path = row.second.get(kVmdkPath, "");

		if (vmdks.find(vmdk_id) == vmdks.end()) {
			vmdks.insert(std::make_pair(std::move(vmdk_id),
				std::move(vmdk_path)));
		} else {
			vmdks[vmdk_id] = std::move(vmdk_path);
		}
	}

	return vmdks;
}

} // namespace config.
} // namespace pio.
