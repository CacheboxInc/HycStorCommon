#include <string>
#include "SetCkptBmapConfig.h"
#include "gen-cpp2/MetaData_types.h"
#include "VmdkConfig.h"
#include "JsonConfig.h"
#include "DaemonUtils.h"
#include "DaemonCommon.h"

using namespace ::ondisk;

namespace pio { namespace config {
const std::string SetCkptBmapConfig::kCkptID = "CkptID";
const std::string SetCkptBmapConfig::kCommit = "Commit";
const std::string SetCkptBmapConfig::kExtents = "Extents";

SetCkptBmapConfig::SetCkptBmapConfig(const std::string& config): JsonConfig(config) {}

SetCkptBmapConfig::SetCkptBmapConfig() {}

bool SetCkptBmapConfig::GetCkptID(::ondisk::CheckPointID& ckpt_id) {
	return JsonConfig::GetKey(kCkptID, ckpt_id);
}

bool SetCkptBmapConfig::GetCommitVal(bool& val) {
	return JsonConfig::GetKey(kCommit, val);
}

bool SetCkptBmapConfig::GetExtents(std::string& val) {
	return JsonConfig::GetKey(kExtents, val);
}

}}
