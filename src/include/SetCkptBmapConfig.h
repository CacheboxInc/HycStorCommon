#pragma once

#include <string>

#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "JsonConfig.h"

namespace pio { namespace config {
class SetCkptBmapConfig : public JsonConfig {
public:
	SetCkptBmapConfig(const std::string& config);
	SetCkptBmapConfig();

	bool GetCkptID(::ondisk::CheckPointID& ckpt_id);
	bool GetCommitVal(bool& val);
	bool GetExtents(std::string& val);

public:
	static const std::string kCkptID;
	static const std::string kCommit;
	static const std::string kExtents;
};
}}
