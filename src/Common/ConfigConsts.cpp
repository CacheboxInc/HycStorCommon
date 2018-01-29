#include <string>

#include "ConfigConsts.h"

const std::string VmConfig::kVmID = "VmID";
const std::string VmdkConfig::kVmdkID = "VmdkID";
const std::string VmdkConfig::kBlockSize = "BlockSize";
const std::string VmdkConfig::kCompression = "Compression";
const std::string VmdkConfig::kEncryptionKey = "EncryptionKey";

const std::vector<std::string> VmdkConfig::kCompressAlgos = {
	"snappy",
	"lzw",
};
