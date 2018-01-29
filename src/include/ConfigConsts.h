#pragma once

#include <vector>
#include <string>

struct VmConfig {
	static const std::string kVmID;
};

struct VmdkConfig {
	static const std::string kVmdkID;
	static const std::string kBlockSize;
	static const std::string kCompression;
	static const std::string kEncryptionKey;
	static const std::vector<std::string> kCompressAlgos;
};