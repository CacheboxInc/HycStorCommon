#pragma once

#include <string>
#include <vector>

#include "IDs.h"
#include "JsonConfig.h"

namespace pio { namespace config {
class VmdkConfig : public JsonConfig {
public:
	VmdkConfig(const std::string& config);
	VmdkConfig();
	void SetVmdkId(const VmdkID& id);
	bool GetVmdkId(VmdkID& id) const;
	void SetVmId(const VmID& id);
	bool GetVmId(VmID& id) const;
	void SetBlockSize(uint32_t size);
	bool GetBlockSize(uint32_t& size) const;

	void DisableCompression();
	void ConfigureCompression(const std::string& algo, uint16_t level);
	bool IsCompressionEnabled() const;
	std::string GetCompressionType() const;
	uint16_t GetCompressionLevel() const;

	void DisableEncryption();
	void ConfigureEncrytption(const std::string& ekey);
	bool IsEncryptionEnabled() const;
	std::string GetEncryptionKey() const;

	void DisableRamCache();
	void ConfigureRamCache(uint16_t size_mb);
	bool IsRamCacheEnabled() const;
	uint16_t GetRamCacheMemoryLimit() const;

	void DisableFileCache();
	void ConfigureFileCache(const std::string& file_path);
	bool IsFileCacheEnabled() const;
	std::string GetFileCachePath() const;

public:
	static const std::string kEnabled;
	static const std::string kVmdkID;
	static const std::string kVmID;
	static const std::string kBlockSize;

	static const std::string kCompression;
	static const std::string kCompressionType;
	static const std::string kCompressionLevel;
	static const std::vector<std::string> kCompressAlgos;

	static const std::string kEncryption;
	static const std::string kEncryptionKey;

	static const std::string kRamCache;
	static const std::string kRamCacheMemoryInMB;

	static const std::string kFileCache;
	static const std::string kFileCachePath;
};

}
}