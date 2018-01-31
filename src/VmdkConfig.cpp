#include <algorithm>

#include "VmdkConfig.h"
#include "JsonConfig.h"
#include "Utils.h"

namespace pio { namespace config {

const std::string VmdkConfig::kEnabled = "Enabled";
const std::string VmdkConfig::kVmID = "VmID";
const std::string VmdkConfig::kVmdkID = "VmdkID";
const std::string VmdkConfig::kBlockSize = "BlockSize";
const std::string VmdkConfig::kEncryption = "Encryption";
const std::string VmdkConfig::kEncryptionKey = "EncryptionKey";

const std::string VmdkConfig::kCompression = "Compression";
const std::string VmdkConfig::kCompressionType = "Type";
const std::string VmdkConfig::kCompressionLevel = "Level";
const std::vector<std::string> VmdkConfig::kCompressAlgos = {
	"snappy",
	"lzw",
};

const std::string VmdkConfig::kRamCache = "RamCache";
const std::string VmdkConfig::kRamCacheMemoryInMB = "MemoryInMB";
const std::string VmdkConfig::kFileCache = "FileCache";
const std::string VmdkConfig::kFileCachePath = "Path";

VmdkConfig::VmdkConfig(const std::string& config) : JsonConfig(config) {
}

VmdkConfig::VmdkConfig() {

}

void VmdkConfig::SetVmdkId(const VmdkID& id) {
	JsonConfig::SetKey(kVmdkID, id);
}

bool VmdkConfig::GetVmdkId(VmdkID& id) const {
	return JsonConfig::GetKey(kVmdkID, id);
}

void VmdkConfig::SetVmId(const VmID& id) {
	JsonConfig::SetKey(kVmID, id);
}

bool VmdkConfig::GetVmId(VmID& id) const {
	return JsonConfig::GetKey(kVmID, id);
}

void VmdkConfig::SetBlockSize(uint32_t size) {
	JsonConfig::SetKey(kBlockSize, size);
}

bool VmdkConfig::GetBlockSize(uint32_t& size) const {
	return JsonConfig::GetKey(kBlockSize, size);
}

void VmdkConfig::DisableCompression() {
	std::string key;

	StringDelimAppend(key, '.', {kCompression, kEnabled});
	JsonConfig::SetKey(key, false);
}

void VmdkConfig::ConfigureCompression(const std::string& algo, uint16_t level) {
	std::string key;

	StringDelimAppend(key, '.', {kCompression, kEnabled});
	JsonConfig::SetKey(key, true);

	StringDelimAppend(key, '.', {kCompression, kCompressionType});
	auto it = std::find(kCompressAlgos.begin(), kCompressAlgos.end(), algo);
	if (it == kCompressAlgos.end()) {
		throw std::invalid_argument("Invalid Compression Argument");
	}
	JsonConfig::SetKey(key, algo);

	if (level > 0) {
		StringDelimAppend(key, '.', {kCompression, kCompressionLevel});
		JsonConfig::SetKey(key, level);
	}
}

bool VmdkConfig::IsCompressionEnabled() const {
	bool enabled;
	std::string key;
	StringDelimAppend(key, '.', {kCompression, kEnabled});
	auto rc = JsonConfig::GetKey(key, enabled);
	return rc and enabled;
}

std::string VmdkConfig::GetCompressionType() const {
	std::string type;
	std::string key;
	StringDelimAppend(key, '.', {kCompression, kCompressionType});
	auto rc = JsonConfig::GetKey(key, type);
	if (not rc) {
		type.clear();
	}

	return std::move(type);
}

uint16_t VmdkConfig::GetCompressionLevel() const {
	uint16_t level;
	std::string key;
	StringDelimAppend(key, '.', {kCompression, kCompressionLevel});
	auto rc = JsonConfig::GetKey(key, level);
	return rc ? level : 0;
}

void VmdkConfig::DisableEncryption() {
	std::string key;

	StringDelimAppend(key, '.', {kEncryption, kEnabled});
	JsonConfig::SetKey(key, false);
}

void VmdkConfig::ConfigureEncrytption(const std::string& ekey) {
	std::string key;

	StringDelimAppend(key, '.', {kEncryption, kEnabled});
	JsonConfig::SetKey(key, true);

	StringDelimAppend(key, '.', {kEncryption, kEncryptionKey});
	JsonConfig::SetKey(key, ekey);
}

bool VmdkConfig::IsEncryptionEnabled() const {
	std::string key;
	StringDelimAppend(key, '.', {kEncryption, kEnabled});

	bool enabled;
	auto rc = JsonConfig::GetKey(key, enabled);
	return rc and enabled;
}

std::string VmdkConfig::GetEncryptionKey() const {
	std::string key;
	StringDelimAppend(key, '.', {kEncryption, kEncryptionKey});

	std::string e;
	auto rc = JsonConfig::GetKey(key, e);
	if (not rc) {
		e.clear();
	}
	return std::move(e);
}

void VmdkConfig::DisableRamCache() {
	std::string key;

	StringDelimAppend(key, '.', {kRamCache, kEnabled});
	JsonConfig::SetKey(key, false);
}

void VmdkConfig::ConfigureRamCache(uint16_t size_mb) {
	std::string key;

	StringDelimAppend(key, '.', {kRamCache, kEnabled});
	JsonConfig::SetKey(key, true);

	StringDelimAppend(key, '.', {kRamCache, kRamCacheMemoryInMB});
	JsonConfig::SetKey(key, size_mb);
}

bool VmdkConfig::IsRamCacheEnabled() const {
	std::string key;
	StringDelimAppend(key, '.', {kRamCache, kEnabled});

	bool enabled;
	auto rc = JsonConfig::GetKey(key, enabled);
	return rc and enabled;
}

uint16_t VmdkConfig::GetRamCacheMemoryLimit() const {
	std::string key;
	uint16_t size_mb;

	StringDelimAppend(key, '.', {kRamCache, kRamCacheMemoryInMB});
	auto rc = JsonConfig::GetKey(key, size_mb);
	return rc ? size_mb : 0;
}

void VmdkConfig::DisableFileCache() {
	std::string key;

	StringDelimAppend(key, '.', {kFileCache, kEnabled});
	JsonConfig::SetKey(key, false);
}

void VmdkConfig::ConfigureFileCache(const std::string& file_path) {
	std::string key;

	StringDelimAppend(key, '.', {kFileCache, kEnabled});
	JsonConfig::SetKey(key, true);

	StringDelimAppend(key, '.', {kFileCache, kFileCachePath});
	JsonConfig::SetKey(key, file_path);
}

bool VmdkConfig::IsFileCacheEnabled() const {
	std::string key;
	StringDelimAppend(key, '.', {kFileCache, kEnabled});

	bool enabled;
	auto rc = JsonConfig::GetKey(key, enabled);

	return rc and enabled;
}

std::string VmdkConfig::GetFileCachePath() const {
	std::string key;

	StringDelimAppend(key, '.', {kFileCache, kFileCachePath});
	std::string fp;

	auto rc = JsonConfig::GetKey(key, fp);

	if (not rc) {
		fp.clear();
	}

	return std::move(fp);
}

}}