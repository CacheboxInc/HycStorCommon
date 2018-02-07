#include <algorithm>

#include <glog/logging.h>

#include "VmdkConfig.h"
#include "JsonConfig.h"
#include "Utils.h"
#include "Common.h"

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

const std::string VmdkConfig::kErrorHandler = "ErrorHnalder";
const std::string VmdkConfig::kErrorType = "Type";
const std::string VmdkConfig::kReturnValue = "ReturnValue";
const std::string VmdkConfig::kFrequency = "Frequency";
const std::map<VmdkConfig::ErrorType, std::string> VmdkConfig::kErrorToString = {
	{VmdkConfig::ErrorType::kThrow, "throw"},
	{VmdkConfig::ErrorType::kReturnError, "error"}
};

const std::string VmdkConfig::kSuccessHandler = "SuccessHandler";

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

std::ostream& operator <<(std::ostream& os, const VmdkConfig::ErrorType& type) {
	auto it = VmdkConfig::kErrorToString.find(type);
	if (it == VmdkConfig::kErrorToString.end()) {
		os << "Undefined";
	} else {
		os << it->second;
	}

	return os;
}

std::istream& operator >>(std::istream& in, VmdkConfig::ErrorType& type) {
	std::string k;
	in >> k;

	type = VmdkConfig::ErrorType::kReturnError;
	for (auto& e : VmdkConfig::kErrorToString) {
		if (e.second == k) {
			type = e.first;
			break;
		}
	}

	return in;
}

void VmdkConfig::ConfigureErrorHandler(ErrorType type, uint32_t frequency,
		int error) {
	log_assert(type == ErrorType::kThrow or type == ErrorType::kReturnError);

	std::string key;
	StringDelimAppend(key, '.', {kErrorHandler, kEnabled});
	JsonConfig::SetKey(key, true);

	StringDelimAppend(key, '.', {kErrorHandler, kErrorType});
	JsonConfig::SetKey(key, type);

	StringDelimAppend(key, '.', {kErrorHandler, kFrequency});
	JsonConfig::SetKey(key, frequency);

	if (type == ErrorType::kReturnError) {
		StringDelimAppend(key, '.', {kErrorHandler, kReturnValue});
		JsonConfig::SetKey(key, error);
	}
}

void VmdkConfig::DisableErrorHandler() {
	std::string key;
	StringDelimAppend(key, '.', {kErrorHandler, kEnabled});
	JsonConfig::SetKey(key, false);
}

bool VmdkConfig::ErrorHandlerEnabled() const {
	std::string key;
	StringDelimAppend(key, '.', {kErrorHandler, kEnabled});

	bool enabled{false};
	auto rc = JsonConfig::GetKey(key, enabled);
	return rc and enabled;
}

bool VmdkConfig::ErrorHandlerShouldThrow() const {
	if (not ErrorHandlerEnabled()) {
		return false;
	}

	std::string key;
	StringDelimAppend(key, '.', {kErrorHandler, kErrorType});

	ErrorType type;
	auto rc = JsonConfig::GetKey(key, type);
	return rc and type == ErrorType::kThrow;
}

int VmdkConfig::ErrorHandlerReturnValue() const {
	if (not ErrorHandlerEnabled()) {
		return false;
	}

	std::string key;
	StringDelimAppend(key, '.', {kErrorHandler, kErrorType});

	ErrorType type;
	auto rc = JsonConfig::GetKey(key, type);
	if (not (rc and type == ErrorType::kThrow)) {
		return 0;
	}

	int rv;
	StringDelimAppend(key, '.', {kErrorHandler, kReturnValue});
	rc = JsonConfig::GetKey(key, rv);
	return rc ? rv : 0;
}

uint32_t VmdkConfig::ErrorHandlerFrequency() const {
	if (not ErrorHandlerEnabled()) {
		return 0;
	}

	std::string key;
	uint32_t frequency;
	StringDelimAppend(key, '.', {kErrorHandler, kFrequency});
	auto rc = JsonConfig::GetKey(key, frequency);
	return rc ? frequency : 0;
}

void VmdkConfig::EnableSuccessHandler() {
	std::string key;
	StringDelimAppend(key, '.', {kSuccessHandler, kEnabled});
	JsonConfig::SetKey(key, true);
}

void VmdkConfig::DisableSuccessHandler() {
	std::string key;
	StringDelimAppend(key, '.', {kSuccessHandler, kEnabled});
	JsonConfig::SetKey(key, false);
}


bool VmdkConfig::IsSuccessHandlerEnabled() const {
	std::string key;
	StringDelimAppend(key, '.', {kSuccessHandler, kEnabled});

	bool enabled{false};
	auto rc = JsonConfig::GetKey(key, enabled);
	return rc and enabled;
}

}}