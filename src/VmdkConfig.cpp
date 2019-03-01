#include <algorithm>

#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "VmdkConfig.h"
#include "JsonConfig.h"
#include "DaemonUtils.h"
#include "DaemonCommon.h"

using namespace ::ondisk;

namespace pio { namespace config {

const std::string VmdkConfig::kEnabled = "Enabled";
const std::string VmdkConfig::kVmID = "VmID";
const std::string VmdkConfig::kVmUUID = "VmUUID";
const std::string VmdkConfig::kVmdkID = "VmdkID";
const std::string VmdkConfig::kVmdkUUID = "VmdkUUID";
const std::string VmdkConfig::kBlockSize = "BlockSize";

const std::string VmdkConfig::kEncryption = "Encryption";
const std::string VmdkConfig::kEncryptionType = "Type";
const std::string VmdkConfig::kEncryptionKey = "EncryptionKey";
const std::vector<std::string> VmdkConfig::kEncryptionAlgos = {
	"aes128-gcm",
	"aes256-gcm",
};

const std::string VmdkConfig::kCompression = "Compression";
const std::string VmdkConfig::kCompressionType = "Type";
const std::string VmdkConfig::kCompressionLevel = "Level";
const std::string VmdkConfig::kMinCompressRatio = "MinRatio";
const std::vector<std::string> VmdkConfig::kCompressAlgos = {
	"snappy",
	"lz4",
};

const std::string VmdkConfig::kRamCache = "RamCache";
const std::string VmdkConfig::kRamCacheMemoryInMB = "MemoryInMB";
const std::string VmdkConfig::kFileCache = "FileCache";
const std::string VmdkConfig::kFileCachePath = "Path";

const std::string VmdkConfig::kNetworkTarget = "NetworkTarget";

const std::string VmdkConfig::kFileTarget = "FileTarget";
const std::string VmdkConfig::kFileTargetPath = "TargetFilePath";
const std::string VmdkConfig::kFileTargetSize = "TargetFileSize";
const std::string VmdkConfig::kFileTargetCreateFile = "CreateFile";
const std::string VmdkConfig::kErrorHandler = "ErrorHandler";
const std::string VmdkConfig::kErrorType = "Type";
const std::string VmdkConfig::kReturnValue = "ReturnValue";
const std::string VmdkConfig::kFrequency = "Frequency";
const std::map<VmdkConfig::ErrorType, std::string> VmdkConfig::kErrorToString = {
	{VmdkConfig::ErrorType::kThrow, "throw"},
	{VmdkConfig::ErrorType::kReturnError, "error"}
};

const std::string VmdkConfig::kSuccessHandler = "SuccessHandler";
const std::string VmdkConfig::kSuccessCompressEnabled = "CompressData";
const std::string VmdkConfig::kDelay = "Delay";

const std::string VmdkConfig::kTargetID = "TargetID";
const std::string VmdkConfig::kLunID = "LunID";
const std::string VmdkConfig::kDevPath = "DevPath";

const std::string VmdkConfig::kRamMetaDataKV = "RamMetaDataKV";
const std::string VmdkConfig::kAeroMetaDataKV = "AeroMetaDataKV";
const std::string VmdkConfig::kMetaDataKV = "MetaDataKV";
const std::string VmdkConfig::kParentDiskName = "ParentDiskName";
const std::string VmdkConfig::kParentDiskVmdkID = "ParentDiskVmdkID";
const std::string VmdkConfig::kCleanupOnWrite = "CleanupOnWrite";
const std::string VmdkConfig::kReadAhead = "ReadAhead";
const std::string VmdkConfig::kPreload = "Preload";
const std::string VmdkConfig::kOffset = "Offsets";
const std::string VmdkConfig::kDiskSizeBytes = "DiskSizeBytes";

const std::string VmdkConfig::kAeroCache = "AeroSpikeCache";

VmdkConfig::VmdkConfig(const std::string& config) : JsonConfig(config) {
}

VmdkConfig::VmdkConfig() {

}

void VmdkConfig::SetParentDisk(const std::string& parent_name) {
	return JsonConfig::SetKey(kParentDiskName, parent_name);
}

bool VmdkConfig::GetParentDisk(std::string& parent_name) {
	auto rc = JsonConfig::GetKey(kParentDiskName, parent_name);
	if (not rc) {
		parent_name.clear();
	}
	return rc;
}

void VmdkConfig::SetVmdkId(const VmdkID& id) {
	JsonConfig::SetKey(kVmdkID, id);
}

bool VmdkConfig::GetVmdkId(VmdkID& id) const {
	return JsonConfig::GetKey(kVmdkID, id);
}

void VmdkConfig::SetVmdkUUID(const VmdkUUID& uuid) {
	JsonConfig::SetKey(kVmdkUUID, uuid);
}

bool VmdkConfig::GetVmdkUUID(VmdkUUID& uuid) const {
	return JsonConfig::GetKey(kVmdkUUID, uuid);
}

void VmdkConfig::SetVmId(const VmID& id) {
	JsonConfig::SetKey(kVmID, id);
}

bool VmdkConfig::GetVmId(VmID& id) const {
	return JsonConfig::GetKey(kVmID, id);
}

void VmdkConfig::SetVmUUID(const VmUUID& uuid) {
	JsonConfig::SetKey(kVmUUID, uuid);
}

bool VmdkConfig::GetVmUUID(VmUUID& uuid) const {
	return JsonConfig::GetKey(kVmUUID, uuid);
}

bool VmdkConfig::GetParentDiskVmdkId(VmdkID& id) const {
	return JsonConfig::GetKey(kParentDiskVmdkID, id);
}

void VmdkConfig::SetParentDiskVmdkId(const VmdkID& id) {
	JsonConfig::SetKey(kParentDiskVmdkID, id);
}

void VmdkConfig::SetBlockSize(uint32_t size) {
	JsonConfig::SetKey(kBlockSize, size);
}

bool VmdkConfig::GetBlockSize(uint32_t& size) const {
	return JsonConfig::GetKey(kBlockSize, size);
}

void VmdkConfig::DisableAeroSpikeCache() {
	std::string key;
	StringDelimAppend(key, '.', {kAeroCache, kEnabled});
	JsonConfig::SetKey(key, false);
}

void VmdkConfig::EnableAeroSpikeCache() {
	std::string key;
	StringDelimAppend(key, '.', {kAeroCache, kEnabled});
	JsonConfig::SetKey(key, true);
}

bool VmdkConfig::IsAeroSpikeCacheDisabled() const {
	bool enabled;
	std::string key;
	StringDelimAppend(key, '.', {kAeroCache, kEnabled});
	auto rc = JsonConfig::GetKey(key, enabled);
	return rc ? not enabled : false;
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
		type = kCompressAlgos[0];
	}

	auto it = std::find(kCompressAlgos.begin(), kCompressAlgos.end(), type);
	if (it == kCompressAlgos.end()) {
		LOG(ERROR) << "Invalid Compression Type Argument: " << type;
		throw std::invalid_argument("Invalid Compression Type Argument");
	}

	return type;
}

uint16_t VmdkConfig::GetCompressionLevel() const {
	uint16_t level;
	std::string key;
	StringDelimAppend(key, '.', {kCompression, kCompressionLevel});
	auto rc = JsonConfig::GetKey(key, level);
	return rc ? level : 0;
}

uint32_t VmdkConfig::GetMinCompressRatio() const {
	uint32_t min_ratio;
	std::string key;
	StringDelimAppend(key, '.', {kCompression, kMinCompressRatio});
	auto rc = JsonConfig::GetKey(key, min_ratio);
	return rc ? min_ratio : 1;
}

void VmdkConfig::DisableEncryption() {
	std::string key;

	StringDelimAppend(key, '.', {kEncryption, kEnabled});
	JsonConfig::SetKey(key, false);
}

void VmdkConfig::ConfigureEncryption(const std::string& algo,
		const std::string& ekey) {
	std::string key;

	StringDelimAppend(key, '.', {kEncryption, kEnabled});
	JsonConfig::SetKey(key, true);

	StringDelimAppend(key, '.', {kEncryption, kEncryptionKey});
	JsonConfig::SetKey(key, ekey);

	StringDelimAppend(key, '.', {kEncryption, kEncryptionType});
	auto it = std::find(kEncryptionAlgos.begin(), kEncryptionAlgos.end(), algo);
	if (it == kEncryptionAlgos.end()) {
		throw std::invalid_argument("Invalid Encryption Argument");
	}
	JsonConfig::SetKey(key, algo);
}

bool VmdkConfig::IsEncryptionEnabled() const {
	std::string key;
	StringDelimAppend(key, '.', {kEncryption, kEnabled});

	bool enabled;
	auto rc = JsonConfig::GetKey(key, enabled);
	return rc and enabled;
}

std::string VmdkConfig::GetEncryptionType() const {
	std::string type;
	std::string key;
	StringDelimAppend(key, '.', {kEncryption, kEncryptionType});
	auto rc = JsonConfig::GetKey(key, type);
	if (not rc) {
		type = kEncryptionAlgos[0];
	}

	auto it = std::find(kEncryptionAlgos.begin(), kEncryptionAlgos.end(), type);
	if (it == kEncryptionAlgos.end()) {
		LOG(ERROR) << "Invalid Encryption Type Argument: " << type;
		throw std::invalid_argument("Invalid Encryption Type Argument");
	}
	return type;
}

std::string VmdkConfig::GetEncryptionKey() const {
	std::string key;
	StringDelimAppend(key, '.', {kEncryption, kEncryptionKey});

	std::string e;
	auto rc = JsonConfig::GetKey(key, e);
	if (not rc) {
		e.clear();
	}
	return e;
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

	return fp;
}

void VmdkConfig::DisableNetworkTarget() {
	std::string key;

	StringDelimAppend(key, '.', {kNetworkTarget, kEnabled});
	JsonConfig::SetKey(key, false);
}

bool VmdkConfig::IsNetworkTargetEnabled() const {
	std::string key;
	StringDelimAppend(key, '.', {kNetworkTarget, kEnabled});

	bool enabled;
	/* by default NetworkTarget is enabled */
	auto rc = JsonConfig::GetKey(key, enabled);
	if (rc == false) {
		return true;
	}

	return enabled;
}

void VmdkConfig::DisableFileTarget() {
	std::string key;

	StringDelimAppend(key, '.', {kFileTarget, kEnabled});
	JsonConfig::SetKey(key, false);
}

void VmdkConfig::ConfigureFileTarget(const std::string& file_path) {
	std::string key;

	StringDelimAppend(key, '.', {kFileTarget, kEnabled});
	JsonConfig::SetKey(key, true);

	StringDelimAppend(key, '.', {kFileTarget, kFileTargetPath});
	JsonConfig::SetKey(key, file_path);
}

void VmdkConfig::ConfigureFileTargetCreate(const bool& file_create) {
	std::string key;

	StringDelimAppend(key, '.', {kFileTarget, kFileTargetCreateFile});
	JsonConfig::SetKey(key, file_create);

}

void VmdkConfig::ConfigureFileTargetSize(const off_t& file_size) {
	std::string key;

	StringDelimAppend(key, '.', {kFileTarget, kFileTargetSize});
	JsonConfig::SetKey(key, file_size);
}

bool VmdkConfig::IsFileTargetEnabled() const {
	std::string key;
	StringDelimAppend(key, '.', {kFileTarget, kEnabled});

	bool enabled;
	auto rc = JsonConfig::GetKey(key, enabled);

	return rc and enabled;
}

std::string VmdkConfig::GetFileTargetPath() const {
	LOG(ERROR) << __func__ << "Called";
	std::string key;

	StringDelimAppend(key, '.', {kFileTarget, kFileTargetPath});
	std::string fp;

	auto rc = JsonConfig::GetKey(key, fp);

	if (not rc) {
		fp.clear();
	}

	return fp;
}

off_t VmdkConfig::GetFileTargetSize() const {
	LOG(ERROR) << __func__ << "Started";
	std::string key;
	StringDelimAppend(key, '.', {kFileTarget, kFileTargetSize});
	off_t size;
	auto rc = JsonConfig::GetKey(key, size);
	if (not rc) {
		return 0;
	}
	return size;
}

bool VmdkConfig::GetFileTargetCreate() const {
	LOG(ERROR) << __func__ << "Started";
	std::string key;
	StringDelimAppend(key, '.', {kFileTarget, kFileTargetCreateFile});
	bool create;
	auto rc = JsonConfig::GetKey(key, create);
	if (not rc) {
		return false;
	}
	return create;
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

	std::string type;
	auto rc = JsonConfig::GetKey(key, type);
	if (not rc) {
		return 0;
	}

	ErrorType t;
	std::istringstream iss(type);
	iss >> t;
	if (t == ErrorType::kThrow) {
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

bool VmdkConfig::SuccessHandlerCompressData() const {
	std::string key;
	StringDelimAppend(key, '.', {kSuccessHandler, kSuccessCompressEnabled});

	bool enabled{false};
	auto rc = JsonConfig::GetKey(key, enabled);
	return rc and enabled;
}

void VmdkConfig::SetSuccessHandlerCompressData() {
	std::string key;
	StringDelimAppend(key, '.', {kSuccessHandler, kSuccessCompressEnabled});
	JsonConfig::SetKey(key, true);
}

void VmdkConfig::SetSuccessHandlerDelay(int32_t delay) {
	std::string key;
	StringDelimAppend(key, '.', {kSuccessHandler, kDelay});
	JsonConfig::SetKey(key, delay);
}

int32_t VmdkConfig::GetSuccessHandlerDelay() const {
	std::string key;
	StringDelimAppend(key, '.', {kSuccessHandler, kDelay});

	int32_t rv;
	auto rc = JsonConfig::GetKey(key, rv);
	return rc ? rv : 0;
}

void VmdkConfig::SetTargetId(uint32_t target_id) {
	JsonConfig::SetKey(kTargetID, target_id);
}

bool VmdkConfig::GetTargetId(uint32_t& target_id) const {
	return JsonConfig::GetKey(kTargetID, target_id);
}

void VmdkConfig::SetLunId(uint32_t lun_id) {
	JsonConfig::SetKey(kLunID, lun_id);
}

bool VmdkConfig::GetLunId(uint32_t& lun_id) const {
	return JsonConfig::GetKey(kLunID, lun_id);
}

void VmdkConfig::SetDevPath(const std::string& dev_path) {
	return JsonConfig::SetKey(kDevPath, dev_path);
}

std::string VmdkConfig::GetDevPath() const {
	std::string dev_path;
	auto rc = JsonConfig::GetKey(kDevPath, dev_path);
	if (not rc) {
		dev_path.clear();
	}
	return dev_path;
}

void VmdkConfig::SetRamMetaDataKV() {
	JsonConfig::SetKey(kMetaDataKV, kRamMetaDataKV);
}

bool VmdkConfig::IsRamMetaDataKV() {
	std::string kv;
	auto rc = JsonConfig::GetKey(kMetaDataKV, kv);
	if (not rc) {
		return false;
	}

	return kv == kRamMetaDataKV;
}

void VmdkConfig::SetCleanupOnWrite(const bool& val) {
	JsonConfig::SetKey(kCleanupOnWrite, val);
}

bool VmdkConfig::GetCleanupOnWrite(bool& val) const {
	return JsonConfig::GetKey(kCleanupOnWrite, val);
}

void VmdkConfig::EnableReadAhead() {
	std::string key;
	StringDelimAppend(key, '.', {kReadAhead, kEnabled});
	JsonConfig::SetKey(key, true);
}

void VmdkConfig::DisableReadAhead() {
	std::string key;
	StringDelimAppend(key, '.', {kReadAhead, kEnabled});
	JsonConfig::SetKey(key, false);
}

bool VmdkConfig::IsReadAheadEnabled() const {
	std::string key;
	StringDelimAppend(key, '.', {kReadAhead, kEnabled});

	bool enabled{false};
	auto rc = JsonConfig::GetKey(key, enabled);
	return rc and enabled;
}

void VmdkConfig::SetDiskSize(int64_t size) {
    JsonConfig::SetKey(kDiskSizeBytes, size);
}

bool VmdkConfig::GetDiskSize(int64_t& size) const {
    return JsonConfig::GetKey(kDiskSizeBytes, size);
}

}}
