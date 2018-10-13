#pragma once

#include <string>
#include <vector>
#include <map>

#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "JsonConfig.h"

namespace pio { namespace config {
class VmdkConfig : public JsonConfig {
public:
	VmdkConfig(const std::string& config);
	VmdkConfig();
	void SetVmdkId(const ::ondisk::VmdkID& id);
	bool GetVmdkId(::ondisk::VmdkID& id) const;
	void SetParentDiskVmdkId(const ::ondisk::VmdkID& id);
	bool GetParentDiskVmdkId(::ondisk::VmdkID& id) const;
	void SetVmId(const ::ondisk::VmID& id);
	bool GetVmId(::ondisk::VmID& id) const;
	void SetBlockSize(uint32_t size);
	bool GetBlockSize(uint32_t& size) const;
	void SetParentDisk(const std::string& parent_name);
	bool GetParentDisk(std::string& parent_name);

	void DisableCompression();
	void ConfigureCompression(const std::string& algo, uint16_t level);
	bool IsCompressionEnabled() const;
	std::string GetCompressionType() const;
	uint32_t GetMinCompressRatio() const;
	uint16_t GetCompressionLevel() const;

	void DisableEncryption();
	void ConfigureEncryption(const std::string& algo, const std::string& ekey);
	bool IsEncryptionEnabled() const;
	std::string GetEncryptionKey() const;
	std::string GetEncryptionType() const;


	void DisableRamCache();
	void ConfigureRamCache(uint16_t size_mb);
	bool IsRamCacheEnabled() const;
	uint16_t GetRamCacheMemoryLimit() const;

	void DisableNetworkTarget();
	bool IsNetworkTargetEnabled() const;

	void DisableFileCache();
	void DisableFileTarget();
	void ConfigureFileCache(const std::string& file_path);
	void ConfigureFileTarget(const std::string& file_path);
	bool IsFileCacheEnabled() const;
	bool IsFileTargetEnabled() const;
	std::string GetFileCachePath() const;
	std::string GetFileTargetPath() const;
	off_t GetFileTargetSize() const;
	bool GetFileTargetCreate() const;
	void ConfigureFileTargetSize(const off_t& file_size);
	void ConfigureFileTargetCreate(const bool& file_create);
	void SetCleanupOnWrite(const bool& val);
	bool GetCleanupOnWrite(bool& val) const;

	enum class ErrorType {
		kThrow,
		kReturnError,
	};
	bool ErrorHandlerEnabled() const;
	bool ErrorHandlerShouldThrow() const;
	int ErrorHandlerReturnValue() const;
	uint32_t ErrorHandlerFrequency() const;
	void DisableErrorHandler();
	void ConfigureErrorHandler(ErrorType type, uint32_t frequency, int error);

	bool IsSuccessHandlerEnabled() const;
	void EnableSuccessHandler();
	void DisableSuccessHandler();
	void SetSuccessHandlerDelay(int32_t delay);
	int32_t GetSuccessHandlerDelay() const;

	void SetTargetId(uint32_t target_id);
	bool GetTargetId(uint32_t& target_id) const;
	void SetLunId(uint32_t lun_id);
	bool GetLunId(uint32_t& lun_id) const;
	void SetDevPath(const std::string& dev_path);
	std::string GetDevPath() const;

	void SetRamMetaDataKV();
	bool IsRamMetaDataKV();
public:
	static const std::string kEnabled;
	static const std::string kVmdkID;
	static const std::string kVmID;
	static const std::string kBlockSize;

	static const std::string kCompression;
	static const std::string kCompressionType;
	static const std::string kCompressionLevel;
	static const std::string kMinCompressRatio;
	static const std::vector<std::string> kCompressAlgos;

	static const std::string kEncryption;
	static const std::string kEncryptionType;
	static const std::string kEncryptionKey;
	static const std::vector<std::string> kEncryptionAlgos;

	static const std::string kRamCache;
	static const std::string kRamCacheMemoryInMB;

	static const std::string kFileCache;
	static const std::string kFileCachePath;

	static const std::string kNetworkTarget;

	static const std::string kFileTarget;
	static const std::string kFileTargetPath;
	static const std::string kFileTargetSize;
	static const std::string kFileTargetCreateFile;

	static const std::string kErrorHandler;
	static const std::string kErrorType;
	static const std::string kReturnValue;
	static const std::string kFrequency;
	static const std::map<ErrorType, std::string> kErrorToString;

	static const std::string kSuccessHandler;
	static const std::string kDelay;

	static const std::string kTargetID;
	static const std::string kLunID;
	static const std::string kDevPath;
	static const std::string kRamMetaDataKV;
	static const std::string kAeroMetaDataKV;
	static const std::string kMetaDataKV;
	static const std::string kParentDiskName;
	static const std::string kParentDiskVmdkID;
	static const std::string kCleanupOnWrite;
};

std::ostream& operator <<(std::ostream& os, const VmdkConfig::ErrorType& type);
std::istream& operator >>(std::istream& in, VmdkConfig::ErrorType& type);
}
}
