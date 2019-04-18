#pragma once

#include <map>
#include <string>
#include <utility>
#include <mutex>

#include <folly/futures/Future.h>
#include "AeroOps.h"

namespace pio {
class ActiveVmdk;

class MetaDataKV {
public:
	using ReadResult = std::pair<std::string, int32_t>;
	virtual ~MetaDataKV() = default;
	virtual folly::Future<int> Write(const MetaDataKey& key,
			const std::string& value) = 0;
	virtual folly::Future<ReadResult> Read(
		const MetaDataKey& key) = 0;
	virtual folly::Future<int> Delete(const MetaDataKey&) = 0;
};

class RamMetaDataKV : public MetaDataKV {
public:
	RamMetaDataKV();
	~RamMetaDataKV();
	virtual folly::Future<int> Write(const MetaDataKey& key,
		const std::string& value) override;
	virtual folly::Future<MetaDataKV::ReadResult>
		Read(const MetaDataKey& key) override;
	virtual folly::Future<int> Delete(const MetaDataKey&) override;
private:
	std::mutex mutex_;
	std::map<MetaDataKey, std::string> data_;
};

class AeroMetaDataKV : public MetaDataKV {
public:
	AeroMetaDataKV(ActiveVmdk* vmdkp);
	~AeroMetaDataKV();
	virtual folly::Future<int> Write(const MetaDataKey& key,
		const std::string& value) override;
	virtual folly::Future<MetaDataKV::ReadResult>
		Read(const MetaDataKey& key) override;
	virtual folly::Future<int> Delete(const MetaDataKey&) override;
private:
	ActiveVmdk*	vmdkp_;
	std::unique_ptr<AeroSpike> aero_obj_{nullptr};
	std::shared_ptr<AeroSpikeConn> aero_conn_;
};

}
