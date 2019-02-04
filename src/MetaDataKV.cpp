#include "MetaDataKV.h"

namespace pio {

RamMetaDataKV::RamMetaDataKV() {

}

RamMetaDataKV::~RamMetaDataKV() {

}

folly::Future<int> RamMetaDataKV::Write(const MetaDataKey& key,
		const std::string& value) {
	std::lock_guard<std::mutex> lock(mutex_);
	data_.insert(std::make_pair(key, value));
	return 0;
}

folly::Future<MetaDataKV::ReadResult>
		RamMetaDataKV::Read(const MetaDataKey& key) {
	std::lock_guard<std::mutex> lock(mutex_);
	if (auto it = data_.find(key); it != data_.end()) {
		return std::make_pair(it->second, 0);
	}
	return std::make_pair(std::string(), -ENOENT);
}

AeroMetaDataKV::AeroMetaDataKV() {

}

AeroMetaDataKV::~AeroMetaDataKV() {

}

folly::Future<int> AeroMetaDataKV::Write(const MetaDataKey&,
		const std::string&) {
	return 0;
}

folly::Future<MetaDataKV::ReadResult>
		AeroMetaDataKV::Read(const MetaDataKey&) {
	return std::make_pair(std::string(), -ENOENT);
}

}
