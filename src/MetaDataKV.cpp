#include "MetaDataKV.h"
#include "DaemonTgtInterface.h"
#include <ctime>
#if 0
#define METARW_TEST 0
#endif

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

folly::Future<int> RamMetaDataKV::Delete(const MetaDataKey& key) {
	std::lock_guard<std::mutex> lock(mutex_);
	data_.erase(key);
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

AeroMetaDataKV::AeroMetaDataKV(ActiveVmdk* vmdkp): vmdkp_(vmdkp) {
	aero_obj_ = std::make_unique<AeroSpike>();
	aero_conn_ = pio::GetAeroConn(vmdkp);
}

AeroMetaDataKV::~AeroMetaDataKV() {

}

folly::Future<int> AeroMetaDataKV::Write(const MetaDataKey& key,
		const std::string& value) {

#ifdef METARW_TEST
	/* This is for testing */
	/* Call the AeroSpike metadata write function */
	/* Create a dummy large value here*/
	/* Write a pattern at the aligned offsets of KwriteBlock size */
	const int32_t kAeroWriteBlockSize = 1024 * 1024;
	std::string value1 (7340022,'1');
	uint64_t size = value1.size();
	uint64_t offset = 0;
	int count = 2;
	do {
		LOG(ERROR) << __func__ << "New offset::" << offset;
		char c = count;
		memset(((char *) value1.c_str()) + offset, c, 1);
		offset += kAeroWriteBlockSize;
		count++;
	} while(offset < size);

	(void) value;
	LOG(ERROR) << __func__ << "Write..";
	folly::Future<int> f = aero_obj_->AeroMetaWriteCmd(vmdkp_, key, value1, aero_conn_);
	f.wait();

	LOG(ERROR) << __func__ << "Trying to Read Back, should not see error";
	std::string value2;
	auto rc = aero_obj_->AeroMetaReadCmd(vmdkp_, key, value2, aero_conn_);
	if (!rc) {
		if((value1.compare(value2)) == 0) {
			LOG(ERROR) <<  "Equal.... ";
		} else {
			LOG(ERROR) <<  "Not equal.... ";
		}
	} else {
		LOG(ERROR) << __func__ << "Read failed";
		return rc;
	}

	/* Delete the block */
	LOG(ERROR) << __func__ << "Trying to Del record";
	aero_obj_->AeroMetaDelCmd(vmdkp_, key, aero_conn_);

	/* Try to read back, should be seeing error this time */
	LOG(ERROR) << __func__ << "Trying to Read Back, this time we should see error";
	rc = aero_obj_->AeroMetaReadCmd(vmdkp_, key, value2, aero_conn_);
	if (!rc) {
		if((value1.compare(value2)) == 0) {
			LOG(ERROR) <<  "Equal.... ";
		} else {
			LOG(ERROR) <<  "Not equal.... ";
		}
	} else {
		LOG(ERROR) << __func__ << "Read failed";
		return rc;
	}
	return 0;
#else
	return aero_obj_->AeroMetaWriteCmd(vmdkp_, key, value, aero_conn_);
#endif
}

folly::Future<MetaDataKV::ReadResult>
		AeroMetaDataKV::Read(const MetaDataKey& key) {

	/* Call the AeroSpike metadata read function */
	std::string value;
	auto rc = aero_obj_->AeroMetaReadCmd(vmdkp_, key, value, aero_conn_);
	return std::make_pair(std::string(), rc);
}

folly::Future<int> AeroMetaDataKV::Delete(const MetaDataKey& key) {
	/* Call the AeroSpike metadata delete function */
	auto rc = aero_obj_->AeroMetaDelCmd(vmdkp_, key, aero_conn_);
	return rc;
}

}
