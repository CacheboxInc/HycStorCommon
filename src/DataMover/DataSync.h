#pragma once

#include <memory>
#include <deque>
#include <vector>
#include <mutex>

#include "DataMoverCommonTypes.h"
#include "gen-cpp2/MetaData_constants.h"

namespace pio {

class ActiveVmdk;
class DataCopier;

class DataSync {
public:
	DataSync(DataSync&& rhs) = delete;
	DataSync(const DataSync&) = delete;
	DataSync& operator == (const DataSync&) = delete;
	DataSync& operator == (DataSync&&) = delete;

	DataSync(ActiveVmdk*,
		const uint8_t nckpts_per_copy = 0,
		const size_t max_io_size = 1ul << 20) noexcept;
	void SetDataSource(RequestHandlerPtrVec source);
	void SetDataDestination(RequestHandlerPtrVec dest);
	int SetCheckPoints(CheckPointPtrVec check_points, bool* restartp);

	int ReStart();
	int Start();
	void GetStatus(bool* is_stopped, int* resp) const noexcept;

private:
	int StartInternal();
	void SetStatus(int res) noexcept;

	std::unique_ptr<DataCopier> NewDataCopier(int *errnop);
	CheckPointPtrVec GetNextCheckPointsToSync();
	void SortCheckPoints(CheckPointPtrVec& check_points) const;
private:
	ActiveVmdk* vmdkp_{};
	const size_t kCkptPerCopy{0};
	const size_t kMaxIOSize{1ul << 20};

	RequestHandlerPtrVec data_source_;
	RequestHandlerPtrVec data_dest_;

	std::mutex mutex_;
	std::unique_ptr<DataCopier> copier_;

	std::deque<const CheckPoint*> check_points_;

	struct {
		::ondisk::CheckPointID last_ = ::ondisk::MetaData_constants::kInvalidCheckPointID();
		::ondisk::CheckPointID scheduled_ = ::ondisk::MetaData_constants::kInvalidCheckPointID();
		::ondisk::CheckPointID done_ = ::ondisk::MetaData_constants::kInvalidCheckPointID();
	} ckpt_;

	struct {
		bool stoppped_{true};
		bool failed_{false};
		int res_{0};
	} status_;
};
}
