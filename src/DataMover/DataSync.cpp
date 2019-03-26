#include <numeric>

#include <glog/logging.h>

#include "CommonMacros.h"
#include "DataSync.h"
#include "DataCopier.h"
#include "Vmdk.h"

namespace pio {
DataSync::DataSync(ActiveVmdk* vmdkp,
		const uint8_t nckpts_per_copy,
		const size_t max_io_size) noexcept :
			vmdkp_(vmdkp),
			kCkptPerCopy(nckpts_per_copy),
			kMaxIOSize(max_io_size) {
	SetStatus(0);
}

void DataSync::SetDataSource(RequestHandlerPtrVec source) {
	if (pio_unlikely(not data_source_.empty())) {
		constexpr char msg[] = "DataSync: data source already set";
		LOG(ERROR) << msg;
		throw std::runtime_error(msg);
	} else if (pio_unlikely(source.empty())) {
		constexpr char msg[] = "DataSync: data source empty";
		LOG(ERROR) << msg;
		throw std::runtime_error(msg);
	}
	data_source_.swap(source);
}

void DataSync::SetDataDestination(RequestHandlerPtrVec dest) {
	if (pio_unlikely(not data_dest_.empty())) {
		constexpr char msg[] = "DataSync: data destination already set";
		LOG(ERROR) << msg;
		throw std::runtime_error(msg);
	} else if (pio_unlikely(dest.empty())) {
		constexpr char msg[] = "DataSync: data destination empty";
		LOG(ERROR) << msg;
		throw std::runtime_error(msg);
	}
	data_dest_.swap(dest);
}

uint64_t DataSync::BlocksPending() const noexcept {
	return std::accumulate(check_points_.begin(),
		check_points_.end(),
		static_cast<uint64_t>(0),
		[] (uint64_t init, const CheckPoint* ckptp) {
			const auto& bitmap = ckptp->GetRoaringBitMap();
			return init + bitmap.cardinality();
		}
	);
}

DataSync::Stats DataSync::GetStats() const noexcept {
	DataSync::Stats rc;
	DataCopier::Stats cs;
	if (copier_) {
		cs = copier_->GetStats();
	}

	auto blocks_not_scheduled = BlocksPending();
	rc.sync_total = stats_.sync_total + blocks_not_scheduled;
	rc.sync_pending = blocks_not_scheduled;
	rc.sync_completed = stats_.sync_completed + cs.copy_completed;
	rc.sync_avoided = stats_.sync_avoided +  cs.copy_avoided;

	rc.cbt_sync_done = ckpt_.done_;
	rc.cbt_sync_in_progress = cs.cbt_in_progress;
	rc.cbt_sync_scheduled = ckpt_.last_;

	rc.sync_stopped = status_.stoppped_;
	rc.sync_failed = status_.failed_;
	return rc;
}

void DataSync::UpdateDataCopierStats(std::unique_ptr<DataCopier> copier) noexcept {
	if (pio_unlikely(not copier)) {
		return;
	}
	const auto cs = copier->GetStats();
	stats_.sync_total += cs.copy_total;
	stats_.sync_pending = 0;
	stats_.sync_completed += cs.copy_completed;
	stats_.sync_avoided += cs.copy_avoided;

	stats_.cbt_sync_scheduled = ckpt_.done_;
	stats_.cbt_sync_in_progress = 0;
	stats_.cbt_sync_scheduled = ckpt_.last_;

	stats_.sync_stopped = status_.stoppped_;
	stats_.sync_failed = status_.failed_;
}

int DataSync::SetCheckPoints(CheckPointPtrVec check_points, bool* restartp) {
	*restartp = false;
	if (pio_unlikely(check_points.empty())) {
		return 0;
	}

	SortCheckPoints(check_points);
	auto it = std::adjacent_find(check_points.begin(), check_points.end(),
		[] (const CheckPoint* currp, const CheckPoint* nextp) {
			return not (currp->ID() + 1 == nextp->ID());
		}
	);
	if (pio_unlikely(it != check_points.end())) {
		LOG(ERROR) << "DataSync: list of CBTs are not consecutive";
		return -EINVAL;
	}

	std::lock_guard<std::mutex> lock(mutex_);
	if (check_points.front()->ID() != ckpt_.last_ + 1) {
		LOG(ERROR) << "DataSync: CBTs are not consecutive "
			<< " Previous last CBT ID " << ckpt_.last_
			<< " New CBT ID " << check_points.front()->ID();
		return -EINVAL;
	}

	std::copy(check_points.begin(), check_points.end(),
		std::back_inserter(check_points_));
	if (not status_.stoppped_) {
		log_assert(copier_);
		LOG(INFO) << "DataSync: is in progress "
			<< " updated end CBT from " << ckpt_.last_
			<< " to " << check_points_.back()->ID();
		ckpt_.last_ = check_points_.back()->ID();
		return 0;
	}

	*restartp = true;
	ckpt_.last_ = check_points_.back()->ID();
	return 0;
}

int DataSync::ReStart() {
	return Start();
}

int DataSync::Start() {
	if (pio_unlikely(data_source_.empty() or
			data_dest_.empty())) {
		LOG(ERROR) << "DataSync: not initialized properly "
			<< " not starting sync";
		status_.stoppped_ = true;
		SetStatus(-ENODEV);
		return -ENODEV;
	}

	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (not status_.stoppped_) {
			return 0;
		}
		status_.stoppped_ = false;
		SetStatus(0);
	}
	return StartInternal();
}

int DataSync::StartInternal() {
	if (pio_likely(not status_.failed_)) {
		UpdateDataCopierStats(std::move(copier_));
	}
	copier_ = nullptr;
	log_assert(not copier_);

	{
		std::lock_guard<std::mutex> lock(mutex_);
		while (not check_points_.empty() and check_points_.front()->ID() <= ckpt_.done_) {
			check_points_.pop_front();
		}
		if (pio_unlikely(status_.failed_)) {
			status_.stoppped_ = true;
			return status_.res_;
		}
		if (check_points_.empty()) {
			LOG(INFO) << "DataSync: completed successfully "
				<< " CBT last synced " << ckpt_.done_;
			status_.stoppped_ = true;
			return 0;
		}
		status_.stoppped_ = false;
		SetStatus(0);
	}

	int rc;
	copier_ = NewDataCopier(&rc);
	if (pio_unlikely(not copier_ or rc < 0)) {
		SetStatus(rc);
		return rc;
	}

	copier_->Begin()
	.then([this] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			LOG(ERROR) << "DataSync failed: with error " << rc;
			SetStatus(rc);
			return StartInternal();
		}

		SetStatus(0);
		ckpt_.done_ = ckpt_.scheduled_;
		return StartInternal();
	});
	return 0;
}

std::unique_ptr<DataCopier> DataSync::NewDataCopier(int* errnop) {
	*errnop = 0;
	auto copier = std::make_unique<DataCopier>(vmdkp_, vmdkp_->BlockShift(), kMaxIOSize);
	if (pio_unlikely(not copier)) {
		*errnop = -ENOMEM;
		LOG(ERROR) << "DataSync failed: to start data copy";
		return copier;
	}

	copier->SetDataSource(data_source_.begin(), data_source_.end());
	copier->SetDataDestination(data_dest_.begin(), data_dest_.end());

	copier->SetCheckPoints(GetNextCheckPointsToSync());
	return copier;
}

CheckPointPtrVec DataSync::GetNextCheckPointsToSync() {
	CheckPointPtrVec sync;

	std::lock_guard<std::mutex> lock(mutex_);
	size_t count = kCkptPerCopy == 0 ?
			check_points_.size() :
			std::min(kCkptPerCopy, check_points_.size());

	sync.reserve(count);
	auto begin = check_points_.begin();
	std::copy(begin, std::next(begin, count), std::back_inserter(sync));

	log_assert(ckpt_.done_ + 1 == sync.front()->ID());
	ckpt_.scheduled_ = sync.back()->ID();
	LOG(INFO) << "Scheduling DataCopier till CBT " << ckpt_.scheduled_;
	return sync;
}

void DataSync::SetStatus(int res) noexcept {
	status_.failed_ = res < 0;
	status_.res_ = res;
}

void DataSync::GetStatus(bool* is_stopped, int* resp) const noexcept {
	*is_stopped = status_.stoppped_;
	*resp = status_.res_;
}

void DataSync::SortCheckPoints(CheckPointPtrVec& check_points) const {
	struct {
		bool operator () (const CheckPoint* lp, const CheckPoint* rp) const {
			return lp->ID() < rp->ID();
		}
	} Less;
	if (not std::is_sorted(check_points.begin(), check_points.end(), Less)) {
		std::sort(check_points.begin(), check_points.end(), Less);
	}
}

}
