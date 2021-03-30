#include <iostream>

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "IOTrack.h"

namespace hyc {

void ReqTrack::Print(TrackTimePoint now) {
	auto dur_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_at);
	LOG(ERROR) << "req_id " << req_id <<
		      " req_offset " << req_offset <<
		      " req_size " << req_size <<
		      " elapsed_time(ms) " << dur_ms.count() << 
		      std::endl;
}

uint64_t ReqTrack::GetLatency() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(TrackClock::now() - start_at).count();
}

ReqTrack* DiskTrack::AddReq(uint64_t reqid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto reqi = tracked_reqs_.find(reqid);
	if (reqi != tracked_reqs_.end()) {
		LOG(ERROR) << "reqid " << reqid << " is getting reinserted" << std::endl;
	}
	auto rtrack = std::make_unique<ReqTrack>();
	rtrack->req_id = reqid;
	rtrack->start_at = TrackClock::now();
	tracked_reqs_[reqid] = std::move(rtrack);
	++rstats_.n_arrived;
	return rtrack.get();
}

int DiskTrack::DelReq(uint64_t reqid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto reqi = tracked_reqs_.find(reqid);
	if (reqi == tracked_reqs_.end()) {
		LOG(ERROR) << "reqid " << reqid << " not found in DelReq" << std::endl;
		return -1;
	}
	++rstats_.n_completed;
	rstats_.avg_latency.Add(reqi->second->GetLatency());	
	tracked_reqs_.erase(reqi);
	return 0;
}

ReqTrack* DiskTrack::GetReq(uint64_t reqid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto reqi = tracked_reqs_.find(reqid);
	if (reqi != tracked_reqs_.end()) {
		return reqi->second.get();
	}
	LOG(ERROR) << "reqid " << reqid << " not found in GetReq" << std::endl;
	return nullptr;
}

void DiskTrack::Monitor() {
	std::lock_guard<std::mutex> lock(mutex_);
	auto now = std::chrono::high_resolution_clock::now();
	for (auto & reqi : tracked_reqs_) {
		reqi.second->Print(now);
	}	
}

IoTrack::IoTrack(uint64_t freq) : monitor_freq_(freq) {
	monitor_ = std::thread(&IoMonitor, this);
}

IoTrack::~IoTrack() {
	shutdown_ = true;
	monitor_.join();
}

DiskTrack* IoTrack::AddDisk(uint64_t diskid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto diski = tracked_disks_.find(diskid);
	if (diski != tracked_disks_.end()) {
		LOG(ERROR) << "diskid " << diskid << " is getting reinserted" << std::endl;
	}
	auto vtrack = std::make_unique<DiskTrack>();
	tracked_disks_[diskid] = std::move(vtrack);
	return vtrack.get();
}

int IoTrack::DelDisk(uint64_t diskid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto diski = tracked_disks_.find(diskid);
	if (diski == tracked_disks_.end()) {
		LOG(ERROR) << "diskid " << diskid << " not found in DelDisk" << std::endl;
		return -1;
	}
	tracked_disks_.erase(diski);
	return 0;
}

DiskTrack* IoTrack::GetDisk(uint64_t diskid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto diski = tracked_disks_.find(diskid);
	if (diski != tracked_disks_.end()) {
		return diski->second.get();
	}
	LOG(ERROR) << "diskid " << diskid << " not found in GetDisk" << std::endl;
	return nullptr;
}

void IoTrack::IoMonitorLoop() {
	LOG(ERROR) << "Iotrack monitor thread starting" << std::endl;
	while (!shutdown_) {
		{
			std::lock_guard<std::mutex> lock(mutex_);
			for (auto & diski : tracked_disks_) {
				diski.second->Monitor();
			}	
		}
		std::this_thread::sleep_for(std::chrono::seconds(monitor_freq_));
	}
	LOG(ERROR) << "Iotrack monitor thread exiting" << std::endl;
}

void IoTrack::IoMonitor(IoTrack *itrack) {
	itrack->IoMonitorLoop();
}

} // namespace hyc
