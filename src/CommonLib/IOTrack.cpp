#include <unistd.h>

#include <iostream>

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "IOTrack.h"

#define IOLOG_FREQUENCY    60 //seconds

uint64_t g_iolog_frequency = IOLOG_FREQUENCY;
bool g_iolog_enabled = true;

namespace hyc {

void ReqTrack::Print(TrackTimePoint now) {
	auto dur_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_at);
	LOG(ERROR) << "req_id " << req_id <<
		      " req_offset " << req_offset <<
		      " req_size " << req_size <<
		      " req_type " << req_type <<
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
	auto rtrack = std::make_unique<ReqTrack>(reqid);
	rtrack->start_at = TrackClock::now();
	tracked_reqs_[reqid] = std::move(rtrack);
	++rstats_.n_arrived;
	if (not is_changed_) {
		is_changed_ = true;
	}
	return tracked_reqs_[reqid].get();
}

int DiskTrack::DelReq(uint64_t reqid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto reqi = tracked_reqs_.find(reqid);
	if (reqi == tracked_reqs_.end()) {
		LOG(ERROR) << "reqid " << reqid << " not found in DelReq" << std::endl;
		return -1;
	} else {
		LOG(ERROR) << "reqid " << reqid << " is deleted\n";
	}
	++rstats_.n_completed;
	rstats_.avg_latency.Add(reqi->second->GetLatency());	
	tracked_reqs_.erase(reqi);
	if (not is_changed_) {
		is_changed_ = true;
	}
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
	if (not is_changed_) {
		LOG(ERROR) << "no change for disk " << id_ << std::endl;
		return;
	}
	LOG(ERROR) << "iolog for disk " << id_ << std::endl;
	auto now = std::chrono::high_resolution_clock::now();
	for (auto & reqi : tracked_reqs_) {
		reqi.second->Print(now);
	}
	LOG(ERROR) << " req_arrived " << rstats_.n_arrived <<
		      " req_completed " << rstats_.n_completed <<
		      " avg_latency " << rstats_.avg_latency.Average() <<
		      std::endl;
	is_changed_ = false;
}

IoTrack::IoTrack(uint64_t freq) : monitor_freq_(freq) {
	monitor_ = std::thread(&IoMonitor, this);
	LOG(ERROR) << "iotrack inited" << std::endl;
}

IoTrack::~IoTrack() {
	shutdown_ = true;
	monitor_.join();
}

DiskTrack* IoTrack::AddDisk(std::string diskid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto diski = tracked_disks_.find(diskid);
	if (diski != tracked_disks_.end()) {
		LOG(ERROR) << "diskid " << diskid << " is getting reinserted" << std::endl;
	} else {
		LOG(ERROR) << "diskid " << diskid << " is getting added\n";
	}
	auto dtrack = std::make_unique<DiskTrack>(diskid);
	tracked_disks_[diskid] = std::move(dtrack);
	return tracked_disks_[diskid].get();
}

int IoTrack::DelDisk(std::string diskid) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto diski = tracked_disks_.find(diskid);
	if (diski == tracked_disks_.end()) {
		LOG(ERROR) << "diskid " << diskid << " not found in DelDisk" << std::endl;
		return -1;
	} else {
		LOG(ERROR) << "diskid " << diskid << " found for deldisk\n";
	}
	tracked_disks_.erase(diski);
	return 0;
}

DiskTrack* IoTrack::GetDisk(std::string diskid) {
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
	//const char *debug_file = "/root/hyc/io_debug";
	while (!shutdown_) {
		//if (access(debug_file, F_OK) == 0) {
		if (1) {
			LOG(ERROR) << "loop over disk monitor\n";
			g_iolog_enabled = true;
			std::lock_guard<std::mutex> lock(mutex_);
			for (auto & diski : tracked_disks_) {
				diski.second->Monitor();
			}	
		} else {
			LOG(ERROR) << "iolog_enabled is false\n";
			g_iolog_enabled = false;
		}
		std::this_thread::sleep_for(std::chrono::seconds(monitor_freq_));
		LOG(ERROR) << "wake up after sleep\n";
	}
	LOG(ERROR) << "Iotrack monitor thread exiting" << std::endl;
}

void IoTrack::IoMonitor(IoTrack *itrack) {
	itrack->IoMonitorLoop();
}

} // namespace hyc