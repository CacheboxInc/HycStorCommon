#pragma once

#include <mutex>
#include <memory>
#include <thread>

using TrackClock = std::chrono::high_resolution_clock;
using TrackTimePoint = TrackClock::time_point;

namespace hyc {

struct ReqStats {
public:
	std::atomic<uint64_t> n_arrived{0};
	std::atomic<uint64_t> n_completed{0};
	MovingAverage<uint64_t, 128> avg_latency{};
};

struct DiskStats {
public:
};

struct ReqTrack {
public:
	uint64_t req_id{0};
	TrackTimePoint start_at{0};	
	uint64_t req_offset{0};
	uint64_t req_size{0};

public:
	void Print(uint64_t now);
	uint64_t GetLatency();
};

class DiskTrack {
public:
	ReqTrack* AddReq(uint64_t reqid);
	int DelReq(uint64_t reqid);
	ReqTrack* GetReq(uint64_t reqid);

	void Monitor();

private:
	mutable std::mutex mutex_;
	std::unordered_map<uint64_t, std::unique_ptr<ReqTrack>> tracked_reqs_;
	ReqStats rstats_;
};

class IoTrack {
public:
	IoTrack(uint64_t freq);
	~IoTrack();

	DiskTrack* AddDisk(uint64_t diskid);
	int DelDisk(uint64_t diskid);
	DiskTrack* GetDisk(uint64_t diskid);

	static void IoMonitor(IoTrack *itrack);
	void IoMonitorLoop();

private:
	mutable std::mutex mutex_;
	std::unordered_map<uint64_t, std::unique_ptr<DiskTrack>> tracked_disks_;
	std::thread monitor_;
	uint64_t monitor_freq_{0};
	bool shutdown_{false};
};

} // namespace hyc
