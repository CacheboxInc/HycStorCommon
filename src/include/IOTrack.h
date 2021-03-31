#pragma once

#include <mutex>
#include <memory>
#include <thread>

#include <atomic>
#include <unordered_map>

#include "MovingAverage.h"

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

#define  REQ_UNKNOWN      0
#define  REQ_READ         1
#define  REQ_WRITE        2
#define  REQ_WRITESAME    3
#define  REQ_TRUNCATE     4
#define  REQ_SYNC         5

struct ReqTrack {
public:
	ReqTrack(uint64_t reqid) : req_id(reqid) {};
	~ReqTrack() = default;
 
	ReqTrack(const ReqTrack& rhs) = delete;
	ReqTrack(ReqTrack&& rhs) = delete;

public:
	uint64_t req_id{0};
	TrackTimePoint start_at;	
	uint64_t req_offset{0};
	uint64_t req_size{0};
	uint32_t req_type{0};

public:
	void Print(TrackTimePoint now);
	uint64_t GetLatency();
};

class DiskTrack {
public:
	DiskTrack(std::string diskid) : id_(diskid) {}
	~DiskTrack() = default;

	ReqTrack* AddReq(uint64_t reqid);
	int DelReq(uint64_t reqid);
	ReqTrack* GetReq(uint64_t reqid);
	std::string GetId() { return id_; }

	void Monitor();

private:
	mutable std::mutex mutex_;
	std::unordered_map<uint64_t, std::unique_ptr<ReqTrack>> tracked_reqs_;
	ReqStats rstats_;
	bool is_changed_{false};
	std::string id_{0};
};

class IoTrack {
public:
	IoTrack(uint64_t freq);
	~IoTrack();

	DiskTrack* AddDisk(std::string diskid);
	int DelDisk(std::string diskid);
	DiskTrack* GetDisk(std::string diskid);

	static void IoMonitor(IoTrack *itrack);
	void IoMonitorLoop();

private:
	mutable std::mutex mutex_;
	std::unordered_map<std::string, std::unique_ptr<DiskTrack>> tracked_disks_;
	std::thread monitor_;
	uint64_t monitor_freq_{0};
	bool shutdown_{false};
};

} // namespace hyc
