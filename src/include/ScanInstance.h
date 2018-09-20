#pragma once

#include "DaemonCommon.h"
#include <algorithm>
#include <chrono>
#include "AeroConn.h"

namespace pio {

class ScanInstance {
public:
	ScanInstance(AeroClusterID cluster_id);
	~ScanInstance();

	int StartScanThread();
	void Scanfn(void);
	int ScanStatus(ScanStats& scan_stats);
	int ScanTask();

	std::vector<uint64_t> pending_list_;
	std::vector<uint64_t> working_list_;
	AeroSpikeConn* aero_conn_{nullptr};

private:
	std::chrono::steady_clock::time_point start_time_;
	uint64_t scan_id_;
	AeroClusterID cluster_id_;
};
}
