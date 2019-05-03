#pragma once

#include "DaemonCommon.h"
#include <algorithm>
#include <chrono>
#include "AeroConn.h"
#include "AeroConfig.h"

namespace pio {

using scan_param = std::pair<::ondisk::VmdkID, ::ondisk::CheckPointID>;
class ScanInstance {
public:
	ScanInstance(AeroClusterID cluster_id);
	~ScanInstance();

	int StartScanThread();
	void Scanfn(void);
	int ScanStatus(ScanStats& scan_stats);
	int ScanTask();

	std::vector<scan_param> pending_list_;
	std::vector<scan_param> working_list_;
	AeroSpikeConn* aero_conn_{nullptr};
	std::unordered_map<uint64_t, ::ondisk::CheckPointID> vmdk_ckpt_map_;

private:
	std::chrono::steady_clock::time_point start_time_;
	uint64_t scan_id_;
	AeroClusterID cluster_id_;
};
}
