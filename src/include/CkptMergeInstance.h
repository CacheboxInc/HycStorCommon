#pragma once

#include "DaemonCommon.h"
#include <algorithm>
#include <chrono>
#include "AeroConn.h"
#include "IDs.h"

namespace pio {

enum class MergeStageType {
                kBitmapMergeStage,
                kDataMergeStage,
                kDataDeleteStage,
        };

class CkptMergeInstance {
public:
	CkptMergeInstance(::ondisk::VmID vmid);
	~CkptMergeInstance();

	int CkptMergeStatus(CkptMergeStats& merge_stats);
	int StartCkptMerge(const VmID& vmid, const CheckPointID&);

private:
	::ondisk::VmID vmid_;
	std::chrono::steady_clock::time_point start_time_;
};
}
