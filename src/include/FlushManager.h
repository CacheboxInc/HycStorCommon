#pragma once

#include <memory>
#include <unordered_map>
#include <string>

#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "DaemonTgtTypes.h"
#include "DaemonCommon.h"
#include "SpinLock.h"
#include "ThreadPool.h"
#include "FlushInstance.h"

namespace pio {
class FlushManager {
public:
	struct threadpool {
		std::once_flag initialized_;
		std::unique_ptr<ThreadPool> pool_;
	} threadpool_;

	int InitFlushManager();
	void DeinitFlushManager();
	int CreateInstance();
	int NewInstance(VmID vmid);
	FlushInstance* GetInstance(const VmID& vmid);
	void FreeInstance(const VmID& vmid);
private:
	SpinLock mutex_;
	std::unordered_map<VmID, std::unique_ptr<FlushInstance>> instances_;
};
}
