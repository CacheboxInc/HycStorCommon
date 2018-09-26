#pragma once

#include <memory>
#include <unordered_map>
#include <string>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/MetaData_types.h"
#include "SpinLock.h"
#include "ThreadPool.h"
#include "FlushInstance.h"
#include "halib.h"

namespace pio {
class FlushManager {
public:
	struct threadpool {
		std::once_flag initialized_;
		std::unique_ptr<ThreadPool> pool_;
	} threadpool_;

	int InitFlushManager();
	void DeinitFlushManager();
	int CreateInstance(struct _ha_instance *);
	int NewInstance(::ondisk::VmID vmid, const std::string& config);
	FlushInstance* GetInstance(const ::ondisk::VmID& vmid);
	void FreeInstance(const ::ondisk::VmID& vmid);
	std::mutex lock_;
private:
	SpinLock mutex_;
	std::unordered_map<::ondisk::VmID, std::unique_ptr<FlushInstance>> instances_;
};
}