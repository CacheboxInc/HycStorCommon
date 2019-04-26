#pragma once

#include <memory>
#include <unordered_map>
#include <string>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/MetaData_types.h"
#include "SpinLock.h"
#include "ThreadPool.h"
#include "FlushInstance.h"

struct _ha_instance;
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
	void DestroyInstance();
	int NewInstance(::ondisk::VmID vmid, const std::string& config);
	FlushInstance* GetInstance(const ::ondisk::VmID& vmid);
	void FreeInstance(const ::ondisk::VmID& vmid, int status);
	std::mutex lock_;

	void PopulateHistory(const ::ondisk::VmID& vmid, FlushInstance *fi, int status);
	void FreeHistory(const ::ondisk::VmID& vmid);
	struct instance_history {
		int status;
		int stage;
		uint64_t FlushedBlks{0};
		uint64_t MovedBlks{0};
		uint64_t RunDuration{0};
		uint64_t FlushDuration{0};
		uint64_t MoveDuration{0};
		uint64_t FlushBytes{0};
		uint64_t MoveBytes{0};
		std::chrono::system_clock::time_point StartedAt;
	};
	int GetHistory(const ::ondisk::VmID& vmid, void *history_param);
	std::multimap<::ondisk::VmID, instance_history> flush_history_;
private:
	SpinLock mutex_;
	std::unordered_map<::ondisk::VmID, std::unique_ptr<FlushInstance>> instances_;
};
}
