#pragma once

#include <memory>
#include <unordered_map>
#include <string>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/MetaData_types.h"
#include "SpinLock.h"
#include "ThreadPool.h"
#include "CkptMergeInstance.h"

struct _ha_instance;
namespace pio {
class CkptMergeManager {
public:
	struct threadpool {
		std::once_flag initialized_;
		std::unique_ptr<ThreadPool> pool_;
	} threadpool_;

	int InitCkptMergeManager();
	void DeinitCkptMergeManager();
	int CreateInstance(struct _ha_instance *);
	void DestroyInstance();
	int NewInstance(::ondisk::VmID vmid);
	CkptMergeInstance* GetInstance(const ::ondisk::VmID& vmid);
	void FreeInstance(const ::ondisk::VmID& vmid);
	std::mutex lock_;

private:
	SpinLock mutex_;
	std::unordered_map<::ondisk::VmID, std::unique_ptr<CkptMergeInstance>> instances_;
};
}
