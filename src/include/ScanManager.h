#pragma once

#include <unordered_map>
#include <string>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/MetaData_types.h"
#include "SpinLock.h"
#include "ScanInstance.h"

namespace pio {
class ScanManager {
public:
	int NewInstance(AeroClusterID id);
	ScanInstance* GetInstance(const AeroClusterID& id);
	void FreeInstance(const AeroClusterID& id);
	std::mutex lock_;
private:
	SpinLock mutex_;
	std::unordered_map<AeroClusterID, std::unique_ptr<ScanInstance>> instances_;
};
}
