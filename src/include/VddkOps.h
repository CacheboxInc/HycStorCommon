#pragma once

#include <vector>
#include <memory>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/MetaData_constants.h"
#include "DaemonCommon.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "VddkLibTarget.h"

namespace pio {

struct VddkWriteBatch;
struct VddkWriteRecord {
public:
	VddkWriteRecord(RequestBlock* blockp, VddkWriteBatch* batchp, ActiveVmdk *vmdkp);
	VddkWriteRecord(){};
	~VddkWriteRecord();

	RequestBlock *rq_block_{nullptr};
	VddkWriteBatch *batchp_{nullptr};
};

struct VddkWriteBatch {
	struct {
		std::mutex lock_;
		std::vector<std::unique_ptr<VddkWriteRecord>> recordsp_;
		std::vector<std::unique_ptr<VddkWriteRecord>>::iterator rec_it_;
		uint16_t nwrites_{0};
	} batch;

	std::unique_ptr<folly::Promise<int>> promise_{nullptr};
	ArmVddkFile *vddk_file_{nullptr};

	bool failed_{false};
};

class VddkTarget {
public:
	VddkTarget();
	~VddkTarget();
	std::shared_ptr<ArmVmVcConnection> conn_{nullptr};
public:
	folly::Future<int> VddkWrite(ActiveVmdk *vmdkp,
                const std::vector<RequestBlock*>& process,
                std::vector<RequestBlock *>& failed,
		std::shared_ptr<ArmVddkFile> vddk_file);

	int VddkWriteBatchInit(ActiveVmdk *vmdkp,
			const std::vector<RequestBlock*>& process,
			VddkWriteBatch *vw_batch_rec);

};
}
