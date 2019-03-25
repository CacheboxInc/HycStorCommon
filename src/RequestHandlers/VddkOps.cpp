#include <cerrno>
#include <iterator>
#include <string>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "Vmdk.h"
#include "Request.h"
#include "VddkTargetLib.h"
#include "VddkOps.h"
#include "DaemonTgtInterface.h"

using namespace ::ondisk;

namespace pio {

int VddkTarget::VddkWriteBatchInit(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process, VddkWriteBatch *w_batch_rec) {
	w_batch_rec->batch.recordsp_.reserve(process.size());

	/* Create Batch write records */
	for (auto block : process) {
		auto rec = std::make_unique<VddkWriteRecord>(block, w_batch_rec, vmdkp);
		if (pio_unlikely(not rec)) {
			return -ENOMEM;
		}

		w_batch_rec->batch.recordsp_.emplace_back(std::move(rec));
	}

	w_batch_rec->batch.nwrites_ = process.size();
	w_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

folly::Future<int> WriteCallback<VixDiskLibCompletionCB>(VddkWriteBatch *batchp, VixError result) {
	batchp->promise_->setValue(0);
}

folly::Future<int> VddkTarget::VddkWriteBatchSubmit(VddkWriteBatch *batchp) {

	VddkWriteRecord  *wrp;
	uint16_t         nwrites;

	nwrites = batchp->batch.nwrites_;

	batchp->batch.rec_it_ = (batchp->batch.recordsp_).begin();
	wrp = batchp->batch.rec_it_->get();
	++batchp->batch.rec_it_;

	wrp->vddk_file->AsyncWrite(wrp->GetAlignedOffset(),
			wrp->PayloadSize(),
			wrp->Payload(),
			reinterpret_cast<VixDiskLibCompletionCB>WriteCallBack,
			reinterpret_cast<void *>batchp);

	return batchp->promise_->getFuture()
	.then([this, batchp] (int) mutable {
		return folly::makeFuture(int(batchp->failed_));
	});
}

folly::Future<int> VddkTarget::VddkWrite(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>&,
		std::shared_ptr<ArmVddkFile> vddk_file) {

	auto batch = std::make_unique<VddkWriteBatch>(vmdkp->GetID(), vmdkp->GetVM()->GetSetName());
	if (pio_unlikely(batch == nullptr)) {
		LOG(ERROR) << "VddkWriteBatch allocation failed";
		return -ENOMEM;
	}

	batch->batch.nwrites_ = process.size();
	batch->vddk_file_ = vddk_file; 
	log_assert(batch->vddk_file_ != nullptr);

	auto rc = VddkWriteBatchInit(vmdkp, process, batch.get());
	if (pio_unlikely(rc < 0)) {
		return rc;
	}

	return VddkWriteBatchSubmit(batch.get())
	.then([vmdkp, batch = std::move(batch)]
			(int rc) mutable {
			return rc;
	});
}

}
