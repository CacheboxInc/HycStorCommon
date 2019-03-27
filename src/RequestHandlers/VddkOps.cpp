#include <cerrno>
#include <string>
#include <memory>
#include <mutex>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "Vmdk.h"
#include "Request.h"
#include "VddkLib.h"

using namespace ::ondisk;

namespace pio {

class VddkWriteBatch {
public:
	VddkWriteBatch(const std::vector<RequestBlock*>& process) noexcept;
	folly::Future<int> Submit(ArmVddkFile* filep);
	void WriteComplete(VixError result);
private:
	const std::vector<RequestBlock*>& process_;
	mutable std::mutex mutex_;
	struct {
		uint16_t failed_{0};
		uint16_t submitted_{0};
		uint16_t complete_{0};
	} request_blocks_;

	int result_{0};
	folly::Promise<int> promise_;
};

VddkWriteBatch::VddkWriteBatch(const std::vector<RequestBlock*>& process)
		noexcept : process_(process) {
};

static void WriteCallBack(void *datap, VixError result) {
	auto batchp = (VddkWriteBatch*) datap;
	batchp->WriteComplete(result);
}

folly::Future<int> VddkWriteBatch::Submit(VddkFile* filep) {
	size_t submitted = 0;
	for (auto blockp : process_) {
		if (blockp->GetAlignedOffset() != blockp->Offset()) {
			LOG(ERROR) << "VddkTarget: VDDK does not accept unaligned IOs";
			result_ = -EINVAL;
			break;
		}

		auto buf = blockp->GetBufferAtBack();
		auto rc = filep->AsyncWrite(blockp->GetAlignedOffset(),
			buf->PayloadSize(), buf->Payload(), WriteCallBack, this);
		if (pio_unlikely(rc < 0)) {
			LOG(ERROR) << "VddkTarget: VDDK IO failed to submit " << rc;
			result_ = rc;
			break;
		}
		++submitted;
	}

	std::lock_guard<std::mutex> lock(mutex_);
	request_blocks_.submitted_ = submitted;
	if (request_blocks_.submitted_ == request_blocks_.complete_) {
		process_.setValue(result_);
	}
}

void VddkWriteBatch::WriteComplete(VixError result) {
	std::lock_guard<std::mutex> lock(mutex_);
	++request_blocks_.complete_;
	if (pio_unlikely(VIX_FAILED(result))) {
		++request_blocks_.failed_;
		result_ = VIX_ERROR_CODE(result);
		result_ = result_ < 0 ? result_ : -result_;
		LOG(ERROR) << "VddkTarget: VDDK IO failed " << result_;
	}

	if (request_blocks_.submitted_ == request_blocks_.complete_) {
		promise_.setValue(result_);
	}
}

folly::Future<int> VddkTarget::VddkWrite(ArmVddkFile* filep,
		const std::vector<RequestBlock*>& process) {
	auto batch = std::make_unique<VddkWriteBatch>(process);
	if (pio_unlikely(not batch)) {
		LOG(ERROR) << "VddkWriteBatch allocation failed";
		return -ENOMEM;
	}

	auto batchp = batch.get();

	return batchp->Submit(filep)
	.then([batch = std::move(batch)] (int rc) mutable {
		return rc;
	});
}

}
