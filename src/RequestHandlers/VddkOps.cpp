#include <cerrno>
#include <string>
#include <memory>
#include <mutex>

#include <boost/range.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/algorithm_ext.hpp>
#include <boost/range/any_range.hpp>
#include <boost/range/adaptors.hpp>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "Vmdk.h"
#include "Request.h"
#include "VddkLib.h"
#include "VddkOps.h"

using namespace ::ondisk;

using boost::adaptors::transformed;
using vddk::VddkFile;

namespace pio {
class VddkTarget;

class VddkWriteBatch {
public:
	VddkWriteBatch(const std::vector<RequestBlock*>& process) noexcept;
	folly::Future<int> Submit(VddkFile* filep);
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
#if 1
	filep->ScheduleWrite([this] () {
		return process_ | transformed(
				[this] (RequestBlock* blockp) {
					auto bufferp = blockp->GetRequestBufferAtBack();
					VddkFile::IO io;
					io.offset = blockp->GetAlignedOffset();
					io.size = bufferp->PayloadSize();
					io.bufferp = reinterpret_cast<uint8_t*>(bufferp->Payload());
					io.cbp = WriteCallBack;
					io.datap = this;
					return io;
				}
			);
		}
	);
	submitted = process_.size();
#else
	for (auto blockp : process_) {
		if (blockp->GetAlignedOffset() != blockp->GetOffset()) {
			LOG(ERROR) << "VddkTarget: VDDK does not accept unaligned IOs";
			result_ = -EINVAL;
			break;
		}

		auto buf = blockp->GetRequestBufferAtBack();
		auto rc = filep->AsyncWrite(blockp->GetAlignedOffset(),
			buf->PayloadSize(), (uint8*)buf->Payload(), WriteCallBack, this);
		if (pio_unlikely(rc < 0)) {
			LOG(ERROR) << "VddkTarget: VDDK IO failed to submit " << rc;
			result_ = rc;
			break;
		}
		++submitted;
	}
#endif

	std::lock_guard<std::mutex> lock(mutex_);
	request_blocks_.submitted_ = submitted;
	if (not submitted or request_blocks_.submitted_ == request_blocks_.complete_) {
		promise_.setValue(result_);
	}
	return promise_.getFuture();
}

void VddkWriteBatch::WriteComplete(VixError result) {
	bool complete = false;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		++request_blocks_.complete_;
		if (pio_unlikely(VIX_FAILED(result))) {
			++request_blocks_.failed_;
			result_ = VIX_ERROR_CODE(result);
			result_ = result_ < 0 ? result_ : -result_;
			LOG(ERROR) << "VddkTarget: VDDK IO failed " << result_;
		}
		complete = request_blocks_.submitted_ == request_blocks_.complete_;
	}


	if (complete) {
		promise_.setValue(result_);
	}
}

folly::Future<int> VddkTarget::VddkWrite(VddkFile* filep,
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
