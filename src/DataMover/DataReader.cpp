#include <memory>

#include <folly/io/IOBuf.h>

#include "DataMoverCommonTypes.h"
#include "DataReader.h"
#include "RequestHandler.h"
#include "Request.h"

namespace pio {
using namespace ::ondisk;

DataReader::DataReader(RequestHandlerPtrVec::iterator src_begin,
		RequestHandlerPtrVec::iterator src_end,
		ActiveVmdk* vmdkp,
		const CheckPointID ckpt_id,
		void* bufferp,
		uint64_t offset,
		size_t size) noexcept :
			src_it_(std::move(src_begin)),
			src_end_(std::move(src_end)),
			vmdkp_(vmdkp),
			ckpt_id_(ckpt_id),
			bufferp_(bufferp),
			offset_(offset),
			size_(size) {
}

folly::Future<int> DataReader::Start() {
	try {
		request_ = std::make_unique<Request>(-1, vmdkp_, Request::Type::kRead,
			bufferp_, size_, size_, offset_);
		if (pio_unlikely(not request_)) {
			LOG(ERROR) << "allocating request to read failed";
			return -ENOMEM;
		}
	} catch (const std::exception& e) {
		return -ENOMEM;
	}

	auto failed = std::make_unique<RequestBlockPtrVec>();
	auto process = std::make_unique<RequestBlockPtrVec>();
	process->reserve(request_->NumberOfRequestBlocks());
	request_->ForEachRequestBlock([ckpt_id = this->ckpt_id_, &process]
			(RequestBlock* blockp) mutable {
		/* TODO: this will not work with Linked Clone */
		blockp->SetReadCheckPointId(ckpt_id);
		process->emplace_back(blockp);
		return true;
	});

	return ScheduleRead(std::move(process), std::move(failed))
	.then([this] (int rc) mutable {
		auto r = request_->Complete();
		if (pio_unlikely(rc < 0)) {
			return rc;
		}
		rc = r;
		read_complete_ = true;
		return rc;
	});
}

folly::Future<int> DataReader::ScheduleRead(
		std::unique_ptr<RequestBlockPtrVec> process,
		std::unique_ptr<RequestBlockPtrVec> failed) {
	read_started_ = true;
	if (pio_unlikely(process->empty())) {
		return 0;
	}
	return (*src_it_)->Read(vmdkp_, request_.get(), *process, *failed)
	.then([this, process = std::move(process), failed = std::move(failed)]
			(int rc) mutable -> folly::Future<int> {
		if (failed->empty()) {
			return rc;
		}
		return RecursiveRead(std::move(process), std::move(failed));
	});
}

folly::Future<int> DataReader::RecursiveRead(
		std::unique_ptr<RequestBlockPtrVec> process,
		std::unique_ptr<RequestBlockPtrVec> failed) {
	for (const auto blockp : *failed) {
		if (pio_unlikely(not blockp->IsReadMissed())) {
			LOG(ERROR) << "Reading block of data filed with error "
				<< blockp->GetResult();
			return -EIO;
		}
	}

	/* try next layer */
	if (pio_unlikely(++src_it_ == src_end_)) {
		LOG(ERROR) << "Read failed, reached end of the RequestHandler";
		return -ENODEV;
	}

	process->swap(*failed);
	failed->clear();
	return ScheduleRead(std::move(process), std::move(failed));
}

int DataReader::GetStatus() const noexcept {
	if (pio_unlikely(not read_started_ or not read_complete_)) {
		LOG(ERROR) << "Read either not started or not complete";
		return -EAGAIN;
	}
	return request_->GetResult();
}

std::unique_ptr<Request> DataReader::GetRequest() noexcept {
	return std::move(request_);
}

}
