#include "DataMoverCommonTypes.h"
#include "DataWriter.h"
#include "RequestHandler.h"
#include "Request.h"

using namespace ::ondisk;

namespace pio {
DataWriter::DataWriter(RequestHandlerPtrVec::iterator src_begin,
		RequestHandlerPtrVec::iterator src_end,
		ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		std::unique_ptr<Request> request) noexcept :
			src_it_(src_begin),
			src_end_(src_end),
			vmdkp_(vmdkp),
			ckpt_id_(ckpt_id),
			request_(std::move(request)),
			bufferp_(request_->GetBuffer()),
			offset_(request_->GetOffset()),
			size_(request_->GetTransferLength()) {
}

DataWriter::DataWriter(RequestHandlerPtrVec::iterator src_begin,
		RequestHandlerPtrVec::iterator src_end,
		ActiveVmdk* vmkdp,
		const ::ondisk::CheckPointID ckpt_id,
		void* bufferp,
		const uint64_t offset,
		const size_t size) noexcept :
			src_it_(std::move(src_begin)),
			src_end_(std::move(src_end)),
			vmdkp_(vmkdp),
			ckpt_id_(ckpt_id),
			bufferp_(bufferp),
			offset_(offset),
			size_(size) {
}

int DataWriter::CreateRequest() noexcept {
	try {
		log_assert(not request_);
		request_ = std::make_unique<Request>(-1, vmdkp_, Request::Type::kWrite,
			bufferp_, size_, size_, offset_);
		if (pio_unlikely(not request_)) {
			LOG(ERROR) << "allocating request for write failed";
			return -ENOMEM;
		}
	} catch (const std::exception& e) {
		return -ENOMEM;
	}
	return 0;
}

folly::Future<int> DataWriter::Start() {
	std::vector<folly::Future<int>> futures;

	write_started_ = true;

	for (; src_it_ != src_end_; ++src_it_) {
		/*
		 * TODO: implement request clone - pass separate RequestBlock* to each
		 * write destination. Each destination should be able to update the
		 * status of RequestBlock
		 */
		auto failed = std::make_unique<RequestBlockPtrVec>();
		auto process = std::make_unique<RequestBlockPtrVec>();
		process->reserve(request_->NumberOfRequestBlocks());
		request_->ForEachRequestBlock([&process] (RequestBlock *blockp) mutable {
			process->emplace_back(blockp);
			return true;
		});

		auto f = (*src_it_)->Write(vmdkp_, request_.get(), ckpt_id_, *process, *failed)
		.then([process = std::move(process), failed = std::move(failed)] (int rc) {
			return rc;
		});

		futures.emplace_back(std::move(f));
	}

	return folly::collectAll(std::move(futures))
	.then([this] (std::vector<folly::Try<int>>& tries) {
		this->write_complete_ = true;

		for (const auto& tri : tries) {
			if (pio_unlikely(tri.hasException())) {
				return -EIO;
			} else if (pio_unlikely(tri.value() < 0)) {
				return tri.value();
			}
		}
		return 0;
	});
}

int DataWriter::GetStatus() const noexcept {
	if (pio_unlikely(not write_started_ or not write_complete_)) {
		LOG(ERROR) << "either write is not started or not complete";
		return -EAGAIN;
	}
	return request_->GetResult();
}

std::unique_ptr<Request> DataWriter::GetRequest() noexcept {
	return std::move(request_);
}

bool DataWriter::IsComplete() const noexcept {
	return write_started_ and write_complete_;
}

}
