#include <memory>
#include <utility>
#include <vector>

#include <cstdint>

#include "TgtTypes.h"
#include "Request.h"
#include "RequestHandler.h"
#include "Vmdk.h"
#include "Utils.h"

namespace pio {

std::pair<BlockID, BlockID> Request::Blocks() const {
	return std::make_pair(block_.start_, block_.end_);
}

uint32_t Request::NumberOfRequestBlocks() const {
	return block_.nblocks_;
}

bool Request::IsSuccess() const noexcept {
	return status_.status_ == RequestStatus::kSuccess and GetResult() == 0;
}

bool Request::IsFailed() const noexcept {
	return not IsSuccess();
}

int Request::GetResult() const noexcept {
	return status_.return_value_;
}

const void* Request::GetPrivateData() const noexcept {
	return privatep_;
}

void Request::SetPrivateData(const void* privatep) noexcept {
	privatep_ = privatep;
}

RequestID Request::GetID() const noexcept {
	return in_.req_id_;
}

Request::Request(RequestID id, ActiveVmdk *vmdkp, Request::Type type, void *bufferp,
		size_t buffer_size, size_t transfer_size, Offset offset) : vmdkp_(vmdkp),
		in_(id, type, bufferp, buffer_size, transfer_size, offset) {
	if (pio_unlikely(id == kInvalidRequestID)) {
		throw std::invalid_argument("Invalid RequestID");
	}
	if (pio_unlikely(not IsBlockSizeAlgined(transfer_size, kSectorSize) ||
			not IsBlockSizeAlgined(buffer_size, kSectorSize))) {
		throw std::invalid_argument("Invalid transfer_size or buffer_size");
	}
	if (pio_unlikely(transfer_size >= kMaxIoSize)) {
		throw std::invalid_argument("transfer_size too large");

	}
	if (pio_unlikely(not IsBlockSizeAlgined(offset, kSectorSize))) {
		throw std::invalid_argument("Invalid Offset");
	}

	switch (in_.type_) {
	case Request::Type::kWriteSame:
		if (pio_unlikely(transfer_size < buffer_size)) {
			throw std::invalid_argument("Invalid transfer_size");
		}
		InitWriteSameBuffer();
		break;
	default:
		if (pio_unlikely(buffer_size != transfer_size)) {
			throw std::invalid_argument("buffer_size != transfer_size");
		}
		break;
	}
	InitRequestBlocks();
}

void Request::InitWriteSameBuffer() {
	log_assert(in_.type_ == Request::Type::kWriteSame);
	log_assert(in_.transfer_size_ <= kMaxIoSize);
	auto req_bufp = std::make_unique<RequestBuffer>(in_.transfer_size_);
	auto payload  = req_bufp->Payload();
	auto copy     = in_.transfer_size_;
	for (; copy > 0; copy -= in_.buffer_size_, payload += in_.buffer_size_) {
		::memcpy(payload, in_.bufferp_, in_.buffer_size_);
	}
	/*
	 * Replace original input buffer pointer with new pointer, thus next set of
	 * functions continue to work
	 */
	in_.bufferp_ = req_bufp->Payload();
	write_same_buffer_ = std::move(req_bufp);
}

void Request::InitRequestBlocks() {
	auto offset  = in_.offset_;
	auto pending = in_.transfer_size_;
	auto iobufp  = reinterpret_cast<char *>(in_.bufferp_);
	std::tie(block_.start_, block_.end_) = GetBlockIDs(offset, pending,
			vmdkp_->BlockShift());
	block_.nblocks_ = block_.end_ - block_.start_ + 1;

	request_blocks_.reserve(block_.nblocks_);
	for (BlockID i = 0, bid = block_.start_; i < block_.nblocks_; ++i, ++bid) {
		auto block_size = vmdkp_->BlockSize();
		auto aligned = IsBlockSizeAlgined(offset, block_size);
		auto io_size = block_size;
		if (aligned == false) {
			auto ao = AlignDownToBlockSize(offset, block_size);
			io_size = block_size - offset - ao;
		}
		if (io_size > pending) {
			io_size = pending;
		}

		auto blockp = std::make_unique<RequestBlock>(vmdkp_, this, bid,
				in_.type_, iobufp, io_size, offset);
		request_blocks_.emplace_back(std::move(blockp));

		offset  += io_size;
		iobufp  += io_size;
		pending -= io_size;
	}
	log_assert(pending == 0);
}

int Request::Complete() {
	if (pio_unlikely(IsFailed())) {
		log_assert(GetResult() != 0);
		return GetResult();
	}

	for (const auto& blockp : request_blocks_) {
		auto rc = blockp->Complete();
		if (pio_unlikely(blockp->IsFailed())) {
			log_assert(blockp->GetResult() != 0);
			status_.status_ = blockp->GetStatus();
			status_.return_value_ = blockp->GetResult();
			return blockp->GetResult();
		}
		log_assert(rc == 0);
	}

	log_assert(IsSuccess());
	return 0;
}

bool Request::IsAllReadMissed(const std::vector<RequestBlock *> blocks)
		const noexcept {
	for (const auto blockp : blocks) {
		if (not blockp->IsReadMissed()) {
			return false;
		}
	}
	return true;
}

RequestBlock::RequestBlock(ActiveVmdk *vmdkp, Request *requestp, BlockID block_id,
		Request::Type type, void *bufferp, size_t size, Offset offset) :
		vmdkp_(vmdkp), in_(block_id, type, bufferp, size, offset) {
	aligned_offset_ = in_.offset_;
	partial_ = not IsBlockSizeAlgined(in_.offset_, vmdkp_->BlockSize());
	if (partial_) {
		aligned_offset_ = AlignDownToBlockSize(in_.offset_, vmdkp_->BlockSize());
	} else {
		partial_ = not (in_.size_ == vmdkp->BlockSize());
	}

	InitRequestBuffer();
}

void RequestBlock::InitRequestBuffer() {
	switch (in_.type_) {
	default:
		break;
	case Request::Type::kWriteSame:
	case Request::Type::kWrite: {
		auto bufp = NewRequestBuffer(in_.size_);
		::memcpy(bufp->Payload(), in_.bufferp_, in_.size_);
		PushRequestBuffer(std::move(bufp));
		break;
	}}
}

void RequestBlock::PushRequestBuffer(std::unique_ptr<RequestBuffer> bufferp) {
	request_buffers_.emplace_back(std::move(bufferp));
}

RequestStatus RequestBlock::GetStatus() const noexcept {
	return status_.status_;
}

int RequestBlock::GetResult() const noexcept {
	return status_.return_value_;
}

bool RequestBlock::IsSuccess() const noexcept {
	return GetStatus() == RequestStatus::kSuccess and GetResult() == 0;
}

bool RequestBlock::IsFailed() const noexcept {
	return not IsSuccess();
}

bool RequestBlock::IsReadMissed() const noexcept {
	return GetStatus() == RequestStatus::kMiss;
}

size_t RequestBlock::GetRequestBufferCount() const {
	return request_buffers_.size();
}

RequestBuffer* RequestBlock::GetRequestBufferAtBack() {
	if (request_buffers_.empty()) {
		return nullptr;
	}

	return request_buffers_.back().get();
}

RequestBuffer* RequestBlock::GetRequestBufferAt(size_t index) {
	return request_buffers_.at(index).get();
}

BlockID RequestBlock::GetBlockID() const {
	return in_.block_id_;
}

Offset RequestBlock::GetOffset() const {
	return in_.offset_;
}

Offset RequestBlock::GetAlignedOffset() const {
	return aligned_offset_;
}

bool RequestBlock::IsPartial() const {
	log_assert((not partial_ && in_.offset_ == aligned_offset_) ||
		(partial_ &&
			(in_.offset_ != aligned_offset_ ||
				in_.size_ < vmdkp_->BlockSize())));
	return partial_;
}

int RequestBlock::ReadResultPrepare() {
	log_assert(not IsFailed());

	auto bufferp = GetRequestBufferAtBack();
	if (pio_unlikely(not bufferp)) {
		status_.status_ = RequestStatus::kFailed;
		status_.return_value_ = -EIO;
		return -EIO;
	}

	auto srcp = bufferp->Payload();
	auto gap  = GetOffset() - GetAlignedOffset();
	::memcpy(in_.bufferp_, srcp + gap, in_.size_);
	log_assert(not IsFailed());
	return 0;
}

int RequestBlock::Complete() {
	if (pio_unlikely(IsFailed())) {
		log_assert(GetResult() != 0);
		return GetResult();
	}

	switch (in_.type_) {
	default:
		break;
	case Request::Type::kWriteSame:
	case Request::Type::kWrite: {
		return 0;
	}}

	return ReadResultPrepare();
}

RequestBuffer::RequestBuffer(size_t size) : size_(size) {
	InitBuffer();
}

void RequestBuffer::InitBuffer() {
	void* buf = ::malloc(size_);
	if (buf == nullptr) {
		throw std::bad_alloc();
	}
	data_ = Buffer(reinterpret_cast<char*>(buf), ::free);
}

size_t RequestBuffer::Size() const {
	return size_;
}

char* RequestBuffer::Payload() {
	return data_.get();
}

std::unique_ptr<RequestBuffer> NewRequestBuffer(size_t size) {
	return std::make_unique<RequestBuffer>(size);
}

}