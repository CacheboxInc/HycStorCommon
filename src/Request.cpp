#include <memory>
#include <utility>
#include <vector>

#include <cstdint>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "gen-cpp2/MetaData_types.h"
#include "Request.h"
#include "RequestHandler.h"
#include "Vmdk.h"
#include "DaemonUtils.h"

using namespace ::ondisk;

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

void Request::SetResult(int return_value, RequestStatus status) noexcept {
	status_.return_value_ = return_value;
	status_.status_ = status;
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
	if (pio_unlikely(id == StorRpc_constants::kInvalidRequestID())) {
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
	auto req_bufp = NewRequestBuffer(in_.transfer_size_);
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
			io_size = block_size - (offset - ao);
		}
		if (io_size > pending) {
			io_size = pending;
		}
		log_assert(io_size <= vmdkp_->BlockSize());

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
			SetResult(blockp->GetResult(), blockp->GetStatus());
			return GetResult();
		}
		log_assert(rc == 0);
	}

	log_assert(IsSuccess());
	return GetResult();
}

bool Request::IsAllReadMissed(const std::vector<RequestBlock *>& blocks)
		const noexcept {
	for (const auto blockp : blocks) {
		if (not blockp->IsReadMissed()) {
			return false;
		}
	}
	return true;
}

bool Request::IsAllReadHit(const std::vector<RequestBlock *>& blocks)
		const noexcept {
	for (const auto blockp : blocks) {
		if (not blockp->IsReadHit()) {
			return false;
		}
	}
	return true;
}


RequestBlock::RequestBlock(ActiveVmdk *vmdkp, Request *requestp, BlockID block_id,
		Request::Type type, void *bufferp, size_t size, Offset offset) :
		vmdkp_(vmdkp), requestp_(requestp),
		in_(block_id, type, bufferp, size, offset) {
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
		auto bufp = NewRequestBuffer(reinterpret_cast<char*>(in_.bufferp_), in_.size_);
		PushRequestBuffer(std::move(bufp));
		break;
	}}
}

void RequestBlock::PushRequestBuffer(std::unique_ptr<RequestBuffer> bufferp) {
	request_buffers_.emplace_back(std::move(bufferp));
}

void RequestBlock::SetResult(int return_value, RequestStatus status) noexcept {
	status_.return_value_ = return_value;
	status_.status_ = status;
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

bool RequestBlock::IsReadHit() const noexcept {
	return GetStatus() == RequestStatus::kHit;
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

void RequestBlock::SetReadCheckPointId(CheckPointID ckpt_id) noexcept {
	read_ckpt_id_ = ckpt_id;
}

CheckPointID RequestBlock::GetReadCheckPointId() const noexcept {
	return read_ckpt_id_;
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
		SetResult(-EIO, RequestStatus::kFailed);
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

RequestBuffer::RequestBuffer(Type type, size_t size) : type_(type), size_(size) {
	InitBuffer();
}

RequestBuffer::RequestBuffer(char* payloadp, size_t size) :
		type_(Type::kWrapped), size_(size), payloadp_(payloadp) {
}

void RequestBuffer::InitBuffer() {
	switch (type_) {
	case Type::kWrapped:
		log_assert(0);
		break;
	case Type::kOwned:
		payloadp_ = reinterpret_cast<char*>(::malloc(size_));
		break;
	case Type::kAligned:
		auto rc = ::posix_memalign(reinterpret_cast<void**>(&payloadp_),
			kPageSize, size_);
		if (pio_unlikely(rc < 0)) {
			throw std::bad_alloc();
		}
		break;
	}

	if (payloadp_ == nullptr) {
		throw std::bad_alloc();
	}
}

RequestBuffer::~RequestBuffer() {
	switch (type_) {
	case Type::kWrapped:
		break;
	case Type::kOwned:
	case Type::kAligned:
		::free(payloadp_);
		break;
	}
	payloadp_ = nullptr;
	size_ = 0;
}

size_t RequestBuffer::Size() const {
	return size_;
}

char* RequestBuffer::Payload() {
	return payloadp_;
}

std::unique_ptr<RequestBuffer> NewRequestBuffer(size_t size) {
	return std::make_unique<RequestBuffer>(RequestBuffer::Type::kOwned, size);
}

std::unique_ptr<RequestBuffer> NewAlignedRequestBuffer(size_t size) {
	return std::make_unique<RequestBuffer>(RequestBuffer::Type::kAligned, size);
}

std::unique_ptr<RequestBuffer> NewRequestBuffer(char* payloadp, size_t size) {
	log_assert(payloadp);
	return std::make_unique<RequestBuffer>(payloadp, size);
}

}
