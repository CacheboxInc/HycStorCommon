#include <folly/io/IOBuf.h>

#include "gen-cpp2/MetaData_constants.h"
#include "DataCopier.h"
#include "DataReader.h"
#include "DataWriter.h"
#include "Request.h"
#include "CommonMacros.h"

using namespace ::ondisk;

namespace pio {
using namespace ondisk;

namespace details {
CopyInternalInfo::CopyInternalInfo(CheckPointID ckpt_id,
		std::unique_ptr<folly::IOBuf> buffer,
		[[maybe_unused]] uint64_t offset,
		[[maybe_unused]] uint64_t size) noexcept :
			ckpt_id_(ckpt_id),
			buffer_(std::move(buffer)) {
}

void CopyInternalInfo::SetIOBuffer(std::unique_ptr<folly::IOBuf> buffer) noexcept {
	buffer_ = std::move(buffer);
}

folly::IOBuf* CopyInternalInfo::GetIOBuffer() const noexcept {
	return buffer_.get();
}

std::unique_ptr<folly::IOBuf> CopyInternalInfo::MoveIOBuffer() noexcept {
	return std::move(buffer_);
}

void CopyInternalInfo::SetCheckPointID(CheckPointID ckpt_id) noexcept {
	ckpt_id_ = ckpt_id;
}

const CheckPointID& CopyInternalInfo::GetCheckPointID() const noexcept {
	return ckpt_id_;
}

void CopyInternalInfo::SetRequest(std::unique_ptr<Request> request) noexcept {
	request_ = std::move(request);
}

Request* CopyInternalInfo::GetRequest() noexcept {
	return request_.get();
}

std::unique_ptr<Request> CopyInternalInfo::MoveRequest() noexcept {
	return std::move(request_);
}
}

using namespace details;

DataCopier::DataCopier(ActiveVmdk* vmdkp,
		const size_t vmdk_block_shift,
		const size_t max_io_size) noexcept :
			vmdkp_(vmdkp),
			kBlockShift(vmdk_block_shift),
			kMaxIOSize(max_io_size),
			ckpt_(kMaxIOSize >> kBlockShift) {
}

int DataCopier::SetCheckPoints(CheckPointPtrVec&& check_points) noexcept {
	return ckpt_.traverser_.SetCheckPoints(std::forward<CheckPointPtrVec>(check_points));
}

void DataCopier::SetDataSource(RequestHandlerPtrVec::iterator src_begin,
		RequestHandlerPtrVec::iterator src_end) {
	data_src_.begin_ = std::move(src_begin);
	data_src_.end_ = std::move(src_end);
}

void DataCopier::SetDataDestination(RequestHandlerPtrVec::iterator dest_begin,
		RequestHandlerPtrVec::iterator dest_end) {
	data_dst_.begin_ = std::move(dest_begin);
	data_dst_.end_ = std::move(dest_end);
}

void DataCopier::SetReadIODepth(const size_t io_depth) noexcept {
	read_.io_depth_ = io_depth;
}

void DataCopier::SetWriteIODepth(const size_t io_depth) noexcept {
	write_.io_depth_ = io_depth;
}

folly::Future<int> DataCopier::Begin() {
	auto f = copy_promise_.getFuture();

	{
		std::lock_guard<std::mutex> lock(ckpt_.mutex_);
		if (pio_unlikely(ckpt_.traverser_.IsComplete())) {
			status_.read_complete_ = true;
			LogStatus();
			copy_promise_.setValue(0);
			return f;
		}
	}

	ScheduleMoreIOs();
	return f;
}

bool DataCopier::HasPendingIOs() const noexcept {
	return write_.in_progress_ or
		write_.schedule_pending_ or
		read_.in_progress_ or
		read_.schedule_pending_;
}

bool DataCopier::IsComplete() const noexcept {
	return (status_.failed_ or status_.read_complete_) and (not HasPendingIOs());
}

std::vector<std::tuple<CheckPointID, BlockID, BlockCount>>
DataCopier::GetBlocksToRead() {
	std::vector<std::tuple<CheckPointID,BlockID,BlockCount>> to_read;

	if (WriteQueueSize() >= kMaxWritesPending) {
		/* write queue too large, no point in reading more */
		return to_read;
	}

	std::lock_guard<std::mutex> lock(ckpt_.mutex_);
	if (pio_unlikely(ckpt_.traverser_.IsComplete())) {
		status_.read_complete_ = true;
		LOG(INFO) << "DataCopier: reading from all CBT complete";
		return to_read;
	}

	uint64_t in_progress = read_.in_progress_ + read_.schedule_pending_;
	if (in_progress >= read_.io_depth_) {
		return to_read;
	}

	const size_t count = read_.io_depth_ - in_progress;
	while (not ckpt_.traverser_.IsComplete() and to_read.size() < count) {
		to_read.emplace_back(ckpt_.traverser_.MergeConsecutiveBlocks());
	}
	read_.schedule_pending_ += to_read.size();
	return to_read;
}

void DataCopier::ScheduleMoreIOs() {
	++schedule_more_;
	[[maybe_unused]] auto reads_scheduled = ScheduleDataReads();
	[[maybe_unused]] auto writes_scheduled = ScheduleDataWrites();
	auto schedules = --schedule_more_;

	if (pio_unlikely(not schedules and IsComplete() and
			(status_.failed_ or WriteQueueSize() == 0))) {
		LogStatus();
		copy_promise_.setValue(status_.res_);
	}
}

int DataCopier::ScheduleDataReads() {
	if (pio_unlikely(status_.read_complete_)) {
		return 0;
	}

	int scheduled = 0;
	for (auto [ckpt_id, block_start, nblocks] : GetBlocksToRead()) {
		(void) StartDataRead(ckpt_id, block_start, nblocks);
		++scheduled;
	}
	return scheduled;
}

folly::Future<int> DataCopier::StartDataRead(CheckPointID ckpt_id, BlockID block, BlockCount nblocks) {
	auto buffer = GetBuffer();
	if (pio_unlikely(not buffer)) {
		LOG(ERROR) << "DataCopier: allocating read buffer failed";
		status_.failed_ = true;
		status_.res_ = -ENOMEM;
		--read_.schedule_pending_;
		return -ENOMEM;
	}
	auto bufferp = buffer.get();
	log_assert(static_cast<size_t>(nblocks << kBlockShift) <= kMaxIOSize);
	log_assert(buffer->capacity() >= static_cast<size_t>(nblocks << kBlockShift));

	auto offset = block << kBlockShift;
	auto size = nblocks << kBlockShift;

	auto info = std::make_unique<CopyInternalInfo>(ckpt_id, std::move(buffer), offset, size);
	if (pio_unlikely(not info)) {
		LOG(ERROR) << "DataCopier: allocatig memory failed";
		status_.failed_ = true;
		status_.res_ = -ENOMEM;
		--read_.schedule_pending_;
		return -ENOMEM;
	}

	auto reader = std::make_unique<DataReader>(data_src_.begin_, data_src_.end_,
		vmdkp_, ckpt_id, bufferp->writableData(), offset, size);
	if (pio_unlikely(not reader)) {
		LOG(ERROR) << "DataCopier: allocating memory for reading data failed";
		status_.failed_ = true;
		status_.res_ = -ENOMEM;
		--read_.schedule_pending_;
		return -ENOMEM;
	}
	auto readerp = reader.get();

	++read_.in_progress_;
	--read_.schedule_pending_;
	return readerp->Start()
	.then([this, reader = std::move(reader), info = std::move(info)] (int rc) mutable {
		if (pio_unlikely(rc < 0 or reader->GetStatus() < 0)) {
			status_.failed_ = true;
			status_.res_ = rc < 0 ? rc : reader->GetStatus();
		}

		{
			auto request = reader->GetRequest();
			log_assert(request and
				request->GetResult() == 0 and
				request->RequestType() == Request::Type::kRead
			);
			request->SetRequestType(Request::Type::kWrite);
			info->SetRequest(std::move(request));

			std::lock_guard<std::mutex> lock(write_.mutex_);
			write_.queue_.push(std::move(info));
		}

		--read_.in_progress_;
		ScheduleMoreIOs();
		return 0;
	});
}

std::vector<std::unique_ptr<CopyInternalInfo>> DataCopier::GetBlocksToWrite() {
	std::vector<std::unique_ptr<CopyInternalInfo>> to_write;

	std::lock_guard<std::mutex> lock(write_.mutex_);
	uint64_t in_progress = write_.in_progress_ + write_.schedule_pending_;
	if (in_progress >= write_.io_depth_) {
		/* writer is lagging */
		return to_write;
	}

	if (pio_unlikely(write_.queue_.empty())) {
		/* reads are lagging */
		++read_.io_depth_;
		return to_write;
	}

	const size_t count = write_.io_depth_ - in_progress;
	while (not write_.queue_.empty() and to_write.size() < count) {
		to_write.emplace_back(std::move(write_.queue_.front()));
		write_.queue_.pop();
	}
	write_.schedule_pending_ += to_write.size();
	return to_write;
}

int DataCopier::ScheduleDataWrites() {
	if (pio_unlikely(status_.failed_)) {
		return 0;
	}

	int scheduled = 0;
	for (std::unique_ptr<CopyInternalInfo>& info : GetBlocksToWrite()) {
		(void) StartDataWrite(std::move(info));
		++scheduled;
	}
	return scheduled;
}

folly::Future<int> DataCopier::StartDataWrite(std::unique_ptr<CopyInternalInfo> info) {
	auto writer = std::make_unique<DataWriter>(data_dst_.begin_, data_dst_.end_,
		vmdkp_, info->GetCheckPointID(), info->MoveRequest());
	if (pio_unlikely(not writer)) {
		LOG(ERROR) << "DataCopier: allocating memory to write failed";
		status_.failed_ = true;
		status_.res_ = -ENOMEM;
		--write_.schedule_pending_;
		return -ENOMEM;
	}
	auto writerp = writer.get();

	++write_.in_progress_;
	--write_.schedule_pending_;
	return writerp->Start()
	.then([this, info = std::move(info), writer = std::move(writer)] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			status_.failed_ = true;
			status_.res_ = rc;
		}

		PutBuffer(info->MoveIOBuffer());
		--write_.in_progress_;
		ScheduleMoreIOs();
		return 0;
	});
}

::ondisk::IOBufPtr DataCopier::GetBuffer() {
	std::lock_guard<std::mutex> lock(read_.mutex_);
	if (not read_.buffers_.empty()) {
		auto iobuf = std::move(read_.buffers_.top());
		read_.buffers_.pop();
		return iobuf;
	}
	return folly::IOBuf::create(kMaxIOSize);
}

void DataCopier::PutBuffer(::ondisk::IOBufPtr buf) {
	std::lock_guard<std::mutex> lock(read_.mutex_);
	read_.buffers_.emplace(std::move(buf));
}

size_t DataCopier::WriteQueueSize() const {
	std::lock_guard<std::mutex> lock(write_.mutex_);
	return write_.queue_.size();
}

const std::string& DataCopier::Name() const noexcept {
	return kName_;
}

void DataCopier::LogStatus() const noexcept {
	if (pio_unlikely(not IsComplete())) {
		// dump more stats
		LOG(INFO) << "Data movement for " << Name()
			<< " is in progress ";
	} else {
		LOG(ERROR) << "Data Movement for " << Name() << " finished "
			<< (status_.res_ == 0 ? " successfully" : " because of failre");
	}
}
}
