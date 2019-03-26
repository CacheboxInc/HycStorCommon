#pragma once

#include <memory>
#include <vector>
#include <atomic>
#include <mutex>
#include <stack>
#include <queue>

#include <folly/futures/Future.h>

#include "DataMoverCommonTypes.h"
#include "CheckPointTraverser.h"

namespace pio {
namespace details {
class CopyInternalInfo {
public:
	CopyInternalInfo(const CopyInternalInfo&) = delete;
	CopyInternalInfo(const CopyInternalInfo&&) = delete;
	CopyInternalInfo& operator = (const CopyInternalInfo&) = delete;
	CopyInternalInfo& operator = (CopyInternalInfo&&) = delete;

	CopyInternalInfo() = default;
	~CopyInternalInfo() = default;

	CopyInternalInfo(::ondisk::CheckPointID ckpt_id,
		std::unique_ptr<folly::IOBuf> buffer, uint64_t offset, uint64_t size) noexcept;

	void SetIOBuffer(std::unique_ptr<folly::IOBuf> buffer) noexcept;
	folly::IOBuf* GetIOBuffer() const noexcept;
	std::unique_ptr<folly::IOBuf> MoveIOBuffer() noexcept;

	void SetCheckPointID(::ondisk::CheckPointID ckpt_id) noexcept;
	const ::ondisk::CheckPointID& GetCheckPointID() const noexcept;

	void SetRequest(std::unique_ptr<Request> request) noexcept;
	Request* GetRequest() noexcept;
	std::unique_ptr<Request> MoveRequest() noexcept;
private:
	::ondisk::CheckPointID ckpt_id_{::ondisk::MetaData_constants::kInvalidCheckPointID()};
	std::unique_ptr<folly::IOBuf> buffer_;
	std::unique_ptr<Request> request_;
};
}

class ActiveVmdk;

class DataCopier {
public:
	struct Stats {
		uint64_t copy_total{0};
		uint64_t copy_pending{0};
		uint64_t copy_completed{0};
		uint64_t copy_avoided{0};

		::ondisk::CheckPointID cbt_in_progress{0};
		uint64_t read_in_progress{0};
		uint64_t write_in_progress{0};
		uint64_t write_queue_size{0};

		bool is_read_complete{false};
		bool is_failed{false};
	};

public:
	DataCopier(ActiveVmdk* vmdkp,
		const size_t vmdk_block_shift,
		const size_t max_io_size = 1ul << 20) noexcept;

	int SetCheckPoints(CheckPointPtrVec&& check_points) noexcept;
	void SetDataSource(RequestHandlerPtrVec::iterator src_begin,
		RequestHandlerPtrVec::iterator src_end);
	void SetDataDestination(RequestHandlerPtrVec::iterator dst_begin,
		RequestHandlerPtrVec::iterator dst_end);
	void SetReadIODepth(const size_t io_depth) noexcept;
	void SetWriteIODepth(const size_t io_depth) noexcept;
	folly::Future<int> Begin();
	DataCopier::Stats GetStats() const noexcept;

private:
	bool HasPendingIOs() const noexcept;
	bool IsComplete() const noexcept;
	size_t WriteQueueSize() const;
	const std::string& Name() const noexcept;
	void LogStatus() const noexcept;

	void ScheduleMoreIOs();
	int ScheduleDataReads();
	folly::Future<int> StartDataRead(::ondisk::CheckPointID ckpt_id,
		::ondisk::BlockID block, BlockCount count);
	int ScheduleDataWrites();
	folly::Future<int> StartDataWrite(std::unique_ptr<details::CopyInternalInfo> info);

	std::vector<
		std::tuple<
			::ondisk::CheckPointID,
			::ondisk::BlockID,
			BlockCount
		>
	> GetBlocksToRead();

	std::vector<
		std::unique_ptr<details::CopyInternalInfo>
	> GetBlocksToWrite();

	void PutBuffer(::ondisk::IOBufPtr buf);
	::ondisk::IOBufPtr GetBuffer();

private:
	ActiveVmdk* vmdkp_{};
	const size_t kBlockShift{};
	const size_t kMaxIOSize{1ul << 20};
	const size_t kMaxWritesPending{64};
	const std::string kName_{"sync"};
	folly::Promise<int> copy_promise_;

	struct {
		RequestHandlerPtrVec::iterator begin_;
		RequestHandlerPtrVec::iterator end_;
	} data_src_;

	struct {
		RequestHandlerPtrVec::iterator begin_;
		RequestHandlerPtrVec::iterator end_;
	} data_dst_;

	struct Traverser {
		Traverser(const BlockCount merge_factor) : traverser_(merge_factor) {
		}
		mutable std::mutex mutex_;
		CheckPointUnionTraverser traverser_;
	} ckpt_;

	struct {
		uint64_t io_depth_{32};

		std::atomic<uint64_t> in_progress_{0};
		std::atomic<uint64_t> schedule_pending_{0};

		mutable std::mutex mutex_;
		std::queue<std::unique_ptr<details::CopyInternalInfo>> queue_;
	} write_;

	struct {
		uint64_t io_depth_{64};

		std::atomic<uint64_t> in_progress_{0};
		std::atomic<uint64_t> schedule_pending_{0};

		mutable std::mutex mutex_;
		std::stack<std::unique_ptr<folly::IOBuf>> buffers_;
	} read_;

	struct {
		std::atomic<bool> read_complete_{false};
		std::atomic<bool> failed_{false};
		int res_{0};
	} status_;

	std::atomic<uint64_t> schedule_more_{0};
};
}
