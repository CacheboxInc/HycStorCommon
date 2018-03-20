#pragma once

#include <vector>
#include <memory>
#include <cstdint>

#include <folly/futures/Future.h>

#include "IDs.h"
#include "DaemonCommon.h"

namespace pio {

enum class RequestStatus {
	kSuccess,
	kMiss,
	kFailed,
};

class Request {
public:
	enum class Type {
		kUnknown,
		kRead,
		kWrite,
		kWriteSame,
	};

	Request(RequestID id, ActiveVmdk *vmdkp, Request::Type type, void *bufferp,
		size_t buffer_size, size_t transfer_size, Offset offset);

	bool IsAllReadMissed(const std::vector<RequestBlock *> blocks) const noexcept;

	int Complete();
public:
	std::pair<BlockID, BlockID> Blocks() const;
	uint32_t NumberOfRequestBlocks() const;

	RequestID GetID() const noexcept;

	void SetPrivateData(const void* privatep) noexcept;
	const void* GetPrivateData() const noexcept;

	bool IsSuccess() const noexcept;
	bool IsFailed() const noexcept;
	int GetResult() const noexcept;
	void SetResult(int return_value, RequestStatus status) noexcept;

public:
	template <typename Lambda>
	void ForEachRequestBlock(Lambda&& func) {
		for (auto& blockp : request_blocks_) {
			if (not func(blockp.get())) {
				break;
			}
		}
	}
private:
	void InitWriteSameBuffer();
	void InitRequestBlocks();
private:
	ActiveVmdk* vmdkp_;
	const void* privatep_{nullptr};

	struct Input {
	public:
		Input(RequestID id, Type type, void *bufferp, size_t buffer_size,
				size_t transfer_size, Offset offset) : req_id_(id), type_(type),
				bufferp_(bufferp), buffer_size_(buffer_size),
				transfer_size_(transfer_size), offset_(offset) {
		}
		RequestID req_id_;
		Type      type_;
		void      *bufferp_;
		size_t    buffer_size_;
		size_t    transfer_size_;
		Offset    offset_;
	} in_;

	std::unique_ptr<RequestBuffer> write_same_buffer_{nullptr};

	struct {
		BlockID  start_;
		BlockID  end_;
		uint32_t nblocks_;
	} block_;

	struct {
		RequestStatus status_{RequestStatus::kSuccess};
		int return_value_{0};
	} status_;

	std::vector<std::unique_ptr<RequestBlock>> request_blocks_;
};

class RequestBlock {
public:
	RequestBlock(ActiveVmdk *vmdkp, Request *requestp, BlockID block_id,
		Request::Type type, void *bufferp, size_t size, Offset offset);

	RequestBuffer *GetInputRequestBuffer();
	void PushRequestBuffer(std::unique_ptr<RequestBuffer> bufferp);
public:
	int Complete();
	bool IsPartial() const;
	BlockID GetBlockID() const;
	Offset GetOffset() const;
	Offset GetAlignedOffset() const;

	void SetResult(int return_value, RequestStatus status) noexcept;

	int GetResult() const noexcept;
	RequestStatus GetStatus() const noexcept;
	bool IsSuccess() const noexcept;
	bool IsReadMissed() const noexcept;
	bool IsFailed() const noexcept;

	size_t GetRequestBufferCount() const;
	RequestBuffer* GetRequestBufferAtBack();
	RequestBuffer* GetRequestBufferAt(size_t index);

	template <typename Lambda>
	void ForEachRequestBuffer(Lambda&& func) {
		for (auto& bufferp : request_buffers_) {
			if (not func(bufferp.get())) {
				break;
			}
		}
	}

private:
	void InitRequestBuffer();
	int ReadResultPrepare();
private:
	ActiveVmdk *vmdkp_;
	Request    *requestp_;

	struct Input {
		Input(BlockID block_id, Request::Type type, void *bufferp, size_t size,
				Offset offset) : block_id_(block_id), type_(type),
				bufferp_(bufferp), size_(size), offset_(offset) {

		}
		BlockID       block_id_;
		Request::Type type_;
		void          *bufferp_;
		size_t        size_;
		Offset        offset_;
	} in_;

	bool   partial_{false};
	Offset aligned_offset_;

	struct {
		RequestStatus status_{RequestStatus::kSuccess};
		int return_value_{0};
	} status_;

	std::vector<std::unique_ptr<RequestBuffer>> request_buffers_;
};

class RequestBuffer {
public:
	using Buffer = std::unique_ptr<char, void (*)(void*)> ;

	RequestBuffer(size_t size, bool is_mem_align = false);

	size_t Size() const;
	char* Payload();
private:
	void InitBuffer(bool is_mem_align);
private:
	size_t size_;
	Buffer data_{nullptr, ::free};
};

std::unique_ptr<RequestBuffer> NewRequestBuffer(size_t size,
	bool is_mem_align = false);

}