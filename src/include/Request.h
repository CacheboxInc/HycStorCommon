#pragma once

#include <mutex>
#include <vector>
#include <memory>

#include "TgtTypes.h"
#include "TimePoint.h"
#include "SharedMemory.h"

namespace hyc {

class StordVmdk;

class RequestBase {
public:
	enum class Type {
		kRead,
		kWrite,
		kWriteSame,
		kTruncate,
		kSync,
	};

	std::shared_ptr<StordVmdk> vmdk_;
	RequestID id;
	Type type;
	const void* privatep;
	uint64_t length;
	uint64_t offset;
	int32_t result;
	mutable std::mutex mutex_;

public:
	RequestBase(std::shared_ptr<StordVmdk> vmdk, RequestID id, Type t,
		const void* privatep, uint64_t length, int64_t offset) noexcept;
	virtual ~RequestBase();

	const RequestBase::Type& GetType() const noexcept;
	bool IsOverlapped(uint64_t req_offset, uint64_t req_length) const noexcept;
};

class Request : public RequestBase {
public:
	char* bufferp;
	int32_t buf_sz;
	TimePoint timer;
	RequestBase* sync_req;
	size_t batch_size;
	SharedMemory::Handle shm_;

public:
	Request(std::shared_ptr<StordVmdk> vmdk, RequestID id, Type t,
		const void* privatep, char *bufferp, int32_t buf_sz, uint64_t length,
		int64_t offset, size_t batch_size) noexcept;

	virtual ~Request();
};

class SyncRequest : public RequestBase {
public:
	uint32_t count;
	std::vector<RequestBase*> write_pending;

	SyncRequest(std::shared_ptr<StordVmdk> vmdk, RequestID id, Type t,
		const void* privatep, uint64_t length, int64_t offset) noexcept;
	virtual ~SyncRequest();
};

} // namespace hyc

