#pragma once

#include <mutex>
#include <vector>
#include <memory>

#include "TgtTypes.h"
#include "TimePoint.h"

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

	RequestID id;
	Type type;
	const void* privatep;
	uint64_t length;
	int64_t offset;
	int32_t result;
	mutable std::mutex mutex_;

public:
	RequestBase(RequestID id, Type t, const void* privatep, uint64_t length,
		int64_t offset);
	~RequestBase();

	const RequestBase::Type& GetType() const noexcept;
	bool IsOverlapped(uint64_t req_offset, uint64_t req_length) const noexcept;
};

class Request : public RequestBase {
public:
	char* bufferp;
	int32_t buf_sz;
	TimePoint timer;
	std::shared_ptr<RequestBase> reqp;

public:
	Request(RequestID id, Type t, const void* privatep, char *bufferp,
		int32_t buf_sz, uint64_t length, int64_t offset);

	~Request();
};

class SyncRequest : public RequestBase {
public:
	uint32_t count;
	std::vector<std::weak_ptr<RequestBase>> write_pending;
	StordVmdk* vmdkp;

	SyncRequest(RequestID id, Type t, const void* privatep, uint64_t length,
		int64_t offset, StordVmdk* vmdkp);
	~SyncRequest();
};

} // namespace hyc

