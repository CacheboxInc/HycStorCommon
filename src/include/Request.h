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
    int64_t offset;
    int32_t result;
    mutable std::mutex mutex_;

public:
    RequestBase(RequestID id, Type t, const void* privatep, int64_t offset);
    ~RequestBase();

    const RequestBase::Type& GetType() const noexcept;
};

class Request : public RequestBase {
public:
    char* bufferp;
    int32_t buf_sz;
    int32_t xfer_sz;
    TimePoint timer;
    std::shared_ptr<RequestBase> reqp;

public:
    Request(RequestID id, Type t, const void* privatep, char *bufferp,
        int32_t buf_sz, int32_t xfer_sz, int64_t offset);

    ~Request();
};

class SyncRequest : public RequestBase {
public:
    uint32_t num_blks;
    uint32_t count;
    std::vector<std::weak_ptr<RequestBase>> write_pending;
    StordVmdk* vmdkp;

    SyncRequest(RequestID id, Type t, const void* privatep, uint32_t num_blks,
        int64_t offset, StordVmdk* vmdkp);
    ~SyncRequest();
};

}
