#ifndef __TGT_TYPES_H__
#define __TGT_TYPES_H__

#ifdef __cplusplus
extern "C"  {
#endif

#define kInvalidVmHandle   0
#define kInvalidVmdkHandle 0
#define kInvalidRequestID  0
#define kInvalidRpcHandle  0

typedef int64_t RequestID;
typedef int64_t VmHandle;
typedef int64_t VmdkHandle;
typedef int64_t RpcConnectHandle;

struct RequestResult {
	const void* privatep;
	RequestID   request_id;
	int32_t     result;
};

#ifdef __cplusplus
}
#endif

#endif