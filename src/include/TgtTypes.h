#ifndef __TGT_TYPES_H__
#define __TGT_TYPES_H__

#ifdef __cplusplus
extern "C"  {
#endif

#define kInvalidVmHandle   0
#define kInvalidVmdkHandle 0
#define kInvalidRequestID  0

typedef uint64_t RequestID;
typedef uint64_t VmHandle;
typedef uint64_t VmdkHandle;

struct RequestResult {
	const void* privatep;
	RequestID   request_id;
	int         result;
};

#ifdef __cplusplus
}
#endif

#endif