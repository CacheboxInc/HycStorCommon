#ifndef __TGT_TYPES_H__
#define __TGT_TYPES_H__

#ifdef __cplusplus
extern "C"  {
#endif

#define kInvalidVmHandle   0
#define kInvalidVmdkHandle 0
#define kInvalidRequestID  0

#ifndef hyc_likely
#define hyc_likely(x) (__builtin_expect(!!(x), 1))
#endif

#ifndef hyc_unlikely
#define hyc_unlikely(x) (__builtin_expect(!!(x), 0))
#endif

typedef int64_t RequestID;
typedef int64_t VmHandle;
typedef void* VmdkHandle;

struct RequestResult {
	const void* privatep;
	RequestID   request_id;
	int32_t     result;
};

struct ScheduledRequest {
	const void* privatep;
	RequestID request_id;
};

enum HycDeploymentTarget {
	kDeploymentTest,
	kDeploymentCustomer
};

#ifdef __cplusplus
}
#endif

#endif
