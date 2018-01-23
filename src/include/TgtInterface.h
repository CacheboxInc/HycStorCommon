#ifndef __TGT_INTERFACE_H__

#include "TgtTypes.h"

#ifdef __cplusplus
extern "C"  {
#endif

int InitializeLibrary();

VmHandle NewVm(const char* vmidp);
VmHandle GetVmHandle(const char* vmidp);
VmdkHandle NewActiveVmdk(VmHandle vm_handle, const char* vmdkid);
VmdkHandle GetVmdkHandle(const char* vmdkidp);
int SetVmdkEventFd(VmdkHandle handle, int eventfd);

RequestID ScheduleRead(VmdkHandle handle, const void* privatep,
		char *bufferp, size_t length, uint64_t offset);
RequestID ScheduleWrite(VmdkHandle handle, const void* privatep,
		char *bufferp, size_t length, uint64_t offset);
RequestID ScheduleWriteSame(VmdkHandle handle, const void* privatep,
		char *bufferp, size_t buffer_length, uint64_t transfer_length,
		uint64_t offset);
int RequestAbort(VmdkHandle handle, RequestID request_id);
uint32_t GetCompleteRequests(VmdkHandle handle, struct RequestResult *resultsp,
		uint32_t nresults, bool *has_morep);

#ifdef __cplusplus
}
#endif

#endif