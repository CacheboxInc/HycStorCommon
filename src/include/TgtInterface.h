#ifndef __TGT_INTERFACE_H__
#define __TGT_INTERFACE_H__

#include "TgtTypes.h"

#ifdef __cplusplus
extern "C"  {
#else
#include <stdbool.h>
#endif

void HycStorInitialize(int argc, char *argv[], char *stord_ip, uint16_t stord_port);
int32_t HycStorRpcServerConnect(void);
int32_t HycStorRpcServerDisconnect(void);
int32_t HycOpenVmdk(const char* vmid, const char* vmdkid, int eventfd,
		VmdkHandle* handlep);
int32_t HycCloseVmdk(VmdkHandle handle);
RequestID HycScheduleRead(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset, bool* fetch_completep);
uint32_t HycGetCompleteRequests(VmdkHandle handle, struct RequestResult *resultsp,
		uint32_t nresults, bool *has_morep);
RequestID HycScheduleWrite(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset, bool* fetch_completep);
RequestID HycScheduleWriteSame(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset,
		bool* fetch_completep);

#ifdef __cplusplus
}
#endif

#endif
