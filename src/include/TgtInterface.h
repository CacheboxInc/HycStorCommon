#ifndef __TGT_INTERFACE_H__
#define __TGT_INTERFACE_H__

#include "TgtTypes.h"

#ifdef __cplusplus
extern "C"  {
#else
#include <stdbool.h>
#endif

RpcConnectHandle HycStorRpcServerConnect();
int32_t HycStorRpcServerDisconnect(RpcConnectHandle handle);
int32_t HycOpenVmdk(RpcConnectHandle handle, const char* vmid, const char* vmdkid,
		int eventfd);
int32_t HycCloseVmdk(RpcConnectHandle handle);
RequestID HycScheduleRead(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset);
uint32_t HycGetCompleteRequests(RpcConnectHandle handle, RequestResult *resultsp,
		uint32_t nresults, bool *has_morep);
RequestID HycScheduleWrite(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset);
RequestID HycScheduleWriteSame(RpcConnectHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset);

#ifdef __cplusplus
}
#endif

#endif