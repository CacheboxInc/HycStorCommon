#ifndef __TGT_INTERFACE_H__
#define __TGT_INTERFACE_H__

#include "TgtTypes.h"

#ifdef __cplusplus
extern "C"  {
#else
#include <stdbool.h>
#endif

typedef struct vmdk_stats
{
	int64_t read_requests;
	int64_t read_failed;
	int64_t read_bytes;
	int64_t read_latency;

	int64_t write_requests;
	int64_t write_failed;
	int64_t write_same_requests;
	int64_t write_same_failed;
	int64_t write_bytes;
	int64_t write_latency;

	int64_t truncate_requests;
	int64_t truncate_failed;
	int64_t truncate_latency;

	int64_t sync_requests;
	int64_t sync_ongoing_writes;
	int64_t sync_hold_new_writes;

	int64_t pending;
	int64_t rpc_requests_scheduled;
} vmdk_stats_t;

typedef struct {
	vmdk_stats_t vmdk_stats;
	//and list goes on 
}component_stats_t;

void HycStorInitialize(int argc, char *argv[], char *stord_ip, uint16_t stord_port);
int32_t HycStorRpcServerConnect(void);
int32_t HycStorRpcServerDisconnect(void);
int32_t HycOpenVmdk(const char* vmid, const char* vmdkid, uint64_t lun_size,
		uint32_t lun_blk_shift, int eventfd, VmdkHandle* handlep);
int32_t HycCloseVmdk(VmdkHandle handle);
RequestID HycScheduleRead(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset);
uint32_t HycGetCompleteRequests(VmdkHandle handle, struct RequestResult *resultsp,
		uint32_t nresults, bool *has_morep);
RequestID HycScheduleWrite(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int64_t offset);
RequestID HycScheduleWriteSame(VmdkHandle handle, const void* privatep,
		char* bufferp, int32_t buf_sz, int32_t write_sz, int64_t offset);
RequestID HycScheduleAbort(VmdkHandle handle, const void* privatep);
int32_t HycGetAllScheduledRequests(VmdkHandle handle,
	struct ScheduledRequest** requests, uint32_t* nrequests);

void HycDumpVmdk(VmdkHandle handle);
RequestID HycScheduleTruncate(VmdkHandle handle, const void* privatep,
	char* bufferp, int32_t buf_sz);
RequestID HycScheduleSyncCache(VmdkHandle handle, const void* privatep,
	uint64_t offset, uint64_t length);
int HycGetVmdkStats(const char* vmdkid, vmdk_stats_t *vmdk_stats);
int HycGetComponentStats(component_stats_t* g_stats);
void HycSetBatchingAttributes(uint32_t adaptive_batch, uint32_t wan_latency,
		uint32_t batch_incr_val, uint32_t batch_decr_pct,
		uint32_t system_load_factor, uint32_t debug_log);

void HycSetDeploymentTarget(enum HycDeploymentTarget target);
#ifdef __cplusplus
}
#endif

#endif
