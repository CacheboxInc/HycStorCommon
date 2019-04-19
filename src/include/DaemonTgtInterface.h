#pragma once

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "Vmdk.h"
#include "halib.h"
#include "ReadAhead.h"

namespace pio {

ActiveVmdk* VmdkFromVmdkHandle(::hyc_thrift::VmdkHandle handle);

int InitStordLib();
int DeinitStordLib(void);

VmHandle NewVm(ondisk::VmID vmid, const std::string& config);
AeroClusterHandle NewAeroCluster(pio::AeroClusterID cluster_id, const std::string& config);
AeroClusterHandle DelAeroCluster(pio::AeroClusterID cluster_id, const std::string& config);
hyc_thrift::VmHandle GetVmHandle(const ondisk::VmID& vmid);
std::shared_ptr<AeroSpikeConn> GetAeroConn(const ActiveVmdk *vmdkp);
std::shared_ptr<AeroSpikeConn> GetAeroConnUsingVmID(ondisk::VmID vmid);

void RemoveVm(::hyc_thrift::VmdkHandle vm_handle);
int RemoveVmUsingVmID(ondisk::VmID vmid);
int PrepareCkpt(::hyc_thrift::VmdkHandle vm_handle);
int CommitCkpt(ondisk::VmID vmid, std::string& ckpt_id);
int NewFlushReq(ondisk::VmID vmid, const std::string& config);
int NewScanReq(ondisk::VmID vmid, ondisk::CheckPointID ckptid);
int NewMergeReq(ondisk::VmID vmid, ondisk::CheckPointID ckptid);
int NewDataCkptMergeReq(ondisk::VmID vmid, ondisk::CheckPointID ckptid);
int NewFlushStatusReq(ondisk::VmID vmid, FlushStats &flush_stat);
int FlushHistoryReq(ondisk::VmID vmid, json_t *history_param);
int NewScanStatusReq(pio::AeroClusterID id, ScanStats &scan_stat);
int NewMergeStatusReq(pio::AeroClusterID id, CkptMergeStats &merge_stat);
int NewAeroCacheStatReq(ondisk::VmID vmid, AeroStats *);
int MoveUnflushedToFlushed(::hyc_thrift::VmHandle vm_handle, std::vector<::ondisk::CheckPointID>&);
int CreateNewVmDeltaContext(::hyc_thrift::VmHandle vm_handle, std::string snap_id);
int NewVmDeltaContextSet(VmHandle vm_handle, std::string snap_id);
int NewVmDeltaContextSet(::hyc_thrift::VmHandle vm_handle, std::string snap_id);
int UpdatefdMap(const std::string& vmdkid, const int64_t& snap_id, const int32_t& fd);
int CreateNewVmdkDeltaContext(const std::string& vmdkid, const int64_t& snap_id);
int GlobalStats(ComponentStats* stats);

int NewVmdkStatsReq(const std::string& vmdkid, VmdkCacheStats* vmdk_stats);
VmdkHandle NewActiveVmdk(hyc_thrift::VmHandle vm_handle, ::ondisk::VmdkID vmdkid,
		const std::string& config);
int ReadAheadStatsReq(const std::string& vmdkid, pio::ReadAhead::ReadAheadStats& rh_stats);
int RemoveActiveVmdk(hyc_thrift::VmHandle vm_handle, ::ondisk::VmdkID vmdkid);
int StartPreload(const ::ondisk::VmID& vmid, const ::ondisk::VmdkID& vmdkid);
int SetCkptBitmap(hyc_thrift::VmHandle vm_handle, ::ondisk::VmdkID vmdkid, const std::string& config);
VmdkHandle GetVmdkHandle(const std::string& vmdkid);
void RemoveVmdk(hyc_thrift::VmdkHandle handle);

int AeroSetDelete(ondisk::VmID vmid);
int AeroSetCleanup(pio::AeroClusterID cluster_id, const std::string& config);
}
