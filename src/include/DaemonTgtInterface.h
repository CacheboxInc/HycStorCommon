#pragma once

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "Vmdk.h"
#include "halib.h"

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
int NewFlushReq(ondisk::VmID vmid, const std::string& config);
int NewScanReq(ondisk::VmID vmid, const std::string& config);
int NewFlushStatusReq(ondisk::VmID vmid, FlushStats &flush_stat);
int NewScanStatusReq(pio::AeroClusterID id, ScanStats &scan_stat);
int NewAeroCacheStatReq(ondisk::VmID vmid, AeroStats *);

VmdkHandle NewActiveVmdk(hyc_thrift::VmHandle vm_handle, ::ondisk::VmdkID vmdkid,
		const std::string& config);
int RemoveActiveVmdk(hyc_thrift::VmHandle vm_handle, ::ondisk::VmdkID vmdkid);
VmdkHandle GetVmdkHandle(const std::string& vmdkid);
void RemoveVmdk(hyc_thrift::VmdkHandle handle);

int AeroSetDelete(ondisk::VmID vmid);
int AeroSetCleanup(pio::AeroClusterID cluster_id, const std::string& config);
}
