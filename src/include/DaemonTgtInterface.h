#pragma once

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "Vmdk.h"

namespace pio {

ActiveVmdk* VmdkFromVmdkHandle(::hyc_thrift::VmdkHandle handle);

int InitStordLib(void);
int DeinitStordLib(void);

VmHandle NewVm(ondisk::VmID vmid, const std::string& config);
AeroClusterHandle NewAeroCluster(pio::AeroClusterID cluster_id, const std::string& config);
AeroClusterHandle DelAeroCluster(pio::AeroClusterID cluster_id, const std::string& config);
hyc_thrift::VmHandle GetVmHandle(const ondisk::VmID& vmid);
std::shared_ptr<AeroSpikeConn> GetAeroConn(ActiveVmdk *vmdkp);
void RemoveVm(::hyc_thrift::VmdkHandle vm_handle);
int NewFlushReq(ondisk::VmID vmid);

VmdkHandle NewActiveVmdk(hyc_thrift::VmHandle vm_handle, ::ondisk::VmdkID vmdkid,
		const std::string& config);
VmdkHandle GetVmdkHandle(const std::string& vmdkid);
void RemoveVmdk(hyc_thrift::VmdkHandle handle);

}
