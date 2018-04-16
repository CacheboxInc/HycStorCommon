#pragma once

#include "DaemonTgtTypes.h"
#include "IDs.h"

#include "Vmdk.h"

namespace pio {

ActiveVmdk* VmdkFromVmdkHandle(VmdkHandle handle);

int InitStordLib(void);
int DeinitStordLib(void);

VmHandle NewVm(pio::VmID vmid, const std::string& config);
AeroClusterHandle NewAeroCluster(pio::AeroClusterID cluster_id, const std::string& config);
AeroClusterHandle DelAeroCluster(pio::AeroClusterID cluster_id, const std::string& config);
VmHandle GetVmHandle(const std::string& vmid);
void RemoveVm(VmdkHandle vm_handle);

VmdkHandle NewActiveVmdk(VmHandle vm_handle, VmdkID vmdkid,
		const std::string& config);
VmdkHandle GetVmdkHandle(const std::string& vmdkid);
void RemoveVmdk(VmdkHandle handle);

}
