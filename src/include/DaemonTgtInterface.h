#pragma once

#include "DaemonTgtTypes.h"
#include "IDs.h"

#include "Vmdk.h"

namespace pio {

ActiveVmdk* VmdkFromVmdkHandle(VmdkHandle handle);

int InitializeLibrary(void);

VmHandle NewVm(pio::VmID vmid, const std::string& config);
VmHandle GetVmHandle(const std::string& vmid);
void RemoveVm(VmdkHandle vm_handle);

VmdkHandle NewActiveVmdk(VmHandle vm_handle, VmdkID vmdkid,
		const std::string& config);
VmdkHandle GetVmdkHandle(const std::string& vmdkid);
void RemoveVmdk(VmdkHandle handle);

}
