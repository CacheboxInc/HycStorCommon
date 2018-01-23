#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <time.h>

#include "TgtInterface.h"

#define VMID "1"

static void GenerateId(char* id, size_t size) {
	(void) snprintf(id, size, "%lu", time(NULL));
}

static void Test_AddVmWithSameID() {
	char vmid[20];
	GenerateId(vmid, sizeof(vmid));
	VmHandle vm = NewVm(vmid);
	assert(vm != kInvalidVmHandle);
	assert(NewVm(vmid) == kInvalidVmHandle);
	assert(GetVmHandle(vmid) == vm);

	RemoveVm(vm);
	assert(GetVmHandle(vmid) == kInvalidVmHandle);

	/* try to add VMDK to removed VM */
	char vmdkid[20];
	GenerateId(vmdkid, sizeof(vmdkid));
	assert(NewActiveVmdk(vm, vmdkid) == kInvalidVmdkHandle);
}

static void Test_AddVmdkWithInvalidVmHandle() {
	char vmdkid[20];
	GenerateId(vmdkid, sizeof(vmdkid));

	/* Adding VMDK to invalid VmHandle fails */
	VmHandle vm = kInvalidVmHandle;
	assert(NewActiveVmdk(vm, vmdkid) == kInvalidVmdkHandle);
}

static void Test_AddVmdk() {
	char vmid[20];
	GenerateId(vmid, sizeof(vmid));
	VmHandle vm = NewVm(vmid);
	assert(vm != kInvalidVmHandle);
	assert(NewVm(vmid) == kInvalidVmHandle);
	assert(GetVmHandle(vmid) == vm);

	const int kVmdks = 4;
	VmdkHandle vmdks[kVmdks];
	for (int i = 0; i < kVmdks; ++i) {
		char vmdkid[20];
		snprintf(vmdkid, sizeof(vmdkid), "%d", i);

		vmdks[i] = NewActiveVmdk(vm, vmdkid);
		assert(vmdks[i] != kInvalidVmdkHandle);

		assert(NewActiveVmdk(kInvalidVmHandle, vmdkid) == kInvalidVmdkHandle);
		assert(NewActiveVmdk(vm, vmdkid) == kInvalidVmdkHandle);
		assert(GetVmdkHandle(vmdkid) == vmdks[i]);
	}

	for (int i = 0; i < kVmdks; ++i) {
		RemoveVmdk(vmdks[i]);
	}

	for (int i = 0; i < kVmdks; ++i) {
		RemoveVmdk(vmdks[i]);
	}

	for (int i = 0; i < kVmdks; ++i) {
		char vmdkid[20];
		snprintf(vmdkid, sizeof(vmdkid), "%d", i);
		assert(GetVmdkHandle(vmdkid) == kInvalidVmdkHandle);
	}

	RemoveVm(vm);
}

int main(int argc, char* agrv[]) {
	int rc = InitializeLibrary();
	if (rc < 0) {
		fprintf(stderr, "InitializeLibrary failed\n");
		return -rc;
	}

	Test_AddVmWithSameID();
	Test_AddVmdkWithInvalidVmHandle();
	Test_AddVmdk();
	return 0;
}