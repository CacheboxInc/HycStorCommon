#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>

#include "TgtInterface.h"

const char kVmConfig[] = "{\"VmID\":\"1\"}";
const char kVmdkConfig[] = "{\"VmID\":\"1\",\"VmdkID\":\"1\",\"BlockSize\":"
	"\"4096\",\"Compreesion\":\"snappy\",\"EncryptionKey\":\"abcd\"}";

static void GenerateId(char* id, size_t size) {
	(void) snprintf(id, size, "%lu", time(NULL));
}

static int FindNumberOfActiveThreads() {
	const char threads_str[] = "Threads:";
	char t[sizeof(threads_str)];
	long int threads = 0;

	ssize_t read;
	char *linep = NULL;
	size_t len = 0;

	char path[4096];
	snprintf(path, sizeof(path), "/proc/%d/status", getpid());

	FILE *fp = fopen(path, "r");
	assert(fp != NULL);

	while ((read = getline(&linep, &len, fp)) != -1) {
		if (strncmp(linep, threads_str, sizeof(threads_str) - 1)) {
			continue;
		}

		sscanf(linep, "%s\t%ld", (char *) &t, &threads);
		assert(threads > 0);
		break;
	}
	free(linep);

	fclose(fp);
	assert(threads > 0);
	return threads;
}

static void* ThreadInitLibrary(void* datap) {
	pthread_barrier_t *barrierp = datap;

	int rc = pthread_barrier_wait(barrierp);
	assert(rc == PTHREAD_BARRIER_SERIAL_THREAD || rc == 0);

	rc = InitializeLibrary();
	if (rc != 0) {
		fprintf(stderr, "InitializeLibrary failed %d\n", rc);
	}
	return NULL;
}

static void Test_MultiplInitialize() {
	/*
	 * Test ensures - multiple InitializeLibrary() does not result in multiple
	 * initializations.
	 */
	int threads_before = FindNumberOfActiveThreads();

	const int kMaxThreads = 32;
	pthread_t threads[kMaxThreads];
	pthread_barrier_t barrier;

	int rc = pthread_barrier_init(&barrier, NULL, kMaxThreads);
	assert(rc == 0);

	for (int i = 0; i < kMaxThreads; ++i) {

		int rc = pthread_create(&threads[i], NULL, ThreadInitLibrary, &barrier);
		assert(rc == 0);
	}

	for (int i = 0; i < kMaxThreads; ++i) {
		pthread_join(threads[i], NULL);
	}

	int threads_after = FindNumberOfActiveThreads();
	assert(threads_after > threads_before);
	assert(threads_after - get_nprocs() == threads_before);

	for (int i = 0; i < 32; ++i) {
		assert(InitializeLibrary() == 0);
	}

	assert(threads_after == FindNumberOfActiveThreads());
}

static void Test_AddVmWithSameID() {
	assert(InitializeLibrary() == 0);

	char vmid[20];
	GenerateId(vmid, sizeof(vmid));
	VmHandle vm = NewVm(vmid, kVmConfig);
	assert(vm != kInvalidVmHandle);
	assert(NewVm(vmid, kVmConfig) == kInvalidVmHandle);
	assert(GetVmHandle(vmid) == vm);

	RemoveVm(vm);
	assert(GetVmHandle(vmid) == kInvalidVmHandle);

	/* try to add VMDK to removed VM */
	char vmdkid[20];
	GenerateId(vmdkid, sizeof(vmdkid));
	assert(NewActiveVmdk(vm, vmdkid, kVmdkConfig) == kInvalidVmdkHandle);
}

static void Test_AddVmdkWithInvalidVmHandle() {
	assert(InitializeLibrary() == 0);

	char vmdkid[20];
	GenerateId(vmdkid, sizeof(vmdkid));

	/* Adding VMDK to invalid VmHandle fails */
	VmHandle vm = kInvalidVmHandle;
	assert(NewActiveVmdk(vm, vmdkid, kVmdkConfig) == kInvalidVmdkHandle);
}

static void Test_AddVmdk() {
	assert(InitializeLibrary() == 0);

	char vmid[20];
	GenerateId(vmid, sizeof(vmid));
	VmHandle vm = NewVm(vmid, kVmConfig);
	assert(vm != kInvalidVmHandle);
	assert(NewVm(vmid, kVmConfig) == kInvalidVmHandle);
	assert(GetVmHandle(vmid) == vm);

	const int kVmdks = 4;
	VmdkHandle vmdks[kVmdks];
	for (int i = 0; i < kVmdks; ++i) {
		char vmdkid[20];
		snprintf(vmdkid, sizeof(vmdkid), "%d", i);

		vmdks[i] = NewActiveVmdk(vm, vmdkid, kVmdkConfig);
		assert(vmdks[i] != kInvalidVmdkHandle);

		assert(NewActiveVmdk(kInvalidVmHandle, vmdkid, kVmdkConfig) == kInvalidVmdkHandle);
		assert(NewActiveVmdk(vm, vmdkid, kVmdkConfig) == kInvalidVmdkHandle);
		assert(GetVmdkHandle(vmdkid) == vmdks[i]);
	}

	for (int j = 0; j < 2; ++j) {
		for (int i = 0; i < kVmdks; ++i) {
			char vmdkid[20];
			snprintf(vmdkid, sizeof(vmdkid), "%d", i);
			RemoveVmdk(vmdks[i]);
			assert(GetVmdkHandle(vmdkid) == kInvalidVmdkHandle);
		}
	}

	RemoveVm(vm);
}

int main(int argc, char* agrv[]) {
	Test_MultiplInitialize();
	Test_AddVmWithSameID();
	Test_AddVmdkWithInvalidVmHandle();
	Test_AddVmdk();
	return 0;
}