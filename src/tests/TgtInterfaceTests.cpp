#include <vector>
#include <string>
#include <numeric>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "DaemonTgtInterface.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "TgtInterfaceImpl.h"

using namespace pio;
using namespace pio::config;
using namespace ::ondisk;
using namespace ::hyc_thrift;

static const VmdkUUID kVmdkUUID = "kVmdkUUID";
static const VmUUID kVmUUID = "kVmUUID";

class TgtInterfaceTest : public ::testing::Test {
public:

	::StorD stord_instance;
	static void SetUpTestCase() {
	}

	static void TearDownTestCase() {
	}

	virtual void SetUp() {
		stord_instance.InitStordLib();
	}

	virtual void TearDown() {
		stord_instance.DeinitStordLib();
	}

	VmHandle AddVm(const std::string& vmid) {
		VmConfig config;
		config.SetVmId(vmid);
		config.SetVmUUID(kVmUUID);
		return NewVm(vmid.c_str(), config.Serialize().c_str());
	}

	VmdkHandle AddVmdk(VmHandle vm_handle, const VmdkID& vmdkid,
			VmdkConfig& config) {
		config.SetVmUUID(kVmUUID);
		config.SetVmdkUUID(kVmdkUUID);
		return NewActiveVmdk(vm_handle, vmdkid.c_str(),
				config.Serialize().c_str());
	}
};

TEST_F(TgtInterfaceTest, AddRemoveVms) {
	const int kMaxVms = 10;
	std::vector<VmHandle> handles;

	for (auto id = 0; id < kMaxVms; ++id) {
		auto h = AddVm(std::to_string(id));
		EXPECT_NE(h, StorRpc_constants::kInvalidVmHandle());
		handles.emplace_back(h);
	}

	auto id = 0;
	for (auto& h : handles) {
		auto handle = GetVmHandle(std::to_string(id).c_str());
		EXPECT_EQ(handle, h);
		RemoveVm(h);
		++id;
	}
	EXPECT_EQ(id, kMaxVms);

	for (auto id = 0u; id < handles.size(); id += 2) {
		auto handle = GetVmHandle(std::to_string(id).c_str());
		EXPECT_EQ(handle, StorRpc_constants::kInvalidVmHandle());
	}
}

TEST_F(TgtInterfaceTest, RemoveInvalidVms) {
	const int kMaxVms = 1024;
	std::vector<VmHandle> handles(kMaxVms);
	std::iota(handles.begin(), handles.end(), 0);

	for (auto& h : handles) {
		EXPECT_NO_THROW(RemoveVm(h));
	}
}

TEST_F(TgtInterfaceTest, ReadWriteSuccess) {
	const VmID kVmid = "VmID";
	const VmdkID kVmdkid = "VmdkID";
	auto vm_handle = AddVm(kVmid);
	EXPECT_NE(vm_handle, StorRpc_constants::kInvalidVmHandle());

	VmdkConfig config;
	config.SetVmdkId(kVmdkid);
	config.SetVmId(kVmid);
	config.SetBlockSize(4096);
	config.ConfigureCompression("snappy", 1);
	config.ConfigureEncryption("aes128-gcm", "abcd", {0});
	config.DisableEncryption();
	config.DisableFileCache();
	config.DisableNetworkTarget();
	config.DisableRamCache();
	config.DisableErrorHandler();
	config.EnableSuccessHandler();

	auto vmdk_handle = AddVmdk(vm_handle, kVmdkid, config);
	EXPECT_NE(vmdk_handle, StorRpc_constants::kInvalidVmdkHandle());

	RemoveVmdk(vmdk_handle);
	RemoveVm(vm_handle);
}
