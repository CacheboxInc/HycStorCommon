#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "DaemonTgtInterface.h"
#include "DaemonUtils.h"
#include "Singleton.h"
#include "VirtualMachine.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "VmdkFactory.h"
#include "VmManager.h"
#include "TgtInterfaceImpl.h"

using namespace pio;
using namespace pio::config;
using namespace ::ondisk;
using namespace ::hyc_thrift;
using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;

const VmID kVmid = "VmID";
const VmdkID kVmdkid = "VmdkID";

class PreloadTest : public TestWithParam<
		::testing::tuple<uint64_t, Offset, Offset, uint32_t>> {
public:
	VmHandle vm_handle_{StorRpc_constants::kInvalidVmHandle()};
	VmdkHandle vmdk_handle_{StorRpc_constants::kInvalidVmdkHandle()};
	struct {
		Offset start_;
		Offset end_;
		uint64_t step_;
		std::vector<PreloadOffset> offsets_;
	} preload_;
	uint64_t block_size{};

	::StorD stord_instance;

	static void SetUpTestCase() {
	}

	static void TearDownTestCase() {
	}

	virtual void SetUp() {
		stord_instance.InitStordLib();

		std::tie(block_size, preload_.start_, preload_.end_, preload_.step_) = GetParam();
		LOG(INFO) << block_size << ','
			<< preload_.start_ << ','
			<< preload_.end_ << ','
			<< preload_.step_;
		vm_handle_ = AddVm();
		EXPECT_NE(vm_handle_, StorRpc_constants::kInvalidVmHandle());
		vmdk_handle_ = AddVmdk(vm_handle_);
		EXPECT_NE(vmdk_handle_, StorRpc_constants::kInvalidVmdkHandle());
	}

	virtual void TearDown() {
		RemoveVmdk(vmdk_handle_);
		RemoveVm(vm_handle_);
		vm_handle_ = StorRpc_constants::kInvalidVmHandle();
		vmdk_handle_ = StorRpc_constants::kInvalidVmdkHandle();
		stord_instance.DeinitStordLib();
	}

	VmHandle AddVm() {
		VmConfig config;
		config.SetVmId(kVmid);
		config.SetAeroClusterID("0");
		return NewVm(kVmid.c_str(), config.Serialize().c_str());
	}

	VmdkHandle AddVmdk(VmHandle vm_handle) {
		preload_.offsets_.clear();
		for (const auto o :
				iter::Range(preload_.start_, preload_.end_, preload_.step_)) {
			preload_.offsets_.emplace_back(o, block_size);
		}
		VmdkConfig c;
		c.SetVmdkId(kVmdkid);
		c.SetVmId(kVmid);
		c.SetBlockSize(block_size);
		c.ConfigureRamCache(1024);
		c.SetPreloadBlocks(preload_.offsets_);
		return NewActiveVmdk(vm_handle, kVmdkid.c_str(), c.Serialize().c_str());
	}
};

TEST_P(PreloadTest, EnsurePreload) {
	using namespace std::chrono_literals;

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(kVmid);
	EXPECT_NE(vmp, nullptr);
	auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(kVmdkid);
	auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
	EXPECT_NE(vmdkp, nullptr);
	EXPECT_EQ(vmp, vmdkp->GetVM());

	if (preload_.end_ == 0 or preload_.step_ == 0) {
		EXPECT_EQ(vmdkp->cache_stats_.read_populates_, 0);
		return;
	}

	auto rm = vmdkp->cache_stats_.read_miss_.load();
	EXPECT_EQ(vmdkp->cache_stats_.read_hits_, 0);

	auto rp = vmdkp->cache_stats_.read_populates_.load();
	uint32_t c = 0;
	const auto& blocks = vmdkp->GetPreloadBlocks();
	for (const auto& p : blocks) {
		c += p.second;
	}
	EXPECT_EQ(rp, c);
	EXPECT_GT(rp, 0);
	EXPECT_EQ(rp, rm);

	auto f = vmp->StartPreload(vmdkp);
	f.wait(1s);
	EXPECT_TRUE(f.isReady());
	EXPECT_EQ(f.value(), 0);
	EXPECT_EQ(vmdkp->cache_stats_.read_populates_, rp);
	EXPECT_EQ(vmdkp->cache_stats_.read_miss_, rm);
	EXPECT_EQ(vmdkp->cache_stats_.read_hits_, c);
}

INSTANTIATE_TEST_CASE_P(PreloadTestParams, PreloadTest,
	Combine(
		Values(4096, 8192, 16*1024, 32*1024),
		Values(0, 511, 1023, 10000),
		Values(0, 1024*1024),
		Values(0, 511, 4096, 10000, 50000)
	)
);
