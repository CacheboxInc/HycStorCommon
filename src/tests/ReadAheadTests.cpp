#include <memory>
#include <vector>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "Request.h"
#include "Vmdk.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "VirtualMachine.h"
#include "CompressHandler.h"
#include "EncryptHandler.h"
#include "ReadAhead.h"
#include "MultiTargetHandler.h"
#include "DaemonUtils.h"
#include "DaemonTgtInterface.h"
#include "Singleton.h"
#include "VmdkFactory.h"
#include "VmManager.h"
#include "TgtInterfaceImpl.h"
#include <iostream>

using namespace pio;
using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;
using namespace ::ondisk;
using namespace ::hyc_thrift;
using namespace std::chrono_literals;
using namespace std;
using namespace ::hyc_thrift;
using namespace pio;
using namespace pio::config;

static const size_t kVmdkBlockSize1 = 16384;
static const VmdkID kVmdkid1 = "kVmdkid1";
static const VmID kVmid1 = "kVmid1";
static const VmUUID kVmUUID1 = "kVmUUID1";
static const VmdkUUID kVmdkUUID1 = "kVmdkUUID1";
static const uint64_t kDiskSize1 = 21474836480; // 20GB
static const size_t kVmdkBlockSize2 = 4096;
static const VmdkID kVmdkid2 = "kVmdkid2";
static const VmID kVmid2 = "kVmid2";
static const VmUUID kVmUUID2 = "kVmUUID2";
static const VmdkUUID kVmdkUUID2 = "kVmdkUUID2";
static const uint64_t kDiskSize2 = 10737418240; // 10GB
#define N_ACCESSES	(32)
#define N_STREAMS	(4)

using namespace pio;
using namespace pio::config;
using pio::ReadResultVec;
using pio::ReqBlockVec;
using ReadRequestVec = std::vector<::hyc_thrift::ReadRequest>;

class ReadAheadTests : public ::testing::Test {
protected:
	std::unique_ptr<ActiveVmdk> vmdk1p_;
	std::unique_ptr<ActiveVmdk> vmdk2p_;
	VmHandle vm1_handle_{StorRpc_constants::kInvalidVmHandle()};
	VmHandle vm2_handle_{StorRpc_constants::kInvalidVmHandle()};
	VmdkHandle vmdk1_handle_{StorRpc_constants::kInvalidVmdkHandle()};
	VmdkHandle vmdk2_handle_{StorRpc_constants::kInvalidVmdkHandle()};
	::StorD stord_instance;
	VmdkConfig config1_;
	VmdkConfig config2_;

	virtual void TearDown() {
		ReleaseResources();
		stord_instance.DeinitStordLib();
	}
	
	void ReleaseResources() {
		RemoveVmdk(vmdk1_handle_);
		RemoveVmdk(vmdk2_handle_);
		RemoveVm(vm1_handle_);
		RemoveVm(vm2_handle_);
		vm1_handle_ = StorRpc_constants::kInvalidVmHandle();
		vm2_handle_ = StorRpc_constants::kInvalidVmHandle();
		vmdk1_handle_ = StorRpc_constants::kInvalidVmdkHandle();
		vmdk2_handle_ = StorRpc_constants::kInvalidVmdkHandle();
	}
	
	VmHandle AddVm1() {
		VmConfig config;
		config.SetVmId(kVmid1);
		config.SetVmUUID(kVmUUID1);
		config.SetAeroClusterID("0");
		return NewVm(kVmid1.c_str(), config.Serialize().c_str());
	}

	VmdkHandle AddVmdk1(VmHandle vm1_handle) {
		config1_.SetVmdkId(kVmdkid1);
		config1_.SetVmId(kVmid1);
		config1_.SetVmdkUUID(kVmdkUUID1);
		config1_.SetVmUUID(kVmUUID1);
		config1_.SetBlockSize(kVmdkBlockSize1);
		config1_.EnableReadAhead();
		config1_.SetDiskSize(kDiskSize1);
		config1_.DisableCompression();
		config1_.DisableEncryption();
		config1_.DisableFileCache();
		config1_.DisableRamCache();
		config1_.DisableErrorHandler();
		config1_.EnableSuccessHandler();
		config1_.SetSuccessHandlerDelay(10000);
		config1_.SetRamMetaDataKV();
		config1_.DisableFileTarget();
	    config1_.DisableNetworkTarget();
		return NewActiveVmdk(vm1_handle, kVmdkid1.c_str(), config1_.Serialize().c_str());
	}
	
	VmHandle AddVm2() {
		VmConfig config;
		config.SetVmId(kVmid2);
		config.SetVmUUID(kVmUUID2);
		config.SetAeroClusterID("0");
		return NewVm(kVmid2.c_str(), config.Serialize().c_str());
	}

	VmdkHandle AddVmdk2(VmHandle vm2_handle) {
		config2_.SetVmdkId(kVmdkid2);
		config2_.SetVmId(kVmid2);
		config2_.SetVmdkUUID(kVmdkUUID2);
		config2_.SetVmUUID(kVmUUID2);
		config2_.SetBlockSize(kVmdkBlockSize2);
		config2_.EnableReadAhead();
		config2_.SetDiskSize(kDiskSize2);
		config2_.DisableCompression();
		config2_.DisableEncryption();
		config2_.DisableFileCache();
		config2_.DisableRamCache();
		config2_.DisableErrorHandler();
		config2_.EnableSuccessHandler();
		config2_.SetSuccessHandlerDelay(10000);
		config2_.SetRamMetaDataKV();
		config2_.DisableFileTarget();
	    config2_.DisableNetworkTarget();
		return NewActiveVmdk(vm2_handle, kVmdkid2.c_str(), config2_.Serialize().c_str());
	}
	
	virtual void SetUp() {
		stord_instance.InitStordLib();
		Initialize();
	}
	
	void Initialize() {
		vm1_handle_ = AddVm1();
		EXPECT_NE(vm1_handle_, StorRpc_constants::kInvalidVmHandle());
		vmdk1_handle_ = AddVmdk1(vm1_handle_);
		EXPECT_NE(vmdk1_handle_, StorRpc_constants::kInvalidVmdkHandle());

		auto vmp1 = SingletonHolder<VmManager>::GetInstance()->GetInstance(kVmid1);
		EXPECT_NE(vmp1, nullptr);
		auto p1 = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(kVmdkid1);
		auto vmdkp1 = dynamic_cast<ActiveVmdk*>(p1);
		EXPECT_NE(vmdkp1, nullptr);
		EXPECT_EQ(vmp1, vmdkp1->GetVM());
		
		vmdk1p_ = std::make_unique<ActiveVmdk>(vm1_handle_, kVmdkid1, vmp1, config1_.Serialize());
		EXPECT_NE(vmdk1p_, nullptr);
		EXPECT_NE(vmdk1p_->read_aheadp_, nullptr);

		auto multi_target1 = std::make_unique<MultiTargetHandler>(
							vmdk1p_.get(), &config1_);
		EXPECT_NE(multi_target1, nullptr);

		vmdk1p_->RegisterRequestHandler(std::move(multi_target1));
		
		vm2_handle_ = AddVm2();
		EXPECT_NE(vm2_handle_, StorRpc_constants::kInvalidVmHandle());
		vmdk2_handle_ = AddVmdk2(vm2_handle_);
		EXPECT_NE(vmdk2_handle_, StorRpc_constants::kInvalidVmdkHandle());
		
		auto vmp2 = SingletonHolder<VmManager>::GetInstance()->GetInstance(kVmid2);
		EXPECT_NE(vmp2, nullptr);
		auto p2 = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(kVmdkid2);
		auto vmdkp2 = dynamic_cast<ActiveVmdk*>(p2);
		EXPECT_NE(vmdkp2, nullptr);
		EXPECT_EQ(vmp2, vmdkp2->GetVM());
		
		vmdk2p_ = std::make_unique<ActiveVmdk>(vm2_handle_, kVmdkid2, vmp2, config2_.Serialize());
		EXPECT_NE(vmdk2p_, nullptr);
		EXPECT_NE(vmdk2p_->read_aheadp_, nullptr);

		auto multi_target2 = std::make_unique<MultiTargetHandler>(
							vmdk2p_.get(), &config2_);
		EXPECT_NE(multi_target2, nullptr);
		
		vmdk2p_->RegisterRequestHandler(std::move(multi_target2));

	}

	typedef enum {
  		SEQUENTIAL =	0,
  		POS_STRIDE_2 =	1,
  		NEG_STRIDE_3 =	2,
  		CORRELATED =	3,
  		RANDOM =		4
	}pattern_t;

	int get_random(int rmax) {
  		return (rand() % rmax);
	}
	
	void generate_accesses(pattern_t pattern, uint64_t lbas[]) {
		int i;
		for (i=0; i<N_ACCESSES; i++) {
			lbas[i] = generate_one_access(pattern, i);
		}
	}

	uint64_t generate_one_access(pattern_t pattern, int i) {
  		int i_stream = (i%N_STREAMS), i_off = (i/N_STREAMS);
  		uint64_t lba_base = (((1+i_stream)*1024ULL*1024*1024) +
		       				(1)*1024ULL*1024);
  		
		if(!IsBlockSizeAlgined(lba_base, kVmdkBlockSize1)) {
			lba_base = AlignUpToBlockSize(lba_base, kVmdkBlockSize1);
		}
		switch(pattern) {
  		case RANDOM:
    		return (lba_base + get_random(1024*kVmdkBlockSize1));
  		case CORRELATED: {
      		int corr_stride[3] = {0, 1, 2};
      		return (lba_base + (i_off + corr_stride[i_off%3]) * kVmdkBlockSize1);
    	}    
  		case NEG_STRIDE_3:
    		return (lba_base + i_off*((-3) * kVmdkBlockSize1));
  		case POS_STRIDE_2:
    		return (lba_base + i_off*(2 * kVmdkBlockSize1));
  		case SEQUENTIAL:
  		default:
    		return (lba_base + i_off*kVmdkBlockSize1);
  		}
	}
	
	void RunTest(pattern_t pattern) {
		auto config = const_cast<config::VmdkConfig*>(vmdk1p_->GetJsonConfig());
		if(not config->IsReadAheadEnabled()) {
			config->EnableReadAhead();
		}
		
		EXPECT_TRUE(config->IsReadAheadEnabled());
		
		uint64_t lbas[N_ACCESSES];
		auto process = std::make_unique<ReqBlockVec>();
		std::vector<std::unique_ptr<Request>> requests;

		generate_accesses(pattern, lbas);
		for(int i=0; i<N_ACCESSES; i++) {
			auto bufferp = NewRequestBuffer(kVmdkBlockSize1);
			auto p = bufferp->Payload();
			auto offset = lbas[i];
			auto req = std::make_unique<Request>(i+1, vmdk1p_.get(),
						Request::Type::kRead, p, bufferp->Size(), bufferp->Size(),
						offset);
			req->ForEachRequestBlock([&] (RequestBlock *blockp) mutable {
				process->emplace_back(blockp);
				return true;
			});
			requests.emplace_back(std::move(req));
		}
		if(vmdk1p_->read_aheadp_ != nullptr) {
			auto future = vmdk1p_->read_aheadp_->Run(*process, requests);
			future.wait();
			EXPECT_TRUE(future.isReady());
			auto value = std::move(future.value());
			if(pattern != pattern_t::RANDOM) {
				EXPECT_NE((*value).size(), 0);
				return;
			}
			EXPECT_EQ((*value).size(), 0);
		}
	}

	void RunConfigTest(bool config_switch) {
		auto config = const_cast<config::VmdkConfig*>(vmdk1p_->GetJsonConfig());
		if(config_switch == true) {
			config->EnableReadAhead();
		}
		else {
			config->DisableReadAhead();
		}

		EXPECT_TRUE(config_switch == vmdk1p_->GetJsonConfig()->IsReadAheadEnabled());
	}
	
	bool VerifyConfigEqual(const VmdkConfig& config1, const VmdkConfig& config2) {
		uint32_t value1, value2;
		uint64_t disk_size1, disk_size2;
		return  (
				config1.GetAggregateRandomOccurrences(value1) == config2.GetAggregateRandomOccurrences(value2)
				&& value1 == value2
				&& config1.GetReadAheadMaxPatternStability(value1) == config2.GetReadAheadMaxPatternStability(value2)
				&& value1 == value2
				&& config1.GetReadAheadIoMissWindow(value1) == config2.GetReadAheadIoMissWindow(value2)
				&& value1 == value2
				&& config1.GetReadAheadIoMissThreshold(value1) == config2.GetReadAheadIoMissThreshold(value2)
				&& value1 == value2
				&& config1.GetReadAheadPatternStability(value1) == config2.GetReadAheadPatternStability(value2)
				&& value1 == value2
				&& config1.GetReadAheadGhbHistoryLength(value1) == config2.GetReadAheadGhbHistoryLength(value2)
				&& value1 == value2
				&& config1.GetReadAheadMaxPredictionSize(value1) == config2.GetReadAheadMaxPredictionSize(value2)
				&& value1 == value2
				&& config1.GetReadAheadMinPredictionSize(value1) == config2.GetReadAheadMinPredictionSize(value2)
				&& value1 == value2
				&& config1.GetReadAheadMaxPacketSize(value1) == config2.GetReadAheadMaxPacketSize(value2)
				&& value1 == value2
				&& config1.GetReadAheadMaxIoSize(value1) == config2.GetReadAheadMaxIoSize(value2)
				&& value1 == value2
				&& config1.GetReadAheadMinDiskSize(disk_size1) == config2.GetReadAheadMinDiskSize(disk_size2)
				&& disk_size1 == disk_size2
				);
	}	
	
	void GetConfig(config::VmdkConfig& config) {
		config.SetAggregateRandomOccurrences(16);
		config.SetReadAheadMaxPatternStability(32);
		config.SetReadAheadIoMissWindow(24);
		config.SetReadAheadIoMissThreshold(45);
		config.SetReadAheadPatternStability(30);
		config.SetReadAheadGhbHistoryLength(2048);
		config.SetReadAheadMaxPredictionSize(1 << 18);
		config.SetReadAheadMinPredictionSize(1 << 16);
		config.SetReadAheadMaxPacketSize(1 << 18);
		config.SetReadAheadMinDiskSize(5ULL << 30);
		config.SetReadAheadMaxIoSize(1 << 16);
	}

	void GlobalConfigTest() {
		config::VmdkConfig in_config, out_config;
		// Verify that global config initializes with default values
		ReadAhead::GetGlobalConfig(out_config);
		ReadAhead::GetDefaultConfig(in_config);
		EXPECT_TRUE(VerifyConfigEqual(in_config, out_config));
		// Verify config validity and equality
		GetConfig(in_config);
		EXPECT_TRUE(ReadAhead::SetGlobalConfig(in_config));
		ReadAhead::GetGlobalConfig(out_config);
		EXPECT_TRUE(VerifyConfigEqual(in_config, out_config));
		// Verify config inequality
		in_config.SetAggregateRandomOccurrences(32);
		EXPECT_FALSE(VerifyConfigEqual(in_config, out_config));
	}
	
	void GlobalConfigTestNegative() {
		config::VmdkConfig in_config, out_config;
		GetConfig(in_config);
		in_config.SetReadAheadMaxIoSize(12); // Value of 12 is unacceptable for max_io_size
		EXPECT_FALSE(ReadAhead::SetGlobalConfig(in_config));
	}
	
	void LocalConfigTest() {
		config::VmdkConfig in_config, out_config;
		// Verify that local config initializes with global config
		EXPECT_NE(vmdk1p_, nullptr);
		EXPECT_NE(vmdk1p_->read_aheadp_, nullptr);
		vmdk1p_->read_aheadp_->GetLocalConfig(in_config);
		ReadAhead::GetGlobalConfig(out_config);
		EXPECT_TRUE(VerifyConfigEqual(in_config, out_config));
		// Verify config validity and equality
		GetConfig(in_config);
		vmdk1p_->read_aheadp_->SetLocalConfig(in_config);
		vmdk1p_->read_aheadp_->GetLocalConfig(out_config);
		EXPECT_TRUE(VerifyConfigEqual(in_config, out_config));
		// Verify config inequality
		in_config.SetAggregateRandomOccurrences(32);
		EXPECT_FALSE(VerifyConfigEqual(in_config, out_config));
	}
	
	void LocalConfigTestNegative() {
		config::VmdkConfig in_config, out_config;
		GetConfig(in_config);
		in_config.SetReadAheadMaxIoSize(12); // Value of 12 is unacceptable for max_io_size
		EXPECT_FALSE(vmdk1p_->read_aheadp_->SetLocalConfig(in_config));
	}

	void TestNegativeConfig() {
		auto config = const_cast<config::VmdkConfig*>(vmdk2p_->GetJsonConfig());
		config->SetReadAheadMinDiskSize(0);
		try {
			auto rh_object = new ReadAhead(vmdk2p_.get());
			delete rh_object;
		}
		catch(const runtime_error& error) {
			EXPECT_TRUE(strlen(error.what()) > 0);
			return;
		}
		EXPECT_TRUE(false);
	}
	
	void TestPositiveConfig() {
		auto config = const_cast<config::VmdkConfig*>(vmdk2p_->GetJsonConfig());
		config->SetReadAheadMinDiskSize(1ULL << 30);
		try {
			auto rh_object = new ReadAhead(vmdk2p_.get());
			delete rh_object;
		}
		catch(const runtime_error& error) {
			EXPECT_FALSE(strlen(error.what()) > 0);
			return;
		}
		EXPECT_TRUE(true);
	}

	void AllVmGlobalConfigApplyTest() {
		config::VmdkConfig in_config, out_config;
		ReleaseResources();
		GetConfig(config1_);
		GetConfig(config2_);
		EXPECT_TRUE(VerifyConfigEqual(config1_, config2_));
		EXPECT_TRUE(ReadAhead::SetGlobalConfig(config1_));
		Initialize();
		auto config1 = const_cast<config::VmdkConfig*>(vmdk1p_->GetJsonConfig());
		auto config2 = const_cast<config::VmdkConfig*>(vmdk2p_->GetJsonConfig());
		EXPECT_TRUE(VerifyConfigEqual(*config1, *config2));
		ReadAhead::GetGlobalConfig(out_config);
		EXPECT_TRUE(VerifyConfigEqual(*config1, out_config));
		EXPECT_TRUE(VerifyConfigEqual(*config2, out_config));
	}
	
	void AllVmdkLocalConfigDisableEnableTest() {
		config::VmdkConfig config, config1, config2;
		vmdk1p_->read_aheadp_.reset(new ReadAhead(vmdk1p_.get()));
		vmdk2p_->read_aheadp_.reset(new ReadAhead(vmdk2p_.get()));
		EXPECT_NE(vmdk1p_->read_aheadp_, nullptr);
		EXPECT_NE(vmdk2p_->read_aheadp_, nullptr);
		vmdk1p_->read_aheadp_->GetLocalConfig(config1);
		vmdk2p_->read_aheadp_->GetLocalConfig(config2);
		EXPECT_TRUE(VerifyConfigEqual(config1, config2));
		ReadAhead::GetGlobalConfig(config);
		EXPECT_TRUE(VerifyConfigEqual(config1, config));
		EXPECT_TRUE(VerifyConfigEqual(config2, config));
	}
	
	void AllVmdkDefaultConfigDisableEnableTest() {
		config::VmdkConfig config, config1, config2;
		ReadAhead::GetDefaultConfig(config);
		ReadAhead::SetGlobalConfig(config);
		vmdk1p_->read_aheadp_.reset(new ReadAhead(vmdk1p_.get()));
		vmdk2p_->read_aheadp_.reset(new ReadAhead(vmdk2p_.get()));
		EXPECT_NE(vmdk1p_->read_aheadp_, nullptr);
		EXPECT_NE(vmdk2p_->read_aheadp_, nullptr);
		vmdk1p_->read_aheadp_->GetLocalConfig(config1);
		vmdk2p_->read_aheadp_->GetLocalConfig(config2);
		ReadAhead::GetGlobalConfig(config);
		EXPECT_TRUE(VerifyConfigEqual(config1, config));
		EXPECT_TRUE(VerifyConfigEqual(config2, config));
		ReadAhead::GetDefaultConfig(config);
		EXPECT_TRUE(VerifyConfigEqual(config1, config));
		EXPECT_TRUE(VerifyConfigEqual(config2, config));
	}
	
	void AllVmdkLocalConfigRunningEqualTest() {
		config::VmdkConfig config, config1, config2;
		EXPECT_NE(vmdk1p_->read_aheadp_, nullptr);
		EXPECT_NE(vmdk2p_->read_aheadp_, nullptr);
		GetConfig(config);
		vmdk1p_->read_aheadp_->SetLocalConfig(config);
		vmdk2p_->read_aheadp_->SetLocalConfig(config);
		vmdk1p_->read_aheadp_->GetLocalConfig(config1);
		vmdk2p_->read_aheadp_->GetLocalConfig(config2);
		EXPECT_TRUE(VerifyConfigEqual(config1, config2));
	}
	
	void AllVmdkLocalConfigRunningUnEqualTest() {
		config::VmdkConfig config, config1, config2;
		EXPECT_NE(vmdk1p_->read_aheadp_, nullptr);
		EXPECT_NE(vmdk2p_->read_aheadp_, nullptr);
		GetConfig(config);
		vmdk1p_->read_aheadp_->SetLocalConfig(config);
		config.SetReadAheadMaxPredictionSize(1ULL << 19);
		vmdk2p_->read_aheadp_->SetLocalConfig(config);
		vmdk1p_->read_aheadp_->GetLocalConfig(config1);
		vmdk2p_->read_aheadp_->GetLocalConfig(config2);
		EXPECT_FALSE(VerifyConfigEqual(config1, config2));
	}
};
	
TEST_F(ReadAheadTests, SequentialPattern) {
	RunTest(SEQUENTIAL);
}

TEST_F(ReadAheadTests, PositiveStrided2Pattern) {
	RunTest(POS_STRIDE_2);
}

TEST_F(ReadAheadTests, NegativeStrided3Pattern) {
	RunTest(NEG_STRIDE_3);
}

TEST_F(ReadAheadTests, CorrelatedPattern) {
	RunTest(CORRELATED);
}

TEST_F(ReadAheadTests, ConfigTestDisable) {
	RunConfigTest(false);
}

TEST_F(ReadAheadTests, ConfigTestEnable) {
	RunConfigTest(true);
}

TEST_F(ReadAheadTests, GlobalConfigTest) {
	GlobalConfigTest();
}

TEST_F(ReadAheadTests, GlobalConfigTestNegative) {
	GlobalConfigTestNegative();
}

TEST_F(ReadAheadTests, LocalConfigTest) {
	LocalConfigTest();
}

TEST_F(ReadAheadTests, LocalConfigTestNegative) {
	LocalConfigTestNegative();
}

TEST_F(ReadAheadTests, TestNullConfig) {
	TestPositiveConfig();
}

TEST_F(ReadAheadTests, TestNegativeConfig) {
	TestNegativeConfig();
}

TEST_F(ReadAheadTests, AllVmGlobalConfigTest) {
	AllVmGlobalConfigApplyTest();
}

TEST_F(ReadAheadTests, AllVmdkLocalConfigDisableEnableTest) {
	AllVmdkLocalConfigDisableEnableTest();
}

TEST_F(ReadAheadTests, AllVmdkDefaultConfigDisableEnableTest) {
	AllVmdkDefaultConfigDisableEnableTest();
}

TEST_F(ReadAheadTests, AllVmdkLocalConfigRunningEqualTest) {
	AllVmdkLocalConfigRunningEqualTest();
}

TEST_F(ReadAheadTests, AllVmdkLocalConfigRunningUnEqualTest) {
	AllVmdkLocalConfigRunningUnEqualTest();
}

TEST_F(ReadAheadTests, NullVmdkPassCtor) {
	try {
		auto rh_object = new ReadAhead(nullptr);
		delete rh_object;
	}
	catch(const runtime_error& error) {
		EXPECT_TRUE(strlen(error.what()) > 0);
		return;
	}
	EXPECT_TRUE(false);
}
