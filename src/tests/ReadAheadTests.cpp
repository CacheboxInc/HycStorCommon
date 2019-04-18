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

static const size_t kVmdkBlockSize = 16384;
static const VmdkID kVmdkid = "kVmdkid";
static const VmID kVmid = "kVmid";
static const VmUUID kVmUUID = "kVmUUID";
static const VmdkUUID kVmdkUUID = "kVmdkUUID";
static const int64_t kDiskSize = 21474836480; // 20GB
#define N_ACCESSES	(32)
#define N_STREAMS	(4)

using namespace pio;
using namespace pio::config;
using pio::ReadResultVec;
using pio::ReqBlockVec;
using ReadRequestVec = std::vector<::hyc_thrift::ReadRequest>;

class ReadAheadTests : public ::testing::Test {
protected:
	std::unique_ptr<ActiveVmdk> vmdkp_;
	VmHandle vm_handle_{StorRpc_constants::kInvalidVmHandle()};
	VmdkHandle vmdk_handle_{StorRpc_constants::kInvalidVmdkHandle()};
	::StorD stord_instance;

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
		config.SetVmUUID(kVmUUID);
		config.SetAeroClusterID("0");
		return NewVm(kVmid.c_str(), config.Serialize().c_str());
	}

	VmdkHandle AddVmdk(VmHandle vm_handle, VmdkConfig& c) {
		c.SetVmdkId(kVmdkid);
		c.SetVmId(kVmid);
		c.SetVmdkUUID(kVmdkUUID);
		c.SetVmUUID(kVmUUID);
		c.SetBlockSize(kVmdkBlockSize);
		c.EnableReadAhead();
		c.SetDiskSize(kDiskSize);
		c.DisableCompression();
		c.DisableEncryption();
		c.DisableFileCache();
		c.DisableRamCache();
		c.DisableErrorHandler();
		c.EnableSuccessHandler();
		c.SetSuccessHandlerDelay(10000);
		c.SetRamMetaDataKV();
		c.DisableFileTarget();
	    c.DisableNetworkTarget();
		return NewActiveVmdk(vm_handle, kVmdkid.c_str(), c.Serialize().c_str());
	}
	
	virtual void SetUp() {
		VmdkConfig config;
		
		stord_instance.InitStordLib();
		vm_handle_ = AddVm();
		EXPECT_NE(vm_handle_, StorRpc_constants::kInvalidVmHandle());
		vmdk_handle_ = AddVmdk(vm_handle_, config);
		EXPECT_NE(vmdk_handle_, StorRpc_constants::kInvalidVmdkHandle());

		auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(kVmid);
		EXPECT_NE(vmp, nullptr);
		auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(kVmdkid);
		auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
		EXPECT_NE(vmdkp, nullptr);
		EXPECT_EQ(vmp, vmdkp->GetVM());
		
		vmdkp_ = std::make_unique<ActiveVmdk>(1, "1", vmp, config.Serialize());
		EXPECT_NE(vmdkp_, nullptr);
		EXPECT_NE(vmdkp_->read_aheadp_, nullptr);

		auto multi_target = std::make_unique<MultiTargetHandler>(
							vmdkp_.get(), &config);
		EXPECT_NE(multi_target, nullptr);

		vmdkp_->RegisterRequestHandler(std::move(multi_target));
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
  		
		if(!IsBlockSizeAlgined(lba_base, kVmdkBlockSize)) {
			lba_base = AlignUpToBlockSize(lba_base, kVmdkBlockSize);
		}
		switch(pattern) {
  		case RANDOM:
    		return (lba_base + get_random(1024*kVmdkBlockSize));
  		case CORRELATED: {
      		int corr_stride[3] = {0, 1, 2};
      		return (lba_base + (i_off + corr_stride[i_off%3]) * kVmdkBlockSize);
    	}    
  		case NEG_STRIDE_3:
    		return (lba_base + i_off*((-3) * kVmdkBlockSize));
  		case POS_STRIDE_2:
    		return (lba_base + i_off*(2 * kVmdkBlockSize));
  		case SEQUENTIAL:
  		default:
    		return (lba_base + i_off*kVmdkBlockSize);
  		}
	}
	
	void RunTest(pattern_t pattern) {
		auto config = const_cast<config::VmdkConfig*>(vmdkp_->GetJsonConfig());
		if(not config->IsReadAheadEnabled()) {
			config->EnableReadAhead();
		}
		
		EXPECT_TRUE(config->IsReadAheadEnabled());
		
		uint64_t lbas[N_ACCESSES];
		auto process = std::make_unique<ReqBlockVec>();
		std::vector<std::unique_ptr<Request>> requests;

		generate_accesses(pattern, lbas);
		for(int i=0; i<N_ACCESSES; i++) {
			auto bufferp = NewRequestBuffer(kVmdkBlockSize);
			auto p = bufferp->Payload();
			auto offset = lbas[i];
			auto req = std::make_unique<Request>(i+1, vmdkp_.get(),
						Request::Type::kRead, p, bufferp->Size(), bufferp->Size(),
						offset);
			req->ForEachRequestBlock([&] (RequestBlock *blockp) mutable {
				process->emplace_back(blockp);
				return true;
			});
			requests.emplace_back(std::move(req));
		}
		if(vmdkp_->read_aheadp_ != nullptr && vmdkp_->read_aheadp_->IsReadAheadEnabled()) {
			auto future = vmdkp_->read_aheadp_->Run(*process, requests);
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
		auto config = const_cast<config::VmdkConfig*>(vmdkp_->GetJsonConfig());
		if(config_switch == true) {
			config->EnableReadAhead();
		}
		else {
			config->DisableReadAhead();
		}

		EXPECT_TRUE(config_switch == vmdkp_->GetJsonConfig()->IsReadAheadEnabled());
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

TEST_F(ReadAheadTests, NullVmdkPassCtor) {
	try {
		auto rh_object = new ReadAhead(NULL);
		delete rh_object;
	}
	catch(const runtime_error& error) {
		EXPECT_TRUE(strlen(error.what()) > 0);
		return;
	}
	EXPECT_TRUE(false);
}
