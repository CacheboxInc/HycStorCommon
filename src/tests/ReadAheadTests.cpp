#include <memory>
#include <vector>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "Request.h"
#include "Vmdk.h"
#include "VmdkConfig.h"
#include "VirtualMachine.h"
#include "LockHandler.h"
#include "CompressHandler.h"
#include "EncryptHandler.h"
#include "ReadAhead.h"
#include "MultiTargetHandler.h"
#include "DaemonUtils.h"
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

static const size_t kVmdkBlockSize = 4096;
static const VmdkID kVmdkid = "kVmdkid";
static const VmID kVmid = "kVmid";
static const int64_t kDiskSize = 21474836480; // 20GB
#define N_ACCESSES	(64)
#define N_STREAMS	(4)

using namespace pio;
using namespace pio::config;
using pio::ReadResultVec;
using pio::ReqBlockVec;
using ReadRequestVec = std::vector<::hyc_thrift::ReadRequest>;

class ReadAheadTests : public ::testing::Test {
protected:
	std::unique_ptr<ActiveVmdk> vmdkp_;
	
	virtual void SetUp() {
		VmdkConfig config;
		DefaultVmdkConfig(config);

		vmdkp_ = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize());
		EXPECT_NE(vmdkp_, nullptr);

		auto lock = std::make_unique<LockHandler>();
		auto multi_target = std::make_unique<MultiTargetHandler>(
			vmdkp_.get(), &config);
		EXPECT_NE(lock, nullptr);
		EXPECT_NE(multi_target, nullptr);

		vmdkp_->RegisterRequestHandler(std::move(lock));
		vmdkp_->RegisterRequestHandler(std::move(multi_target));
	}

	void DefaultVmdkConfig(VmdkConfig& config) {
		config.SetVmdkId(kVmdkid);
		config.SetVmId(kVmid);
		config.SetBlockSize(kVmdkBlockSize);
		config.DisableCompression();
		config.DisableEncryption();
		config.DisableFileCache();
		config.DisableRamCache();
		config.DisableErrorHandler();
		config.EnableSuccessHandler();
		config.DisableFileTarget();
		config.DisableNetworkTarget();
		config.EnableReadAhead();
		config.SetDiskSize(kDiskSize);
	}

	typedef enum {
  		SEQUENTIAL =	0,
  		POS_STRIDE_2 =	1,
  		NEG_STRIDE_3 =	2,
  		CORRELATED =	3,
  		RANDOM =		4
	} pattern_t;

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
  		switch(pattern) {
  		case RANDOM:
    		return (lba_base + get_random(1024*1024));
  		case CORRELATED: {
      		int corr_stride[3] = {0, 1, 2};
      		return (lba_base + i_off + corr_stride[i_off%3]);
    	}    
  		case NEG_STRIDE_3:
    		return (lba_base + i_off*(-3));
  		case POS_STRIDE_2:
    		return (lba_base + i_off*(2));
  		case SEQUENTIAL:
  		default:
    		return (lba_base + i_off);
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
			auto bufferp = NewRequestBuffer(vmdkp_->BlockSize());
			auto p = bufferp->Payload();
			auto offset = lbas[i];
			if(not IsBlockSizeAlgined(offset, kVmdkBlockSize)) {
				offset = AlignDownToBlockSize(offset, kVmdkBlockSize);
			}
			auto req = std::make_unique<Request>(i+1, vmdkp_.get(),
						Request::Type::kRead, p, bufferp->Size(), bufferp->Size(),
						offset);
			req->ForEachRequestBlock([&] (RequestBlock *blockp) mutable {
				process->emplace_back(blockp);
				return true;
			});
			requests.emplace_back(std::move(req));
		}
		bool is_ready = false;
		if(vmdkp_->read_aheadp_ != NULL && vmdkp_->read_aheadp_->IsReadAheadEnabled()) {
			auto future = vmdkp_->read_aheadp_->Run(*process, requests);
			future.wait();
			is_ready = future.isReady();
		}
		EXPECT_TRUE(is_ready);
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

TEST_F(ReadAheadTests, ConfigTestEnable) {
	RunConfigTest(true);
}

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

TEST_F(ReadAheadTests, RandomPattern) {
	RunTest(RANDOM);
}

TEST_F(ReadAheadTests, ConfigTestDisable) {
	RunConfigTest(false);
}
