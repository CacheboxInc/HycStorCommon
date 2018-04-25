#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "DaemonTgtInterface.h"
#include "Singleton.h"
#include "VirtualMachine.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "VmdkFactory.h"
#include "VmManager.h"

using namespace pio;
using namespace pio::config;

const VmID kVmid = "VmID";
const VmdkID kVmdkid = "VmdkID";
const uint64_t kBlockSize = 4096;

class VirtualMachineTest : public ::testing::Test {
public:
	VmHandle vm_handle_{kInvalidVmHandle};
	VmdkHandle vmdk_handle_{kInvalidVmdkHandle};

	static void SetUpTestCase() {
		InitStordLib();
	}

	static void TearDownTestCase() {
		DeinitStordLib();
	}

	virtual void SetUp() {
		vm_handle_ = AddVm();
		EXPECT_NE(vm_handle_, kInvalidVmHandle);
		vmdk_handle_ = AddVmdk(vm_handle_);
		EXPECT_NE(vmdk_handle_, kInvalidVmdkHandle);
	}

	virtual void TearDown() {
		RemoveVmdk(vmdk_handle_);
		RemoveVm(vm_handle_);
		vm_handle_ = kInvalidVmHandle;
		vmdk_handle_ = kInvalidVmdkHandle;
	}

	VmHandle AddVm() {
		VmConfig config;
		config.SetVmId(kVmid);
		config.SetAeroClusterID("0");
		return NewVm(kVmid.c_str(), config.Serialize().c_str());
	}

	VmdkHandle AddVmdk(VmHandle vm_handle) {
		VmdkConfig c;
		c.SetVmdkId(kVmdkid);
		c.SetVmId(kVmid);
		c.SetBlockSize(kBlockSize);
		c.DisableCompression();
		c.DisableEncryption();
		c.DisableFileCache();
		c.DisableRamCache();
		c.DisableErrorHandler();
		c.EnableSuccessHandler();
		c.SetSuccessHandlerDelay(10000);
		c.SetRamMetaDataKV();
		return NewActiveVmdk(vm_handle, kVmdkid.c_str(), c.Serialize().c_str());
	}
};

TEST_F(VirtualMachineTest, CheckPointSingleIO) {
	char buffer[kBlockSize];
	::memset(buffer, 'A', sizeof(buffer));

	EXPECT_NE(vm_handle_, kInvalidVmHandle);
	EXPECT_NE(vmdk_handle_, kInvalidVmdkHandle);

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(kVmid);
	EXPECT_NE(vmp, nullptr);
	auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(kVmdkid);
	auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
	EXPECT_NE(vmdkp, nullptr);
	EXPECT_EQ(vmp, vmdkp->GetVM());

	std::vector<folly::Future<int>> futures;
	for (auto i = 0; i < 10; ++i) {
		auto reqp = std::make_unique<Request>(i+1, vmdkp, Request::Type::kWrite,
			buffer, kBlockSize, kBlockSize, i * kBlockSize);
		auto fut = vmp->Write(vmdkp, reqp.get())
		.then([reqp = std::move(reqp)] (int rc) mutable {
			EXPECT_EQ(rc, 0);
			return 0;
		});

		auto f = vmp->TakeCheckPoint();
		f.wait();
		auto [ckpt_id, rc] = f.value();
		EXPECT_EQ(ckpt_id, i+1);
		EXPECT_EQ(rc, 0);
		futures.emplace_back(std::move(fut));
	}

	auto f = folly::collectAll(std::move(futures))
	.then([] (const std::vector<folly::Try<int>>& results) {
		for (const auto& t : results) {
			EXPECT_TRUE(t.hasValue());
			EXPECT_EQ(t.value(), 0);
		}
	});

	f.wait();

	for (auto i = 0; i < 10; ++i) {
		auto ckptp = vmdkp->GetCheckPoint(i + 1);
		EXPECT_NE(ckptp, nullptr);
		EXPECT_EQ(ckptp->ID(), i+1);
		EXPECT_TRUE(ckptp->IsSerialized());
		EXPECT_FALSE(ckptp->IsFlushed());

		auto [start, end] = ckptp->Blocks();
		EXPECT_EQ(start, end);
		EXPECT_EQ(start, i);

		const auto& bitmap = ckptp->GetRoaringBitMap();
		EXPECT_EQ(bitmap.minimum(), bitmap.maximum());
		EXPECT_TRUE(bitmap.contains(i));
		EXPECT_EQ(bitmap.cardinality(), 1);
	}

	for (auto i = 100; i < 120; ++i) {
		auto ckptp = vmdkp->GetCheckPoint(i + 1);
		EXPECT_EQ(ckptp, nullptr);
	}
}

/*
 * Test Aim
 * ========
 * - Schedule async sequential concurrent IOs
 * - Schedule async checkpoint
 * - Ensure every block is part of a single checkpoint
 */
TEST_F(VirtualMachineTest, CheckPointConcurrent) {
	const int kCheckPoints = 10;
	const int kWritesPerCheckpoint = 512;

	char buffer[kBlockSize];
	::memset(buffer, 'A', sizeof(buffer));

	EXPECT_NE(vm_handle_, kInvalidVmHandle);
	EXPECT_NE(vmdk_handle_, kInvalidVmdkHandle);

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(kVmid);
	EXPECT_NE(vmp, nullptr);
	auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(kVmdkid);
	auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
	EXPECT_NE(vmdkp, nullptr);
	EXPECT_EQ(vmp, vmdkp->GetVM());

	std::vector<folly::Future<int>> write_futures;
	std::vector<folly::Future<CheckPointResult>> ckpt_futures;
	write_futures.reserve(kCheckPoints * kWritesPerCheckpoint);
	ckpt_futures.reserve(kCheckPoints);

	for (auto ckpts = 0; ckpts < kCheckPoints; ++ckpts) {
		for (auto req = 1; req <= kWritesPerCheckpoint; ++req) {
			auto id = (ckpts * kWritesPerCheckpoint) + req;
			auto reqp = std::make_unique<Request>(id, vmdkp,
				Request::Type::kWrite, buffer, kBlockSize, kBlockSize,
				(id-1) * kBlockSize);
			auto fut = vmp->Write(vmdkp, reqp.get())
			.then([reqp = std::move(reqp)] (int rc) mutable {
				EXPECT_EQ(rc, 0);
				return 0;
			});
			write_futures.emplace_back(std::move(fut));
		}

		bool scheduled = false;
		while (not scheduled) {
			auto fut = vmp->TakeCheckPoint();
			if (fut.isReady()) {
				auto [ckpt_id, rc] = fut.value();
				if (rc == -EAGAIN) {
					EXPECT_EQ(ckpt_id, kInvalidCheckPointID);
					continue;
				}
				EXPECT_TRUE(rc == 0);
				EXPECT_NE(ckpt_id, kInvalidCheckPointID);
			}
			ckpt_futures.emplace_back(std::move(fut));
			scheduled = true;
		}
	}

	folly::collectAll(std::move(write_futures))
	.then([] (const std::vector<folly::Try<int>>& results) mutable {
		for (const auto& t : results) {
			EXPECT_TRUE(t.hasValue());
			EXPECT_EQ(t.value(), 0);
		}
	}).wait();

	ckpt_futures.emplace_back(vmp->TakeCheckPoint());

	folly::collectAll(std::move(ckpt_futures))
	.then([] (const std::vector<folly::Try<CheckPointResult>>& results) mutable {
		for (const auto& t : results) {
			EXPECT_TRUE(t.hasValue());
			auto [ckpt_id, rc] = t.value();
			EXPECT_NE(ckpt_id, kInvalidCheckPointID);
			EXPECT_EQ(rc, 0);
		}
	}).wait();

	std::set<BlockID> blocks;
	for (auto i = 1u, blockid = 0u; i <= kCheckPoints+1; ++i) {
		auto ckptp = vmdkp->GetCheckPoint(i);
		assert(ckptp != nullptr);
		EXPECT_NE(ckptp, nullptr);
		EXPECT_EQ(ckptp->ID(), i);
		EXPECT_TRUE(ckptp->IsSerialized());
		EXPECT_FALSE(ckptp->IsFlushed());

		const auto& bitmap = ckptp->GetRoaringBitMap();
		for (const auto& block : bitmap) {
			EXPECT_TRUE(blocks.find(block) == blocks.end());
			blocks.insert(block);
			EXPECT_EQ(blockid, block);
			++blockid;
		}
	}

	EXPECT_EQ(blocks.size(), kCheckPoints * kWritesPerCheckpoint);
}