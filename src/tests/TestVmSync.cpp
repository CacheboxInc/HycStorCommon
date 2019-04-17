#include <fstream>

#include <experimental/filesystem>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "DaemonTgtInterface.h"
#include "Singleton.h"
#include "VirtualMachine.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "VmdkFactory.h"
#include "VmManager.h"
#include "TgtInterfaceImpl.h"
#include "SyncAny.h"
#include "RamCacheHandler.h"
#include "FileCacheHandler.h"

using namespace pio;
using namespace pio::config;
using namespace ::ondisk;
using namespace ::hyc_thrift;
using namespace std::chrono_literals;

namespace fs = std::experimental::filesystem;

const VmID kVmid = "VmID";
const VmdkID kVmdkid = "VmdkID";
const VmUUID kVmUUID = "VmUUID";
const VmdkUUID kVmdkUUID = "VmdkUUID";
const uint64_t kBlockSize = 4096;
const uint16_t kBatchSize = 4;
const size_t kOneMB = 1024 * 1024;

struct VmdkInfo {
	VmdkInfo(VmID id,
				VmdkHandle handle,
				std::string path,
				ActiveVmdk* vmdkp
			) noexcept :
				id_(std::move(id)),
				handle_(handle),
				path_(std::move(path)),
				vmdkp_(vmdkp) {
	}

	VmdkInfo(VmdkInfo&& rhs) noexcept :
			id_(std::move(rhs.id_)),
			path_(std::move(rhs.path_)) {
		handle_ = rhs.handle_;
		vmdkp_ = rhs.vmdkp_;

		rhs.handle_ = StorRpc_constants::kInvalidVmdkHandle();
		rhs.vmdkp_ = nullptr;
	}

	VmdkInfo(const VmdkInfo&) = delete;

	VmdkID id_;
	VmdkHandle handle_;
	std::string path_;
	ActiveVmdk* vmdkp_;
	size_t size_mb_{0};
};

class VmSyncTest : public ::testing::Test {
public:
	std::atomic<RequestID> req_id_{0};

	VmHandle vm_handle_{StorRpc_constants::kInvalidVmHandle()};
	VirtualMachine* vmp_{nullptr};

	std::vector<VmdkInfo> vmdks_;
	SyncAny* syncp_{};

	::StorD stord_instance;

	static void SetUpTestCase() {
	}

	static void TearDownTestCase() {
	}

	virtual void SetUp() {
		stord_instance.InitStordLib();
		vm_handle_ = AddVm();
		EXPECT_NE(vm_handle_, StorRpc_constants::kInvalidVmHandle());

		vmp_ = SingletonHolder<VmManager>::GetInstance()->GetInstance(kVmid);
		EXPECT_NE(vmp_, nullptr);

		for (auto i : iter::Range(0, 2)) {
			::ondisk::VmdkID id = kVmdkid + std::to_string(i);
			::ondisk::VmdkUUID uuid = kVmdkUUID + std::to_string(i);
			std::ostringstream os;
			os << "/tmp/file_cache_" << i << ".txt";
			std::string path = os.str();

			auto handle = AddVmdk(vm_handle_, id, uuid, path);
			EXPECT_NE(handle, StorRpc_constants::kInvalidVmdkHandle());

			auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(id);
			auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
			EXPECT_NE(vmdkp, nullptr);

			vmdks_.emplace_back(std::move(id), handle, std::move(path), vmdkp);
		}

		ASSERT_EQ(vmdks_.size(), 2);
		vmdks_[0].size_mb_ = 32;
		vmdks_[1].size_mb_ = 64;

		/* Initial CheckPoint taken during migrate to cloud */
		PrepareCkpt(vm_handle_);
	}

	virtual void TearDown() {
		for (const auto& vmdk : vmdks_) {
			RemoveVmdk(vmdk.handle_);
		}
		vmdks_.clear();

		RemoveVm(vm_handle_);
		vm_handle_ = StorRpc_constants::kInvalidVmHandle();
		vmp_ = nullptr;
		syncp_ = nullptr;

		stord_instance.DeinitStordLib();
		req_id_ = 0;
	}

	VmHandle AddVm() {
		VmConfig config;
		config.SetVmId(kVmid);
		config.SetVmUUID(kVmUUID);
		config.SetAeroClusterID("0");
		return NewVm(kVmid.c_str(), config.Serialize().c_str());
	}

	VmdkHandle AddVmdk(VmHandle vm_handle, std::string& id, std::string& uuid, std::string& path) {
		VmdkConfig c;
		c.SetVmUUID(kVmUUID);
		c.SetVmId(kVmid);

		c.SetVmdkId(id);
		c.SetVmdkUUID(uuid);

		c.SetBlockSize(kBlockSize);
		c.DisableCompression();
		c.DisableEncryption();
		c.DisableFileCache();
		c.DisableErrorHandler();
		c.SetRamMetaDataKV();
		c.DisableFileTarget();
		c.DisableNetworkTarget();
		c.DisableSuccessHandler();

		c.ConfigureFileCache(path);
		c.ConfigureRamCache(1024);

		return NewActiveVmdk(vm_handle, id.c_str(), c.Serialize().c_str());
	}

	folly::Future<int> Write(ActiveVmdk* vmdkp, uint64_t offset, char* bufferp,
			size_t size) {
		auto req = std::make_unique<Request>(++req_id_, vmdkp,
			Request::Type::kWrite, bufferp, size, size, offset);
		EXPECT_TRUE(req);
		auto reqp = req.get();
		return vmp_->Write(vmdkp, reqp)
		.then([req = std::move(req)] (int rc) mutable -> folly::Future<int> {
			EXPECT_EQ(rc, 0);
			return 0;
		});
	}
};

TEST_F(VmSyncTest, Basic) {
	auto sync = std::make_unique<SyncAny>(vmp_, VmSync::Type::kSyncTest,
		vmp_->GetCurCkptID()-1, kBatchSize);
	ASSERT_NE(sync, nullptr);

	syncp_ = sync.get();
	vmp_->AddVmSync(std::move(sync));

	RequestHandlerNames src;
	RequestHandlerNames dst;
	src.emplace_back(RamCacheHandler::kName);
	dst.emplace_back(FileCacheHandler::kName);

	/* Initialize Sync - SyncStart should not sync anything */
	auto rc = syncp_->SyncStart(src, dst);
	ASSERT_EQ(rc, 0);

	for (const auto& vmdk : vmdks_) {
		ASSERT_EQ(fs::file_size(vmdk.path_), 0);
	}

	char fill_char = 'A';

	{
		/* write data */
		auto buffer = NewRequestBuffer(kOneMB);
		ASSERT_TRUE(buffer);
		auto bufp = buffer->Payload();
		std::memset(bufp, fill_char, kOneMB);

		std::vector<folly::Future<int>> futures;
		for (const auto& vmdk : vmdks_) {
			for (auto mb : iter::Range((size_t) 0, vmdk.size_mb_)) {
				auto offset = mb * kOneMB;
				futures.emplace_back(
					Write(vmdk.vmdkp_, offset, bufp, kOneMB)
				);
			}
		}

		auto f = folly::collectAll(std::move(futures))
		.then([] (const folly::Try<std::vector<folly::Try<int>>>& tries) {
			EXPECT_FALSE(tries.hasException());
			for (const auto& tri : tries.value()) {
				EXPECT_FALSE(tri.hasException());
				EXPECT_EQ(tri.value(), 0);
			}
			return 0;
		});
		ASSERT_TRUE(f.isReady());
		ASSERT_EQ(f.value(), 0);
	}

	{
		/* ensure file is empty */
		for (const auto& vmdk : vmdks_) {
			ASSERT_EQ(fs::file_size(vmdk.path_), 0);
		}
	}

	{
		/* take a check point */
		auto f = vmp_->TakeCheckPoint();
		f.wait(1s);
		EXPECT_TRUE(f.isReady());
		auto [ckpt_id, rc] = f.value();
		(void) ckpt_id;
		EXPECT_EQ(rc, 0);
	}

	{
		/* while sync is in progress - write more data */
		auto buffer = NewRequestBuffer(kOneMB);
		ASSERT_TRUE(buffer);
		auto bufp = buffer->Payload();
		std::memset(bufp, fill_char+1, kOneMB);

		std::vector<folly::Future<int>> futures;
		for (const auto& vmdk : vmdks_) {
			for (auto mb : iter::Range((size_t) 0, vmdk.size_mb_)) {
				auto offset = mb * kOneMB;
				futures.emplace_back(
					Write(vmdk.vmdkp_, offset, bufp, kOneMB)
				);
			}
		}

		auto f = folly::collectAll(std::move(futures))
		.then([] (const folly::Try<std::vector<folly::Try<int>>>& tries) {
			EXPECT_FALSE(tries.hasException());
			for (const auto& tri : tries.value()) {
				EXPECT_FALSE(tri.hasException());
				EXPECT_EQ(tri.value(), 0);
			}
			return 0;
		});
		ASSERT_TRUE(f.isReady());
		ASSERT_EQ(f.value(), 0);
	}

	{
		/* ensure existing sync is finished */
		bool stopped = true;
		int count = 0;
		do {
			std::this_thread::sleep_for(2s);
			ASSERT_LE(++count, 10);
		} while (not stopped);
	}

	{
		/* ensure file size matches and verify data */
		for (const auto& vmdk : vmdks_) {
			EXPECT_GE(fs::file_size(vmdk.path_) / kOneMB, vmdk.size_mb_);
			char ch;
			std::fstream fin(vmdk.path_, std::fstream::in);
			while (fin >> std::noskipws >> ch) {
				ASSERT_EQ(ch, fill_char);
			}
		}
	}

	{
		/* take a check point */
		auto f = vmp_->TakeCheckPoint();
		f.wait(1s);
		EXPECT_TRUE(f.isReady());
		auto [ckpt_id, rc] = f.value();
		EXPECT_EQ(rc, 0);
		++fill_char;
	}

	{
		/* ensure sync is finished */
		bool stopped = true;
		int result;
		int count = 0;
		do {
			std::this_thread::sleep_for(2s);
			syncp_->SyncStatus(&stopped, &result);
			ASSERT_LE(++count, 10);
		} while (not stopped);
	}

	{
		/* ensure file size matches and verify data */
		for (const auto& vmdk : vmdks_) {
			EXPECT_GE(fs::file_size(vmdk.path_) / kOneMB, vmdk.size_mb_);
			char ch;
			std::fstream fin(vmdk.path_, std::fstream::in);
			while (fin >> std::noskipws >> ch) {
				ASSERT_EQ(ch, fill_char);
			}
		}
	}
}

TEST_F(VmSyncTest, FewInitialCheckPoints) {
	ASSERT_EQ(vmdks_.size(), 2);

	char fill_char = 'A';
	for (int i = 0; i < 10; ++i) {
		/* write data */
		auto buffer = NewRequestBuffer(kOneMB);
		ASSERT_TRUE(buffer);
		auto bufp = buffer->Payload();
		std::memset(bufp, fill_char, kOneMB);
		++fill_char;

		std::vector<folly::Future<int>> futures;
		for (const auto& vmdk : vmdks_) {
			for (auto mb : iter::Range((size_t) 0, vmdk.size_mb_)) {
				auto offset = mb * kOneMB;
				futures.emplace_back(
					Write(vmdk.vmdkp_, offset, bufp, kOneMB)
				);
			}
		}

		LOG(ERROR) << "futures " << futures.size();
		auto f = folly::collectAll(std::move(futures))
		.then([] (const folly::Try<std::vector<folly::Try<int>>>& tries) {
			EXPECT_FALSE(tries.hasException());
			for (const auto& tri : tries.value()) {
				EXPECT_FALSE(tri.hasException());
				EXPECT_EQ(tri.value(), 0);
			}
			return 0;
		});
		ASSERT_TRUE(f.isReady());
		ASSERT_EQ(f.value(), 0);

		{
			/* take a check point */
			auto f = vmp_->TakeCheckPoint();
			f.wait(1s);
			EXPECT_TRUE(f.isReady());
			auto [ckpt_id, rc] = f.value();
			EXPECT_EQ(rc, 0);
		}
	}
	--fill_char;

	/* create a sync */
	RequestHandlerNames src;
	RequestHandlerNames dst;
	src.emplace_back(RamCacheHandler::kName);
	dst.emplace_back(FileCacheHandler::kName);

	auto sync = std::make_unique<SyncAny>(vmp_, VmSync::Type::kSyncTest, 1, kBatchSize);
	ASSERT_NE(sync, nullptr);
	syncp_ = sync.get();
	syncp_->SetCheckPoints(vmp_->GetCurCkptID()-1,
		MetaData_constants::kInvalidCheckPointID());
	vmp_->AddVmSync(std::move(sync));
	auto rc = syncp_->SyncStart(src, dst);
	ASSERT_EQ(rc, 0);

	{
		/* ensure sync is finished */
		bool stopped = true;
		int result;
		int count = 0;
		do {
			std::this_thread::sleep_for(2s);
			syncp_->SyncStatus(&stopped, &result);
			ASSERT_LE(++count, 10);
		} while (not stopped);
	}

	{
		/* ensure file size matches and verify data */
		for (const auto& vmdk : vmdks_) {
			EXPECT_GE(fs::file_size(vmdk.path_) / kOneMB, vmdk.size_mb_);
			size_t count = 0;
			char ch;
			std::fstream fin(vmdk.path_, std::fstream::in);
			while (fin >> std::noskipws >> ch) {
				ASSERT_EQ(ch, fill_char);
				++count;
			}
			EXPECT_EQ(count, vmdk.size_mb_ * kOneMB);
		}
	}
}
