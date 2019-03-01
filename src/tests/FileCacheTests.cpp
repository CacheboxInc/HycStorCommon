#include <numeric>
#include <chrono>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "Request.h"
#include "Vmdk.h"
#include "LockHandler.h"
#include "UnalignedHandler.h"
#include "MultiTargetHandler.h"
#include "DaemonUtils.h"
#include "VmdkConfig.h"

using namespace pio;
using namespace pio::config;
using namespace ::ondisk;

static const size_t kVmdkBlockSize = 8192;
static const VmdkID kVmdkid = "kVmdkid";
static const VmID kVmid = "kVmid";
static const VmdkUUID kVmdkUUID = "kVmdkUUID";
static const VmUUID kVmUUID = "kVmUUID";

class FileCacheTest : public ::testing::Test {
protected:
	const CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdkp;
	std::atomic<RequestID> req_id;

	void DefaultVmdkConfig(VmdkConfig& config) {
		config.SetVmdkId(kVmdkid);
		config.SetVmId(kVmid);
		config.SetVmdkUUID(kVmdkUUID);
		config.SetVmUUID(kVmUUID);
		config.SetBlockSize(kVmdkBlockSize);
		config.ConfigureCompression("snappy", 1);
		config.ConfigureEncryption("aes128-gcm", "abcd");
		config.DisableCompression();
		config.DisableEncryption();
		config.DisableRamCache();
		config.DisableErrorHandler();
		config.DisableSuccessHandler();
		config.DisableFileTarget();
		config.DisableNetworkTarget();
		config.DisableAeroSpikeCache();
		config.ConfigureFileCache("/var/tmp/file_cache_0");
	}

	virtual void SetUp() {
		config::VmdkConfig config;
		DefaultVmdkConfig(config);

		vmdkp = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize());
		EXPECT_NE(vmdkp, nullptr);

		auto configp = vmdkp->GetJsonConfig();
		auto lock = std::make_unique<LockHandler>();
		auto multi_target = std::make_unique<MultiTargetHandler>(
				vmdkp.get(), configp);
		EXPECT_NE(lock, nullptr);
		EXPECT_NE(multi_target, nullptr);

		vmdkp->RegisterRequestHandler(std::move(lock));
		vmdkp->RegisterRequestHandler(std::move(multi_target));
		req_id.store(StorRpc_constants::kInvalidRequestID());
	}

	virtual void TearDown() {
		vmdkp = nullptr;
	}

	RequestID NextRequestID() {
		return ++req_id;
	}

	folly::Future<int> VmdkWrite(BlockID block, size_t skip, size_t size,
			char fillchar) {
		EXPECT_LE(size + skip, vmdkp->BlockSize());

		Offset offset = (block << vmdkp->BlockShift()) + skip;
		auto req_id = NextRequestID();
		auto bufferp = NewAlignedRequestBuffer(size);
		auto payload = bufferp->Payload();
		::memset(payload, fillchar, bufferp->Size());

		auto reqp = std::make_unique<Request>(req_id, vmdkp.get(),
			Request::Type::kWrite, payload, bufferp->Size(), bufferp->Size(),
			offset);

		return vmdkp->Write(reqp.get(), ckpt_id)
		.then([reqp = std::move(reqp)] (int rc) {
			return rc;
		});
	}

	folly::Future<std::unique_ptr<RequestBuffer>>
			VmdkRead(BlockID block, size_t skip, size_t size) {
		EXPECT_LE(size + skip, vmdkp->BlockSize());

		Offset offset = (block << vmdkp->BlockShift()) + skip;
		auto req_id = NextRequestID();
		auto bufferp = NewAlignedRequestBuffer(size);
		auto payload = bufferp->Payload();

		auto reqp = std::make_unique<Request>(req_id, vmdkp.get(),
			Request::Type::kRead, payload, bufferp->Size(), bufferp->Size(),
			offset);

		return vmdkp->Read(reqp.get(), std::make_pair(1, 1))
		.then([bufferp = std::move(bufferp), reqp = std::move(reqp)] (int rc) mutable {
			EXPECT_EQ(rc, 0);
			return std::move(bufferp);
		});
	}
};

TEST_F(FileCacheTest, FileCacheVerify) {
	EXPECT_TRUE(vmdkp);

	const BlockID kNBlocks = 1;

	// issue aligned writes
	std::vector<folly::Future<int>> write_futures;

	for (BlockID block = 0; block < kNBlocks; ++block) {
		auto fut = VmdkWrite(block, 0, vmdkp->BlockSize(), 'A' + (block % 26));
		write_futures.emplace_back(std::move(fut));
	}

	folly::collectAll(std::move(write_futures)).wait();

	// issue read for already written blocks
	std::vector<folly::Future<std::unique_ptr<RequestBuffer>>> read_futures;

	for (BlockID block = 0; block < kNBlocks; ++block) {
		auto fut = VmdkRead(block, 0, vmdkp->BlockSize());
		read_futures.emplace_back(std::move(fut));

	}

	// verify data
	folly::collectAll(std::move(read_futures))
	.then([this]
			(const std::vector<folly::Try<std::unique_ptr<RequestBuffer>>>& tries) {
		BlockID block = 0;

		auto bufferp = NewAlignedRequestBuffer(vmdkp->BlockSize());
		auto payload = bufferp->Payload();

		for (const auto& t : tries) {
			::memset(payload, 'A' + (block %26), vmdkp->BlockSize());
			const auto& bufp = t.value();

			auto p = bufp->Payload();
			auto rc = ::memcmp(payload, p, bufp->Size());
			EXPECT_EQ(rc, 0);
			++block;
		}
		return 0;
	})
	.wait();
}

TEST_F(FileCacheTest, FileCacheVerifyBulk) {
	using namespace std::chrono_literals;
	EXPECT_TRUE(vmdkp);

	const BlockID kNBlocks = 1;
	std::vector<BlockID> blocks(kNBlocks);
	std::iota(blocks.begin(), blocks.end(), 0);

	auto requests = std::make_unique<std::vector<std::unique_ptr<Request>>>();
	auto process = std::make_unique<std::vector<RequestBlock*>>();
	auto buffers = std::make_unique<std::vector<std::unique_ptr<RequestBuffer>>>();
	for (auto block : blocks) {
		auto offset = block << vmdkp->BlockShift();
		auto req_id = NextRequestID();
		auto bufferp = NewRequestBuffer(vmdkp->BlockSize());
		auto p = bufferp->Payload();
		::memset(p, 'A' + (block % 26), bufferp->Size());

		auto req = std::make_unique<Request>(req_id, vmdkp.get(),
			Request::Type::kWrite, p, bufferp->Size(), bufferp->Size(),
			offset);

		req->ForEachRequestBlock([&] (RequestBlock *blockp) mutable {
			process->emplace_back(blockp);
			return true;
		});

		requests->emplace_back(std::move(req));
		buffers->emplace_back(std::move(bufferp));
	}

	auto wf = vmdkp->BulkWrite(ckpt_id, *requests, *process);
	wf.wait(1s);
	EXPECT_TRUE(wf.isReady());
	EXPECT_EQ(wf.value(), 0);

	requests->clear();
	process->clear();
	for (auto block : blocks) {
		auto offset = block << vmdkp->BlockShift();
		auto req_id = NextRequestID();
		auto bufferp = NewRequestBuffer(vmdkp->BlockSize());
		auto p = bufferp->Payload();

		auto req = std::make_unique<Request>(req_id, vmdkp.get(),
			Request::Type::kRead, p, bufferp->Size(), bufferp->Size(),
			offset);

		req->ForEachRequestBlock([&] (RequestBlock *blockp) mutable {
			process->emplace_back(blockp);
			return true;
		});

		requests->emplace_back(std::move(req));
		buffers->emplace_back(std::move(bufferp));
	}

	auto ckpts = std::make_pair(1, 1);
	auto rf = vmdkp->BulkRead(ckpts, *requests, *process);
	rf.wait(1s);
	EXPECT_TRUE(rf.isReady());
	EXPECT_EQ(rf.value(), 0);

	auto wit = buffers->begin();
	auto weit = std::next(wit, blocks.size());
	EXPECT_NE(weit, buffers->end());
	auto rit = weit;
	auto reit = std::next(rit, blocks.size());
	EXPECT_EQ(reit, buffers->end());

	for (; rit != reit; ++rit, ++wit) {
		EXPECT_NE(wit, weit);
		auto rp = (*rit)->Payload();
		auto wp = (*wit)->Payload();

		EXPECT_EQ((*rit)->Size(), (*wit)->Size());
		auto rc = ::memcmp(rp, wp, (*rit)->Size());
		EXPECT_EQ(rc, 0);
	}
}
