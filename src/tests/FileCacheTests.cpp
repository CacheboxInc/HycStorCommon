
#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "Request.h"
#include "Vmdk.h"
#include "UnalignedHandler.h"
#include "LockHandler.h"
#include "FileCacheHandler.h"
#include "DaemonUtils.h"
#include "VmdkConfig.h"

using namespace pio;

const size_t kVmdkBlockSize = 4096;

class FileCacheTest : public ::testing::Test {
protected:
	const CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdkp;
	std::atomic<RequestID> req_id;

	void DefaultVmdkConfig(config::VmdkConfig& config, uint64_t block_size) {
		config.SetVmId("vmdkid");
		config.SetVmdkId("vmdkid");
		config.SetBlockSize(block_size);
		config.ConfigureFileCache("/var/tmp/file_cache_0");
	}

	virtual void SetUp() {
		config::VmdkConfig config;
		DefaultVmdkConfig(config, kVmdkBlockSize);

		vmdkp = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize());

		auto unaligned_handler = std::make_unique<UnalignedHandler>();
		auto lock_handler = std::make_unique<LockHandler>();
		auto filecache_handler = std::make_unique<FileCacheHandler>(vmdkp->GetJsonConfig());

		vmdkp->RegisterRequestHandler(std::move(lock_handler));
		vmdkp->RegisterRequestHandler(std::move(unaligned_handler));
		vmdkp->RegisterRequestHandler(std::move(filecache_handler));

		req_id.store(kInvalidRequestID);
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

		return vmdkp->Read(reqp.get())
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
