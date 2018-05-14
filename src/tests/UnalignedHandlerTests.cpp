#include <memory>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "Request.h"
#include "Vmdk.h"
#include "UnalignedHandler.h"
#include "LockHandler.h"
#include "RamCacheHandler.h"
#include "DaemonUtils.h"
#include "VmdkConfig.h"

using namespace pio;
using namespace ::ondisk;
using namespace ::hyc_thrift;

const size_t kVmdkBlockSize = 8192;

class UnalignedHandlerTest : public ::testing::Test {
protected:
	const CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdkp;
	std::atomic<RequestID> req_id_;

	void DefaultVmdkConfig(config::VmdkConfig& config, uint64_t block_size) {
		config.SetVmId("vmdkid");
		config.SetVmdkId("vmdkid");
		config.SetBlockSize(block_size);
		config.ConfigureRamCache(1024);
	}

	virtual void SetUp() {
		config::VmdkConfig config;
		DefaultVmdkConfig(config, kVmdkBlockSize);

		vmdkp = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize());

		auto unaligned_handler = std::make_unique<UnalignedHandler>();
		auto lock_handler = std::make_unique<LockHandler>();
		auto ramcache_handler = std::make_unique<RamCacheHandler>(vmdkp->GetJsonConfig());


		vmdkp->RegisterRequestHandler(std::move(lock_handler));
		vmdkp->RegisterRequestHandler(std::move(unaligned_handler));
		vmdkp->RegisterRequestHandler(std::move(ramcache_handler));

		req_id_.store(StorRpc_constants::kInvalidRequestID());
	}

	virtual void TearDown() {
		vmdkp = nullptr;
	}

	RequestID NextRequestID() {
		return ++req_id_;
	}

	folly::Future<int> VmdkWrite(BlockID block, size_t skip, size_t size,
			char fillchar) {
		EXPECT_LE(size + skip, vmdkp->BlockSize());

		Offset offset = (block << vmdkp->BlockShift()) + skip;
		auto req_id = NextRequestID();
		auto bufferp = NewRequestBuffer(size);
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
		auto bufferp = NewRequestBuffer(size);
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

TEST_F(UnalignedHandlerTest, AlignedWrite) {
	EXPECT_TRUE(vmdkp);

	const BlockID kNBlocks = 64;

	/* issue aligned writes */
	std::vector<folly::Future<int>> write_futures;
	for (BlockID block = 0; block < kNBlocks; ++block) {
		auto fut = VmdkWrite(block, 0, vmdkp->BlockSize(), 'A' + (block % 26));
		write_futures.emplace_back(std::move(fut));
	}

	folly::collectAll(std::move(write_futures)).wait();

	/* issue aligned reads */
	std::vector<folly::Future<std::unique_ptr<RequestBuffer>>> read_futures;
	for (BlockID block = 0; block < kNBlocks; ++block) {
		auto fut = VmdkRead(block, 0, vmdkp->BlockSize());
		read_futures.emplace_back(std::move(fut));
	}

	/* verify data */
	folly::collectAll(std::move(read_futures))
	.then([this]
			(const std::vector<folly::Try<std::unique_ptr<RequestBuffer>>>& tries) {
		BlockID block = 0;

		auto bufferp = NewRequestBuffer(vmdkp->BlockSize());
		auto payload = bufferp->Payload();

		for (const auto& t : tries) {
			::memset(payload, 'A' + (block % 26), vmdkp->BlockSize());
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

TEST_F(UnalignedHandlerTest, AlignedWrite_UnalignedOverWrite) {
	EXPECT_TRUE(vmdkp);

	const BlockID kNBlocks = 64;

	/* issue aligned writes */
	std::vector<folly::Future<int>> write_futures;
	for (BlockID block = 0; block < kNBlocks; ++block) {
		auto fut = VmdkWrite(block, 0, vmdkp->BlockSize(), 'A' + (block % 26));
		write_futures.emplace_back(std::move(fut));
	}

	folly::collectAll(std::move(write_futures)).wait();
	write_futures.clear();

	/* overwrite each block */
	const auto nsecotrs = kNBlocks * vmdkp->BlockSize() / kSectorSize;
	uint64_t sect = 0;
	for (; sect < nsecotrs; ++sect) {
		Offset offset = sect * kSectorSize;
		char fillchar = 'A' + (sect % 26);
		auto blocks = GetBlockIDs(offset, kSectorSize, vmdkp->BlockShift());
		EXPECT_EQ(blocks.first, blocks.second);

		auto skip = offset - blocks.first * vmdkp->BlockSize();
		auto fut  = VmdkWrite(blocks.first, skip, kSectorSize, fillchar);
		write_futures.emplace_back(std::move(fut));
	}
	folly::collectAll(std::move(write_futures)).wait();
	EXPECT_EQ(sect, nsecotrs);

	/* read each block and verify data */
	std::vector<folly::Future<std::unique_ptr<RequestBuffer>>> read_futures;
	for (sect = 0u; sect < nsecotrs; ++sect) {
		Offset offset = sect * kSectorSize;
		auto blocks = GetBlockIDs(offset, kSectorSize, vmdkp->BlockShift());
		EXPECT_EQ(blocks.first, blocks.second);

		auto skip = offset - blocks.first * vmdkp->BlockSize();
		auto fut  = VmdkRead(blocks.first, skip, kSectorSize);
		read_futures.emplace_back(std::move(fut));
	}

	sect = 0;
	folly::collectAll(std::move(read_futures))
	.then([&sect] (const std::vector<folly::Try<std::unique_ptr<RequestBuffer>>>& tries) {
		auto bufferp = NewRequestBuffer(kSectorSize);
		auto payload = bufferp->Payload();

		for (const auto& t : tries) {
			const char fillchar = 'A' + (sect % 26);
			::memset(payload, fillchar, bufferp->Size());

			const auto& bufp = t.value();
			auto p = bufp->Payload();
			EXPECT_EQ(bufferp->Size(), bufp->Size());

			auto rc = ::memcmp(payload, p, bufp->Size());
			EXPECT_EQ(rc, 0);
			++sect;
		}
	})
	.wait();
	EXPECT_EQ(sect, nsecotrs);
}
