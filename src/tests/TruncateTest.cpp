#include <memory>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "Request.h"
#include "Vmdk.h"
#include "RamCacheHandler.h"
#include "RamCache.h"
#include "DaemonUtils.h"
#include "VmdkConfig.h"

using namespace pio;
using namespace ::ondisk;
using namespace ::hyc_thrift;

const size_t kVmdkBlockSize = 8192;

class TruncateTest : public ::testing::Test {
protected:
	const CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdkp;
	std::atomic<RequestID> req_id_;
	RamCacheHandler* ramcache_handlerp_;

	void DefaultVmdkConfig(config::VmdkConfig& config, uint64_t block_size) {
		config.SetVmId("vmdkid");
		config.SetVmdkId("vmdkid");
		config.SetVmUUID("vmdkUUID");
		config.SetVmdkUUID("vmdkUUID");
		config.SetBlockSize(block_size);
		config.ConfigureRamCache(1024);
	}

	virtual void SetUp() {
		config::VmdkConfig config;
		DefaultVmdkConfig(config, kVmdkBlockSize);

		vmdkp = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize());

		auto ramcache_handler = std::make_unique<RamCacheHandler>(vmdkp->GetJsonConfig());
		ramcache_handlerp_ = ramcache_handler.get();

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

	folly::Future<int> VmdkTruncate(const std::vector<std::pair<int64_t, int64_t>>& offsets) {
		std::vector<TruncateReq> requests;
		for (const auto offset : offsets) {
			requests.emplace_back(apache::thrift::FragileConstructor(),
				offset.first, offset.second);
		}
		return vmdkp->TruncateBlocks(NextRequestID(), ckpt_id, requests);
	}

	folly::Future<int> VmdkRead(BlockID block, size_t skip, size_t size) {
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
			return rc;
		});
	}
};

TEST_F(TruncateTest, AlignedDeleteBlocks) {
	EXPECT_TRUE(vmdkp);

	const BlockID kNBlocks = 8;
	int64_t nwrites = 0;

	auto SyncWriteAll = [&] {
		/* issue aligned writes */
		std::vector<folly::Future<int>> write_futures;
		for (BlockID block = 0; block < kNBlocks; ++block) {
			auto fut = VmdkWrite(block, 0, vmdkp->BlockSize(), 'A' + (nwrites++ % 26));
			write_futures.emplace_back(std::move(fut));
		}
		folly::collectAll(std::move(write_futures)).wait();
		EXPECT_EQ(ramcache_handlerp_->Cache(ckpt_id)->Size(), kNBlocks);
	};

	{
		/* truncate all */
		SyncWriteAll();
		VmdkTruncate({std::make_pair(0, kNBlocks * vmdkp->BlockSize())})
		.wait();
		EXPECT_EQ(ramcache_handlerp_->Cache(ckpt_id)->Size(), 0);
	}

	{
		/* truncate all */
		SyncWriteAll();
		std::vector<std::pair<int64_t, int64_t>> offsets;
		for (BlockID block = 0; block < kNBlocks; ++block) {
			offsets.emplace_back(block << vmdkp->BlockShift(), vmdkp->BlockSize());
		}
		VmdkTruncate(offsets).wait();
		EXPECT_EQ(ramcache_handlerp_->Cache(ckpt_id)->Size(), 0);
	}
}

TEST_F(TruncateTest, UnalignedDeleteBlocks) {
	EXPECT_TRUE(vmdkp);

	const BlockID kNBlocks = 8;
	int64_t nwrites = 0;

	auto SyncWriteAll = [&] {
		/* issue aligned writes */
		std::vector<folly::Future<int>> write_futures;
		for (BlockID block = 0; block < kNBlocks; ++block) {
			auto fut = VmdkWrite(block, 0, vmdkp->BlockSize(), 'A' + (nwrites++ % 26));
			write_futures.emplace_back(std::move(fut));
		}
		folly::collectAll(std::move(write_futures)).wait();
		EXPECT_EQ(ramcache_handlerp_->Cache(ckpt_id)->Size(), kNBlocks);
	};

	{
		/* do not delete last */
		SyncWriteAll();
		VmdkTruncate({std::make_pair(0, kNBlocks * vmdkp->BlockSize()-1)})
		.wait();
		EXPECT_EQ(ramcache_handlerp_->Cache(ckpt_id)->Size(), 1);

		/* reading last block should not fail */
		auto f = VmdkRead(kNBlocks-1, 0, vmdkp->BlockSize());
		f.wait();
		EXPECT_EQ(f.value(), 0);
	}

	{
		/* do not delete first and last */
		SyncWriteAll();
		VmdkTruncate({std::make_pair(1, kNBlocks * vmdkp->BlockSize()-2)})
		.wait();
		EXPECT_EQ(ramcache_handlerp_->Cache(ckpt_id)->Size(), 2);

		auto f = VmdkRead(0, 0, vmdkp->BlockSize());
		f.wait();
		EXPECT_EQ(f.value(), 0);

		f = VmdkRead(kNBlocks-1, 0, vmdkp->BlockSize());
		f.wait();
		EXPECT_EQ(f.value(), 0);
	}

	{
		/* no delete */
		SyncWriteAll();
		std::vector<std::pair<int64_t, int64_t>> in;
		for (BlockID block = 0; block < kNBlocks; ++block) {
			auto so = (block << vmdkp->BlockShift()) + 512;
			auto eo = ((block+1) << vmdkp->BlockShift()) - 512;
			in.emplace_back(so, eo-so);
		}
		VmdkTruncate(in).wait();

		EXPECT_EQ(ramcache_handlerp_->Cache(ckpt_id)->Size(), kNBlocks);
	}

	{
		/* no delete */
		SyncWriteAll();
		std::vector<std::pair<int64_t, int64_t>> in;
		for (BlockID block = 0; block < kNBlocks; ++block) {
			auto so = (block << vmdkp->BlockShift()) + 512;
			auto eo = ((block+2) << vmdkp->BlockShift()) - 512;
			in.emplace_back(so, eo-so);
		}
		VmdkTruncate(in).wait();

		EXPECT_EQ(ramcache_handlerp_->Cache(ckpt_id)->Size(), kNBlocks);
	}
}
