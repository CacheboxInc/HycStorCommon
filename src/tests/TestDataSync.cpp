#include <stdexcept>
#include <condition_variable>
#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "DaemonUtils.h"
#include "Request.h"
#include "Vmdk.h"
#include "RamCache.h"
#include "VmdkConfig.h"
#include "DataReader.h"
#include "DataWriter.h"
#include "DataCopier.h"
#include "DataSync.h"
#include "RamCacheHandler.h"
#include "SuccessHandler.h"

#if 0
- SetDataSource and SetDataDestination throws on empty input
- SetCheckPoints
  - check_points - empty vector of check points
  - CheckPointIDs are not consecutive
  - Previous CheckPointID (SetCheckPoints) and new CheckPointID are not consecutive
  - (Existing Sync is in progress)
- Calling Start on empty source and destination returns error
- Calling Start on empty checkpoints returns success
- Calling Start on already running sync returns immediately with 0
#endif

using namespace pio;
using namespace ::ondisk;
using namespace std::chrono_literals;
using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;

const std::string kVmdkId = "vmdkid";
const size_t kVmdkBlockSize = 4096;
const BlockID kNumberOfBlocks = 1024;

/*
 * std::map<BlockID, CheckPointID>
 *  - BlockID to CheckPointID mapping
 */
using BlockToCkpt = std::map<BlockID, CheckPointID>;

class TestDataSync : public TestWithParam<::testing::tuple<bool, int>> {
protected:
	CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdk;
	std::vector<std::unique_ptr<RequestHandler>> handlers;
	RequestHandler* src_handlerp;
	RequestHandler* dst_handlerp;

	std::map<CheckPointID, std::unique_ptr<CheckPoint>> check_points;

	/*
	 * For every CheckPointID
	 *  - BlockID to CheckPointID mapping
	 */
	std::map<CheckPointID, BlockToCkpt> ckpt_to_block_ckpt;

	/*
	 * Current mapping of BlockID to CheckPointID
	 */
	BlockToCkpt block_to_ckpt;

	bool data_verification;
	int merge_factor;

	virtual void TearDown() {
		vmdk = nullptr;
		src_handlerp = nullptr;
		dst_handlerp = nullptr;
		handlers.clear();

		check_points.clear();
		ckpt_to_block_ckpt.clear();
		block_to_ckpt.clear();
	}

	void DefaultVmdkConfig(config::VmdkConfig& config, uint64_t block_size) {
		config.SetVmId("vmid");
		config.SetVmdkId(kVmdkId);
		config.SetVmUUID("vmdkUUID");
		config.SetVmdkUUID("vmdkUUID");
		config.SetBlockSize(block_size);
		config.ConfigureRamCache(1024);
		config.EnableSuccessHandler();
		config.SetSuccessHandlerDelay(10);
	}

	virtual void SetUp() {
		std::tie(data_verification, merge_factor) = GetParam();
		LOG(INFO) << "data_verification " << data_verification;

		config::VmdkConfig config;
		DefaultVmdkConfig(config, kVmdkBlockSize);

		vmdk = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize());

		if (data_verification) {
			auto src = std::make_unique<RamCacheHandler>(vmdk->GetJsonConfig());
			EXPECT_TRUE(src);
			src_handlerp = src.get();
			handlers.emplace_back(std::move(src));

			auto dst = std::make_unique<RamCacheHandler>(vmdk->GetJsonConfig());
			EXPECT_TRUE(dst);
			dst_handlerp = dst.get();
			handlers.emplace_back(std::move(dst));
		} else {
			auto src = std::make_unique<SuccessHandler>(vmdk->GetJsonConfig());
			EXPECT_TRUE(src);
			src_handlerp = src.get();
			handlers.emplace_back(std::move(src));

			auto dst = std::make_unique<SuccessHandler>(vmdk->GetJsonConfig());
			EXPECT_TRUE(dst);
			dst_handlerp = dst.get();
			handlers.emplace_back(std::move(dst));
		}

		SetUpDataSource();
	}

	char CheckPointIDToFillChar(CheckPointID ckpt) {
		return 'A' + ckpt - 1;
	}

	void WriteToDataSource(RequestHandler* dstp, CheckPointID ckpt,
			std::unordered_set<BlockID> blocks, char fill) {
		auto io_buf = folly::IOBuf::create(vmdk->BlockSize());
		auto bufferp = io_buf->writableData();
		std::memset(bufferp, fill, vmdk->BlockSize());

		RequestHandlerPtrVec dst;
		dst.emplace_back(dstp);

		std::vector<folly::Future<int>> futures;
		for (const auto& block : blocks) {
			auto writer = std::make_unique<DataWriter>(dst.begin(), dst.end(),
				vmdk.get(), ckpt, bufferp, block << vmdk->BlockShift(),
				vmdk->BlockSize());
			auto w = writer.get();
			w->CreateRequest();
			futures.emplace_back(w->Start()
				.then([writer = std::move(writer)] (int rc) {
					EXPECT_EQ(rc, 0);
					return rc;
				})
			);
		}

		auto f = folly::collectAll(std::move(futures)).
		then([] (folly::Try<std::vector<folly::Try<int>>> tries) {
			int rc = 0;
			if (tries.hasException()) {
				EXPECT_TRUE(false);
			} else {
				const auto& vec = tries.value();
				for (const auto& tri : vec) {
					if (tri.hasException()) {
						EXPECT_TRUE(false);
					}
					if (tri.value()) {
						rc = tri.value();
						break;
					}
				}
			}
			return rc;
		});
		f.wait(5s);
		EXPECT_TRUE(f.isReady());
		EXPECT_EQ(f.value(), 0);
	}

	void TakeCheckPoint(CheckPointID ckpt, std::unordered_set<BlockID>& blocks) {
		auto check_point = std::make_unique<CheckPoint>(vmdk->GetID(), ckpt);
		check_point->SetModifiedBlocks(blocks);
		check_points.emplace(ckpt, std::move(check_point));
		std::ostringstream os;
		os << "*** Blocks modified in CKPT " << ckpt << std::endl;
		std::copy(blocks.begin(), blocks.end(),
			std::ostream_iterator<BlockID>(os, " "));
		VLOG(5) << os.str();
	}

	void UpdateBlockToCheckPointMapping(CheckPointID ckpt,
			std::unordered_set<BlockID>& blocks) {
		for (auto block : blocks) {
			auto [it, inserted] = block_to_ckpt.emplace(block, ckpt);
			if (not inserted) {
				it->second = ckpt;
			}
		}
		ckpt_to_block_ckpt.emplace(ckpt, block_to_ckpt);
	}

	void SetUpDataSource() {
		CheckPointID ckpt = 1;

		/* write all blocks for first check point */
		auto r = iter::Range(static_cast<BlockID>(0), kNumberOfBlocks);
		std::unordered_set<BlockID> blocks(r.begin(), r.end());
		WriteToDataSource(src_handlerp, ckpt, blocks, CheckPointIDToFillChar(ckpt));
		TakeCheckPoint(ckpt, blocks);
		UpdateBlockToCheckPointMapping(ckpt, blocks);

		for (BlockID step : std::vector<int>{1, 2, 3, 4, 8}) {
			++ckpt;
			std::unordered_set<BlockID> blocks;
			bool set = true;
			for (auto block : iter::Range(static_cast<BlockID>(0), kNumberOfBlocks, step)) {
				if (not set) {
					set = not set;
					continue;
				}
				set = not set;
				auto r = iter::Range(block, std::min(kNumberOfBlocks, block+step));
				blocks.insert(r.begin(), r.end());
			}

			WriteToDataSource(src_handlerp, ckpt, blocks, CheckPointIDToFillChar(ckpt));
			TakeCheckPoint(ckpt, blocks);
			UpdateBlockToCheckPointMapping(ckpt, blocks);
		}

		for (auto& bc : block_to_ckpt) {
			VLOG(5) << "block " << bc.first << " ckpt " << bc.second;
		}
	}

	void VerifyData(RamCacheHandler* ram, BlockToCkpt& block_to_ckpt) {
		RequestHandlerPtrVec from;
		from.emplace_back(ram);

		const size_t block_size = vmdk->BlockSize();
		auto io_buf = folly::IOBuf::create(block_size);
		EXPECT_TRUE(io_buf);
		auto bufferp = io_buf->writableData();

		for (auto [block, ckpt] : block_to_ckpt) {
			EXPECT_GE(ram->Cache(ckpt)->Size(), 1);
			auto offset = block << vmdk->BlockShift();
			DataReader reader(from.begin(), from.end(), vmdk.get(), ckpt,
				bufferp, offset, vmdk->BlockSize());
			auto f = reader.Start();
			f.wait(1s);
			EXPECT_TRUE(f.isReady());
			EXPECT_EQ(f.value(), 0);

			char fill = CheckPointIDToFillChar(ckpt);
			for (size_t i = 0; i < block_size; ++i) {
				EXPECT_EQ(bufferp[i], fill);
				if (bufferp[i] != fill) {
					assert(0);
					exit(1);
				}
			}
		}
	}
};

#if 0
- SetDataSource and SetDataDestination throws on empty input
- SetCheckPoints
  - check_points - empty vector of check points
  - CheckPointIDs are not consecutive
  - Previous CheckPointID (SetCheckPoints) and new CheckPointID are not consecutive
#endif

TEST_P(TestDataSync, InitializationTests) {
	DataSync sync(vmdk.get(), 1);

	EXPECT_ANY_THROW(sync.SetDataSource(RequestHandlerPtrVec()));
	EXPECT_ANY_THROW(sync.SetDataDestination(RequestHandlerPtrVec()));

	/* set empty checkpoint vector */
	bool start;
	auto rc = sync.SetCheckPoints(CheckPointPtrVec(), &start);
	EXPECT_EQ(rc, 0);
	EXPECT_FALSE(start);

	auto fut = sync.Start();
	fut.wait(1s);
	EXPECT_NE(fut.value(), 0);

	{
		/* now set source and destinations */
		RequestHandlerPtrVec srcs;
		srcs.emplace_back(src_handlerp);

		RequestHandlerPtrVec dsts;
		dsts.emplace_back(dst_handlerp);

		EXPECT_NO_THROW(sync.SetDataSource(srcs));
		EXPECT_NO_THROW(sync.SetDataDestination(dsts));
	}

	{
		auto ckpt1 = std::make_unique<CheckPoint>(vmdk->GetID(), 1);
		auto ckpt2 = std::make_unique<CheckPoint>(vmdk->GetID(), 10);
		CheckPointPtrVec check_points;
		check_points.emplace_back(ckpt1.get());
		check_points.emplace_back(ckpt2.get());
		auto rc = sync.SetCheckPoints(check_points, &start);
		EXPECT_NE(rc, 0);
		EXPECT_FALSE(start);

		auto fut = sync.Start();
		fut.wait(1s);
		EXPECT_EQ(fut.value(), 0);
	}

	{
		auto ckpt1 = std::make_unique<CheckPoint>(vmdk->GetID(), 1);
		auto ckpt2 = std::make_unique<CheckPoint>(vmdk->GetID(), 2);
		CheckPointPtrVec check_points;
		check_points.emplace_back(ckpt1.get());
		check_points.emplace_back(ckpt2.get());
		auto rc = sync.SetCheckPoints(check_points, &start);
		EXPECT_EQ(rc, 0);
		EXPECT_TRUE(start);

		{
		auto fut = sync.Start();
		fut.wait(1s);
		EXPECT_EQ(fut.value(), 0);
		}

		check_points.clear();
		auto ckpt3 = std::make_unique<CheckPoint>(vmdk->GetID(), 10);
		auto ckpt4 = std::make_unique<CheckPoint>(vmdk->GetID(), 11);
		check_points.emplace_back(ckpt3.get());
		check_points.emplace_back(ckpt4.get());
		rc = sync.SetCheckPoints(check_points, &start);
		EXPECT_NE(rc, 0);
		EXPECT_FALSE(start);

		{
		auto fut = sync.Start();
		fut.wait(1s);
		EXPECT_EQ(fut.value(), 0);
		}
	}
}

TEST_P(TestDataSync, DataSync) {
	DataSync sync(vmdk.get(), merge_factor);

	RequestHandlerPtrVec srcs;
	srcs.emplace_back(src_handlerp);

	RequestHandlerPtrVec dsts;
	dsts.emplace_back(dst_handlerp);

	EXPECT_NO_THROW(sync.SetDataSource(srcs));
	EXPECT_NO_THROW(sync.SetDataDestination(dsts));

	std::vector<folly::Future<int>> futures;
	for (auto& ckpt : check_points) {
		CheckPointPtrVec ckpts;
		ckpts.emplace_back(ckpt.second.get());

		bool start = false;
		auto rc = sync.SetCheckPoints(ckpts, &start);
		EXPECT_EQ(rc, 0);

		futures.emplace_back(sync.Start());
	}

#if 0
	int count = 0;
	while (true) {
		bool is_stopped = false;
		int rc;
		sync.GetStatus(&is_stopped, &rc);
		if (is_stopped == true) {
			EXPECT_EQ(rc, 0);
			break;
		}
		ASSERT_LT(count, 2);
		++count;

		std::mutex m;
		std::condition_variable cv;
		std::unique_lock<std::mutex> lk(m);
		cv.wait_for(lk, 1s);
	}
#else
	auto fut = folly::collectAll(std::move(futures))
	.then([] (const folly::Try<std::vector<folly::Try<int>>>& tries) {
		if (tries.hasException()) {
			return -EIO;
		}
		for (const auto& trie : tries.value()) {
			if (trie.hasException()) {
				return -EIO;
			}
			if (trie.value() < 0) {
				return trie.value();
			}
		}
		return 0;
	});
	fut.wait();
	EXPECT_EQ(fut.value(), 0);
#endif
	if (data_verification) {
		LOG(INFO) << "Verifying data @ source ";
		VerifyData(dynamic_cast<RamCacheHandler*>(src_handlerp), block_to_ckpt);
		LOG(INFO) << "Verifying data @ dest ";
		VerifyData(dynamic_cast<RamCacheHandler*>(dst_handlerp), block_to_ckpt);
	}
}

INSTANTIATE_TEST_CASE_P(CkptToMerge, TestDataSync,
	Combine(
		Values(true, false),
		Values(0, 1, 2)
	)
);
