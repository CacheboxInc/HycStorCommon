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
#include "RamCacheHandler.h"
#include "SuccessHandler.h"

#if 0
need following unit tests
=========================
- ensure DataCopier sets promise only after last DataWrite is complete
- ensure DataCopier completes if
  - read_.io_depth_ = write_.io_depth_ = 1
  - read_.io_depth_ = write_.io_depth_ = 2
  - read_.io_depth_ = 1 and write_.io_depth_ = 2
  - read_.io_depth_ = 2 and write_.io_depth_ = 1
- During IO Error, ensure DataCopier set promise only after all in_progress_
  IOs are complete. It is okay to have write_.queue_ not empty. However,
  read_.in_progress_ and write_.in_progress_ must be 0
- ensure IOs progress even after WriteQueueSize() >= kMaxWritesPending
- ensure IOs progress after read_.in_progress_ >= read_.io_depth_ is reached
- ensure IOs progress after write_.in_progress_ >= write_.io_depth_ is reached
#endif

#if 0
- take 256 blocks
- ckpt 1 modified every block with 'A'
- ckpt 2 modified every alternate block with 'B'
- ckpt 3 modified every alternate 2 blocks with 'C'
- ckpt 4 modified every alternate 3 blocks with 'D'
- ckpt 5 modified every alternate 4 blocks with 'E'
- ckpt 6 modified every alternate 8 blocks with 'F'

data source -> ram cache
data destination -> file target
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

class TestDataCopier : public TestWithParam<::testing::tuple<bool, int, int>> {
protected:
	std::unique_ptr<ActiveVmdk> vmdk;
	std::vector<std::unique_ptr<RequestHandler>> handlers;
	RequestHandler* src_handlerp;
	RequestHandler* dst_handlerp;

	std::map<CheckPointID, std::unique_ptr<CheckPoint>> check_points;
	std::map<BlockID, CheckPointID> block_to_ckpt;
	uint64_t kTotalBlocksWritten{0};

	bool data_verification;
	int read_io_depth;
	int write_io_depth;

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
		std::tie(data_verification, read_io_depth, write_io_depth) = GetParam();
		LOG(ERROR) << "Read IO Depth " << read_io_depth
			<< " Write IO Depth " << write_io_depth
			<< " Data verification " << data_verification;

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

	virtual void TearDown() {
		vmdk = nullptr;
		src_handlerp = nullptr;
		dst_handlerp = nullptr;
		check_points.clear();
		block_to_ckpt.clear();
		handlers.clear();
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
		ASSERT_TRUE(f.isReady());
		EXPECT_EQ(f.value(), 0);
		kTotalBlocksWritten += blocks.size();
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
		UpdateBlockToCheckPointMapping(ckpt, blocks);
	}

	void UpdateBlockToCheckPointMapping(CheckPointID ckpt,
			std::unordered_set<BlockID>& blocks) {
		for (auto block : blocks) {
			auto [it, inserted] = block_to_ckpt.emplace(block, ckpt);
			if (not inserted) {
				it->second = ckpt;
			}
		}
	}

	void SetUpDataSource() {
		CheckPointID ckpt = 1;

		/* write all blocks for first check point */
		auto r = iter::Range(static_cast<BlockID>(0), kNumberOfBlocks);
		std::unordered_set<BlockID> blocks(r.begin(), r.end());
		WriteToDataSource(src_handlerp, ckpt, blocks, CheckPointIDToFillChar(ckpt));
		TakeCheckPoint(ckpt, blocks);

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
		}

		for (auto& bc : block_to_ckpt) {
			VLOG(5) << "block " << bc.first << " ckpt " << bc.second;
		}
	}

	void VerifyData(RamCacheHandler* ram) {
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

TEST_P(TestDataCopier, SimpleDataSync) {
	RequestHandlerPtrVec src;
	src.emplace_back(src_handlerp);

	RequestHandlerPtrVec dst;
	dst.emplace_back(dst_handlerp);

	DataCopier copier(vmdk.get(), vmdk->BlockShift(), vmdk->BlockSize() * 2);
	copier.SetDataSource(src.begin(), src.end());
	copier.SetDataDestination(dst.begin(), dst.end());
	copier.SetReadIODepth(read_io_depth);
	copier.SetWriteIODepth(write_io_depth);

	CheckPointPtrVec ckpts;
	for (const auto& id_to_ckpt : check_points) {
		ckpts.emplace_back(id_to_ckpt.second.get());
	}
	copier.SetCheckPoints(std::move(ckpts));

	auto f = copier.Begin();
	f.wait(5s);
	EXPECT_TRUE(f.isReady());
	EXPECT_EQ(f.value(), 0);

	auto stats = copier.GetStats();
	EXPECT_TRUE(stats.is_read_complete);
	EXPECT_FALSE(stats.is_failed);
	EXPECT_EQ(stats.copy_total, kTotalBlocksWritten);
	EXPECT_EQ(stats.copy_pending, 0);
	EXPECT_EQ(stats.copy_completed, block_to_ckpt.size());
	EXPECT_LT(block_to_ckpt.size(), kTotalBlocksWritten);
	EXPECT_EQ(stats.copy_avoided, kTotalBlocksWritten - block_to_ckpt.size());
	EXPECT_EQ(stats.cbt_in_progress, check_points.begin()->second->ID());

	if (data_verification) {
		/* Read and verify data */
		LOG(INFO) << "Verifying data @ source ";
		VerifyData(dynamic_cast<RamCacheHandler*>(src_handlerp));
		LOG(INFO) << "Verifying data @ dest ";
		VerifyData(dynamic_cast<RamCacheHandler*>(dst_handlerp));
	}
}

INSTANTIATE_TEST_CASE_P(VerificationAndDepths, TestDataCopier,
	Combine(
		Values(true, false),
		Values(1, 2, 7, 32),
		Values(1, 2, 11, 32)
	)
);
