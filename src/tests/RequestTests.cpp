#include <memory>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "Request.h"
#include "Vmdk.h"
#include "IDs.h"
#include "VmdkConfig.h"

using namespace pio;

static void DefaultVmdkConfig(config::VmdkConfig& config, uint64_t block_size) {
	config.SetVmId("vmid");
	config.SetVmdkId("vmdkid");
	config.SetBlockSize(block_size);
}

TEST(RequestTest, Constructor_Exception) {
	config::VmdkConfig config;
	DefaultVmdkConfig(config, 4096);

	ActiveVmdk vmdk(nullptr, 1, "1", config.Serialize());

	/* RequestID == 0 */
	EXPECT_THROW(
		Request(0, &vmdk, Request::Type::kRead, nullptr, 1024, 1024, 0),
		std::invalid_argument);

	/* tansfer_size is not in multiple of sector */
	EXPECT_THROW(
		Request(1, &vmdk, Request::Type::kRead, nullptr, 1024, 1111, 0),
		std::invalid_argument);

	/* buffer_size is not in multiple of sector */
	EXPECT_THROW(
		Request(2, &vmdk, Request::Type::kRead, nullptr, 1111, 1024, 0),
		std::invalid_argument);

	/* transfer_size greator than max io size */
	EXPECT_THROW(
		Request(3, &vmdk, Request::Type::kRead, nullptr, 1ul<<30, 1ul<<30, 0),
		std::invalid_argument);

	/* offset not aligned to sector */
	EXPECT_THROW(
		Request(4, &vmdk, Request::Type::kRead, nullptr, 1024, 1024, 1111),
		std::invalid_argument);

	/* buffer_size != transfer_size */
	EXPECT_THROW(
		Request(5, &vmdk, Request::Type::kRead, nullptr, 1024, 2048, 0),
		std::invalid_argument);

	/* kWriteSame and buffer_size > transfer_size */
	EXPECT_THROW(
		Request(6, &vmdk, Request::Type::kWriteSame, nullptr, 2048, 1024, 0),
		std::invalid_argument);
}

TEST(RequestTest, ReadTest) {
	for (auto blocks_size = 512; blocks_size <= 4096; blocks_size <<= 1) {
		config::VmdkConfig config;
		DefaultVmdkConfig(config, blocks_size);
		ActiveVmdk vmdk(nullptr, 1, "1", config.Serialize());
		for (auto nblocks = 2; nblocks <= 10; ++nblocks) {
			size_t buffer_size = blocks_size * nblocks;
			auto bufferp = std::make_unique<RequestBuffer>(buffer_size);
			for (auto i = kInvalidRequestID + 1; i <= 3000; ++i) {
				Offset offset = i * kSectorSize;
				Request r(i, &vmdk, Request::Type::kRead, bufferp->Payload(),
					buffer_size, buffer_size, offset);

				auto[start, end] = r.Blocks();
				if ((offset & (blocks_size-1)) == 0) {
					EXPECT_EQ(start + nblocks, end + 1);
				} else {
					EXPECT_EQ(start + nblocks, end);
				}

				auto cur_block = start;
				r.ForEachRequestBlock([&cur_block] (RequestBlock *blockp) {
					EXPECT_EQ(blockp->GetBlockID(), cur_block);
					EXPECT_EQ(blockp->GetRequestBufferAtBack(), nullptr);
					EXPECT_EQ(blockp->GetRequestBufferCount(), 0);

					blockp->ForEachRequestBuffer([] (RequestBuffer *req_bufp) {
						EXPECT_TRUE(false);
						return true;
					});
					++cur_block;
					return true;
				});
				EXPECT_EQ(cur_block - 1, end);
			}
		}
	}
}

TEST(RequestTest, WriteTest) {
	for (auto blocks_size = 512; blocks_size <= 4096; blocks_size <<= 1) {
		config::VmdkConfig config;
		DefaultVmdkConfig(config, blocks_size);
		ActiveVmdk vmdk(nullptr, 1, "1", config.Serialize());
		for (auto nblocks = 2; nblocks <= 10; ++nblocks) {
			size_t buffer_size = blocks_size * nblocks;
			auto bufferp = std::make_unique<RequestBuffer>(buffer_size);
			auto payload = bufferp->Payload();
			::memset(payload, 'A', bufferp->Size());

			for (auto i = kInvalidRequestID + 1; i <= 3000; ++i) {
				Offset offset = i * kSectorSize;
				Request r(i, &vmdk, Request::Type::kWrite, bufferp->Payload(),
					buffer_size, buffer_size, offset);

				auto[start, end] = r.Blocks();
				if ((offset & (blocks_size-1)) == 0) {
					EXPECT_EQ(start + nblocks, end + 1);
				} else {
					EXPECT_EQ(start + nblocks, end);
				}

				auto cur_block = start;
				r.ForEachRequestBlock([&] (RequestBlock *blockp) {
					EXPECT_EQ(blockp->GetBlockID(), cur_block);
					EXPECT_EQ(blockp->GetRequestBufferCount(), 1);

					auto req_bufp = blockp->GetRequestBufferAtBack();
					EXPECT_NE(req_bufp, nullptr);

					auto nreq_bufs = 0;
					blockp->ForEachRequestBuffer([&]
							(RequestBuffer *bufp) {
						EXPECT_EQ(req_bufp, bufp);
						++nreq_bufs;

						auto rc = ::memcmp(req_bufp->Payload(), payload, req_bufp->Size());
						EXPECT_EQ(rc, 0);
						return true;
					});
					EXPECT_EQ(nreq_bufs, 1);

					++cur_block;
					return true;
				});
				EXPECT_EQ(cur_block - 1, end);
			}
		}
	}
}

TEST(RequestTest, WriteSameTest) {
	auto blocks_size   = 4096;
	auto buffer_size   = 512;
	auto transfer_size = blocks_size * 2;

	config::VmdkConfig config;
	DefaultVmdkConfig(config, blocks_size);
	ActiveVmdk vmdk(nullptr, 1, "1", config.Serialize());
	auto bufferp = std::make_unique<RequestBuffer>(buffer_size);
	auto payload = bufferp->Payload();
	::memset(payload, 'A', bufferp->Size());

	Request r(1, &vmdk, Request::Type::kWriteSame, payload, buffer_size,
		transfer_size, 1024);
	auto[start, end] = r.Blocks();
	EXPECT_EQ(start, 0);
	EXPECT_EQ(end, 2);

	auto cur_block = 0;
	int32_t to_copy = transfer_size;
	r.ForEachRequestBlock([&] (RequestBlock *blockp) {
		EXPECT_EQ(blockp->GetBlockID(), cur_block);
		EXPECT_EQ(blockp->GetRequestBufferCount(), 1);

		auto req_bufp = blockp->GetRequestBufferAtBack();
		EXPECT_NE(req_bufp, nullptr);
		auto datap = req_bufp->Payload();
		auto data_size = req_bufp->Size();

		EXPECT_LE(data_size, blocks_size);

		for (auto c = 0u; c < data_size; c += bufferp->Size()) {
			auto rc = ::memcmp(datap, payload, bufferp->Size());
			EXPECT_EQ(rc, 0);
		}
		to_copy -= data_size;
		++cur_block;
		return true;
	});
	EXPECT_EQ(to_copy, 0);
	EXPECT_EQ(cur_block-1, end);
}
