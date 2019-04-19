#include <memory>
#include <vector>
#include <algorithm>
#include <numeric>
#include <random>
#include <limits>
#include <chrono>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "Request.h"
#include "Vmdk.h"
#include "VmdkConfig.h"

#include "UnalignedHandler.h"
#include "CompressHandler.h"
#include "EncryptHandler.h"

#include "MultiTargetHandler.h"

using namespace pio;
using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;
using namespace ::ondisk;
using namespace ::hyc_thrift;
using namespace std::chrono_literals;

using namespace ::hyc_thrift;

class CompressHandlerTest : public TestWithParam<
		::testing::tuple<int, bool, bool>> {
protected:
	const CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdkp;
	std::atomic<RequestID> req_id_;

public:
	void DefaultVmdkConfig(config::VmdkConfig& config, uint64_t block_size,
			bool is_compression_enabled, bool is_encryption_enabled) {
		config.SetVmId("vmdkid");
		config.SetVmdkId("vmdkid");
		config.SetVmUUID("vmuuid");
		config.SetVmdkUUID("vmdkuuid");
		config.SetBlockSize(block_size);
		if (is_compression_enabled)
			config.ConfigureCompression("snappy", 1);
		if (is_encryption_enabled)
			config.ConfigureEncryption("aes256-gcm", "abc", {0});
		if (not is_compression_enabled)
			config.DisableCompression();
		if (not is_compression_enabled)
			config.DisableEncryption();
		config.ConfigureRamCache(1024);
		config.DisableNetworkTarget();
		config.DisableAeroSpikeCache();
	}

	virtual void SetUp() {
		config::VmdkConfig config;
		auto [block_size, is_compression_enabled,
			is_encryption_enabled] = GetParam();
		DefaultVmdkConfig(config, block_size,
			is_compression_enabled, is_encryption_enabled);

		vmdkp = std::make_unique<ActiveVmdk>(1, "1", nullptr,
			config.Serialize());
		EXPECT_NE(vmdkp, nullptr);

		auto unalign = std::make_unique<UnalignedHandler>();
		EXPECT_NE(unalign, nullptr);

		auto configp = vmdkp->GetJsonConfig();
		auto multi_target = std::make_unique<MultiTargetHandler>(
			vmdkp.get(), configp);
		EXPECT_NE(multi_target, nullptr);

		vmdkp->RegisterRequestHandler(std::move(unalign));

		if (is_compression_enabled) {
			auto compress = std::make_unique<CompressHandler>(configp);
			EXPECT_NE(compress, nullptr);
			vmdkp->RegisterRequestHandler(std::move(compress));
		}

		if (is_encryption_enabled) {
			auto encrypt = std::make_unique<EncryptHandler>(vmdkp.get(), configp);
			EXPECT_NE(encrypt, nullptr);
			vmdkp->RegisterRequestHandler(std::move(encrypt));
		}

		vmdkp->RegisterRequestHandler(std::move(multi_target));

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

static void FillRandomBuffer(char* bufferp, size_t size) {
	auto x = size / sizeof(uint64_t);
	std::vector<uint64_t> v(x);

	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<uint64_t> dis(std::numeric_limits<uint64_t>::min(),
		std::numeric_limits<uint64_t>::max());

	std::generate(v.begin(), v.end(), [&dis, &gen] () {
		return dis(gen);
	});

	auto *np = reinterpret_cast<uint64_t*>(bufferp);
	for (const auto& number : v) {
		*np = number;
		++np;
	}
}

TEST_P(CompressHandlerTest, UncompressableAlignedOverWrite) {
	/* write compressable data */
	auto write_a = NewRequestBuffer(vmdkp->BlockSize());
	::memset(write_a->Payload(), 'A', write_a->Size());

	{
		auto write_reqp = std::make_unique<Request>(NextRequestID(), vmdkp.get(),
			Request::Type::kWrite, write_a->Payload(), write_a->Size(),
			write_a->Size(), 0);
		auto write_fut = vmdkp->Write(write_reqp.get(), 1);
		write_fut.wait();
		EXPECT_EQ(write_fut.value(), 0);
		EXPECT_EQ(write_reqp->NumberOfRequestBlocks(), 1);

		/*
		write_reqp->ForEachRequestBlock([this] (RequestBlock *blockp) mutable {
			auto comp_buf = blockp->GetRequestBufferAtBack();
			auto comp_headerp = reinterpret_cast<CompressHeader*>(comp_buf->Payload());
			EXPECT_EQ(comp_headerp->original_size_, vmdkp->BlockSize());
			EXPECT_LT(comp_headerp->compress_size_, vmdkp->BlockSize());
			EXPECT_EQ(comp_headerp->type_, CompressType::kZstd);
			EXPECT_TRUE(comp_headerp->is_compressed_);
			return true;
		});
		*/

		auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
		read_fut.wait();
		auto read_bufp = std::move(read_fut.value());
		auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
		EXPECT_EQ(rc, 0);
	}

	/* overwrite with uncompressable data */
	FillRandomBuffer(write_a->Payload(), write_a->Size());

	{
		auto write_reqp = std::make_unique<Request>(NextRequestID(), vmdkp.get(),
			Request::Type::kWrite, write_a->Payload(), write_a->Size(),
			write_a->Size(), 0);
		auto write_fut = vmdkp->Write(write_reqp.get(), 1);
		write_fut.wait();
		EXPECT_EQ(write_fut.value(), 0);
		EXPECT_EQ(write_reqp->NumberOfRequestBlocks(), 1);

		/*
		write_reqp->ForEachRequestBlock([this] (RequestBlock *blockp) mutable {
			auto comp_buf = blockp->GetRequestBufferAtBack();
			auto comp_headerp = reinterpret_cast<CompressHeader*>(comp_buf->Payload());
			LOG(ERROR) << *comp_headerp;
			EXPECT_EQ(comp_headerp->original_size_, vmdkp->BlockSize());
			EXPECT_FALSE(comp_headerp->is_compressed_);
			return true;
		});
		*/

		auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
		read_fut.wait();
		auto read_bufp = std::move(read_fut.value());
		auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
		log_assert(rc == 0);
		EXPECT_EQ(rc, 0);
	}

	/* overwrite with compressable data */
	::memset(write_a->Payload(), 'B', write_a->Size());

	{
		auto write_reqp = std::make_unique<Request>(NextRequestID(), vmdkp.get(),
			Request::Type::kWrite, write_a->Payload(), write_a->Size(),
			write_a->Size(), 0);
		auto write_fut = vmdkp->Write(write_reqp.get(), 1);
		write_fut.wait();
		EXPECT_EQ(write_fut.value(), 0);
		EXPECT_EQ(write_reqp->NumberOfRequestBlocks(), 1);

		/*
		write_reqp->ForEachRequestBlock([this] (RequestBlock *blockp) mutable {
			auto comp_buf = blockp->GetRequestBufferAtBack();
			auto comp_headerp = reinterpret_cast<CompressHeader*>(comp_buf->Payload());
			EXPECT_EQ(comp_headerp->original_size_, vmdkp->BlockSize());
			EXPECT_LT(comp_headerp->compress_size_, vmdkp->BlockSize());
			EXPECT_EQ(comp_headerp->type_, CompressType::kZstd);
			EXPECT_TRUE(comp_headerp->is_compressed_);
			return true;
		});
		*/

		auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
		read_fut.wait();
		auto read_bufp = std::move(read_fut.value());
		auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
		EXPECT_EQ(rc, 0);
	}
}

TEST_P(CompressHandlerTest, CompressableUnAlignedOverWrite) {
	/* write uncompressable data */
	auto write_a = NewRequestBuffer(vmdkp->BlockSize());
	FillRandomBuffer(write_a->Payload(), write_a->Size());
	::memset(write_a->Payload(), 'A', write_a->Size());
	{
		auto write_reqp = std::make_unique<Request>(NextRequestID(), vmdkp.get(),
			Request::Type::kWrite, write_a->Payload(), write_a->Size(),
			write_a->Size(), 0);
		auto write_fut = vmdkp->Write(write_reqp.get(), 1);
		write_fut.wait();
		EXPECT_EQ(write_fut.value(), 0);
		EXPECT_EQ(write_reqp->NumberOfRequestBlocks(), 1);

		/*
		write_reqp->ForEachRequestBlock([this] (RequestBlock *blockp) mutable {
			auto comp_buf = blockp->GetRequestBufferAtBack();
			auto comp_headerp = reinterpret_cast<CompressHeader*>(comp_buf->Payload());
			EXPECT_EQ(comp_headerp->original_size_, vmdkp->BlockSize());
			EXPECT_FALSE(comp_headerp->is_compressed_);
			return true;
		});
		*/

		auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
		read_fut.wait();
		auto read_bufp = std::move(read_fut.value());

		auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
		EXPECT_EQ(rc, 0);
	}

	/* unaligned write of compressable data */
	const auto kSkip = 512;
	const auto kWriteSize = vmdkp->BlockSize()-kSkip;
	::memset(write_a->Payload()+kSkip, 'B', kWriteSize);

	{
		auto write_reqp = std::make_unique<Request>(NextRequestID(), vmdkp.get(),
			Request::Type::kWrite, write_a->Payload()+kSkip, kWriteSize,
			kWriteSize, kSkip);
		auto write_fut = vmdkp->Write(write_reqp.get(), 1);
		write_fut.wait();
		EXPECT_EQ(write_fut.value(), 0);
		EXPECT_EQ(write_reqp->NumberOfRequestBlocks(), 1);

		/*
		write_reqp->ForEachRequestBlock([this] (RequestBlock *blockp) mutable {
			auto comp_buf = blockp->GetRequestBufferAtBack();
			auto comp_headerp = reinterpret_cast<CompressHeader*>(comp_buf->Payload());
			LOG(ERROR) << *comp_headerp;
			EXPECT_EQ(comp_headerp->original_size_, vmdkp->BlockSize());
			EXPECT_LT(comp_headerp->compress_size_, vmdkp->BlockSize());
			EXPECT_EQ(comp_headerp->type_, CompressType::kZstd);
			EXPECT_TRUE(comp_headerp->is_compressed_);
			return true;
		});
		*/

		{
			auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
			read_fut.wait();
			auto read_bufp = std::move(read_fut.value());
			auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
			EXPECT_EQ(rc, 0);
		}

		{
			auto read_fut = VmdkRead(0, kSkip, kWriteSize);
			read_fut.wait();
			auto read_bufp = std::move(read_fut.value());
			//std::cout << std::hex << read_bufp->Payload() << std::endl;
			auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload()+kSkip, kWriteSize);
			EXPECT_EQ(rc, 0);
		}
	}
}

TEST_P(CompressHandlerTest, UncompressableUnAlignedOverWrite) {
	/* write compressable data */
	auto write_a = NewRequestBuffer(vmdkp->BlockSize());
	::memset(write_a->Payload(), 'A', write_a->Size());

	{
		auto write_reqp = std::make_unique<Request>(NextRequestID(), vmdkp.get(),
			Request::Type::kWrite, write_a->Payload(), write_a->Size(),
			write_a->Size(), 0);
		auto write_fut = vmdkp->Write(write_reqp.get(), 1);
		write_fut.wait();
		EXPECT_EQ(write_fut.value(), 0);
		EXPECT_EQ(write_reqp->NumberOfRequestBlocks(), 1);

		/*
		write_reqp->ForEachRequestBlock([this] (RequestBlock *blockp) mutable {
			auto comp_buf = blockp->GetRequestBufferAtBack();
			auto comp_headerp = reinterpret_cast<CompressHeader*>(comp_buf->Payload());
			EXPECT_EQ(comp_headerp->original_size_, vmdkp->BlockSize());
			EXPECT_LT(comp_headerp->compress_size_, vmdkp->BlockSize());
			EXPECT_EQ(comp_headerp->type_, CompressType::kZstd);
			EXPECT_TRUE(comp_headerp->is_compressed_);
			return true;
		});
		*/

		auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
		read_fut.wait();
		auto read_bufp = std::move(read_fut.value());
		auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
		EXPECT_EQ(rc, 0);
	}

	/* unaligned write of uncompressable data */
	const auto kSkip = 512;
	const auto kWriteSize = 1024;
	FillRandomBuffer(write_a->Payload()+kSkip, kWriteSize);

	{
		auto write_reqp = std::make_unique<Request>(NextRequestID(), vmdkp.get(),
			Request::Type::kWrite, write_a->Payload()+kSkip, kWriteSize,
			kWriteSize, kSkip);
		auto write_fut = vmdkp->Write(write_reqp.get(), 1);
		write_fut.wait();
		EXPECT_EQ(write_fut.value(), 0);
		EXPECT_EQ(write_reqp->NumberOfRequestBlocks(), 1);

		/*
		write_reqp->ForEachRequestBlock([this] (RequestBlock *blockp) mutable {
			auto comp_buf = blockp->GetRequestBufferAtBack();
			auto comp_headerp = reinterpret_cast<CompressHeader*>(comp_buf->Payload());
			LOG(ERROR) << *comp_headerp;
			EXPECT_EQ(comp_headerp->original_size_, vmdkp->BlockSize());
			EXPECT_LT(comp_headerp->compress_size_, vmdkp->BlockSize());
			EXPECT_EQ(comp_headerp->type_, CompressType::kZstd);
			EXPECT_TRUE(comp_headerp->is_compressed_);
			return true;
		});
		*/

		{
			auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
			read_fut.wait();
			auto read_bufp = std::move(read_fut.value());
			auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
			EXPECT_EQ(rc, 0);
		}

		{
			auto read_fut = VmdkRead(0, kSkip, kWriteSize);
			read_fut.wait();
			auto read_bufp = std::move(read_fut.value());
			auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload()+kSkip, kWriteSize);
			EXPECT_EQ(rc, 0);
		}
	}
}


TEST_P(CompressHandlerTest, BulkCompressableAlignedWrite) {
	const BlockID kNBlocks = 32;
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

	auto bulk_write_fut = vmdkp->BulkWrite(ckpt_id, *requests, *process);
	bulk_write_fut.wait(1s);
	EXPECT_TRUE(bulk_write_fut.isReady());
	EXPECT_EQ(bulk_write_fut.value(), 0);


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
	auto read_fut = vmdkp->BulkRead(ckpts, *requests, *process);
	read_fut.wait(1s);
	EXPECT_TRUE(read_fut.isReady());
	EXPECT_EQ(read_fut.value(), 0);

	auto wit = buffers->begin();
	auto weit = std::next(wit, blocks.size());
	EXPECT_NE(weit, buffers->end());

	auto rit = weit;
	auto reit = std::next(rit, blocks.size());
	EXPECT_EQ(reit, buffers->end());

	for (; rit != reit; ++rit, ++wit) {
		auto rp = (*rit)->Payload();
		auto wp = (*wit)->Payload();

		EXPECT_EQ((*rit)->Size(), (*wit)->Size());
		auto rc = ::memcmp(rp, wp, (*rit)->Size());
		EXPECT_FALSE(rc);
	}
}


INSTANTIATE_TEST_CASE_P(BlkSzCompressEncryptCombination, CompressHandlerTest,
	Combine(
		Values(4096, 8192),
		Values(false, true),
		Values(false, true)));
