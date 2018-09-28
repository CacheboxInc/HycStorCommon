#include <memory>
#include <vector>
#include <algorithm>
#include <numeric>
#include <random>
#include <limits>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "Request.h"
#include "Vmdk.h"
#include "VmdkConfig.h"

#include "LockHandler.h"
#include "UnalignedHandler.h"
#include "EncryptHandler.h"
#include "MultiTargetHandler.h"

using namespace pio;
using namespace ::ondisk;
using namespace ::hyc_thrift;

const size_t kVmdkBlockSize = 8192;

class EncryptHandlerTest : public ::testing::Test {
protected:
	const CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdkp;
	std::atomic<RequestID> req_id_;

public:
	void DefaultVmdkConfig(config::VmdkConfig& config, uint64_t block_size) {
		config.SetVmId("vmdkid");
		config.SetVmdkId("vmdkid");
		config.SetBlockSize(block_size);
		config.ConfigureEncryption("aes128-gcm", "abcd");
		config.DisableCompression();
		config.ConfigureRamCache(1024);
		config.DisableNetworkTarget();
	}

	virtual void SetUp() {
		config::VmdkConfig config;
		DefaultVmdkConfig(config, kVmdkBlockSize);

		vmdkp = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize());
		EXPECT_NE(vmdkp, nullptr);

		auto lock = std::make_unique<LockHandler>();
		EXPECT_NE(lock, nullptr);

		auto unalign = std::make_unique<UnalignedHandler>();
		EXPECT_NE(unalign, nullptr);

		auto configp = vmdkp->GetJsonConfig();
		auto encrypt = std::make_unique<EncryptHandler>(configp);
		EXPECT_NE(encrypt, nullptr);

		auto multi_target = std::make_unique<MultiTargetHandler>(vmdkp.get(), configp);
		EXPECT_NE(multi_target, nullptr);

		vmdkp->RegisterRequestHandler(std::move(lock));
		vmdkp->RegisterRequestHandler(std::move(unalign));
		vmdkp->RegisterRequestHandler(std::move(encrypt));
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

TEST_F(EncryptHandlerTest, AlignedOverWrite) {
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

		auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
		read_fut.wait();
		auto read_bufp = std::move(read_fut.value());
		auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
		EXPECT_EQ(rc, 0);
	}

	FillRandomBuffer(write_a->Payload(), write_a->Size());
	{
		auto write_reqp = std::make_unique<Request>(NextRequestID(), vmdkp.get(),
			Request::Type::kWrite, write_a->Payload(), write_a->Size(),
			write_a->Size(), 0);
		auto write_fut = vmdkp->Write(write_reqp.get(), 1);
		write_fut.wait();
		EXPECT_EQ(write_fut.value(), 0);
		EXPECT_EQ(write_reqp->NumberOfRequestBlocks(), 1);

		auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
		read_fut.wait();
		auto read_bufp = std::move(read_fut.value());
		auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
		log_assert(rc == 0);
		EXPECT_EQ(rc, 0);
	}

	::memset(write_a->Payload(), 'B', write_a->Size());
	{
		auto write_reqp = std::make_unique<Request>(NextRequestID(), vmdkp.get(),
			Request::Type::kWrite, write_a->Payload(), write_a->Size(),
			write_a->Size(), 0);
		auto write_fut = vmdkp->Write(write_reqp.get(), 1);
		write_fut.wait();
		EXPECT_EQ(write_fut.value(), 0);
		EXPECT_EQ(write_reqp->NumberOfRequestBlocks(), 1);

		auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
		read_fut.wait();
		auto read_bufp = std::move(read_fut.value());
		auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
		EXPECT_EQ(rc, 0);
	}
}

TEST_F(EncryptHandlerTest, UnAlignedOverWrite) {
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

		auto read_fut = VmdkRead(0, 0, vmdkp->BlockSize());
		read_fut.wait();
		auto read_bufp = std::move(read_fut.value());

		auto rc = ::memcmp(read_bufp->Payload(), write_a->Payload(), read_bufp->Size());
		EXPECT_EQ(rc, 0);
	}

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

TEST_F(EncryptHandlerTest, BulkAlignedWrite) {
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
	bulk_write_fut.wait();
	EXPECT_EQ(bulk_write_fut.value(), 0);
}
