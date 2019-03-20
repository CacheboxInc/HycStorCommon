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
#include "DataReader.h"
#include "DataWriter.h"

using namespace pio;
using namespace ::ondisk;
using namespace ::hyc_thrift;
using namespace std::chrono_literals;

const size_t kVmdkBlockSize = 512;
const CheckPointID kCheckPoint = 1;

class TestDataWriterReader : public ::testing::Test {
protected:
	const CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdk;
	std::atomic<RequestID> req_id_;
	std::vector<std::unique_ptr<RamCacheHandler>> handlers_;
	RamCacheHandler* ram1p{};
	RamCacheHandler* ram2p{};

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

		vmdk = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize());

		auto ram1 = std::make_unique<RamCacheHandler>(vmdk->GetJsonConfig());
		auto ram2 = std::make_unique<RamCacheHandler>(vmdk->GetJsonConfig());

		ram1p = ram1.get();
		ram2p = ram2.get();

		handlers_.emplace_back(std::move(ram1));
		handlers_.emplace_back(std::move(ram2));

		req_id_.store(StorRpc_constants::kInvalidRequestID());
	}

	virtual void TearDown() {
		vmdk = nullptr;
	}

};

TEST_F(TestDataWriterReader, SimpleWriteBoth) {
	std::vector<RequestHandler*> write_dest;
	write_dest.emplace_back(ram1p);
	write_dest.emplace_back(ram2p);

	BlockID block = 1;
	int64_t offset = block * kVmdkBlockSize;
	int64_t size = 1 * kVmdkBlockSize;
	auto io_buf = folly::IOBuf::create(size);
	auto bufferp = io_buf->writableData();
	::memset(bufferp, 'A', size);

	DataWriter writer(write_dest.begin(), write_dest.end(), vmdk.get(),
		kCheckPoint, bufferp, offset, size);

	auto rc = writer.CreateRequest();
	EXPECT_EQ(rc, 0);

	{
		/*
		 * Parallely write a block to both ram1 and ram2
		 */
		auto f = writer.Start();
		f.wait(1s);
		EXPECT_TRUE(f.isReady());
		EXPECT_EQ(f.value(), 0);
	}

	{
		/* verify data is present in ram1 */
		EXPECT_EQ(ram1p->Cache(kCheckPoint)->Size(), 1);

		std::vector<RequestHandler*> read_source;
		read_source.emplace_back(ram1p);

		auto read_io_buf = folly::IOBuf::create(size);
		auto read_bufferp = read_io_buf->writableData();

		DataReader reader(read_source.begin(), read_source.end(), vmdk.get(),
			kCheckPoint, read_bufferp, offset, size);
		auto f = reader.Start();
		f.wait(1s);
		EXPECT_TRUE(f.isReady());
		EXPECT_EQ(f.value(), 0);

		EXPECT_EQ(std::memcmp(bufferp, read_bufferp, size), 0);
	}

	{
		/* verify data is present in ram2 */
		EXPECT_EQ(ram2p->Cache(kCheckPoint)->Size(), 1);

		std::vector<RequestHandler*> read_source;
		read_source.emplace_back(ram2p);

		auto read_io_buf = folly::IOBuf::create(size);
		auto read_bufferp = read_io_buf->writableData();

		DataReader reader(read_source.begin(), read_source.end(), vmdk.get(),
			kCheckPoint, read_bufferp, offset, size);
		auto f = reader.Start();
		f.wait(1s);
		EXPECT_TRUE(f.isReady());
		EXPECT_EQ(f.value(), 0);

		EXPECT_EQ(std::memcmp(bufferp, read_bufferp, size), 0);
	}

	{
		/*
		 * Verify data read from both ram1/ram2 completes successfully
		 */
		std::vector<RequestHandler*> read_source;
		read_source.emplace_back(ram1p);
		read_source.emplace_back(ram2p);

		auto read_io_buf = folly::IOBuf::create(size);
		auto read_bufferp = read_io_buf->writableData();

		DataReader reader(read_source.begin(), read_source.end(), vmdk.get(),
			kCheckPoint, read_bufferp, offset, size);
		auto f = reader.Start();
		f.wait(1s);
		EXPECT_TRUE(f.isReady());
		EXPECT_EQ(f.value(), 0);

		EXPECT_EQ(std::memcmp(bufferp, read_bufferp, size), 0);
	}
}

TEST_F(TestDataWriterReader, ReadMiss) {
	{
		/* write block 1 in ram1 */
		std::vector<RequestHandler*> write_dest;
		write_dest.emplace_back(ram1p);

		BlockID block = 1;
		int64_t offset = block * kVmdkBlockSize;
		int64_t size = 1 * kVmdkBlockSize;
		auto io_buf = folly::IOBuf::create(size);
		auto bufferp = io_buf->writableData();
		::memset(bufferp, 'A', size);

		DataWriter writer(write_dest.begin(), write_dest.end(), vmdk.get(),
			kCheckPoint, bufferp, offset, size);

		auto rc = writer.CreateRequest();
		EXPECT_EQ(rc, 0);
		auto f = writer.Start();
		f.wait(1s);
		EXPECT_TRUE(f.isReady());
		EXPECT_EQ(f.value(), 0);
	}

	{
		/* write block 2 in ram2 */
		std::vector<RequestHandler*> write_dest;
		write_dest.emplace_back(ram2p);

		BlockID block = 2;
		int64_t offset = block * kVmdkBlockSize;
		int64_t size = 1 * kVmdkBlockSize;
		auto io_buf = folly::IOBuf::create(size);
		auto bufferp = io_buf->writableData();
		::memset(bufferp, 'B', size);

		DataWriter writer(write_dest.begin(), write_dest.end(), vmdk.get(),
			kCheckPoint, bufferp, offset, size);

		auto rc = writer.CreateRequest();
		EXPECT_EQ(rc, 0);
		auto f = writer.Start();
		f.wait(1s);
		EXPECT_TRUE(f.isReady());
		EXPECT_EQ(f.value(), 0);
	}

	/* ram1 and ram2 both have single block now */
	EXPECT_EQ(ram1p->Cache(kCheckPoint)->Size(), 1);
	EXPECT_EQ(ram2p->Cache(kCheckPoint)->Size(), 1);

	{
		/*
		 * Verify data read from both ram1/ram2 completes successfully
		 */
		std::vector<RequestHandler*> read_source;
		read_source.emplace_back(ram1p);
		read_source.emplace_back(ram2p);

		BlockID block = 1;
		int64_t offset = block * kVmdkBlockSize;
		int64_t size = 2 * kVmdkBlockSize;
		auto read_io_buf = folly::IOBuf::create(size);
		auto read_bufferp = read_io_buf->writableData();

		DataReader reader(read_source.begin(), read_source.end(), vmdk.get(),
			kCheckPoint, read_bufferp, offset, size);
		auto f = reader.Start();
		f.wait(1s);
		EXPECT_TRUE(f.isReady());
		EXPECT_EQ(f.value(), 0);
		EXPECT_EQ(reader.GetStatus(), 0);

		auto bp = reinterpret_cast<char*>(read_bufferp);
		for (size_t i = 0; i < static_cast<size_t>(size); ++i) {
			if (i < kVmdkBlockSize) {
				EXPECT_EQ(bp[i], 'A');
			} else {
				EXPECT_EQ(bp[i], 'B');
			}
		}
	}
}

TEST_F(TestDataWriterReader, ReadError) {
	std::vector<RequestHandler*> read_source;
	read_source.emplace_back(ram1p);
	read_source.emplace_back(ram2p);

	BlockID block = 1;
	int64_t offset = block * kVmdkBlockSize;
	int64_t size = 2 * kVmdkBlockSize;
	auto read_io_buf = folly::IOBuf::create(size);
	auto read_bufferp = read_io_buf->writableData();

	DataReader reader(read_source.begin(), read_source.end(), vmdk.get(),
			kCheckPoint, read_bufferp, offset, size);
	auto f = reader.Start();
	f.wait(1s);
	EXPECT_TRUE(f.isReady());
	EXPECT_NE(f.value(), 0);
}
