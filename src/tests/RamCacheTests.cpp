#include <mutex>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "RamCache.h"
#include "Request.h"
#include "Vmdk.h"
#include "VmdkConfig.h"

using namespace pio;
using namespace ::ondisk;

const size_t kVmdkBlockSize = 8192;

static void DefaultVmdkConfig(config::VmdkConfig& config, uint64_t block_size) {
	config.SetVmId("vmid");
	config.SetVmdkId("vmdkid");
	config.SetBlockSize(block_size);
}

TEST(RamCacheTest, DataVerify) {
	config::VmdkConfig config;
	DefaultVmdkConfig(config, kVmdkBlockSize);
	ActiveVmdk vmdk(1, "1", nullptr, config.Serialize());
	RamCache cache;

	for (auto offset = 0, i = 0; i < 100; ++i, offset += vmdk.BlockSize()) {
		auto bufp = NewRequestBuffer(vmdk.BlockSize());
		auto payload = bufp->Payload();
		char c = 'A' + (i % 26);
		::memset(payload, c, bufp->Size());
		cache.Write(&vmdk, payload, offset, bufp->Size());
	}

	auto cmp_bufp = NewRequestBuffer(vmdk.BlockSize());
	auto cmp_payload = cmp_bufp->Payload();

	for (auto offset = 0, i = 0; i < 100; ++i, offset += vmdk.BlockSize()) {
		char c = 'A' + (i % 26);
		::memset(cmp_payload, c, cmp_bufp->Size());

		auto [read_bufp, found] = cache.Read(&vmdk, offset);
		EXPECT_TRUE(found);
		auto read_payload = read_bufp->Payload();
		auto rc = ::memcmp(read_payload, cmp_payload, cmp_bufp->Size());
		EXPECT_EQ(rc, 0);
	}
}

TEST(RamCacheTest, ReadMiss) {
	const Offset offset = 0;
	config::VmdkConfig config;
	DefaultVmdkConfig(config, kVmdkBlockSize);
	ActiveVmdk vmdk(1, "1", nullptr, config.Serialize());
	RamCache cache;

	auto zero_bufp = NewRequestBuffer(vmdk.BlockSize());
	auto zpp = zero_bufp->Payload();
	::memset(zpp, 0, zero_bufp->Size());

	{
		auto [read_bufp, found] = cache.Read(&vmdk, offset);
		EXPECT_FALSE(found);
		EXPECT_FALSE(read_bufp);
	}

	{
		auto [read_bufp, found] = cache.Read(&vmdk, offset);
		EXPECT_FALSE(found);
		EXPECT_FALSE(read_bufp);
	}

	::memset(zpp, 'A', zero_bufp->Size());
	cache.Write(&vmdk, zpp, offset, zero_bufp->Size());

	auto [read_bufp, found] = cache.Read(&vmdk, offset);
	EXPECT_TRUE(found);
	EXPECT_EQ(read_bufp->Size(), zero_bufp->Size());
	auto rc = ::memcmp(read_bufp->Payload(), zpp, read_bufp->Size());
	EXPECT_EQ(rc, 0);
}

TEST(RamCacheTest, OverWrite) {
	config::VmdkConfig config;
	DefaultVmdkConfig(config, kVmdkBlockSize);
	ActiveVmdk vmdk(1, "1", nullptr, config.Serialize());
	RamCache cache;

	auto write_bufp = NewRequestBuffer(vmdk.BlockSize());
	auto wdp = write_bufp->Payload();

	for (auto i = 0; i < 26; ++i) {
		char c = 'A' + (i % 26);
		::memset(wdp, c, write_bufp->Size());
		cache.Write(&vmdk, wdp, 0, write_bufp->Size());

		auto [read_bufp, found] = cache.Read(&vmdk, 0);
		EXPECT_TRUE(found);
		EXPECT_EQ(read_bufp->Size(), write_bufp->Size());

		auto rc = ::memcmp(wdp, read_bufp->Payload(), read_bufp->Size());
		EXPECT_EQ(rc, 0);
	}
}
