#include <mutex>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "RamCache.h"
#include "Request.h"
#include "Vmdk.h"
#include "JsonConfig.h"
#include "ConfigConsts.h"

using namespace pio;

const size_t kVmdkBlockSize = 8192;

static void DefaultVmdkConfig(config::JsonConfig& config, uint64_t block_size) {
	config.SetKey(VmConfig::kVmID, "vmid");
	config.SetKey(VmdkConfig::kVmdkID, "vmdkid");
	config.SetKey(VmdkConfig::kBlockSize, block_size);
}

TEST(RamCacheTest, DataVerify) {
	auto config = std::make_unique<config::JsonConfig>();
	DefaultVmdkConfig(*config, kVmdkBlockSize);
	ActiveVmdk vmdk(nullptr, 1, "1", std::move(config));
	RamCache cache;

	for (auto offset = 0, i = 0; i < 100; ++i, offset += vmdk.BlockSize()) {
		auto bufp = NewRequestBuffer(vmdk.BlockSize());
		auto payload = bufp->Payload();
		char c = 'A' + (i % 26);
		::memset(payload, c, bufp->Size());
		cache.Write(&vmdk, payload, offset);
	}

	auto cmp_bufp = NewRequestBuffer(vmdk.BlockSize());
	auto cmp_payload = cmp_bufp->Payload();
	auto read_bufp = NewRequestBuffer(vmdk.BlockSize());
	auto read_payload = read_bufp->Payload();

	for (auto offset = 0, i = 0; i < 100; ++i, offset += vmdk.BlockSize()) {
		char c = 'A' + (i % 26);
		::memset(cmp_payload, c, cmp_bufp->Size());

		cache.Read(&vmdk, read_payload, offset);

		auto rc = ::memcmp(read_payload, cmp_payload, cmp_bufp->Size());
		EXPECT_EQ(rc, 0);
	}
}

TEST(RamCacheTest, ReadMiss) {
	const Offset offset = 0;
	auto config = std::make_unique<config::JsonConfig>();
	DefaultVmdkConfig(*config, kVmdkBlockSize);
	ActiveVmdk vmdk(nullptr, 1, "1", std::move(config));
	RamCache cache;

	auto zero_bufp = NewRequestBuffer(vmdk.BlockSize());
	auto zpp = zero_bufp->Payload();
	::memset(zpp, 0, zero_bufp->Size());

	auto read_bufp = NewRequestBuffer(vmdk.BlockSize());
	auto read_payload = read_bufp->Payload();

	cache.Read(&vmdk, read_payload, offset);
	auto rc = ::memcmp(read_payload, zpp, read_bufp->Size());
	EXPECT_EQ(rc, 0);

	cache.Read(&vmdk, read_payload, offset);
	rc = ::memcmp(read_payload, zpp, read_bufp->Size());
	EXPECT_EQ(rc, 0);

	::memset(zpp, 'A', zero_bufp->Size());
	cache.Write(&vmdk, zpp, offset);

	cache.Read(&vmdk, read_payload, offset);
	rc = ::memcmp(read_payload, zpp, read_bufp->Size());
	EXPECT_EQ(rc, 0);
}

TEST(RamCacheTest, OverWrite) {
	auto config = std::make_unique<config::JsonConfig>();
	DefaultVmdkConfig(*config, kVmdkBlockSize);
	ActiveVmdk vmdk(nullptr, 1, "1", std::move(config));
	RamCache cache;

	auto write_bufp = NewRequestBuffer(vmdk.BlockSize());
	auto wdp = write_bufp->Payload();

	auto read_bufp = NewRequestBuffer(vmdk.BlockSize());
	auto rdp = read_bufp->Payload();

	for (auto offset = 0, i = 0; i < 100; ++i) {
		char c = 'A' + (i % 26);
		::memset(wdp, c, write_bufp->Size());
		cache.Write(&vmdk, wdp, offset);

		cache.Read(&vmdk, rdp, offset);

		auto rc = ::memcmp(wdp, rdp, read_bufp->Size());
		EXPECT_EQ(rc, 0);
	}
}