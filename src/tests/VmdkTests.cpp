#include <gtest/gtest.h>
#include <glog/logging.h>

#include "Vmdk.h"
#include "JsonConfig.h"
#include "ConfigConsts.h"

using namespace pio;

static void DefaultVmdkConfig(config::JsonConfig& config, uint64_t block_size) {
	config.SetKey(VmConfig::kVmID, "vmid");
	config.SetKey(VmdkConfig::kVmdkID, "vmdkid");
	config.SetKey(VmdkConfig::kBlockSize, block_size);
}

TEST(ActiveVmdkTest, Constructor_Exception) {
	for (auto block_size = 513; block_size < 1024; ++block_size) {
		auto config = std::make_unique<config::JsonConfig>();
		DefaultVmdkConfig(*config, block_size);

		/* block_size must be power of 2 */
		EXPECT_THROW(
			ActiveVmdk vmdk(nullptr, 1, "1", std::move(config)),
			std::invalid_argument);
	}

	for (auto i = 0; i < 30; ++i) {
		auto config = std::make_unique<config::JsonConfig>();
		DefaultVmdkConfig(*config, 1ull << i);
		ActiveVmdk vmdk(nullptr, 1, "1", std::move(config));
	}
}