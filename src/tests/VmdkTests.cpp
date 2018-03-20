#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "Vmdk.h"
#include "VmdkConfig.h"

using namespace pio;

static void DefaultVmdkConfig(config::VmdkConfig& config, uint64_t block_size) {
	config.SetVmId("vmid");
	config.SetVmdkId("vmdkid");
	config.SetBlockSize(block_size);
}

TEST(ActiveVmdkTest, Constructor_Exception) {
	for (auto block_size = 513; block_size < 1024; ++block_size) {
		config::VmdkConfig config;
		DefaultVmdkConfig(config, block_size);

		/* block_size must be power of 2 */
		EXPECT_THROW(
			ActiveVmdk vmdk(nullptr, 1, "1", config.Serialize()),
			std::invalid_argument);
	}

	for (auto i = 0; i < 30; ++i) {
		config::VmdkConfig config;
		DefaultVmdkConfig(config, 1ull << i);
		ActiveVmdk vmdk(nullptr, 1, "1", config.Serialize());
	}
}