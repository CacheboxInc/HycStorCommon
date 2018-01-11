#include <gtest/gtest.h>
#include <glog/logging.h>

#include "Vmdk.h"

using namespace pio;

TEST(ActiveVmdkTest, Constructor_Exception) {
	for (auto block_size = 513; block_size < 1024; ++block_size) {
		/* block_size must be power of 2 */
		EXPECT_THROW(
			ActiveVmdk vmdk(nullptr, "1", block_size),
			std::invalid_argument);
	}

	for (auto i = 0; i < 30; ++i) {
		ActiveVmdk vmdk(nullptr, "1", 1ull << i);
	}
}