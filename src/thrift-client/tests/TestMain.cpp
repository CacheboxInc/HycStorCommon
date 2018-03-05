#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <folly/init/Init.h>

int main(int argc, char* argv[]) {
	FLAGS_v = 2;
	FLAGS_logtostderr = 1;

	::testing::InitGoogleTest(&argc, argv);
	folly::init(&argc, &argv);
	return RUN_ALL_TESTS();
}
