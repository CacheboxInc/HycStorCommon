#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

int main(int argc, char* argv[]) {
	FLAGS_v = 2;
	FLAGS_logtostderr = 1;

	google::InitGoogleLogging(argv[0]);
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
