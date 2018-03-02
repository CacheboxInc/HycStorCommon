#include <gtest/gtest.h>
#include <glog/logging.h>

#include "JsonConfig.h"

using namespace hyc;
using namespace hyc::config;

TEST(JsonConfigTest, KeySetGet) {
	const std::string kKey{"vmid"};
	const std::string kValue{"1"};

	JsonConfig config;

	std::string vmid;
	auto is_set = config.GetKey(kKey, vmid);
	EXPECT_FALSE(is_set);

	config.SetKey(kKey, kValue);
	is_set = config.GetKey(kKey, vmid);
	EXPECT_TRUE(is_set);
	EXPECT_EQ(vmid, kValue);
}

TEST(JsonConfigTest, Serialize) {
	const std::string kKey{"vmid"};
	const std::string kValue{"1"};

	JsonConfig config;
	config.SetKey(kKey, kValue);

	auto ser = config.Serialize();

	{
		JsonConfig c;
		c.Deserialize(ser);

		std::string vmid;
		auto is_set = config.GetKey(kKey, vmid);

		EXPECT_TRUE(is_set);
		EXPECT_EQ(vmid, kValue);
	}
}
