#include <mutex>
#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "Singleton.h"

using namespace pio;

TEST(SingletonTests, Basic) {
	struct Foo {
		Foo(int value) : value_(value) {

		}

		int value_;
	};

	SingletonHolder<Foo>::CreateInstance(10);
	const auto fp1 = SingletonHolder<Foo>::GetInstance();
	const auto fp2 = SingletonHolder<Foo>::GetInstance();

	SingletonHolder<Foo>::CreateInstance(20);
	const auto fp3 = SingletonHolder<Foo>::GetInstance();
	const auto fp4 = SingletonHolder<Foo>::GetInstance();

	EXPECT_EQ(fp1, fp2);
	EXPECT_EQ(fp1->value_, 10);
	EXPECT_EQ(fp2->value_, 10);

	EXPECT_EQ(fp3, fp4);
	EXPECT_EQ(fp3->value_, 20);
	EXPECT_EQ(fp4->value_, 20);

	EXPECT_NE(fp1, fp3);
	EXPECT_NE(fp2, fp4);

	SingletonHolder<Foo>::DestroyInstance();
	const auto fp5 = SingletonHolder<Foo>::GetInstance();
	EXPECT_FALSE(fp5);

	EXPECT_EQ(fp1, fp2);
	EXPECT_EQ(fp1->value_, 10);
	EXPECT_EQ(fp2->value_, 10);

	EXPECT_EQ(fp3, fp4);
	EXPECT_EQ(fp3->value_, 20);
	EXPECT_EQ(fp4->value_, 20);

	EXPECT_NE(fp1, fp3);
	EXPECT_NE(fp2, fp4);
}