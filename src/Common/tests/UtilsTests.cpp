#include <vector>
#include <set>
#include <numeric>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "DaemonUtils.h"
#include "IDs.h"

using namespace pio;

TEST(UtilsTest, VectorUniquePtrMove) {
	const size_t kBlockSize = 4096u;
	const size_t kMaxBlocks = 50u;

	std::vector<std::unique_ptr<Offset>> offsets;

	for (auto block = 1u; block <= kMaxBlocks; ++block) {
		Offset offset = block * kBlockSize;
		auto r = std::make_unique<Offset>(offset);
		offsets.emplace_back(std::move(r));
	}

	EXPECT_EQ(offsets.size(), kMaxBlocks);

	std::set<Offset> ids;
	auto to_move = 11u;
	while (not offsets.empty()) {
		std::vector<std::unique_ptr<Offset>> dst;

		auto sz1 = offsets.size();
		pio::MoveLastElements(dst, offsets, to_move);
		auto sz2 = offsets.size();

		EXPECT_LT(sz2, sz1);

		if (not offsets.empty()) {
			EXPECT_EQ(sz2 + to_move, sz1);
		} else {
			EXPECT_GT(sz2 + to_move, sz1);
		}

		for (const auto& offset : dst) {
			auto it = ids.find(*offset);
			EXPECT_EQ(it, ids.end());

			ids.insert(*offset);
		}
	}

	EXPECT_EQ(ids.size(), kMaxBlocks);
	auto block = 1u;
	for (auto it = ids.begin(); it != ids.end(); ++it, ++block) {
		Offset offset = block * kBlockSize;
		EXPECT_EQ(*it, offset);
	}
}

TEST(UtilsTest, MoveErasesElemetsFromSource) {
	const auto kMaxElements = 20;
	for (auto i = 1; i < 30; ++i) {
		std::vector<uint32_t> src(kMaxElements);
		std::iota(src.begin(), src.end(), 0);

		std::vector<uint32_t> dst;
		MoveLastElements(dst, src, i);

		EXPECT_EQ(dst.size() + src.size(), kMaxElements);

		for (auto e : dst) {
			auto it = std::find(src.begin(), src.end(), e);
			EXPECT_TRUE(it == src.end());
		}
	}
}

TEST(RangeTest, PositiveDefaultStep) {
		auto r = pio::iter::Range(1, 10);
		std::vector<int> v1(std::begin(r), std::end(r));
		std::vector<int> v2(9);
		std::iota(v2.begin(), v2.end(), 1);
		EXPECT_EQ(v1, v2);
}

TEST(RangeTest, PositiveMultipleStep) {
	auto r = pio::iter::Range(0, 6, 2);
	std::vector<int> v1(r.begin(), r.end());
	std::vector<int> v2{0, 2, 4};
	EXPECT_EQ(v1, v2);
}

TEST(RangeTest, PositiveMultipleStepOddEnd) {
	auto r = pio::iter::Range(0, 7, 2);
	std::vector<int> v1(r.begin(), r.end());
	std::vector<int> v2{0, 2, 4, 6};
	EXPECT_EQ(v1, v2);
}

TEST(RangeTest, PositiveMultipleOddStep) {
	auto r = pio::iter::Range(1, 10, 3);
	std::vector<int> v1(r.begin(), r.end());
	std::vector<int> v2{1, 4, 7};
	EXPECT_EQ(v1, v2);
}

TEST(RangeTest, EmptyRange1) {
	auto r = pio::iter::Range(0, 0);
	EXPECT_EQ(r.begin(), r.end());
}

TEST(RangeTest, EmptyRange2) {
	auto r = pio::iter::Range(5, 0);
	std::vector<int> v(r.begin(), r.end());
	EXPECT_TRUE(v.empty());
}

TEST(RangeTest, NegativeSteps) {
	auto r = pio::iter::Range(0, -3, -1);
	std::vector<int> v1(r.begin(), r.end());
	std::vector<int> v2{0, -1, -2};
	EXPECT_EQ(v1, v2);
}

TEST(RangeTest, PositiveToNegative) {
	auto r = pio::iter::Range(5, -5, -3);
	std::vector<int> v1(r.begin(), r.end());
	std::vector<int> v2{5, 2, -1, -4};
	EXPECT_EQ(v1, v2);
}
