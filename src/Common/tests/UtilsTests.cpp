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