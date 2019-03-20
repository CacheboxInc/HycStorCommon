#include <gtest/gtest.h>

#include "Vmdk.h"
#include "CheckPointTraverser.h"

using namespace pio;
using namespace ondisk;

static const std::string kVmdkId = "1";

TEST(CheckPointUnionTraverserTest, NoCheckPoint) {
	CheckPointUnionTraverser traverser(4);
	EXPECT_TRUE(traverser.IsComplete());

	auto [ckpt, block, count] = traverser.MergeConsecutiveBlocks();
	EXPECT_EQ(count, 0);
	(void) ckpt;
	(void) block;
}

TEST(CheckPointUnionTraverserTest, CheckPointAllEmpty) {
	CheckPointUnionTraverser traverser(4);
	std::vector<std::unique_ptr<CheckPoint>> empty_check_points;
	CheckPointPtrVec check_points;
	for (auto i : iter::Range(1, 10)) {
		auto check_point = std::make_unique<CheckPoint>(kVmdkId, i);
		check_points.emplace_back(check_point.get());
		empty_check_points.emplace_back(std::move(check_point));
	}

	traverser.SetCheckPoints(check_points);
	EXPECT_TRUE(traverser.IsComplete());
}

TEST(CheckPointUnionTraverserTest, CheckPointEmptyFirstAndLast) {
	const int kNumberOfModifiedBlocks = 4;
	std::vector<std::unique_ptr<CheckPoint>> check_points;
	CheckPointPtrVec ckpt_ptrs;

	{
		auto check_point = std::make_unique<CheckPoint>(kVmdkId, 1);
		ckpt_ptrs.emplace_back(check_point.get());
		check_points.emplace_back(std::move(check_point));
	}
	{
		std::unordered_set<BlockID> blocks;
		for (auto block : iter::Range(0, kNumberOfModifiedBlocks)) {
			blocks.emplace(block);
		}
		auto check_point = std::make_unique<CheckPoint>(kVmdkId, 2);
		check_point->SetModifiedBlocks(blocks);
		ckpt_ptrs.emplace_back(check_point.get());
		check_points.emplace_back(std::move(check_point));
	}
	{
		auto check_point = std::make_unique<CheckPoint>(kVmdkId, 3);
		ckpt_ptrs.emplace_back(check_point.get());
		check_points.emplace_back(std::move(check_point));
	}

	CheckPointUnionTraverser traverser(kNumberOfModifiedBlocks);
	traverser.SetCheckPoints(ckpt_ptrs);
	EXPECT_FALSE(traverser.IsComplete());

	auto [id, block, count] = traverser.MergeConsecutiveBlocks();
	(void) id;
	EXPECT_TRUE(traverser.IsComplete());
	EXPECT_EQ(block, 0);
	EXPECT_EQ(count, kNumberOfModifiedBlocks);
}

TEST(CheckPointUnionTraverserTest, CheckPointAlternateEmpty) {
	std::map<CheckPointID, std::unordered_set<BlockID>> map_blocks_modified;
	BlockID last_modified = 0;
	std::set<BlockID> all_modified_blocks;

	for (auto ckpt_id : iter::Range(1, 10)) {
		if (ckpt_id % 2 != 0) {
			map_blocks_modified.insert(std::make_pair(ckpt_id, std::unordered_set<BlockID>()));
			continue;
		}

		const uint64_t s = last_modified + 1;
		const uint64_t e = last_modified + 10;
		std::unordered_set<BlockID> modified_blocks;
		for (auto block : iter::Range(s, e)) {
			modified_blocks.emplace(block);
			auto it = all_modified_blocks.find(block);
			EXPECT_EQ(it, all_modified_blocks.end());
			all_modified_blocks.emplace(block);
			last_modified = block;
		}
		map_blocks_modified.insert(std::make_pair(ckpt_id, std::move(modified_blocks)));
	}

	std::vector<std::unique_ptr<CheckPoint>> check_points;
	CheckPointPtrVec ckpt_ptrs;
	for (const auto& ckpt_blocks : map_blocks_modified) {
		auto check_point = std::make_unique<CheckPoint>(kVmdkId, ckpt_blocks.first);
		auto ckptp = check_point.get();
		check_points.emplace_back(std::move(check_point));
		ckpt_ptrs.emplace_back(ckptp);

		ckptp->SetModifiedBlocks(ckpt_blocks.second);
	}

	const int kNumberOfModifiedBlocks = 4;
	CheckPointUnionTraverser traverser(kNumberOfModifiedBlocks);
	traverser.SetCheckPoints(ckpt_ptrs);
	EXPECT_FALSE(traverser.IsComplete());

	while (not traverser.IsComplete()) {
		auto [ckpt, block, count] = traverser.MergeConsecutiveBlocks();
		EXPECT_NE(count, 0);
		EXPECT_NE(ckpt, 0);
		EXPECT_EQ(ckpt % 2, 0);

		for (BlockID b : iter::Range(block, block + count)) {
			auto it = all_modified_blocks.find(b);
			EXPECT_NE(it, all_modified_blocks.end());

			all_modified_blocks.erase(it);
		}
	}
	EXPECT_TRUE(all_modified_blocks.empty());
}

TEST(CheckPointUnionTraverserTest, CheckPointSameBlocksModified) {
	/*
	 * Same set of blocks modified in each check point
	 */
	const int kNumberOfModifiedBlocks = 10;
	const CheckPointID kMaxCkptId = 4;
	std::unordered_set<BlockID> modified_blocks;
	for (BlockID id : iter::Range(10, 20)) {
		modified_blocks.emplace(id);
	}

	std::vector<std::unique_ptr<CheckPoint>> check_points;
	CheckPointPtrVec ckpt_ptrs;
	for (CheckPointID id : iter::Range(static_cast<CheckPointID>(1), kMaxCkptId + 1)) {
		auto check_point = std::make_unique<CheckPoint>(kVmdkId, id);
		auto ckptp = check_point.get();
		check_points.emplace_back(std::move(check_point));
		ckpt_ptrs.emplace_back(ckptp);
		ckptp->SetModifiedBlocks(modified_blocks);
	}

	CheckPointUnionTraverser traverser(kNumberOfModifiedBlocks);
	traverser.SetCheckPoints(ckpt_ptrs);
	EXPECT_FALSE(traverser.IsComplete());

	std::unordered_set<BlockID> all_modified_blocks(modified_blocks);
	while (not traverser.IsComplete()) {
		auto [ckpt, block, count] = traverser.MergeConsecutiveBlocks();
		for (BlockID b : iter::Range(block, block + count)) {
			auto it = all_modified_blocks.find(b);
			EXPECT_NE(it, all_modified_blocks.end());

			all_modified_blocks.erase(it);
		}
		EXPECT_EQ(ckpt, kMaxCkptId);
	}
	EXPECT_TRUE(all_modified_blocks.empty());
}

TEST(CheckPointUnionTraverserTest, CheckPointExpectedResult) {
	const BlockID kLargestBlockId = 8;
	const std::map<CheckPointID, std::unordered_set<BlockID>> ckpt_blocks = {
		{1, {0, 1, 2, 3, 4, 5, 6, 7, 8}},
		{2, {0, 2, 4, 6, 8}},
		{3, {0, 1, 4, 5, 8}},
		{4, {0, 1, 2, 3, 8}},
	};

	const std::map<BlockID, CheckPointID> expected = {
		{0, 4}, {1, 4}, {2, 4},  {3, 4}, {8, 4},
		{4, 3}, {5, 3},
		{6, 2},
		{7, 1},
	};

	std::vector<std::unique_ptr<CheckPoint>> check_points;
	CheckPointPtrVec ckpt_ptrs;

	for (const auto& ckpt_block : ckpt_blocks) {
		auto check_point = std::make_unique<CheckPoint>(kVmdkId, ckpt_block.first);
		auto ckptp = check_point.get();
		check_points.emplace_back(std::move(check_point));
		ckpt_ptrs.emplace_back(ckptp);
		ckptp->SetModifiedBlocks(ckpt_block.second);
	}

	for (int merge_factor = 1; merge_factor <= 6; ++merge_factor) {
		std::unordered_set<BlockID> found_blocks;

		CheckPointUnionTraverser traverser(merge_factor);
		traverser.SetCheckPoints(ckpt_ptrs);
		while (not traverser.IsComplete()) {
			auto [ckpt_id, block, count] = traverser.MergeConsecutiveBlocks();
			EXPECT_LE(block, kLargestBlockId);
			for (auto b = block; b < block + count; ++b) {
				auto it = expected.find(b);
				EXPECT_NE(it, expected.end());
				EXPECT_EQ(it->second, ckpt_id);
				found_blocks.emplace(b);
			}
		}

		for (BlockID b : iter::Range(0, 9)) {
			auto it = found_blocks.find(b);
			EXPECT_NE(it, found_blocks.end());
			found_blocks.erase(it);
		}
		EXPECT_TRUE(found_blocks.empty());
	}
}
