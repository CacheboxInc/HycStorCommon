#include <numeric>
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

	const auto& stats = traverser.GetStats();
	EXPECT_EQ(stats.blocks_total, 0);
	EXPECT_EQ(stats.blocks_pending, 0);
	EXPECT_EQ(stats.blocks_traserved, 0);
	EXPECT_EQ(stats.blocks_optimized, 0);
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

	const auto& stats = traverser.GetStats();
	EXPECT_EQ(stats.blocks_total, 0);
	EXPECT_EQ(stats.blocks_pending, 0);
	EXPECT_EQ(stats.blocks_traserved, 0);
	EXPECT_EQ(stats.blocks_optimized, 0);
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

	{
		const auto& stats = traverser.GetStats();
		EXPECT_EQ(stats.blocks_total, kNumberOfModifiedBlocks);
		EXPECT_EQ(stats.blocks_pending, kNumberOfModifiedBlocks);
		EXPECT_EQ(stats.blocks_traserved, 0);
		EXPECT_EQ(stats.blocks_optimized, 0);
	}

	auto [id, block, count] = traverser.MergeConsecutiveBlocks();
	(void) id;
	EXPECT_TRUE(traverser.IsComplete());
	EXPECT_EQ(block, 0);
	EXPECT_EQ(count, kNumberOfModifiedBlocks);

	{
		const auto& stats = traverser.GetStats();
		EXPECT_EQ(stats.blocks_total, kNumberOfModifiedBlocks);
		EXPECT_EQ(stats.blocks_pending, 0);
		EXPECT_EQ(stats.blocks_traserved, kNumberOfModifiedBlocks);
		EXPECT_EQ(stats.blocks_optimized, 0);
	}
}

TEST(CheckPointUnionTraverserTest, CheckPointAlternateEmpty) {
	BlockID last_modified = 0;
	std::map<CheckPointID, std::unordered_set<BlockID>> map_blocks_modified;
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

	{
		/* ensure all blocks are consecutive starting with 1 */
		auto it = std::adjacent_find(all_modified_blocks.begin(),
			all_modified_blocks.end(),
			[] (const auto& curr, const auto& next) {
				EXPECT_EQ(curr + 1, next);
				return not (curr + 1 == next);
			}
		);
		EXPECT_EQ(it, all_modified_blocks.end());
		EXPECT_EQ(*all_modified_blocks.begin(), 1);
		EXPECT_EQ(*all_modified_blocks.rbegin(), last_modified);
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
	{
		const auto& stats = traverser.GetStats();
		EXPECT_EQ(stats.blocks_total, last_modified);
		EXPECT_EQ(stats.blocks_pending, last_modified);
		EXPECT_EQ(stats.blocks_traserved, 0);
		EXPECT_EQ(stats.blocks_optimized, 0);
	}

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

	{
		const auto& stats = traverser.GetStats();
		EXPECT_EQ(stats.blocks_total, last_modified);
		EXPECT_EQ(stats.blocks_pending, 0);
		EXPECT_EQ(stats.blocks_traserved, last_modified);
		EXPECT_EQ(stats.blocks_optimized, 0);
	}
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
	{
		const auto& stats = traverser.GetStats();
		EXPECT_EQ(stats.blocks_total, kNumberOfModifiedBlocks * kMaxCkptId);
		EXPECT_EQ(stats.blocks_pending, stats.blocks_total);
		EXPECT_EQ(stats.blocks_traserved, 0);
		EXPECT_EQ(stats.blocks_optimized, 0);
	}

	std::unordered_set<BlockID> all_modified_blocks(modified_blocks);
	uint64_t nblocks = 0;
	while (not traverser.IsComplete()) {
		auto [ckpt, block, count] = traverser.MergeConsecutiveBlocks();
		nblocks += count;
		for (BlockID b : iter::Range(block, block + count)) {
			auto it = all_modified_blocks.find(b);
			EXPECT_NE(it, all_modified_blocks.end());

			all_modified_blocks.erase(it);
		}
		EXPECT_EQ(ckpt, kMaxCkptId);

		if (not traverser.IsComplete()) {
			const auto& stats = traverser.GetStats();
			EXPECT_EQ(stats.blocks_total, kNumberOfModifiedBlocks * kMaxCkptId);
			EXPECT_EQ(stats.blocks_pending, stats.blocks_total - nblocks);
			EXPECT_EQ(stats.blocks_traserved, nblocks);
			EXPECT_EQ(stats.blocks_optimized, 0);
		}
	}
	EXPECT_TRUE(all_modified_blocks.empty());

	{
		const auto& stats = traverser.GetStats();
		EXPECT_EQ(stats.blocks_total, kNumberOfModifiedBlocks * kMaxCkptId);
		EXPECT_EQ(stats.blocks_pending, 0);
		EXPECT_EQ(stats.blocks_traserved, kNumberOfModifiedBlocks);
		EXPECT_EQ(stats.blocks_optimized, (kMaxCkptId - 1) * kNumberOfModifiedBlocks);
	}
}

TEST(CheckPointUnionTraverserTest, CheckPointExpectedResult) {
	const BlockID kLargestBlockId = 8;
	const CheckPointID kMaxCkptId = 4;
	const std::map<CheckPointID, std::unordered_set<BlockID>> ckpt_blocks = {
		{1, {0, 1, 2, 3, 4, 5, 6, 7, 8}},
		{2, {0, 2, 4, 6, 8}},
		{3, {0, 1, 4, 5, 8}},
		{4, {0, 1, 2, 3, 8}},
	};
	const uint64_t kTotalBlocks = std::accumulate(
		ckpt_blocks.begin(),
		ckpt_blocks.end(),
		0,
		[] (uint64_t init, auto& entry) { return init + entry.second.size(); }
	);

	const std::map<BlockID, CheckPointID> expected = {
		{0, 4}, {1, 4}, {2, 4},  {3, 4}, {8, 4},
		{4, 3}, {5, 3},
		{6, 2},
		{7, 1},
	};
	const uint64_t kTotalTraversed = expected.size();

	const std::map<CheckPointID, uint64_t> not_visited = {
		{1, 8}, /* 8 blocks from CheckPointID==1 will not be visited */
		{2, 4},
		{3, 3},
		{4, 0}
	};
	const uint64_t kTotalOptimized = std::accumulate(
		not_visited.begin(),
		not_visited.end(),
		0,
		[] (uint64_t init, auto& entry) { return init + entry.second; }
	);

	[[maybe_unused]] size_t modified_blocks_count = 0;

	std::vector<std::unique_ptr<CheckPoint>> check_points;
	CheckPointPtrVec ckpt_ptrs;

	for (const auto& ckpt_block : ckpt_blocks) {
		auto check_point = std::make_unique<CheckPoint>(kVmdkId, ckpt_block.first);
		auto ckptp = check_point.get();
		check_points.emplace_back(std::move(check_point));
		ckpt_ptrs.emplace_back(ckptp);
		ckptp->SetModifiedBlocks(ckpt_block.second);
		modified_blocks_count = ckpt_block.second.size();
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

			auto expected_optimized = 0;
			for (auto c = std::max(ckpt_id-1, static_cast<CheckPointID>(1));
					c < kMaxCkptId;
					++c) {
				auto it = not_visited.find(c);
				ASSERT_NE(it, not_visited.end());
				expected_optimized += it->second;
			}
			const auto& stats = traverser.GetStats();
			EXPECT_LE(stats.blocks_optimized, expected_optimized);
			EXPECT_GE(kTotalOptimized, stats.blocks_optimized);
		}

		{
			const auto& stats = traverser.GetStats();
			EXPECT_EQ(stats.blocks_total, kTotalBlocks);
			EXPECT_EQ(stats.blocks_pending, 0);
			EXPECT_EQ(stats.blocks_traserved, kTotalTraversed);
			EXPECT_EQ(stats.blocks_optimized, kTotalOptimized);
		}

		for (BlockID b : iter::Range(static_cast<BlockID>(0), kLargestBlockId+1)) {
			auto it = found_blocks.find(b);
			EXPECT_NE(it, found_blocks.end());
			found_blocks.erase(it);
		}
		EXPECT_TRUE(found_blocks.empty());
	}
}
