#include <vector>

#include <roaring/roaring.hh>

#include "DataMoverCommonTypes.h"
#include "gen-cpp2/MetaData_types.h"

namespace pio {

class CheckPoint;

class CheckPointUnionTraverser {
public:
	struct Stats {
		uint64_t blocks_total{0};
		uint64_t blocks_pending{0};
		uint64_t blocks_traserved{0};
		uint64_t blocks_optimized{0};
		::ondisk::CheckPointID cbt_id{0};
	};
public:
	CheckPointUnionTraverser(const CheckPointUnionTraverser&) = delete;
	CheckPointUnionTraverser(const CheckPointUnionTraverser&&) = delete;
	CheckPointUnionTraverser& operator = (const CheckPointUnionTraverser&) = delete;
	CheckPointUnionTraverser& operator = (CheckPointUnionTraverser&&) = delete;

	CheckPointUnionTraverser(const size_t merge_factor) noexcept;
	int SetCheckPoints(CheckPointPtrVec check_points) noexcept;

	bool IsComplete() const noexcept;
	const Stats& GetStats() const noexcept;

	std::tuple<::ondisk::CheckPointID, ::ondisk::BlockID, BlockCount>
	MergeConsecutiveBlocks() noexcept;
private:
	int Begin() noexcept;
	void SortCheckPoints() noexcept;
	void InitializeNextCheckPoint() noexcept;
	int InitializeCheckPoint(bool *has_more) noexcept;
	uint64_t BlocksPending(CheckPointPtrVec::reverse_iterator begin,
		CheckPointPtrVec::reverse_iterator end) const noexcept;

private:
	CheckPointPtrVec check_points_;
	CheckPointPtrVec::reverse_iterator check_points_iter_;
	CheckPointPtrVec::reverse_iterator check_points_end_;

	/*
	 * The class maintains a traversed bitmap to indicate blocks that are
	 * already seen. Initially the traversed bitmap is empty.  Additionally, we
	 * also maintain a bitmap called traversing which is bitmap we are iterating
	 * over at any moment.
	 *
	 * When traversing bitmap is iterated completely, we union traversed and
	 * traversing bitmap
	 *
	 * traversed |= traversing;
	 * traversing.clear();
	 *
	 * The traversing bitmap for next check point is computed as, difference
	 * between bitmap for checkpoint we want to traverse and bitmap we have
	 * already traversed
	 *
	 * traversing = next_checpoint_bitmap - traversed
	 */
	Roaring bitmap_traversed_{};

	Roaring bitmap_traversing_{};
	Roaring::const_iterator traversing_it_;
	Roaring::const_iterator traversing_end_;

	Stats stats_;

	const BlockCount kMaxMerge{256};
};

}
