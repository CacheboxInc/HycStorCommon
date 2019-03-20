#include "Vmdk.h"
#include "CheckPointTraverser.h"

namespace pio {

CheckPointUnionTraverser::CheckPointUnionTraverser(const size_t merge_factor) noexcept :
		check_points_iter_(check_points_.rbegin()),
		check_points_end_(check_points_.rend()),
		traversing_it_(bitmap_traversing_.begin()),
		traversing_end_(bitmap_traversing_.end()),
		kMaxMerge(merge_factor) {
}

int CheckPointUnionTraverser::SetCheckPoints(CheckPointPtrVec check_points) noexcept {
	check_points.swap(check_points_);
	SortCheckPoints();
	return Begin();
}

void CheckPointUnionTraverser::SortCheckPoints() noexcept {
	struct {
		bool operator () (const CheckPoint* lp, const CheckPoint* rp) const {
			return lp->ID() < rp->ID();
		}
	} Less;
	if (not std::is_sorted(check_points_.begin(), check_points_.end(), Less)) {
		std::sort(check_points_.begin(), check_points_.end(), Less);
	}
}

bool CheckPointUnionTraverser::IsComplete() const noexcept {
	return check_points_iter_ == check_points_end_;
}

int CheckPointUnionTraverser::Begin() noexcept {
	check_points_iter_ = check_points_.rbegin();
	check_points_end_ = check_points_.rend();
	bool more;
	auto rc = InitializeCheckPoint(&more);
	if (pio_unlikely(rc < 0 or not more)) {
		check_points_iter_ = check_points_end_;
		return rc;
	}
	return 0;
}

void CheckPointUnionTraverser::InitializeNextCheckPoint() noexcept { 
	if (check_points_iter_ == check_points_end_) {
		return;
	}

	++check_points_iter_;
	bool more;
	auto rc = InitializeCheckPoint(&more);
	if (pio_unlikely(rc < 0 or not more)) {
		check_points_iter_ = check_points_end_;
	}
}

int CheckPointUnionTraverser::InitializeCheckPoint(bool *has_more) noexcept {
	*has_more = true;

	bitmap_traversed_ |= bitmap_traversing_;

	for (; check_points_iter_ != check_points_end_; ++check_points_iter_) {
		const auto& bitmap = (*check_points_iter_)->GetRoaringBitMap();
		if (pio_unlikely(bitmap.cardinality() == 0)) {
			continue;
		}

		try {
			bitmap_traversing_ = bitmap - bitmap_traversed_;
		} catch (const std::runtime_error& e) {
			LOG(ERROR) << "CBT difference failed " << e.what();
			*has_more = false;
			return -EINVAL;
		}
		if (bitmap_traversing_.cardinality() != 0) {
			break;
		}
	}

	if (pio_unlikely(check_points_iter_ == check_points_end_)) {
		*has_more = false;
		return 0;
	}

	log_assert(bitmap_traversing_.cardinality() != 0);
	traversing_it_ = bitmap_traversing_.begin();
	traversing_end_ = bitmap_traversing_.end();
	return 0;
}

std::tuple<CheckPointID, BlockID, BlockCount>
CheckPointUnionTraverser::MergeConsecutiveBlocks() noexcept {
	if (pio_unlikely(IsComplete())) {
		return {0, 0, 0};
	}
	log_assert(traversing_it_ != traversing_end_);
	if (pio_unlikely(traversing_it_ == traversing_end_)) {
		LOG(ERROR) << "Fatal error: bitmap iterators not initialized correctly";
		return {0, 0, 0};
	}

	CheckPointID ckpt_id = (*check_points_iter_)->ID();

	BlockID block_start = *traversing_it_;
	BlockID block_end = block_start;
	traversing_it_ = std::adjacent_find(traversing_it_, traversing_end_,
		[&] (const auto& curr, const auto& next) {
			if (curr + 1 == next and
					(not kMaxMerge or next - block_start < kMaxMerge)) {
				block_end = next;
				/* std::adjacent_find should continue */
				return false;
			}
			return true;
		}
	);

	if (traversing_it_ == traversing_end_) {
		InitializeNextCheckPoint();
		log_assert(IsComplete() or traversing_it_ != traversing_end_);
	} else {
		++traversing_it_;
	}
	return {ckpt_id, block_start, block_end - block_start + 1};
}

}
