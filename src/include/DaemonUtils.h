#pragma once

#include <utility>
#include <algorithm>

#include "IDs.h"
#include "gen-cpp2/MetaData_types.h"

namespace pio {

bool IsBlockSizeAlgined(uint64_t offset, size_t block_size);
uint64_t AlignDownToBlockSize(uint64_t offset, size_t block_size);
uint64_t AlignUpToBlockSize(uint64_t offset, size_t block_size);
std::pair<::ondisk::BlockID, ::ondisk::BlockID>
GetBlockIDs(Offset offset, size_t size, size_t block_shift);
std::pair<uint64_t, uint64_t> BlockFirstLastOffset(::ondisk::BlockID block,
	size_t block_shift);

uint32_t PopCount(uint32_t number);
void StringDelimAppend(std::string& result, const char delim,
	const std::initializer_list<std::string>& input);

template <typename T>
void MoveLastElements(std::vector<T>& dst, std::vector<T>& src, size_t tomove) {
	if (src.size() < tomove) {
		tomove = src.size();
	}

	auto eit = src.end();
	auto sit = std::prev(eit, tomove);
	std::move(sit, eit, std::back_inserter(dst));
	src.erase(sit, eit);
}

template <typename ForwardIt, typename T, typename Compare = std::less<>>
ForwardIt BinarySearch(ForwardIt first, ForwardIt last, const T& value,
		Compare comp = {}) {
    first = std::lower_bound(first, last, value, comp);
    return first != last and !comp(*first, value) ? first : last;
}

template <typename T>
std::vector<std::pair<T, uint16_t>>
MergeConsecutive(std::vector<T>& numbers, const uint16_t max) {
	/* remove duplicates */
	std::sort(numbers.begin(), numbers.end());
	auto delit = std::unique(numbers.begin(), numbers.end());
	numbers.erase(delit, numbers.end());

	std::vector<std::pair<T, uint16_t>> result;
	auto it = numbers.begin();
	auto eit = numbers.end();
	while (it != eit) {
		const auto& start = *it;
		auto i2 = std::adjacent_find(it, eit, [&] (auto& first, auto& next) {
			return first + 1 != next or next - start >= max;
		});
		auto c = std::distance(it, i2);
		if (i2 == eit) {
			result.emplace_back(start, c);
			break;
		}
		result.emplace_back(start, c+1);
		it = ++i2;
	}
	return result;
}

namespace iter {
template <typename T>
class Range {
public:
	class Iterator {
	public:
		using iterator_category = std::forward_iterator_tag;
		using value_type = T;
		using difference_type = std::ptrdiff_t;
		using pointer = value_type*;
		using reference = value_type;

		constexpr Iterator(T start, T end, T step) noexcept :
				start_(start), end_(end), step_(step) {
			if (step_ >= 0 and start_ > end_) {
				start_ = end_;
			} else if (step_ <= 0 and end_ > start_) {
				start_ = end_;
			}
		}

		constexpr T operator*() const noexcept {
			return start_;
		}

		Iterator& operator++() noexcept {
			if (step_ > 0) {
				if (start_ + step_ <= end_) {
					start_ += step_;
				} else {
					start_ = end_;
				}
			} else {
				if (start_ + step_ >= end_) {
					start_ += step_;
				} else {
					start_ = end_;
				}
			}
			return *this;
		}

		Iterator& operator++(int) noexcept {
			auto copy = *this;
			++*this;
			return copy;
		}

		bool operator==(const Iterator& rhs) const noexcept {
			return start_ == rhs.start_ and end_ == rhs.end_;
		}

		bool operator!=(const Iterator& rhs) const noexcept {
			return not (*this == rhs);
		}
	private:
		T start_;
		const T end_;
		const T step_;
	};

public:
	using iterator = Iterator;

	constexpr Range(T start, T end, T step) noexcept :
			start_(start), end_(end), step_(step) {
	}

	constexpr Range(T start, T end) noexcept :
			start_(start), end_(end) {
	}

	constexpr Iterator begin() const noexcept {
		return {start_, end_, step_};
	}

	constexpr Iterator end() const noexcept {
		return {end_, end_, step_};
	}
private:
	const T start_{};
	const T end_{};
	const T step_{1};
};
}
}
