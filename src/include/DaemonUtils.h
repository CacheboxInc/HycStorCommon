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
void MoveFirstElements(std::vector<T>& dst, std::vector<T>& src, size_t tomove) {
	if (src.size() < tomove) {
		tomove = src.size();
	}

	auto eit = src.begin();
	auto sit = std::next(eit, tomove);
	std::move(eit, sit, std::back_inserter(dst));
	src.erase(eit, sit);
}

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
void RemoveDuplicate(std::vector<T>& numbers) {
	auto begin = numbers.begin();
	auto end = numbers.end();
	if (not std::is_sorted(begin, end)) {
		std::sort(begin, end);
	}
	numbers.erase(std::unique(begin, end), end);
}

template <typename InputIt, typename Lambda,
	typename = typename std::enable_if_t<
		std::is_arithmetic_v<typename InputIt::value_type>
	>
>
InputIt MergeConsecutiveIf(InputIt it, InputIt eit, const uint16_t max, Lambda&& func) {
	while (it != eit) {
		const auto& start = *it;
		auto i2 = std::adjacent_find(it, eit, [&] (auto& first, auto& next) {
			return first + 1 != next or next - start >= max;
		});
		auto c = std::distance(it, i2);
		if (i2 == eit) {
			func(start, c);
			return eit;
		}

		if (not func(start, c+1)) {
			return ++i2;
		}
		it = ++i2;
	}
	return eit;
}

template <typename InputIt, typename OutputIt>
InputIt MergeConsecutive(InputIt it, InputIt eit, OutputIt oit, const uint16_t max) {
	return MergeConsecutiveIf(it, eit, max, [&oit] (auto start, auto count) mutable {
		*oit = std::make_pair(start, count);
		return true;
	});
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
