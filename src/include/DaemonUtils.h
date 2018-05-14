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

uint32_t PopCount(uint32_t number);

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

void StringDelimAppend(std::string& result, const char delim,
	const std::initializer_list<std::string>& input);
}