#pragma once

#include <utility>

#include "IDs.h"

namespace pio {

bool IsBlockSizeAlgined(uint64_t offset, size_t block_size);
uint64_t AlignDownToBlockSize(uint64_t offset, size_t block_size);
uint64_t AlignUpToBlockSize(uint64_t offset, size_t block_size);
std::pair<BlockID, BlockID>
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

void StringDelimAppend(std::string& result, const char delim,
	const std::initializer_list<std::string>& input);
}