#include <vector>

#include "DaemonUtils.h"

namespace pio {

using namespace ::ondisk;

bool IsBlockSizeAlgined(uint64_t offset, size_t block_size) {
	return offset & (block_size - 1) ? false : true;
}

uint64_t AlignDownToBlockSize(uint64_t offset, size_t block_size) {
	return offset - (offset & (block_size - 1));
}

uint64_t AlignUpToBlockSize(uint64_t offset, size_t block_size) {
	return AlignDownToBlockSize(offset + block_size, block_size);
}

std::pair<BlockID, BlockID>
GetBlockIDs(Offset offset, size_t size, size_t block_shift) {
	BlockID s = offset >> block_shift;
	BlockID e = (offset + size - 1) >> block_shift;
	return std::make_pair(s, e);
}

/*
 * Count the number of 1 bits in 32 bit number
 * */
uint32_t PopCount(uint32_t x) {
	/*
	 * Algorithm copied from book Hacker's Delight
	 * */
	x = x - ((x >> 1) & 0x55555555);
	x = (x & 0x33333333) + ((x >> 2) & 0x33333333);
	x = (x + (x >> 4)) & 0x0F0F0F0F;
	x = x + (x >> 8);
	x = x + (x >> 16);
	return x & 0x0000003F;
}

void StringDelimAppend(std::string& result, const char delim,
		const std::initializer_list<std::string>& input) {
	size_t size = input.size();
	for (const auto& e : input) {
		size += e.size();
	}

	result.clear();
	result.reserve(size);
	for (const auto& e : input) {
		if (result.empty()) {
			result.append(e);
		} else {
			result.append(1, delim).append(e);
		}
	}
}

}