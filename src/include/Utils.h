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
}