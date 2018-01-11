#pragma once

#include <string>
#include <cstdint>

namespace pio {
using RequestID = uint64_t;
using CheckPointID = uint64_t;
using SnapshotID = uint64_t;
using VmdkID = std::string;
using VmID = std::string;
using BlockID = uint64_t;
using Offset = uint64_t;
}