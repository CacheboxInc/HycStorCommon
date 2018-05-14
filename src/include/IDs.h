#pragma once

#include <string>
#include <cstdint>

namespace pio {
using AeroClusterID = std::string;
using Offset = uint64_t;
using AeroClusterHandle = uint64_t;
static constexpr AeroClusterHandle kInvalidAeroClusterHandle = 0;
static constexpr AeroClusterHandle kValidAeroClusterHandle = 1;
}