#pragma once

#include <limits>

#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "CommonMacros.h"

namespace pio {
class Vmdk;
class VirtualMachine;

class RequestHandler;
class Request;
class RequestBlock;
class RequestBuffer;

class CheckPoint;
class ActiveVmdk;
class SnapshotVmdk;

static constexpr auto kBlockIDMax = std::numeric_limits<::ondisk::BlockID>::max();
static constexpr auto kBlockIDMin = std::numeric_limits<::ondisk::BlockID>::min();

using CheckPoints = std::pair<::ondisk::CheckPointID, ::ondisk::CheckPointID>;
static constexpr size_t kMBShift     = 20;
static constexpr size_t kMaxIoSize   = 4 << kMBShift;
static constexpr size_t kSectorShift = 9;
static constexpr size_t kSectorSize  = 1 << kSectorShift;
static constexpr size_t kPageShift   = 12;
static constexpr size_t kPageSize    = 1u << kPageShift;
}