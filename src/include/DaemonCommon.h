#pragma once

#include <limits>

#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include <map>
#include "CommonMacros.h"
#include <chrono>

namespace pio {
const std::string kAsNamespaceCacheClean = "CLEAN";
const std::string kAsNamespaceCacheDirty = "DIRTY";
class Vmdk;
class VirtualMachine;

class RequestHandler;
class Request;
class RequestBlock;
class RequestBuffer;

class CheckPoint;
class ActiveVmdk;
class SnapshotVmdk;
using per_disk_flush_stat = std::pair<uint64_t, uint64_t>;
using FlushStats = std::map<::ondisk::VmID, per_disk_flush_stat>;

struct AeroStats {
	uint64_t dirty_cnt_{0};
	uint64_t clean_cnt_{0};
	uint64_t parent_cnt_{0};
};

struct ScanStats {
	std::chrono::steady_clock::time_point start_time_;
	uint32_t progress_pct{0};
	uint64_t records_scanned;
};

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
