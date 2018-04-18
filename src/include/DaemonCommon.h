#pragma once

#include <limits>

#include "IDs.h"
#include "DaemonTgtTypes.h"

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

static const CheckPointID kInvalidCheckPointID = 0;
static const SnapshotID   kInvalidSnapshotID  = 0;
static const BlockID      kBlockIDMax         = std::numeric_limits<BlockID>::max();
static const BlockID      kBlockIDMin         = std::numeric_limits<BlockID>::min();

static const size_t kMBShift     = 20;
static const size_t kMaxIoSize   = 4 << kMBShift;
static const size_t kSectorShift = 9;
static const size_t kSectorSize  = 1 << kSectorShift;
static const size_t kPageShift   = 12;
static const size_t kPageSize    = 1u << kPageShift;

#ifndef pio_likely
#define pio_likely(x) (__builtin_expect(!!(x), 1))
#endif

#ifndef pio_unlikely
#define pio_unlikely(x) (__builtin_expect(!!(x), 0))
#endif

#ifndef LOG_DUMP
#define LOG_DUMP(expr) do { \
	VLOG(1) << "Assertion Failed " << expr; \
} while (0)
#endif

#ifndef log_assert
#define log_assert(expr) do { \
	if (pio_unlikely(!(expr))) { \
		LOG_DUMP(#expr); \
		assert(expr); \
	} \
} while(0)
#endif

}