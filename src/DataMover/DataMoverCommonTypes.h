#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/MetaData_constants.h"

namespace pio {
class Request;
class RequestHandler;
class RequestBlock;
class CheckPoint;

using BlockCount = uint16_t;
using BlockPair = std::pair<::ondisk::BlockID, BlockCount>;
using BlockPairVec = std::vector<BlockPair>;
using RequestVec = std::vector<std::unique_ptr<Request>>;
using RequestHandlerPtrVec = std::vector<RequestHandler*>;
using RequestBlockPtrVec = std::vector<RequestBlock*>;

using CheckPointPtrVec = std::vector<const CheckPoint*>;
using CkptBlockPair = std::pair<::ondisk::CheckPointID, ::ondisk::BlockID>;

/*
 * CkptBatch
 * - CheckPoints are Synced in batches
 * - Captures 3 CheckPointIDs
 *   - base
 *   - sync in progress
 *   - last
 */
using CkptBatch = std::tuple<
	::ondisk::CheckPointID,
	::ondisk::CheckPointID,
	::ondisk::CheckPointID
>;
constexpr CkptBatch kCkptBatchInitial = {
	::ondisk::MetaData_constants::kInvalidCheckPointID(),
	::ondisk::MetaData_constants::kInvalidCheckPointID(),
	::ondisk::MetaData_constants::kInvalidCheckPointID()
};
}
