#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "gen-cpp2/MetaData_types.h"

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
}
