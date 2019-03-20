#pragma once

#include <vector>
#include <memory>

#include <folly/futures/Future.h>

#include "DataMoverCommonTypes.h"
#include "gen-cpp2/MetaData_constants.h"

namespace folly {
class IOBuf;
};

namespace pio {

class Request;
class ActiveVmdk;

class DataReader {
public:
	DataReader(const DataReader&) = delete;
	DataReader& operator = (const DataReader&) = delete;
	DataReader(DataReader&& rhs) = delete ;
	DataReader& operator = (DataReader&& rhs) = delete;

	DataReader(RequestHandlerPtrVec::iterator src_begin,
		RequestHandlerPtrVec::iterator src_end,
		ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		void* bufferp,
		uint64_t offset,
		size_t size) noexcept;


	folly::Future<int> Start();
	int GetStatus() const noexcept;
	std::unique_ptr<Request> GetRequest() noexcept;
private:
	folly::Future<int> ScheduleRead(std::unique_ptr<RequestBlockPtrVec> process,
		std::unique_ptr<RequestBlockPtrVec> failed);
	folly::Future<int> RecursiveRead(std::unique_ptr<RequestBlockPtrVec> process,
		std::unique_ptr<RequestBlockPtrVec> failed);
private:
	RequestHandlerPtrVec::iterator src_it_;
	RequestHandlerPtrVec::iterator src_end_;
	ActiveVmdk* vmdkp_{nullptr};
	const ::ondisk::CheckPointID ckpt_id_{::ondisk::MetaData_constants::kInvalidCheckPointID()};
	void* bufferp_{nullptr};
	const uint64_t offset_{};
	const size_t size_{};

	std::unique_ptr<Request> request_;

	bool read_started_{false};
	bool read_complete_{false};
};

}
