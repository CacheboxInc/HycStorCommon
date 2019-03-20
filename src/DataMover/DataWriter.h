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

class DataWriter {
public:
	DataWriter(const DataWriter&) = delete;
	DataWriter& operator = (const DataWriter&) = delete;
	DataWriter(DataWriter&& rhs) = delete;
	DataWriter& operator = (DataWriter&& rhs) = delete;

	DataWriter(RequestHandlerPtrVec::iterator src_begin,
		RequestHandlerPtrVec::iterator src_end,
		ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		std::unique_ptr<Request> request) noexcept;

	DataWriter(RequestHandlerPtrVec::iterator src_begin,
		RequestHandlerPtrVec::iterator sec_end,
		ActiveVmdk* vmkdp,
		const ::ondisk::CheckPointID ckpt_id,
		void* bufferp,
		const uint64_t offset,
		const size_t size) noexcept;

	folly::Future<int> Start();
	int GetStatus() const noexcept;
	bool IsComplete() const noexcept;
	std::unique_ptr<Request> GetRequest() noexcept;
	int CreateRequest() noexcept;
private:
	RequestHandlerPtrVec::iterator src_it_;
	RequestHandlerPtrVec::iterator src_end_;
	ActiveVmdk* vmdkp_{nullptr};
	const ::ondisk::CheckPointID ckpt_id_{::ondisk::MetaData_constants::kInvalidCheckPointID()};

	std::unique_ptr<Request> request_;
	void* bufferp_{};
	int64_t offset_{};
	size_t size_{};

	bool write_started_{false};
	bool write_complete_{false};
};

}
