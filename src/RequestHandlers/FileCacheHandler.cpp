#include <sys/stat.h>
#include <fcntl.h>
#include <cerrno>

#include <vector>
#include <string>
#include <stdexcept>

#include <folly/futures/Future.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "IDs.h"
#include "DaemonCommon.h"
#include "Vmdk.h"
#include "Request.h"
#include "RequestHandler.h"
#include "FileCacheHandler.h"
#include "VmdkConfig.h"
#include "LibAio.h"

using namespace ::ondisk;

namespace pio {

FileCacheHandler::FileCacheHandler(const config::VmdkConfig* configp) :
		RequestHandler(FileCacheHandler::kName, nullptr) {
	/* FileCacheHandler with encryption and compression not supported */
	log_assert(not configp->IsCompressionEnabled());
	log_assert(not configp->IsEncryptionEnabled());

	enabled_ = configp->IsFileCacheEnabled();
	if (enabled_) {
		file_path_ = configp->GetFileCachePath();
		fd_ = ::open(file_path_.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_TRUNC, 0777);
		if (pio_unlikely(fd_ == -1)) {
			LOG(ERROR) << file_path_ << errno;
			throw std::runtime_error("File open failed");
		}
	}

	thread_ = std::make_unique<std::thread>([basep = &base_] () mutable {
		basep->loopForever();
	});
	if (pio_unlikely(not thread_)) {
		throw std::bad_alloc();
	}

	libaio_ = std::make_unique<LibAio>(&base_, 128);
	if (pio_unlikely(not libaio_)) {
		throw std::bad_alloc();
	}
}

FileCacheHandler::~FileCacheHandler() {
	::close(fd_);
	::remove(file_path_.c_str());

	libaio_ = nullptr;

	base_.terminateLoopSoon();
	thread_->join();

	thread_ = nullptr;
}

const std::string& FileCacheHandler::GetFileCachePath() const {
	return file_path_;
}

folly::Future<int> FileCacheHandler::Read(const size_t block_size,
		const std::vector<RequestBlock*> process,
		std::vector<RequestBlock*>& failed) {
	std::vector<folly::Future<int>> futures;
	for (auto blockp : process) {
		auto dest = NewAlignedRequestBuffer(block_size);
		auto destp = dest.get();

		auto f = libaio_->AsyncRead(fd_, destp->Payload(), destp->Size(), blockp->GetAlignedOffset())
		.then([&failed, blockp, dest = std::move(dest)]
			(const ssize_t read) mutable {
				if (pio_unlikely(read != static_cast<ssize_t>(dest->Size()))) {
					int rc = read < 0 ? read : -EIO;
					blockp->SetResult(rc, RequestStatus::kFailed);
					failed.emplace_back(blockp);
					return rc;
				}
				blockp->SetResult(0, RequestStatus::kSuccess);
				blockp->PushRequestBuffer(std::move(dest));
				return 0;
			}
		);
		futures.emplace_back(std::move(f));
	}
	libaio_->Drain();
	return folly::collectAll(std::move(futures))
	.then([] (const folly::Try<std::vector<folly::Try<int>>>& tries) {
		if (pio_unlikely(tries.hasException())) {
			return -EIO;
		}
		const auto& vec = tries.value();
		for (const auto& tri : vec) {
			if (pio_unlikely(tri.hasException())) {
				return -EIO;
			}
			auto rc = tri.value();
			if (pio_unlikely(rc < 0)) {
				return rc;
			}
		}
		return 0;
	});
}

folly::Future<int> FileCacheHandler::Write(const size_t block_size,
		const std::vector<RequestBlock*> process,
		std::vector<RequestBlock*>& failed) {
	std::vector<folly::Future<int>> futures;
	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		log_assert(srcp->Size() == block_size);

		// copy data to a mem-aligned buffer needed for directIO
		auto buf = NewAlignedRequestBuffer(block_size);
		::memcpy(buf->Payload(), srcp->Payload(), srcp->Size());
		auto bufp = buf.get();

		auto f = libaio_->AsyncWrite(fd_, bufp->Payload(), srcp->Size(),
			blockp->GetAlignedOffset())
		.then([&failed, blockp, buf = std::move(buf)]
			(const ssize_t wrote) mutable {
				if (pio_unlikely(wrote != static_cast<ssize_t>(buf->Size()))) {
					int rc = wrote < 0 ? wrote : -EIO;
					blockp->SetResult(rc, RequestStatus::kFailed);
					failed.emplace_back(blockp);
					return rc;
				}
				blockp->SetResult(0, RequestStatus::kSuccess);
				return 0;
			}
		);
		futures.emplace_back(std::move(f));
	}

	libaio_->Drain();
	return folly::collectAll(std::move(futures))
	.then([] (const folly::Try<std::vector<folly::Try<int>>>& tries) {
		if (pio_unlikely(tries.hasException())) {
			return -EIO;
		}
		const auto& vec = tries.value();
		for (const auto& tri : vec) {
			if (pio_unlikely(tri.hasException())) {
				return -EIO;
			}
			auto rc = tri.value();
			if (pio_unlikely(rc < 0)) {
				return rc;
			}
		}
		return 0;
	});
}

folly::Future<int> FileCacheHandler::Read(ActiveVmdk *vmdkp, Request *,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}
	return Read(vmdkp->BlockSize(), process, failed);
}

folly::Future<int> FileCacheHandler::Write(ActiveVmdk *vmdkp, Request *,
		CheckPointID, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}
	return Write(vmdkp->BlockSize(), process, failed);
}

folly::Future<int> FileCacheHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> FileCacheHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID,
		const std::vector<std::unique_ptr<Request>>&,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return Write(vmdkp->BlockSize(), process, failed);
}

folly::Future<int> FileCacheHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>&,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return Read(vmdkp->BlockSize(), process, failed);
}

folly::Future<int> FileCacheHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	if (pio_unlikely(not nextp_)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENODEV;
	}

	return nextp_->BulkReadPopulate(vmdkp, requests, process, failed);
}

}
