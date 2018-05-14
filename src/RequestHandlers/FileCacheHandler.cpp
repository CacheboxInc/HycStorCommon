#include <vector>
#include <string>
#include <fstream>
#include <sys/stat.h>

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
#include <fcntl.h>

using namespace ::ondisk;

namespace pio {

FileCacheHandler::FileCacheHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	enabled_ = configp->IsFileCacheEnabled();

	if (enabled_) {
		file_path_ = configp->GetFileCachePath();

		std::ofstream {file_path_};
		fd_ = ::open(file_path_.c_str(), O_RDWR | O_SYNC | O_DIRECT);
		if (pio_unlikely(fd_ == -1)) {
			throw std::runtime_error("File open failed");
		}
	}
}

FileCacheHandler::~FileCacheHandler() {
	::close(fd_);
	::remove(file_path_.c_str());
}

const std::string& FileCacheHandler::GetFileCachePath() const {
	return file_path_;
}

folly::Future<int> FileCacheHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	int ret = 0;
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	for (auto blockp : process) {
		auto destp = NewAlignedRequestBuffer(vmdkp->BlockSize());
		ssize_t nread = 0;
		while ((nread = ::pread(fd_, destp->Payload(), destp->Size(),
				blockp->GetAlignedOffset())) < 0) {
			if (nread == -1) {
				if (errno == EINTR) {
					continue;
				}
			ret = -1;
			break;
			}
		}

		if (pio_unlikely(ret != 0)) {
			return ret;
		}
		blockp->PushRequestBuffer(std::move(destp));
	}
	log_assert(failed.empty());

	return ret;
}

folly::Future<int> FileCacheHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	int ret = 0;
	if (pio_unlikely(not failed.empty() || process.empty())) {
		return -EINVAL;
	}

	log_assert(not file_path_.empty());

	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		log_assert(srcp->Size() == vmdkp->BlockSize());

		// copy data to a mem-aligned buffer needed for directIO
		auto bufp = NewAlignedRequestBuffer(vmdkp->BlockSize());
		::memcpy(bufp->Payload(), srcp->Payload(), srcp->Size());

		ssize_t nwrite = 0;
		while ((nwrite = ::pwrite(fd_, bufp->Payload(), srcp->Size(),
				blockp->GetAlignedOffset())) < 0) {
			if (nwrite == -1) {
				if (errno == EINTR) {
					continue;
				}
				// hard error
				ret = -1;
				break;
			}
		}
		if (pio_unlikely(ret != 0)) {
			return ret;
		}
	}
	return ret;
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
}
