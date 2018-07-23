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
#include "FileTargetHandler.h"
#include "VmdkConfig.h"
#include <fcntl.h>
#include "cksum.h"
#include <sys/stat.h>

using namespace ::ondisk;

namespace pio {

FileTargetHandler::FileTargetHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	enabled_ = configp->IsFileTargetEnabled();
	create_file_ = configp->GetFileTargetCreate();

	LOG(ERROR) << __func__ << "Enabled value is:" << enabled_;
	if (enabled_) {
		file_path_ = configp->GetFileTargetPath();
		LOG(ERROR) << __func__ << "Initial Path is:" << file_path_.c_str();

		if (create_file_) {
			VmID vmid;
			configp->GetVmId(vmid);
			VmdkID vmdkid;
			configp->GetVmdkId(vmdkid);

			file_path_ += "/disk_";
			file_path_ += vmid;
			file_path_ += ":" ;
			file_path_ += vmdkid;
		}

		LOG(ERROR) << __func__ << "Final Path is:" << file_path_.c_str();
		fd_ = ::open(file_path_.c_str(), O_RDWR | O_SYNC | O_DIRECT| O_CREAT);
		if (pio_unlikely(fd_ == -1)) {
			throw std::runtime_error("File open failed");
		} else {
			if (create_file_) {
				if(ftruncate(fd_, configp->GetFileTargetSize())) {
					throw std::runtime_error("File truncate failed");
				}
			}
			LOG(ERROR) << __func__ << "file fd_ is:" << fd_;
		}
	}
}

FileTargetHandler::~FileTargetHandler() {
	LOG(ERROR) << __func__ << "Called FileTargetHandler destructor";
	if (fd_ != -1) {
		::close(fd_);
	}
	if (enabled_) {
		if (create_file_) {
			LOG(ERROR) << __func__ << "destructor deleting file";
			::remove(file_path_.c_str());
		}
	}
}

const std::string& FileTargetHandler::GetFileCachePath() const {
	return file_path_;
}

folly::Future<int> FileTargetHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	int ret = 0;

	if (pio_unlikely(not failed.empty() || process.empty())) {
		LOG(ERROR) << __func__ << "Process list is empty, count::" << process.size();
		return -EINVAL;
	}

	if (pio_unlikely(fd_ == -1 || enabled_ == false)) {
		LOG(ERROR) << __func__ << "Device is not already open"
			" or FileTarget is not enabled, fd::-" << fd_;
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

		if(0) {
		LOG(ERROR) << __func__ << "[Cksum]" << blockp->GetAlignedOffset() <<
				":" << destp->Size() <<
				":" << crc_t10dif((unsigned char *) destp->Payload(), destp->Size());
		}
		blockp->PushRequestBuffer(std::move(destp));
	}
	log_assert(failed.empty());

	return ret;
}

folly::Future<int> FileTargetHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	int ret = 0;
	if (pio_unlikely(not failed.empty() || process.empty())) {
		LOG(ERROR) << __func__ << "Process list is empty, count::" << process.size();
		return -EINVAL;
	}

	if (pio_unlikely(fd_ == -1 || enabled_ == false)) {
		LOG(ERROR) << __func__ << "Device is not already open"
			" or FileTarget is not enabled, fd::-" << fd_;
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

folly::Future<int> FileTargetHandler::ReadPopulate(ActiveVmdk *vmdkp,
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
