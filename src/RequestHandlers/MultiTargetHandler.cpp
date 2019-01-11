#include <cerrno>

#include <iterator>
#include <vector>

#include "gen-cpp2/MetaData_types.h"
#include "MultiTargetHandler.h"
#include "CacheTargetHandler.h"
#include "FileTargetHandler.h"
#include "FileCacheHandler.h"
#include "RamCacheHandler.h"
#include "ErrorHandler.h"
#include "SuccessHandler.h"
#include "Vmdk.h"
#include "VmdkConfig.h"
#include "Request.h"
#include "DaemonUtils.h"
#if 0
#include "cksum.h"
#endif

#ifdef USE_NEP
#include "NetworkTargetHandler.h"
#endif

using namespace ::ondisk;

namespace pio {
MultiTargetHandler::MultiTargetHandler(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) : RequestHandler(nullptr) {
	InitializeTargetHandlers(vmdkp, configp);
}

void MultiTargetHandler::InitializeTargetHandlers(const ActiveVmdk* vmdkp,
		const config::VmdkConfig* configp) {
	if (configp->ErrorHandlerEnabled()) {
		auto error = std::make_unique<ErrorHandler>(configp);
		targets_.emplace_back(std::move(error));
	}

	if (configp->IsSuccessHandlerEnabled()) {
		auto success = std::make_unique<SuccessHandler>(configp);
		targets_.emplace_back(std::move(success));
	}

	if (configp->IsRamCacheEnabled()) {
		auto ram_cache = std::make_unique<RamCacheHandler>(configp);
		targets_.emplace_back(std::move(ram_cache));
	}

	if (configp->IsFileCacheEnabled()) {
		auto file_cache = std::make_unique<FileCacheHandler>(configp);
		targets_.push_back(std::move(file_cache));
	}

	auto cache_target = std::make_unique<CacheTargetHandler>(vmdkp, configp);
	targets_.push_back(std::move(cache_target));

	if (configp->IsFileTargetEnabled()) {
		auto file_target = std::make_unique<FileTargetHandler>(configp);
		targets_.push_back(std::move(file_target));
	}

#ifdef USE_NEP
	if (configp->IsNetworkTargetEnabled()) {
		auto network_target = std::make_unique<NetworkTargetHandler>(configp);
		targets_.push_back(std::move(network_target));
	} else
#endif
	if (not configp->IsFileTargetEnabled()) {
		auto new_conf = *configp;
		new_conf.EnableSuccessHandler();
		if (configp->IsCompressionEnabled()) {
			new_conf.SetSuccessHandlerCompressData();
		}
		auto success = std::make_unique<SuccessHandler>(&new_conf);
		targets_.emplace_back(std::move(success));
	}
	log_assert(targets_.size() >= 2);
}

MultiTargetHandler::~MultiTargetHandler() {
}

folly::Future<int> MultiTargetHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());
	return targets_[0]->Read(vmdkp, reqp, process, failed)
	.then([this, vmdkp, reqp, &process, &failed] (int rc) mutable -> folly::Future<int> {
		bool f = false;
		for (const auto blockp : failed) {
			if (not blockp->IsReadMissed()) {
				f = true;
			}
		}
		if (pio_unlikely(rc and f)) {
			vmdkp->cache_stats_.read_failed_ += failed.size();
			LOG(ERROR) << "Read error " << rc;
			return rc < 0 ? rc : -rc;
		}

		/* No read miss to process, return from here */
		if (failed.size() == 0) {
			vmdkp->cache_stats_.read_hits_ += process.size();
			return 0;
		}

		if (pio_unlikely(targets_.size() <= 1)) {
			LOG(ERROR) << __func__ << "No Target handler registered";
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		/* Handle Read Miss */
		auto read_missed = std::make_unique<std::remove_reference<decltype(failed)>::type>();
		read_missed->swap(failed);
		failed.clear();

		/* Initiate ReadAhead and populate cache if ghb sees a pattern based on history */
		if(pio_likely(vmdkp->read_aheadp_ != NULL)) {
			vmdkp->read_aheadp_->Run(*read_missed, reqp);
		}

		/* Read from next StorageLayer - probably Network or File */
		return targets_[1]->Read(vmdkp, reqp, *read_missed, failed)
		.then([this, vmdkp, reqp, read_missed = std::move(read_missed), &failed] (int rc)
				mutable -> folly::Future<int> {
			vmdkp->cache_stats_.read_miss_ += ReadAhead::AdjustReadMisses(*read_missed, reqp);
			if (pio_unlikely(rc != 0)) {
				vmdkp->cache_stats_.read_failed_ += failed.size();
				LOG(ERROR) << __func__ << "Reading from TargetHandler layer for read populate failed";
				return rc;
			}

			log_assert(failed.empty());

			/* now read populate */
			return targets_[0]->ReadPopulate(vmdkp, reqp, *read_missed, failed)
			.then([read_missed = std::move(read_missed), vmdkp]
					(int rc) -> folly::Future<int> {
				vmdkp->cache_stats_.read_populates_ += (*read_missed).size();
				if (rc) {
					LOG(ERROR) << __func__ << "Cache (Read) populate failed";
				}
				return rc;
			});
		});
	});
}

folly::Future<int> MultiTargetHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());
	return targets_[0]->Write(vmdkp, reqp, ckpt, process, failed);
}

folly::Future<int> MultiTargetHandler::ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());
	return targets_[0]->ReadPopulate(vmdkp, reqp, process, failed);
}

folly::Future<int> MultiTargetHandler::BulkFlush(ActiveVmdk *vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());

	/* Read from DIRTY NAMESPACE only */
	return targets_[0]->BulkRead(vmdkp, requests, process, failed)
	.then([this, vmdkp, &requests, &process, &failed] (int rc) mutable
			-> folly::Future<int> {
		/* Read from CacheLayer complete */
		auto vmdkid = vmdkp->GetID();
		if(pio_unlikely(rc != 0)) {
			LOG(ERROR) << __func__ << "Reading from DIRTY namespace failed for vmdkid::"
				<< vmdkid << ", error code::" << rc;
			return rc;
		}

		/* We should not be seeing any Miss */
		if(pio_unlikely(failed.size())) {
			for (auto blockp : failed) {
				if (pio_likely(blockp->IsReadMissed())) {
					LOG(ERROR) << __func__ << "Record not found ::" << vmdkid << ":"
					<< blockp->GetReadCheckPointId() << ":"
					<< blockp->GetAlignedOffset();
				}
			}

			/* TBD: - failure, do we need to move all Request
			 * blocks in failed vector. Get the error code
			 * from layers below
			 */

			return -EIO;
		}

		if (pio_unlikely(targets_.size() <= 1)) {
			LOG(ERROR) << __func__ << "No Target handler registered";
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		#if 0
		for (auto& blockp : process) {
			/* Get the cksum of blocks after read */
			auto destp = blockp->GetRequestBufferAtBack();
			LOG(ERROR) << __func__ << "FlushRead [Cksum]" << blockp->GetAlignedOffset() <<
				":" << destp->Size() <<
				":" << crc_t10dif((unsigned char *) destp->Payload(), destp->Size());
		}
		#endif

		/* Write on prem, Next layer is target handler layer */
		return targets_[1]->BulkWrite(vmdkp, 0, requests, process, failed)
		.then([&failed] (int rc) mutable -> folly::Future<int> {
			if (pio_unlikely(rc != 0)) {
				LOG(ERROR) << __func__ << "In future context on prem write error";
				//log_assert(not failed.empty());
				return rc;
			}

			log_assert(failed.empty());
			return 0;
		});
	});
}

folly::Future<int> MultiTargetHandler::Flush(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());

	/* Read from DIRTY NAMESPACE only */
	return targets_[0]->Read(vmdkp, reqp, process, failed)
	.then([this, vmdkp, reqp, &process, &failed] (int rc) mutable -> folly::Future<int> {
		/* Read from CacheLayer complete */
		auto vmdkid = vmdkp->GetID();
		if(pio_unlikely(rc != 0)) {
			LOG(ERROR) << __func__ << "Reading from DIRTY namespace failed for vmdkid::"
				<< vmdkid << ", error code::" << rc;
			return rc;
		}

		/* We should not be seeing any Miss */
		if(pio_unlikely(failed.size())) {
			for (auto blockp : failed) {
				if (pio_likely(blockp->IsReadMissed())) {
					LOG(ERROR) << __func__ << "Record not found ::" << vmdkid << ":"
					<< blockp->GetReadCheckPointId() << ":"
					<< blockp->GetAlignedOffset();
				}
			}

			/* TBD: - failure, do we need to move all Request
			 * blocks in failed vector. Get the error code
			 * from layers below
			 */

			return -EIO;
		}

		if (pio_unlikely(targets_.size() <= 1)) {
			LOG(ERROR) << __func__ << "No Target handler registered";
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -ENODEV;
		}

		#if 0
		for (auto& blockp : process) {
			/* Get the cksum of blocks after read */
			auto destp = blockp->GetRequestBufferAtBack();
			LOG(ERROR) << __func__ << "FlushRead [Cksum]" << blockp->GetAlignedOffset() <<
				":" << destp->Size() <<
				":" << crc_t10dif((unsigned char *) destp->Payload(), destp->Size());
		}
		#endif

		/* Write on prem, Next layer is target handler layer */
		return targets_[1]->Write(vmdkp, reqp, 0, process, failed)
		.then([&failed] (int rc) mutable -> folly::Future<int> {
			if (pio_unlikely(rc != 0)) {
				LOG(ERROR) << __func__ << "In future context on prem write error";
				log_assert(not failed.empty());
				return rc;
			}

			log_assert(failed.empty());
			return 0;
		});
	});
}

folly::Future<int> MultiTargetHandler::Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	log_assert(not targets_.empty());
	return targets_[0]->Move(vmdkp, reqp, process, failed);
}

folly::Future<int> MultiTargetHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return targets_[0]->BulkWrite(vmdkp, ckpt, requests, process, failed);
}

folly::Future<int> MultiTargetHandler::BulkReadComplete(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process, std::vector<RequestBlock*>& failed) {
	if (failed.empty()) {
		vmdkp->cache_stats_.read_hits_ += process.size();
		return 0;
	}

	if (pio_unlikely(targets_.size() <= 1)) {
		return -ENODEV;
	}

	auto missed = std::make_unique<std::remove_reference_t<decltype(failed)>>();
	if (pio_unlikely(not missed)) {
		LOG(ERROR) << "Failed to allocate memory for missed vector";
		return -ENOMEM;
	}
	missed->swap(failed);
	
	/* Initiate ReadAhead and populate cache if ghb sees a pattern based on history */
	if(pio_likely(vmdkp->read_aheadp_ != NULL)) {
		vmdkp->read_aheadp_->Run(*missed, requests);
	}

	return targets_[1]->BulkRead(vmdkp, requests, *missed, failed)
	.then([this, vmdkp, &requests, &failed, missed = std::move(missed)]
			(int rc) mutable -> folly::Future<int> {
		vmdkp->cache_stats_.read_miss_ += ReadAhead::AdjustReadMisses(*missed, requests);
		if (pio_unlikely(rc)) {
			vmdkp->cache_stats_.read_failed_ += failed.size();
			return rc < 0 ? rc : -rc;
		}
		if (pio_unlikely(not failed.empty())) {
			LOG(ERROR) << "Few requests failed on second target";
			return -ENODEV;
		}

		return targets_[0]->BulkReadPopulate(vmdkp, requests, *missed, failed)
		.then([missed = std::move(missed), vmdkp] (int rc) mutable -> folly::Future<int> {
			vmdkp->cache_stats_.read_populates_ += (*missed).size();
			return rc;
		});
	});
}


folly::Future<int> MultiTargetHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return targets_[0]->BulkRead(vmdkp, requests, process, failed)
	.then([this, vmdkp, &requests, &process, &failed] (int rc) mutable
			-> folly::Future<int> {
		bool f = false;
		for (const auto blockp : failed) {
			if (not blockp->IsReadMissed()) {
				f = true;
			}
		}
		if (pio_unlikely(rc and f)) {
			vmdkp->cache_stats_.read_failed_ += failed.size();
			LOG(ERROR) << "Read error " << rc;
			return rc < 0 ? rc : -rc;
		}

		return BulkReadComplete(vmdkp, requests, process, failed);
	});
}

folly::Future<int> MultiTargetHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return targets_[0]->BulkReadPopulate(vmdkp, requests, process, failed);
}
}
