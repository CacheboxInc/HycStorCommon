#include <cerrno>
#include <iterator>
#include <vector>
#include "Singleton.h"
#include "VmdkConfig.h"
#include "Request.h"
#include "Vmdk.h"

#include <TargetManager.hpp>
#include "NetworkTargetHandler.h"

#if 0
#include "cksum.h"
#endif

#define MAYBE_UNUSED(x) [[maybe_unused]]x

namespace pio {

using namespace hyc;
using namespace std;
using req_buf_type = std::unique_ptr<RequestBuffer>;

NetworkTargetHandler::NetworkTargetHandler(const config::VmdkConfig* configp) :
	RequestHandler(NetworkTargetHandler::kName, nullptr) {
	configp->GetVmId(vm_id_);
	configp->GetVmdkId(vmdk_id_);
	TargetManager *tmgr = SingletonHolder<TargetManager>::GetInstance().get();
	target_ = tmgr->GetVmdk(vm_id_, vmdk_id_);
	Open();
}

NetworkTargetHandler::~NetworkTargetHandler() {
	if (io_session_) {
		UnRegisterIOProcessor();
	}

	if (target_) {
		TargetManager *tmgr = SingletonHolder<TargetManager>::GetInstance().get();
		tmgr->CloseVmdk(vm_id_, vmdk_id_, 0);
	}
}

int NetworkTargetHandler::Open() {
	TargetManager *tmgr = SingletonHolder<TargetManager>::GetInstance().get();
	log_assert(tmgr != nullptr);

	LOG(ERROR) << "vmid::" << vm_id_ << "vmdk_id:::" << vmdk_id_;
	assert(target_);
	int rc = tmgr->OpenVmdk(vm_id_, vmdk_id_, 0); // ToFix: Hardcoded snap_id
	if(rc) {
		LOG(ERROR) << "openvmdk failed";
		return -1;
	}
	//TBD: revisit the abstraction
	if (tmgr->GetVmdkIds(vm_id_, vmdk_id_, &srcid_, &destid_) != 0) {
		LOG(ERROR) << "getvmdkids failed";
		return -1;
	}

	RegisterIOProcessor(target_, true, srcid_, destid_);
	return 0;
}

folly::Future<int> NetworkTargetHandler::Read(ActiveVmdk *vmdkp, MAYBE_UNUSED(Request *reqp),
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	std::shared_ptr<hyc::IO> io = std::make_shared<IO>(READDIR);
	if (pio_unlikely(not io)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENOMEM;
	}

	auto req_blocks = std::make_shared<std::vector<req_buf_type>>();
	for (auto blockp : process) {
		auto snap_id = GetSnapID(vmdkp, blockp->GetReadCheckPointId());
		io->AddIoVec(snap_id, blockp->GetAlignedOffset(), vmdkp->BlockSize(),
			[req_blocks](int buflen) -> void* {
				auto destp = NewRequestBuffer(buflen);
				if (pio_unlikely(not destp))
					return nullptr;
				auto bufp = destp->Payload();
				req_blocks->push_back(std::move(destp));
				return bufp;
			});
	}

	auto promise = std::make_shared<folly::Promise<int>>();
	io->SetOpaque((void *) promise.get());
	auto ret = io_session_->ProcessIO(io);
	if (ret != 0) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -EAGAIN;
	}

	return promise->getFuture()
	.then([io, req_blocks, promise, &process, &failed, vmdkp] (int rc) mutable {

		if (rc != 0) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return rc;
		}

		auto it = req_blocks->begin();
		auto eit = req_blocks->end();
		for (auto blockp : process) {
			#if 0
			RequestBuffer *bufferp = it->get();
			LOG(ERROR) << "Response Offset :" << blockp->GetAlignedOffset();
			LOG(ERROR) << __func__ << "[TCksum]" << blockp->GetAlignedOffset() <<
                                ":" << bufferp->Size() <<
                                ":" << crc_t10dif((unsigned char *) bufferp->Payload(), bufferp->Size());
			#endif
			log_assert(it != eit);
			vmdkp->stats_->IncrNwReadBytes((*it)->PayloadSize());
			vmdkp->stats_->IncrNwTotalReads(1);
			blockp->PushRequestBuffer(std::move(*it));
			blockp->SetResult(0, RequestStatus::kSuccess);

			++it;
		}

		log_assert(req_blocks->size() == 0);
		return 0;
	});
}

folly::Future<int> NetworkTargetHandler::BulkWrite(ActiveVmdk* vmdkp,
		MAYBE_UNUSED(::ondisk::CheckPointID ckpt),
		MAYBE_UNUSED(const std::vector<std::unique_ptr<Request>>& requests),
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	std::shared_ptr<hyc::IO> io = std::make_shared<IO>(WRITEDIR);
	if (pio_unlikely(not io)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENOMEM;
	}

	size_t curr_bytes_write = 0;
	for (auto& blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		io->AddIoVec(0, blockp->GetAlignedOffset(), srcp->PayloadSize(), srcp->Payload());
		curr_bytes_write += srcp->PayloadSize();
	}

	auto promise = std::make_shared<folly::Promise<int>>();
	io->SetOpaque((void *) promise.get());
	auto ret = io_session_->ProcessIO(io);
	if (ret != 0) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -EAGAIN;
	}

	return promise->getFuture()
	.then([io, promise, &process, &failed, vmdkp, curr_bytes_write] (int rc) mutable {
		if (pio_unlikely(rc != 0)) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return rc < 0 ? rc : -rc;
		}
		vmdkp->stats_->IncrNwWriteBytes(curr_bytes_write);
		vmdkp->stats_->IncrNwTotalWrites(process.size());
		return 0;
	});
}

int64_t NetworkTargetHandler::GetSnapID(ActiveVmdk* vmdkp, const uint64_t& ckpt_id) {
	return vmdkp->GetVM()->GetSnapID(vmdkp, ckpt_id);
}

folly::Future<int> NetworkTargetHandler::BulkRead(ActiveVmdk* vmdkp,
		MAYBE_UNUSED(const std::vector<std::unique_ptr<Request>>& requests),
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	std::shared_ptr<hyc::IO> io = std::make_shared<IO>(READDIR);
	if (pio_unlikely(not io)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENOMEM;
	}

	auto buffers = std::make_shared<std::vector<std::unique_ptr<RequestBuffer>>>();
	for (auto blockp : process) {

		auto ckpt_id = blockp->GetReadCheckPointId();
		auto snap_id = GetSnapID(vmdkp, ckpt_id);
		if (pio_unlikely(snap_id < 0)) {
			LOG(ERROR) << __func__ << "Got invalid snap id: "
				<< snap_id << " for checkpoint ID:" <<  ckpt_id;
			log_assert(0);
		}

		io->AddIoVec(snap_id, blockp->GetAlignedOffset(), vmdkp->BlockSize(),
			[buffers](int buflen) -> void* {
				auto destp = NewRequestBuffer(buflen);
				if (pio_unlikely(not destp))
					return nullptr;
				auto bufp = destp->Payload();
				buffers->push_back(std::move(destp));
				return bufp;
			});
	}

	auto promise = std::make_unique<folly::Promise<int>>();
	io->SetOpaque(reinterpret_cast<void*>(promise.get()));
	auto rc = io_session_->ProcessIO(io);
	if (pio_unlikely(rc != 0)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return rc < 0 ? rc : -rc;
	}

	return promise->getFuture()
	.then([&process, &failed, io, buffers, vmdkp,
			promise = std::move(promise)]  (int rc) mutable {
		if (pio_unlikely(rc != 0)) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return rc < 0 ? rc : -rc;
		}

		auto it = buffers->begin();
		auto eit = buffers->end();
		for (auto blockp : process) {
			log_assert(it != eit);
			vmdkp->stats_->IncrNwReadBytes((*it)->PayloadSize());
			vmdkp->stats_->IncrNwTotalReads(1);
			blockp->PushRequestBuffer(std::move(*it));
			blockp->SetResult(0, RequestStatus::kSuccess);

			++it;
		}
		return 0;
	});

}

folly::Future<int> NetworkTargetHandler::BulkReadPopulate(
		MAYBE_UNUSED(ActiveVmdk* vmdkp),
		MAYBE_UNUSED(const std::vector<std::unique_ptr<Request>>& requests),
		MAYBE_UNUSED(const std::vector<RequestBlock*>& process),
		MAYBE_UNUSED(std::vector<RequestBlock*>& failed)) {
	log_assert(0);
	return -ENODEV;
}

folly::Future<int> NetworkTargetHandler::Write(ActiveVmdk *vmdkp,
		MAYBE_UNUSED(Request *reqp),
		MAYBE_UNUSED(::ondisk::CheckPointID ckpt),
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	std::shared_ptr<hyc::IO> io = std::make_shared<IO>(WRITEDIR);
	if (pio_unlikely(not io)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENOMEM;
	}

	size_t curr_bytes_write = 0;
	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		#if 0
		auto payload = srcp->Payload();
		LOG(ERROR) << __func__ << "Offset::-" << blockp->GetAlignedOffset()
			<< "Start::-" << payload[0] << "End::-" << payload[4095];
		#endif
		io->AddIoVec(0, blockp->GetAlignedOffset(), srcp->PayloadSize(),
			srcp->Payload());
		curr_bytes_write += srcp->PayloadSize();
	}

	auto promise = std::make_shared<folly::Promise<int>>();
	io->SetOpaque((void *) promise.get());
	auto ret = io_session_->ProcessIO(io);
	if (ret != 0) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -EAGAIN;
	}

	return promise->getFuture()
	.then([io, promise, &process, &failed, vmdkp, curr_bytes_write] (int rc) mutable {
		#if 0
		LOG(ERROR) << __func__ << "In NetworkTargetHandler::Write future";
		#endif
		if (rc != 0) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return rc < 0 ? rc : -rc;
		}
		vmdkp->stats_->IncrNwWriteBytes(curr_bytes_write);
		vmdkp->stats_->IncrNwTotalWrites(1);
		return 0;
	});
}

int NetworkTargetHandler::IOProcessed(MAYBE_UNUSED(IOSession *session),
		std::shared_ptr<IO> io) {
	auto promise = reinterpret_cast<folly::Promise<int>*>(io->GetOpaque());
	promise->setValue(io->GetStatus());
	return 0;
}

folly::Future<int> NetworkTargetHandler::ReadPopulate(
		MAYBE_UNUSED(ActiveVmdk *vmdkp),
		MAYBE_UNUSED(Request *reqp),
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	failed.reserve(process.size());
	std::copy(process.begin(), process.end(), std::back_inserter(failed));
	return -ENODEV;
}

int NetworkTargetHandler::RegisterIOProcessor(IOProcessor *io_processor,
		MAYBE_UNUSED(bool preferred),
		int srcid, int destid) {

	io_session_ = new IOSession(this, io_processor, srcid, destid);
	if (io_session_ == nullptr) {
		LOG(ERROR) <<__func__ << "io_session create failed";
		return -1;
	}

	return 0;
}

int NetworkTargetHandler::UnRegisterIOProcessor() {
	delete io_session_;
	return 0;
}

int NetworkTargetHandler::Cleanup(ActiveVmdk *vmdkp) {
	#if 0
	UnRegisterIOProcessor();
	TargetManager *tmgr = SingletonHolder<TargetManager>::GetInstance().get();
	tmgr->CloseVmdk(vm_id_, vmdk_id_);
	#endif
	if (pio_unlikely(not nextp_)) {
		return 0;
	}

	return nextp_->Cleanup(vmdkp);
}

}
