#include <cerrno>
#include <iterator>
#include <vector>
#include "Singleton.h"
#include <TargetManager.hpp>
#include "TargetHandler.h"
#include "Request.h"
#include "Vmdk.h"
#include "cksum.h"

namespace pio {

using namespace hyc;
using namespace std;
using req_buf_type = std::unique_ptr<RequestBuffer>;

TargetHandler::TargetHandler(std::string vm_id, std::string vmdk_id) :
       RequestHandler(nullptr), vm_id_(vm_id), vmdk_id_(vmdk_id) {
}

TargetHandler::~TargetHandler() {
	if (io_session_) {
		UnRegisterIOProcessor();
	}

	if (target_) {
		TargetManager *tmgr = SingletonHolder<TargetManager>::GetInstance().get();
		tmgr->CloseVmdk(vm_id_, vmdk_id_);
	}
}

int TargetHandler::Open() {
	TargetManager *tmgr = SingletonHolder<TargetManager>::GetInstance().get();
	log_assert(tmgr != nullptr);

	LOG(ERROR) << "vmid::" << vm_id_ << "vmdk_id:::" << vmdk_id_;
	target_ = tmgr->OpenVmdk(vm_id_, vmdk_id_);
	if (target_ == nullptr) {
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

folly::Future<int> TargetHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {

	failed.clear();
	std::shared_ptr<hyc::IO> io = std::make_shared<IO>(READDIR);
	if (pio_unlikely(not io)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENOMEM;
	}

	std::shared_ptr<std::vector<req_buf_type>> req_blocks = std::make_shared<std::vector<req_buf_type>>();
	for (auto blockp : process) {
		auto destp = NewRequestBuffer(vmdkp->BlockSize());
		if (pio_unlikely(not destp)) {
			blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
			failed.emplace_back(blockp);
			return -ENOMEM;
		}

		LOG(ERROR) << "Request Offset :" << blockp->GetAlignedOffset() << "Size::" << destp->Size();
		io->AddIoVec(blockp->GetAlignedOffset(), destp->Size(), destp->Payload());
		req_blocks->push_back(std::move(destp));
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
	.then([this, vmdkp, reqp, io, req_blocks, promise, &process, &failed] (int rc) mutable {

		if (rc != 0) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return rc;
		}

		for (auto blockp : process) {
			auto it = req_blocks->begin();
			RequestBuffer *bufferp = it->get();
			LOG(ERROR) << "Response Offset :" << blockp->GetAlignedOffset();
			LOG(ERROR) << __func__ << "[TCksum]" << blockp->GetAlignedOffset() <<
                                ":" << bufferp->Size() <<
                                ":" << crc_t10dif((unsigned char *) bufferp->Payload(), bufferp->Size());

			blockp->PushRequestBuffer(std::move(*it));
			blockp->SetResult(0, RequestStatus::kSuccess);
			req_blocks->erase(it);
		}

		log_assert(req_blocks->size() == 0);
		return 0;
	});
}

folly::Future<int> TargetHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {

	failed.clear();
	std::shared_ptr<hyc::IO> io = std::make_shared<IO>(WRITEDIR);
	if (pio_unlikely(not io)) {
		failed.reserve(process.size());
		std::copy(process.begin(), process.end(), std::back_inserter(failed));
		return -ENOMEM;
	}

	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		auto payload = srcp->Payload();
		LOG(ERROR) << __func__ << "Offset::-" << blockp->GetAlignedOffset()
			<< "Start::-" << payload[0] << "End::-" << payload[4095];
		io->AddIoVec(blockp->GetAlignedOffset(), srcp->Size(), srcp->Payload());
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
	.then([this, vmdkp, reqp, ckpt, io, promise, &process, &failed] (int rc) mutable {
		LOG(ERROR) << __func__ << "In TargetHandler::Write future";
		if (rc != 0) {
			failed.reserve(process.size());
			std::copy(process.begin(), process.end(), std::back_inserter(failed));
			return -EIO;
		}
		return 0;
	});
}

int TargetHandler::IOProcessed(IOSession *session, std::shared_ptr<IO> io) {
	auto promise = reinterpret_cast<folly::Promise<int>*>(io->GetOpaque());
	promise->setValue(io->GetStatus());
	return 0;
}

folly::Future<int> TargetHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {

	failed.clear();
	failed.reserve(process.size());
	std::copy(process.begin(), process.end(), std::back_inserter(failed));
	return -ENODEV;
}

int TargetHandler::RegisterIOProcessor(IOProcessor *io_processor, bool preferred,
		int srcid, int destid) {

	io_session_ = new IOSession(this, io_processor, srcid, destid);
	if (io_session_ == nullptr) {
		LOG(ERROR) <<__func__ << "io_session create failed";
		return -1;
	}

	return 0;
}

int TargetHandler::UnRegisterIOProcessor() {
	delete io_session_;
	return 0;
}

int TargetHandler::Cleanup(ActiveVmdk *vmdkp) {
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
