#include <cerrno>

#include <iterator>
#include <vector>
#include <memory>
#include <deque>
#include <chrono>
#include <mutex>
#include <thread>
#include <condition_variable>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "Request.h"
#include "VmdkConfig.h"
#include "SuccessHandler.h"
#include "Vmdk.h"
#include "Work.h"
#include "hyc_compress.h"
#include "BackGroundWorker.h"

using namespace std::chrono_literals;
using namespace pio::hyc;
using namespace ::ondisk;

namespace pio {
struct SuccessWork {
	enum Type {
		kRead,
		kWrite,
		kBulkWrite,
		kBulkRead,
	};

	SuccessHandler* selfp_;
	ActiveVmdk* vmdkp_;

	Request* reqp_{nullptr};
	const std::vector<std::unique_ptr<Request>>* requestsp_{nullptr};

	CheckPointID ckpt_;
	const std::vector<RequestBlock*>& process_;
	std::vector<RequestBlock *>& failed_;
	Type type_;
	folly::Promise<int> promise_;

	SuccessWork(SuccessHandler* selfp, ActiveVmdk* vmdkp, Request* reqp,
			CheckPointID ckpt, const std::vector<RequestBlock*>& process,
			std::vector<RequestBlock*>& failed, Type type) : selfp_(selfp),
			vmdkp_(vmdkp), reqp_(reqp), ckpt_(ckpt), process_(process),
			failed_(failed), type_(type) {
	}

	SuccessWork(SuccessHandler* selfp, ActiveVmdk* vmdkp, CheckPointID ckpt,
			const std::vector<std::unique_ptr<Request>>* requestsp,
			const std::vector<RequestBlock*>& process,
			std::vector<RequestBlock*>& failed, Type type) : selfp_(selfp),
			vmdkp_(vmdkp), requestsp_(requestsp), ckpt_(ckpt),
			process_(process), failed_(failed), type_(type) {
	}

	void DoWork() {
		int rc = 0;
		switch (type_) {
		case kRead:
			rc = selfp_->ReadNow(vmdkp_, reqp_, process_, failed_);
			break;
		case kWrite:
			rc = selfp_->WriteNow(vmdkp_, reqp_, ckpt_, process_, failed_);
			break;
		case kBulkWrite:
			rc = selfp_->BulkWriteNow(vmdkp_, ckpt_, *requestsp_, process_,
				failed_);
			break;
		case kBulkRead:
			rc = selfp_->BulkReadNow(vmdkp_, *requestsp_, process_, failed_);
			break;
		}

		promise_.setValue(rc);
	}
};

void SuccessHandler::InitializeCompression(const config::VmdkConfig* configp) {
	hyc_disk_uuid_t uuid = {0, 0};
	const auto algo = configp->GetCompressionType();
	const auto type = get_compress_type(algo.c_str());
	log_assert(type != HYC_COMPRESS_UNKNOWN);
	compress_.ctxp_ = hyc_compress_ctx_init(uuid, type, 0, true);
	log_assert(compress_.ctxp_);

	uint32_t bs;
	auto bs_set = configp->GetBlockSize(bs);
	log_assert(bs_set == true);

	auto zero_buf = pio::NewRequestBuffer(bs);
	log_assert(zero_buf and zero_buf->Size() == bs);
	auto zbufp = zero_buf->Payload();
	std::memset(zbufp, 0, bs);

	auto sz = hyc_compress_get_maxlen(compress_.ctxp_, bs);
	auto compress_buffer = pio::NewRequestBuffer(sz);
	auto cbufp = compress_buffer->Payload();
	auto rc = hyc_compress(compress_.ctxp_, zbufp, bs, cbufp, &sz);
	log_assert(rc == HYC_COMPRESS_SUCCESS and sz < bs);

	compress_.buffer_ = pio::NewRequestBuffer(sz);
	log_assert(compress_.buffer_);
	std::memcpy(compress_.buffer_->Payload(), cbufp, sz);

	LOG(ERROR) << "Using Compression Algorithm " << algo
		<< " Compressed Buffer Size " << sz;
}

SuccessHandler::SuccessHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr), rd_(), gen_(rd_()) {
	delay_ = 0;
	enabled_ = configp->IsSuccessHandlerEnabled();
	if (enabled_) {
		delay_ = configp->GetSuccessHandlerDelay();
	}

	if (configp->IsCompressionEnabled() and configp->SuccessHandlerCompressData()) {
		InitializeCompression(configp);
	}

	if (not delay_) {
		return;
	}

	distr_ = std::uniform_int_distribution<int32_t>(1, delay_);
	work_.scheduler_ = std::make_unique<BackGroundWorkers>(1);
}

SuccessHandler::~SuccessHandler() {
	if (compress_.ctxp_) {
		hyc_compress_ctx_dinit(compress_.ctxp_);
	}
}

int SuccessHandler::ReadNowCommon(ActiveVmdk* vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	for (auto blockp : process) {
		if (pio_unlikely(blockp->IsReadHit())) {
			blockp->SetResult(0, RequestStatus::kSuccess);
			continue;
		}

		auto buffer = [&] () {
			if (compress_.ctxp_) {
				return pio::CloneRequestBuffer(compress_.buffer_.get());
			}
			auto buffer = NewRequestBuffer(vmdkp->BlockSize());
			if (pio_unlikely(not buffer)) {
				return buffer;
			}
			std::memset(buffer->Payload(), 0, buffer->Size());
			return buffer;
		} ();

		if (pio_unlikely(not buffer)) {
			blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
			failed.emplace_back(blockp);
			return -ENOMEM;
		}

		blockp->PushRequestBuffer(std::move(buffer));
		blockp->SetResult(0, RequestStatus::kSuccess);
	}
	return 0;
}

int SuccessHandler::ReadNow(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	return ReadNowCommon(vmdkp, process, failed);
}

folly::Future<int> SuccessHandler::ReadDelayed(ActiveVmdk *vmdkp,
		Request *reqp, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	auto usec = distr_(gen_);
	assert(usec > 0);
	auto read_work = std::make_shared<SuccessWork>(this, vmdkp, reqp, 0,
		process, failed, SuccessWork::kRead);
	auto work = std::make_shared<Work>(read_work,
		Work::Clock::now() + (1us * usec));
	work_.scheduler_->WorkAdd(work);

	std::lock_guard<std::mutex> lock(work_.mutex_);
	work_.scheduled_.emplace_back(work);
	return read_work->promise_.getFuture();
}

folly::Future<int> SuccessHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->Read(vmdkp, reqp, process, failed);
	}

	if (delay_) {
		return ReadDelayed(vmdkp, reqp, process, failed);
	}
	auto rc = ReadNow(vmdkp, reqp, process, failed);
	return folly::makeFuture(rc);
}

int SuccessHandler::WriteNow(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	for (auto blockp : process) {
		blockp->SetResult(0, RequestStatus::kSuccess);
	}
	return 0;
}

folly::Future<int> SuccessHandler::WriteDelayed(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	auto usec = distr_(gen_);
	assert(usec > 0);
	auto write_work = std::make_shared<SuccessWork>(this, vmdkp, reqp, ckpt,
		process, failed, SuccessWork::kWrite);
	auto work = std::make_shared<Work>(write_work,
		Work::Clock::now() + (1us * usec));
	work_.scheduler_->WorkAdd(work);

	std::lock_guard<std::mutex> lock(work_.mutex_);
	work_.scheduled_.emplace_back(work);
	return write_work->promise_.getFuture();
}

folly::Future<int> SuccessHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->Write(vmdkp, reqp, ckpt, process, failed);
	}

	if (delay_) {
		return WriteDelayed(vmdkp, reqp, ckpt, process, failed);
	}

	auto rc = WriteNow(vmdkp, reqp, ckpt, process, failed);
	return folly::makeFuture(rc);
}

folly::Future<int> SuccessHandler::ReadPopulate(ActiveVmdk *vmdkp,
		Request *reqp, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->ReadPopulate(vmdkp, reqp, process, failed);
	}

	failed.clear();
	for (auto blockp : process) {
		blockp->SetResult(0, RequestStatus::kSuccess);
	}
	return 0;
}

folly::Future<int> SuccessHandler::BulkWriteDelayed(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	auto usec = distr_(gen_);
	assert(usec > 0);
	auto write_work = std::make_shared<SuccessWork>(this, vmdkp, ckpt,
		&requests, process, failed, SuccessWork::kBulkWrite);
	auto work = std::make_shared<Work>(write_work,
		Work::Clock::now() + (1us * usec));
	work_.scheduler_->WorkAdd(work);

	std::lock_guard<std::mutex> lock(work_.mutex_);
	work_.scheduled_.emplace_back(work);
	return write_work->promise_.getFuture();
}

int SuccessHandler::BulkWriteNow(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	failed.clear();
	for (auto blockp : process) {
		blockp->SetResult(0, RequestStatus::kSuccess);
	}
	return 0;
}

folly::Future<int> SuccessHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->BulkWrite(vmdkp, ckpt, requests, process, failed);
	}

	if (delay_) {
		return BulkWriteDelayed(vmdkp, ckpt, requests, process, failed);
	}

	return BulkWriteNow(vmdkp, ckpt, requests, process, failed);
}

folly::Future<int> SuccessHandler::BulkReadDelayed(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	auto usec = distr_(gen_);
	assert(usec > 0);
	auto read = std::make_shared<SuccessWork>(this, vmdkp, 0, &requests,
		process, failed, SuccessWork::kBulkRead);
	auto work = std::make_shared<Work>(read, Work::Clock::now() + (1us * usec));
	work_.scheduler_->WorkAdd(work);

	std::lock_guard<std::mutex> lock(work_.mutex_);
	work_.scheduled_.emplace_back(work);
	return read->promise_.getFuture();
}

int SuccessHandler::BulkReadNow(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return ReadNowCommon(vmdkp, process, failed);
}

folly::Future<int> SuccessHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->BulkRead(vmdkp, requests, process, failed);
	}

	if (delay_) {
		return BulkReadDelayed(vmdkp, requests, process, failed);
	}

	return BulkReadNow(vmdkp, requests, process, failed);
}

folly::Future<int> SuccessHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	if (pio_likely(not enabled_)) {
		return nextp_->BulkReadPopulate(vmdkp, requests, process, failed);
	}

	if (delay_) {
		return BulkWriteDelayed(vmdkp, 0, requests, process, failed);
	}

	return BulkWriteNow(vmdkp, 0, requests, process, failed);
}
}
