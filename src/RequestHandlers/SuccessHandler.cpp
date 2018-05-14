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
#include "BackGroundWorker.h"

using namespace std::chrono_literals;
using namespace ::ondisk;

namespace pio {
struct SuccessWork {
	enum Type {
		kRead,
		kWrite,
	};

	SuccessHandler* selfp_;
	ActiveVmdk* vmdkp_;
	Request* reqp_;
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

	void DoWork() {
		int rc = 0;
		switch (type_) {
		case kRead:
			rc = selfp_->ReadNow(vmdkp_, reqp_, process_, failed_);
			break;
		case kWrite:
			rc = selfp_->WriteNow(vmdkp_, reqp_, ckpt_, process_, failed_);
			break;
		}

		promise_.setValue(rc);
	}
};

SuccessHandler::SuccessHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr), rd_(), gen_(rd_()) {
	delay_ = 0;
	enabled_ = configp->IsSuccessHandlerEnabled();
	if (enabled_) {
		delay_ = configp->GetSuccessHandlerDelay();
	}

	if (not delay_) {
		return;
	}

	distr_ = std::uniform_int_distribution<int32_t>(1, delay_);
	work_.scheduler_ = std::make_unique<BackGroundWorkers>(1);
}

SuccessHandler::~SuccessHandler() {
}

int SuccessHandler::ReadNow(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) {
	failed.clear();
	for (auto blockp : process) {
		if (pio_unlikely(blockp->IsReadHit())) {
			blockp->SetResult(0, RequestStatus::kSuccess);
			continue;
		}

		auto destp = NewRequestBuffer(vmdkp->BlockSize());
		if (pio_unlikely(not destp)) {
			blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
			failed.emplace_back(blockp);
			return -ENOMEM;
		}

		::memset(destp->Payload(), 0, destp->Size());
		blockp->PushRequestBuffer(std::move(destp));
		blockp->SetResult(0, RequestStatus::kSuccess);
	}
	return 0;
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
}
