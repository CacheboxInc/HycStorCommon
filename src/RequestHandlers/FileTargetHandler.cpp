#include <vector>
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
#if 0
#include "cksum.h"
#endif

#ifdef FILETARGET_ASYNC
#define MAX_IOs 8192
#define MAX_EVENTS 32
#define MAX_EPOLL_EVENTS 32
typedef void (*event_handler_t)(int fd, int events, void *data);
void process_completions(int fd, int input_events, void *data);
int event_add(int fd, int ep_fd, int events, event_handler_t handler, void *data);
#endif

using namespace ::ondisk;
#if 0
#define INJECT_WRITE_FAILURE 1
#endif

namespace pio {
#ifdef FILETARGET_ASYNC
int event_add(int fd, int ep_fd, int events, event_handler_t handler, void *data)
{
	struct epoll_event ev;
	int err;
	/* TBD : Need to free this */
	struct event_data *data_ptr = new event_data;
	if (!data_ptr) {
		return -ENOMEM;
	}

	memset(data_ptr, 0, sizeof(*data_ptr));
	data_ptr->data = data;
	data_ptr->handler = handler;
	data_ptr->fd = fd;

	memset(&ev, 0, sizeof(ev));
	ev.events = events;
	ev.data.ptr = data_ptr;
	err = epoll_ctl(ep_fd, EPOLL_CTL_ADD, fd, &ev);
	if (err) {
		LOG(ERROR) << "Cannot add fd, err::" << err;
		delete data_ptr;
	}
	return err;
}

void process_completions(int fd, int input_events, void *data) {

	struct io_event* events = new io_event[MAX_EVENTS];
	struct AIORequestBlock *reqblock;
	int ret;
	struct io_event *e;
	FileTargetHandler* obj = (FileTargetHandler*) data;

	/* read from eventfd returns 8-byte int, fails with the error EINVAL
	   if the size of the supplied buffer is less than 8 bytes */

	uint64_t evts_complete;
	unsigned int ncomplete, num_events;

retry_read:
	ret = read(obj->afd_, &evts_complete, sizeof(evts_complete));
	if (pio_unlikely(ret < 0)) {
		LOG(ERROR) << "failed to read AIO completions, ret::" << ret;
		if (errno == EAGAIN || errno == EINTR)
			goto retry_read;

		return;
	}

	ncomplete = (unsigned int) evts_complete;
	while(ncomplete){
		if (ncomplete > MAX_EVENTS){
			num_events = MAX_EVENTS;
		} else {
			num_events = ncomplete;
		}

retry_getevts:
		ret = io_getevents(obj->ctx_, 1, num_events, events, NULL);
		if (pio_likely(ret >= 0)) {
			num_events = ret;
		} else {
			if (ret == -EINTR) {
				goto retry_getevts;
			}
			LOG(ERROR) << __func__ << "io_getevents failed, err:" << -ret;
			return;
		}

		for (unsigned int i = 0; i < num_events; i++) {
			e = &events[i];
			reqblock = reinterpret_cast<AIORequestBlock*> (e->data);
			if (pio_likely(e->res == reqblock->destp_->Size())) {
				if (reqblock->req_->req_type_ == ReqType::OP_READ) {
					reqblock->blockp_->PushRequestBuffer(std::move(reqblock->destp_));
				}
			} else {
				LOG(ERROR) << __func__ << "IO FAILED at offset ::"<< reqblock->blockp_->GetAlignedOffset()
						<< " size::" << reqblock->destp_->Size();
				reqblock->req_->status_ = -1;
			}

			reqblock->req_->n_pending_--;
			if(!reqblock->req_->n_pending_) {
				reqblock->req_->promise_->setValue(reqblock->req_->status_);
			}
		}

		ncomplete -= num_events;
	}

	delete [] events;
	return;
}
#endif

FileTargetHandler::FileTargetHandler(const config::VmdkConfig* configp) :
		RequestHandler(nullptr) {
	/* FileTargetHandler with encryption and compression not supported */
	log_assert(not configp->IsCompressionEnabled());
	log_assert(not configp->IsEncryptionEnabled());

	enabled_ = configp->IsFileTargetEnabled();
	create_file_ = configp->GetFileTargetCreate();

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
		fd_ = ::open(file_path_.c_str(), O_RDWR | O_SYNC | O_DIRECT| O_CREAT, 0777);
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

#ifdef FILETARGET_ASYNC

	/* Create Async IO context */
	memset(&ctx_, 0, sizeof(ctx_));
	if(io_setup(MAX_IOs, &ctx_)!=0){//init
		LOG(ERROR) << __func__ << "Failed to setup IO context";
		throw std::runtime_error("Failed to setup IO context");
	}
	LOG(ERROR) << __func__ << "AIO context setup DONE";

	ep_fd_ = epoll_create(8);
	if (ep_fd_ < 0) {
		close(fd_);
		LOG(ERROR) << "failed to create epoll::" << ep_fd_;
		throw std::runtime_error("failed to create epoll::");
	}

	afd_ = eventfd(0, O_NONBLOCK | O_CLOEXEC);
	if (afd_ < 0) {
		close(ep_fd_);
		close(fd_);
		LOG(ERROR) << "failed to create eventfd::" << afd_;
		throw std::runtime_error("failed to create eventfd::");
	}

	int ret = event_add(afd_, ep_fd_, EPOLLIN | EPOLLET, process_completions, this);
	if (ret) {
		close(fd_);
		close(ep_fd_);
		close(afd_);
		throw std::runtime_error("failed to create eventfd::");
	}

	thread_ = std::thread(&FileTargetHandler::GatherEvents, this);
#endif

}

FileTargetHandler::~FileTargetHandler() {
	LOG(ERROR) << __func__ << "Calling FileTargetHandler destructor";
#ifdef FILETARGET_ASYNC
	/* Set thread running false and Write on afd_ to wakeup the epoll_wait */
	thread_running_ = false;
	if (afd_ != -1) {
		uint64_t value = 1;
		auto rc = write(afd_, &value, sizeof(value));
		(void) rc;
		LOG(ERROR) << __func__ << "Waiting for event gather thread to exit";
		thread_.join();
	}

	if (afd_ != -1 && ep_fd_ != -1) {
		int ret = epoll_ctl(ep_fd_, EPOLL_CTL_DEL, afd_, NULL);
		if (ret != 0) {
			LOG(ERROR) << "Failed to remove aync fd from poll event loop. ret::" << ret;
		}
		::close(afd_);
	}

	if (ep_fd_ != -1) {
		close(ep_fd_);
	}

	io_destroy(ctx_);
#endif

	if (fd_ != -1) {
		::close(fd_);
	}
	if (enabled_) {
		if (create_file_) {
			LOG(ERROR) << __func__ << "destructor deleting file";
			::remove(file_path_.c_str());
		}
	}

	LOG(ERROR) << __func__ << "Waiting for thread to exit complete";
}

const std::string& FileTargetHandler::GetFileCachePath() const {
	return file_path_;
}

#ifdef FILETARGET_ASYNC
AIORequest::AIORequest(int cnt, ReqType type) {
	cnt_ = cnt;
	n_pending_ = cnt;
	iocbs_ = new iocb[cnt];
	if (not iocbs_) {
		LOG(ERROR) << __func__ << "Failed to create iocbs datastr";
		throw std::runtime_error("Failed to create iocbs datastr");
	}

	promise_ = std::make_shared<folly::Promise<int>>();
	req_type_ = type;
}

AIORequest::~AIORequest() {
	if (iocbs_)
		delete [] iocbs_;
}

void FileTargetHandler::GatherEvents(void) {
	struct epoll_event* epoll_events = new epoll_event[MAX_EPOLL_EVENTS];
	int nevent, i;
        struct event_data *data_ptr;

	LOG(ERROR) << __func__ << "GatherEvents thread starts";
	while(thread_running_){
		nevent = epoll_wait(ep_fd_, epoll_events, MAX_EPOLL_EVENTS, -1);
		if (!thread_running_) {
			LOG(ERROR) << __func__ << "Got thread running false, exiting";
			break;
		}
		if (nevent < 0) {
			if (errno != EINTR) {
				LOG(ERROR) << "Error in epoll_wait";
				break;
			}
		} else if (nevent) {
			for (i = 0; i < nevent; i++) {
				data_ptr = (struct event_data *) epoll_events[i].data.ptr;
				data_ptr->handler(fd_, epoll_events[i].events, data_ptr->data);
			}
		}
	}

	LOG(ERROR) << __func__ << "Event loop thread exiting";
	if (thread_running_) {
		/* Poll is exiting due to error case */
		thread_running_ = false;
	}

	LOG(ERROR) << __func__ << "Exiting";
	delete [] epoll_events;
}

folly::Future<int> FileTargetHandler::Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	if (pio_unlikely(not failed.empty() || process.empty())) {
		LOG(ERROR) << __func__ << "Process list is empty, count::" << process.size();
		return -EINVAL;
	}

	if (pio_unlikely(fd_ == -1 || enabled_ == false)) {
		LOG(ERROR) << __func__ << "Device is not already open"
			" or FileTarget is not enabled, fd::-" << fd_;
		return -EINVAL;
	}

	/* Get the list of iocb which need to be created */
	int cnt = process.size();
	pending_io_ += cnt;
	auto aio_req = std::make_shared<AIORequest>(cnt, ReqType::OP_READ);
	int idx = 0;
	struct iocb *iocb;

	for (auto blockp : process) {
		iocb = &aio_req->iocbs_[idx];
		auto reqblock = std::make_shared<AIORequestBlock>();
		reqblock->req_ = aio_req.get();
		reqblock->blockp_ = blockp;
		reqblock->destp_ = NewAlignedRequestBuffer(vmdkp->BlockSize());
		aio_req->reqblocks_list.push_back(reqblock);
		#if 0
		LOG(ERROR) << __func__ << "::Create Req blocks offset ::"<< blockp->GetAlignedOffset()
				<< " size ::" << reqblock->destp_->Size();
		#endif
		io_prep_pread(iocb, fd_, reqblock->destp_->Payload(),
			reqblock->destp_->Size(), blockp->GetAlignedOffset());
		io_set_eventfd(iocb, afd_);
		iocb->data = (void *) reqblock.get();
		idx++;
	}

	log_assert(idx == cnt);
	int start_idx = 0, remaining = cnt;
	int done_cnt = 0;
	int res = 0, i, j;
	struct iocb *piocb_arr[cnt];

retry:
	memset(piocb_arr, 0 , sizeof(piocb_arr));
	for (i = start_idx, j = 0; j < remaining; i++, j++) {
		piocb_arr[j] = &aio_req->iocbs_[i];
	}

	res = io_submit(ctx_, remaining, piocb_arr);
	if (pio_unlikely(res < 0)) {
		if (res == -EAGAIN) {
			usleep(100000);
			goto retry;
		}
		else {
			LOG(ERROR) << __func__ << "Failed to submit IO request, pending cnt is::"
					<< pending_io_ << "::" << res;
			return 1;
		}
	}

	done_cnt += res;
	if (pio_unlikely(res < remaining)) {
		remaining = cnt - done_cnt;
		start_idx = done_cnt;
		goto retry;
	}

	log_assert(done_cnt == cnt);
	return aio_req->promise_->getFuture()
	.then([this, aio_req, &process] (int rc) mutable {
		pending_io_ -= process.size();
		return rc;
	});
}

folly::Future<int> FileTargetHandler::Write(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {

#ifdef INJECT_WRITE_FAILURE
	static std::atomic<int> count = 0;
#endif
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

	/* Get the list of iocb which need to be created */
	int cnt = process.size();
	pending_io_ += cnt;
	auto aio_req = std::make_shared<AIORequest>(cnt, ReqType::OP_WRITE);
	int idx = 0;
	struct iocb *iocb;

	for (auto blockp : process) {
		auto srcp = blockp->GetRequestBufferAtBack();
		log_assert(srcp->Size() == vmdkp->BlockSize());

		iocb = &aio_req->iocbs_[idx];
		auto reqblock = std::make_shared<AIORequestBlock>();
		reqblock->req_ = aio_req.get();
		reqblock->blockp_ = blockp;
		reqblock->destp_ = NewAlignedRequestBuffer(vmdkp->BlockSize());
		::memcpy(reqblock->destp_->Payload(), srcp->Payload(), srcp->Size());
		aio_req->reqblocks_list.push_back(reqblock);
		#if 0
		LOG(ERROR) << __func__ << "::Create Req blocks offset ::"<< blockp->GetAlignedOffset()
				<< " size ::" << reqblock->destp_->Size();
		#endif
		io_prep_pwrite(iocb, fd_, reqblock->destp_->Payload(),
			reqblock->destp_->Size(), blockp->GetAlignedOffset());

		#if 0
		LOG(ERROR) << __func__ << "FlushWrite [Cksum]" << blockp->GetAlignedOffset() <<
				":" << reqblock->destp_->Size() <<
				":" << crc_t10dif((unsigned char *) reqblock->destp_->Payload(),
					reqblock->destp_->Size());
		#endif

		io_set_eventfd(iocb, afd_);
		iocb->data = (void *) reqblock.get();
		idx++;
	}

	log_assert(idx == cnt);
	int start_idx = 0, remaining = cnt;
	int done_cnt = 0;
	int res = 0, i, j;
	struct iocb *piocb_arr[cnt];

retry:
	memset(piocb_arr, 0 , sizeof(piocb_arr));
	for (i = start_idx, j = 0; j < remaining; i++, j++) {
		piocb_arr[j] = &aio_req->iocbs_[i];
	}

	res = io_submit(ctx_, remaining, piocb_arr);
	if (pio_unlikely(res < 0)) {
		if (res == -EAGAIN) {
			usleep(100000);
			goto retry;
		}
		else {
			LOG(ERROR) << __func__ << "Failed to submit IO request, pending cnt is::"
					<< pending_io_ << "::" << res;
			return 1;
		}
	}

	done_cnt += res;
	if (pio_unlikely(res < remaining)) {
		remaining = cnt - done_cnt;
		start_idx = done_cnt;
		goto retry;
	}

	log_assert(done_cnt == cnt);
	return aio_req->promise_->getFuture()
	.then([this, aio_req, &process] (int rc) mutable {
		pending_io_ -= process.size();

#ifdef INJECT_WRITE_FAILURE
		/* Fail every 3rd IO */
		if (++count >= 3) {
			LOG(ERROR) << __func__ << "Injecting Write error, count::" << count;
			count = 0;
			return -EIO;
		}
#endif
		return rc;
	});
}

#else

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
#endif

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

folly::Future<int> FileTargetHandler::BulkReadPopulate(ActiveVmdk* vmdkp,
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

folly::Future<int> FileTargetHandler::BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return this->Read(vmdkp, nullptr, process, failed);
}

folly::Future<int> FileTargetHandler::BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) {
	return this->Write(vmdkp, nullptr, ckpt, process, failed);
}
}
