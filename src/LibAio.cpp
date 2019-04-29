#include <chrono>
#include <thread>

#include "LibAio.h"
#include "CommonMacros.h"

using namespace folly;
using namespace std::chrono_literals;

namespace pio {

LibAio::AIO::AIO() noexcept {
}

void LibAio::AIO::PrepareRead(int event_fd,
		int io_fd,
		void* buf,
		size_t count,
		uint64_t offset) noexcept {
	io_prep_pread(&iocb_, io_fd, buf, count, offset);
	io_set_eventfd(&iocb_, event_fd);
	iocb_.data = reinterpret_cast<void*>(this);
}

void LibAio::AIO::PrepareWrite(int event_fd,
		int io_fd,
		void* buf,
		size_t count,
		uint64_t offset) noexcept {
	io_prep_pwrite(&iocb_, io_fd, buf, count, offset);
	io_set_eventfd(&iocb_, event_fd);
	iocb_.data = reinterpret_cast<void*>(this);
}

void LibAio::AIO::Complete(ssize_t result) noexcept {
	promise_.setValue(result);
}

LibAio::LibAio(folly::EventBase* basep,
			uint16_t capacity
		) :
			capacity_(std::min(capacity, kMaxIOCBToSubmit)) {
	event_.basep_= basep;
	Setup();
}

LibAio::~LibAio() {
	event_.basep_->runInEventBaseThreadAndWait([this] () mutable {
		event_.handler_ = nullptr;
	});

	while (not io_.iocbs_.empty()) {
		AIO* iop = io_.iocbs_.front();
		iop->Complete(-EBADF);
		io_.iocbs_.pop();
	}
	io_destroy(io_.context_);
	if (event_.event_fd_ >= 0) {
		close(event_.event_fd_);
	}
	event_.event_fd_ = -1;
}

void LibAio::Setup() {
	std::memset(&io_.context_, 0, sizeof(io_.context_));
	auto rc = io_setup(capacity_, &io_.context_);
	if (pio_unlikely(rc < 0)) {
		throw std::runtime_error("LibAio: failed to initialize IO context.");
	}

	event_.event_fd_ = ::eventfd(0, EFD_NONBLOCK);
	if (event_.event_fd_ < 0) {
		int rc = errno;
		LOG(ERROR) << "LibAio: creating eventfd failed with error " << rc;
		throw std::runtime_error("LibAio: eventfd creation failed.");
	}

	event_.handler_ = std::make_unique<EventFDHandler>(this, event_.basep_, event_.event_fd_);
	if (not event_.handler_) {
		LOG(ERROR) << "LibAio: memory allocation failed";
		throw std::bad_alloc();
	}

	event_.handler_->registerHandler(EventHandler::READ | EventHandler::PERSIST);
}

int LibAio::EventFD() const noexcept {
	return event_.event_fd_;
}

void LibAio::Drain() noexcept {
	std::lock_guard<std::mutex> lock(io_.mutex_);
	if (stats_.in_progress_ > capacity_) {
		return;
	}

	const int to_submit = std::min(
		static_cast<size_t>(capacity_ - stats_.in_progress_),
		io_.iocbs_.size()
	);
	for (int i = 0; i < to_submit; ++i) {
		AIO* iop = io_.iocbs_.front();
		io_.iocbpp_[i] = &iop->iocb_;
		io_.iocbs_.pop();
	}

	int submitted = 0;
	int failed_count = 0;
	while (submitted < to_submit) {
		int rc = io_submit(io_.context_, to_submit - submitted,
				reinterpret_cast<struct iocb **>(&io_.iocbpp_[submitted]));
		if (pio_unlikely(rc < 0)) {
			++failed_count;
			if (errno == EAGAIN and failed_count < 5) {
				std::this_thread::sleep_for(10ms);
				continue;
			}

			stats_.failed_ += to_submit - submitted;
			rc = errno;
			LOG(ERROR) << "LibAio: io_submit faild with error " << rc
				<< " total " << stats_.total_
				<< " complete " << stats_.complete_
				<< " progress " << stats_.in_progress_
				<< " failed " << stats_.failed_;
			CompletePending(submitted, to_submit, -rc);
			return;
		}
		submitted += rc;
	}
	stats_.total_ += to_submit;
	stats_.in_progress_ += to_submit;
}

folly::Future<LibAio::Result> LibAio::ScheduleIO(AIO* iop) {
	std::lock_guard<std::mutex> lock(io_.mutex_);
	io_.iocbs_.emplace(iop);
	return iop->promise_.getFuture();
}

void LibAio::CompletePending(int from, int size, int error) noexcept {
	for (int i = from; i < size; ++i) {
		auto iop = reinterpret_cast<AIO*>(io_.iocbpp_[i]->data);
		iop->Complete(error);
	}
}

void LibAio::HandleIOCompletions(const int to_reap) const {
	auto IOResult = [] (const struct io_event* ep) {
		return ((ssize_t)(((uint64_t)ep->res2 << 32) | ep->res));
	};

	for (auto ep = io_.events_; ep < io_.events_ + to_reap; ++ep) {
		auto iop = reinterpret_cast<AIO*>(ep->data);
		iop->Complete(IOResult(ep));
	}
	stats_.in_progress_ -= to_reap;
	stats_.complete_ += to_reap;
}

int LibAio::ReadEventFD() noexcept {
	int rc;
	eventfd_t nevents;
	while (1) {
		nevents = 0;
		rc = ::eventfd_read(event_.event_fd_, &nevents);
		if (rc < 0 or nevents == 0) {
			if (rc < 0 and errno != EAGAIN) {
				rc = -errno;
				LOG(ERROR) << "LibAio: eventfd_read failed " << -rc;
			}
			break;
		}

		while (nevents > 0) {
			const long to_reap = std::min(nevents,
				sizeof(io_.events_) / sizeof(io_.events_[0]));

			rc = io_getevents(io_.context_, to_reap, to_reap, io_.events_, nullptr);
			if (rc != to_reap) {
				rc = errno;
				LOG(ERROR) << "failed to get events " << errno;
				return -rc;
			}

			HandleIOCompletions(to_reap);
			nevents -= to_reap;
		}
	}
	return rc;
}

folly::Future<LibAio::Result> LibAio::AsyncRead(int fd,
			void* buf,
			size_t count,
			uint64_t offset) {
	auto io = std::make_unique<LibAio::AIO>();
	if (pio_unlikely(not io)) {
		LOG(ERROR) << "LibAio: allocating memory for IO failed";
		return -ENOMEM;
	}

	auto iop = io.get();
	iop->PrepareRead(EventFD(), fd, buf, count, offset);

	return ScheduleIO(iop)
	.then([io = std::move(io)] (const auto& rc) mutable {
		return rc;
	});
}

folly::Future<LibAio::Result> LibAio::AsyncWrite(int fd,
		void* buf,
		size_t count,
		uint64_t offset) {
	auto io = std::make_unique<LibAio::AIO>();
	if (pio_unlikely(not io)) {
		LOG(ERROR) << "LibAio: allocating memory for IO failed";
		return -ENOMEM;
	}

	auto iop = io.get();
	iop->PrepareWrite(EventFD(), fd, buf, count, offset);

	return ScheduleIO(iop)
	.then([io = std::move(io)] (const auto& rc) mutable {
		return rc;
	});
}

}
