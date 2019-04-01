#pragma once

#include <sys/eventfd.h>
#include <libaio.h>

#include <memory>
#include <thread>
#include <mutex>

#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>

#include "CommonMacros.h"

namespace pio {

class LibAio {
public:
	static constexpr uint16_t kMaxIOCBToSubmit = 128;
	using Result = ssize_t;

	LibAio(folly::EventBase* basep, uint16_t capacity);
	~LibAio();
	folly::Future<Result> AsyncRead(int fd, void* buf, size_t count, uint64_t offset);
	folly::Future<Result> AsyncWrite(int fd, void* buf, size_t count, uint64_t offset);
	void Drain() noexcept;

private:
	class AIO {
	public:
		AIO(const AIO&) = delete;
		AIO(AIO&&) = delete;

		AIO() noexcept;
		void PrepareRead(int event_fd, int io_fd, void* buf, size_t count, uint64_t offset) noexcept;
		void PrepareWrite(int event_fd, int io_fd, void* buf, size_t count, uint64_t offset) noexcept;
		void Complete(ssize_t result) noexcept;

		friend class LibAio;
	private:
		struct iocb iocb_;
		folly::Promise<LibAio::Result> promise_;
	};

	class EventFDHandler : public folly::EventHandler {
	public:
		EventFDHandler(LibAio* aiop, folly::EventBase* basep, int fd) :
				EventHandler(basep, fd), aiop_(aiop) {
		}

		void handlerReady(uint16_t events) noexcept {
			if (events & EventHandler::READ) {
				aiop_->ReadEventFD();
			}
			aiop_->Drain();
		}
	private:
		LibAio* aiop_{};
	};

private:
	void Setup();
	folly::Future<Result> ScheduleIO(AIO* iop);
	void SubmitInternal() noexcept;
	void HandleIOCompletions(const int to_reap) const;
	int ReadEventFD() noexcept;
	int EventFD() const noexcept;
	void CompletePending(int from, int size, int error) noexcept;

private:
	const uint16_t capacity_{kMaxIOCBToSubmit};

	struct {
		folly::EventBase* basep_{nullptr};
		int event_fd_{-1};
		std::unique_ptr<EventFDHandler> handler_;
	} event_;

	struct {
		io_context_t context_;

		mutable std::mutex mutex_;
		std::queue<AIO*> iocbs_;

		struct iocb* iocbpp_[kMaxIOCBToSubmit];
		struct io_event events_[kMaxIOCBToSubmit];
	} io_;

	struct {
		mutable std::atomic<int64_t> total_{0};
		mutable std::atomic<int64_t> complete_{0};
		mutable std::atomic<int64_t> failed_{0};
		mutable std::atomic<uint16_t> in_progress_{0};
	} stats_;
};

}
