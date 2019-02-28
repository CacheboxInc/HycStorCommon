#pragma once

#include "RangeLock.h"

namespace pio {

using range_t = std::pair<uint64_t, uint64_t>;

class LockHandler : public RequestHandler {
public:
	static constexpr char kName[] = "RangeLockHandler";
	LockHandler();
	virtual ~LockHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;

	virtual folly::Future<int> BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Move(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> BulkMove(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> Delete(ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID id,
		const std::pair<::ondisk::BlockID, ::ondisk::BlockID> range) override;
private:
	template <typename Func>
	auto TakeLockAndInvoke(::ondisk::BlockID start, ::ondisk::BlockID end, Func&& func) {
		RangeLock::LockGuard lock(range_lock_.get(), start, end);
		return lock.Lock()
		.then([lock = std::move(lock), func = std::forward<Func>(func)] (int rc) mutable {
			if (pio_unlikely(not lock.IsLocked() or rc < 0)) {
				LOG(ERROR) << "Failed to take lock";
				return folly::makeFuture(rc ? rc : -EINVAL);
			}
			return func()
			.then([lock = std::move(lock)] (auto& rc) {
				return std::move(rc);
			});
		});
	}

	std::vector<pio::RangeLock::range_t> Ranges(
		const std::vector<std::unique_ptr<Request>>& requests);
private:
	const std::unique_ptr<RangeLock::RangeLock> range_lock_;
};

}
