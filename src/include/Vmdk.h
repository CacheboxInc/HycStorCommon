#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include <mutex>
#include <unordered_set>
#include <string>

#include <cstdint>

#include <folly/futures/Future.h>

#include "Common.h"
#include "IDs.h"
#include "Request.h"
#include "RequestHandler.h"

class Roaring;

namespace pio {

class CheckPoint {
public:
	CheckPoint(VmdkID vmdk_id, CheckPointID id);
	void SetModifiedBlocks(std::unordered_set<BlockID>&& blocks, BlockID first,
			BlockID last);
	std::unique_ptr<RequestBuffer> Serialize() const;
	~CheckPoint();
private:
	VmdkID                   vmdk_id_;
	CheckPointID             self_;
	std::unique_ptr<Roaring> blocks_bitset_;
	struct {
		BlockID first_;
		BlockID last_;
	} block_id_;
};

class Vmdk {
public:
	Vmdk(VmdkHandle handle, VmdkID&& vmdk_id);
	virtual ~Vmdk();
	const VmdkID& GetID() const noexcept;
	VmdkHandle GetHandle() const noexcept;

protected:
	VmdkHandle handle_;
	VmdkID     id_;
};

class ActiveVmdk : public Vmdk {
public:
	ActiveVmdk(VirtualMachine *vmp, VmdkHandle handle, VmdkID id,
		uint32_t block_size);
	virtual ~ActiveVmdk();

	void RegisterRequestHandler(std::unique_ptr<RequestHandler> handler);
	void SetEventFd(int eventfd) noexcept;

	folly::Future<int> Read(std::unique_ptr<Request> reqp);
	folly::Future<int> Write(std::unique_ptr<Request> reqp,
		CheckPointID ckpt_id);
	folly::Future<int> WriteSame(std::unique_ptr<Request> reqp,
		CheckPointID ckpt_id);
	folly::Future<int> TakeCheckPoint(CheckPointID check_point);

	uint32_t GetRequestResult(RequestResult* resultsp, uint32_t nresults,
		bool *has_morep);

public:
	size_t BlockSize() const;
	size_t BlockShift() const;
	size_t BlockMask() const;
	VirtualMachine* GetVM() const noexcept;

private:
	folly::Future<int> WriteCommon(std::unique_ptr<Request> reqp,
		CheckPointID ckpt_id);
	int RequestComplete(std::unique_ptr<Request> reqp);

private:
	VirtualMachine *vmp_{nullptr};
	Vmdk           *parentp_{nullptr};
	int            eventfd_{-1};

	uint32_t block_shift_;

	struct {
		std::mutex mutex_;
		std::unordered_set<BlockID> modified_;
		BlockID min_;
		BlockID max_;
	} blocks_;

	struct {
		std::mutex mutex_;
		std::vector<std::unique_ptr<CheckPoint>> unflushed_;
		std::vector<std::unique_ptr<CheckPoint>> flushed_;
	} checkpoints_;

	struct {
		std::atomic<uint64_t> writes_in_progress_;
		std::atomic<uint64_t> reads_in_progress_;
	} stats_;

	struct {
		std::mutex mutex_;
		std::vector<std::unique_ptr<Request>> complete_;
	} requests_;

	std::unique_ptr<RequestHandler> headp_{nullptr};
};

class SnapshotVmdk : public Vmdk {
public:
private:
	Vmdk*               parentp_{nullptr};
	std::vector<Vmdk *> chidren_;
};

}
