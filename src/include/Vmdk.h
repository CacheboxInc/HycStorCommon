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
	Vmdk(VmdkID&& vmdk_id);
	virtual ~Vmdk();
	const VmdkID& GetVmdkID() const;

protected:
	VmdkID vmdk_id_;
};

class ActiveVmdk : public Vmdk {
public:
	ActiveVmdk(VirtualMachine *vmp, VmdkID vmdk_id, uint32_t block_size);
	virtual ~ActiveVmdk();

public:
	void RegisterRequestHandler(std::unique_ptr<RequestHandler> handler);

	folly::Future<int> Read(RequestID req_id, void* bufp, size_t buf_size,
		Offset offset);
	folly::Future<int> Write(RequestID req_id, CheckPointID ckpt_id, void* bufp,
		size_t buf_size, Offset offset);
	folly::Future<int> WriteSame(RequestID req_id, CheckPointID ckpt_id,
		void* bufp, size_t buf_size, size_t transfer_size, Offset offset);
	folly::Future<int> TakeCheckPoint(CheckPointID check_point);
public:
	size_t BlockSize() const;
	size_t BlockShift() const;
	size_t BlockMask() const;

private:
	folly::Future<int> WriteCommon(RequestID req_id, Request::Type type,
		CheckPointID ckpt_id, void* bufp, size_t buf_size, size_t transfer_size,
		Offset offset);

private:
	VirtualMachine *vmp_{nullptr};
	Vmdk           *parentp_{nullptr};

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

	std::unique_ptr<RequestHandler> headp_{nullptr};
};

class SnapshotVmdk : public Vmdk {
public:
private:
	Vmdk*               parentp_{nullptr};
	std::vector<Vmdk *> chidren_;
};

}
