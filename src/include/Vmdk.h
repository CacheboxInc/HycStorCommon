#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include <mutex>
#include <unordered_set>
#include <string>

#include <cstdint>

#include <folly/futures/Future.h>
#include <roaring/roaring.hh>

#include "DaemonCommon.h"
#include "IDs.h"
#include "VirtualMachine.h"
#include "Request.h"
#include "RequestHandler.h"
#include "AeroConn.h"

namespace pio {

/* forward declaration for Pimpl */
namespace config {
	class VmdkConfig;
	class AeroConfig;
}

class CheckPoint {
public:
	CheckPoint(VmdkID vmdk_id, CheckPointID id);
	void SetModifiedBlocks(const std::unordered_set<BlockID>& blocks,
		BlockID first, BlockID last);
	std::unique_ptr<RequestBuffer> Serialize() const;
	~CheckPoint();
	bool operator < (const CheckPoint& rhs) const noexcept;
	CheckPointID ID() const noexcept;

	std::pair<BlockID, BlockID> Blocks() const noexcept;
	const Roaring& GetRoaringBitMap() const noexcept;
private:
	VmdkID vmdk_id_;
	CheckPointID self_;
	Roaring blocks_bitset_;
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
	ActiveVmdk(VmdkHandle handle, VmdkID vmdk_id, VirtualMachine *vmp,
		const std::string& config, std::shared_ptr<AeroSpikeConn> aero_conn);
	virtual ~ActiveVmdk();

	void RegisterRequestHandler(std::unique_ptr<RequestHandler> handler);
	void SetEventFd(int eventfd) noexcept;

	folly::Future<int> Read(Request* reqp);
	folly::Future<int> Write(Request* reqp, CheckPointID ckpt_id);
	folly::Future<int> WriteSame(Request* reqp, CheckPointID ckpt_id);
	folly::Future<int> TakeCheckPoint(CheckPointID check_point);
	const CheckPoint* GetCheckPoint(CheckPointID ckpt_id) const;

public:
	size_t BlockSize() const;
	size_t BlockShift() const;
	size_t BlockMask() const;
	VirtualMachine* GetVM() const noexcept;
	const config::VmdkConfig* GetJsonConfig() const noexcept;
	AeroSpikeConn* GetAeroConnection() const noexcept;

private:
	folly::Future<int> WriteCommon(Request* reqp, CheckPointID ckpt_id);
	int WriteComplete(Request* reqp);
	void CopyDirtyBlocksSet(std::unordered_set<BlockID>& blocks,
		BlockID& start, BlockID& end);

private:
	VirtualMachine *vmp_{nullptr};
	//Vmdk           *parentp_{nullptr};
	int            eventfd_{-1};

	uint32_t block_shift_{0};
	std::unique_ptr<config::VmdkConfig> config_;
	std::shared_ptr<AeroSpikeConn> aero_conn_{nullptr};

	struct {
		std::mutex mutex_;
		std::unordered_set<BlockID> modified_;
		BlockID min_{kBlockIDMax};
		BlockID max_{kBlockIDMin};
	} blocks_;

	struct {
		mutable std::mutex mutex_;
		std::vector<std::unique_ptr<CheckPoint>> unflushed_;
		std::vector<std::unique_ptr<CheckPoint>> flushed_;
	} checkpoints_;

	struct {
		std::atomic<uint64_t> writes_in_progress_{0};
		std::atomic<uint64_t> reads_in_progress_{0};
	} stats_;

	std::unique_ptr<RequestHandler> headp_{nullptr};

private:
	static constexpr uint32_t kDefaultBlockSize{4096};
};

class SnapshotVmdk : public Vmdk {
public:
	SnapshotVmdk(VmdkHandle handle, VmdkID vmdk_id) :
			Vmdk(handle, std::move(vmdk_id)) {
	}
private:
	//Vmdk*               parentp_{nullptr};
	std::vector<Vmdk *> chidren_;
};

}
