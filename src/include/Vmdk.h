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
#include "MetaDataKV.h"

namespace pio {

/* forward declaration for Pimpl */
namespace config {
	class VmdkConfig;
	class AeroConfig;
}

using CheckPoints = std::pair<CheckPointID, CheckPointID>;

class CheckPoint {
public:
	CheckPoint(VmdkID vmdk_id, CheckPointID id);
	void SetModifiedBlocks(const std::unordered_set<BlockID>& blocks);
	std::string Serialize() const;
	std::string SerializationKey() const;
	~CheckPoint();
	bool operator < (const CheckPoint& rhs) const noexcept;
	CheckPointID ID() const noexcept;

	std::pair<BlockID, BlockID> Blocks() const noexcept;
	const Roaring& GetRoaringBitMap() const noexcept;
	void SetSerialized() noexcept;
	bool IsSerialized() const noexcept;
	void SetFlushed() noexcept;
	bool IsFlushed() const noexcept;
private:
	VmdkID vmdk_id_;
	CheckPointID self_;
	Roaring blocks_bitset_;
	struct {
		BlockID first_{0};
		BlockID last_{0};
	} block_id_;

	bool serialized_{false};
	bool flushed_{false};
public:
	static const std::string kCheckPoint;
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
		const std::string& config);
	virtual ~ActiveVmdk();

	void RegisterRequestHandler(std::unique_ptr<RequestHandler> handler);
	void SetEventFd(int eventfd) noexcept;

	folly::Future<int> Read(Request* reqp, const CheckPoints& min_max);
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

private:
	folly::Future<int> WriteCommon(Request* reqp, CheckPointID ckpt_id);
	int WriteComplete(Request* reqp, CheckPointID ckpt_id);
	std::optional<std::unordered_set<BlockID>>
		CopyDirtyBlocksSet(CheckPointID ckpt_id);
	void RemoveDirtyBlockSet(CheckPointID ckpt_id);
	CheckPointID GetModifiedCheckPoint(BlockID block,
		const CheckPoints& min_max) const;
	void SetReadCheckPointId(const std::vector<RequestBlock*>& blockps,
		const CheckPoints& min_max) const;

private:
	VirtualMachine *vmp_{nullptr};
	//Vmdk           *parentp_{nullptr};
	int            eventfd_{-1};

	uint32_t block_shift_{0};
	std::unique_ptr<config::VmdkConfig> config_;

	struct {
		mutable std::mutex mutex_;
		std::unordered_map<CheckPointID, std::unordered_set<BlockID>> modified_;
	} blocks_;

	struct {
		/* Last successful CheckPointID on VMDK */
		std::atomic<CheckPointID> last_checkpoint_{kInvalidCheckPointID};
		mutable std::mutex mutex_;
		std::vector<std::unique_ptr<CheckPoint>> unflushed_;
		std::vector<std::unique_ptr<CheckPoint>> flushed_;
	} checkpoints_;

	std::unique_ptr<MetaDataKV> metad_kv_{nullptr};

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