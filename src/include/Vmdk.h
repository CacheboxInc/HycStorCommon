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

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/MetaData_constants.h"
#include "DaemonCommon.h"
#include "IDs.h"
#include "VirtualMachine.h"
#include "Request.h"
#include "RequestHandler.h"
#include "AeroConn.h"
#include "MetaDataKV.h"
#include "QLock.h"
#include "Rendez.h"

namespace pio {

/* forward declaration for Pimpl */
namespace config {
	class VmdkConfig;
	class AeroConfig;
}

class CheckPoint {
public:
	CheckPoint(::ondisk::VmdkID vmdk_id, ::ondisk::CheckPointID id);
	void SetModifiedBlocks(const std::unordered_set<::ondisk::BlockID>& blocks);
	std::string Serialize() const;
	std::string SerializationKey() const;
	~CheckPoint();
	bool operator < (const CheckPoint& rhs) const noexcept;
	::ondisk::CheckPointID ID() const noexcept;

	std::pair<::ondisk::BlockID, ::ondisk::BlockID> Blocks() const noexcept;
	const Roaring& GetRoaringBitMap() const noexcept;
	void SetSerialized() noexcept;
	bool IsSerialized() const noexcept;
	void SetFlushed() noexcept;
	bool IsFlushed() const noexcept;
private:
	::ondisk::VmdkID vmdk_id_;
	::ondisk::CheckPointID self_;
	Roaring blocks_bitset_;
	struct {
		::ondisk::BlockID first_{0};
		::ondisk::BlockID last_{0};
	} block_id_;

	bool serialized_{false};
	bool flushed_{false};
public:
	static const std::string kCheckPoint;
};

class FlushAuxData {
public:
	enum class FlushStageType {
                kFlushStage,
                kMoveStage,
        };

	QLock lock_;
	Rendez rendez_;
	uint64_t pending_cnt_{0};
	uint64_t flushed_blks_{0};
	uint64_t moved_blks_{0};
	bool sleeping_{false};
	bool done_{false};
	bool failed_{false};
	std::atomic<uint64_t> reqid_{0};
public:
	void InitState(FlushStageType type) {
		/* TBD: rendez reset */
		pending_cnt_ = 0;
		sleeping_ = false;
		done_ = false;
		failed_ = false;
		if (type == FlushStageType::kFlushStage) {
			flushed_blks_ = 0;
			moved_blks_ = 0;
		} else {
			moved_blks_ = 0;
		}
	}

	uint64_t GetFlushedBlksCnt() {
		return flushed_blks_;
	}

	uint64_t GetMovedBlksCnt() {
		return moved_blks_;
	}
};

class Vmdk {
public:
	Vmdk(VmdkHandle handle, ::ondisk::VmdkID&& vmdk_id);
	virtual ~Vmdk();
	const ::ondisk::VmdkID& GetID() const noexcept;
	VmdkHandle GetHandle() const noexcept;

protected:
	VmdkHandle handle_;
	::ondisk::VmdkID     id_;
};

class ActiveVmdk : public Vmdk {
public:
	ActiveVmdk(VmdkHandle handle, ::ondisk::VmdkID vmdk_id, VirtualMachine *vmp,
		const std::string& config);
	virtual ~ActiveVmdk();

	void RegisterRequestHandler(std::unique_ptr<RequestHandler> handler);
	int Cleanup();
	void SetEventFd(int eventfd) noexcept;

	folly::Future<int> Read(Request* reqp, const CheckPoints& min_max);
	folly::Future<int> Flush(Request* reqp, const CheckPoints& min_max);
	folly::Future<int> Move(Request* reqp, const CheckPoints& min_max);
	folly::Future<int> Write(Request* reqp, ::ondisk::CheckPointID ckpt_id);
	folly::Future<int> WriteSame(Request* reqp, ::ondisk::CheckPointID ckpt_id);
	folly::Future<int> TakeCheckPoint(::ondisk::CheckPointID check_point);
	int FlushStages(::ondisk::CheckPointID check_point);
	int FlushStage(::ondisk::CheckPointID check_point);
	int MoveStage(::ondisk::CheckPointID check_point);
	const CheckPoint* GetCheckPoint(::ondisk::CheckPointID ckpt_id) const;

public:
	size_t BlockSize() const;
	size_t BlockShift() const;
	size_t BlockMask() const;
	VirtualMachine* GetVM() const noexcept;
	const config::VmdkConfig* GetJsonConfig() const noexcept;

private:
	folly::Future<int> WriteCommon(Request* reqp, ::ondisk::CheckPointID ckpt_id);
	int WriteComplete(Request* reqp, ::ondisk::CheckPointID ckpt_id);
	std::optional<std::unordered_set<::ondisk::BlockID>>
		CopyDirtyBlocksSet(::ondisk::CheckPointID ckpt_id);
	void RemoveDirtyBlockSet(::ondisk::CheckPointID ckpt_id);
	::ondisk::CheckPointID GetModifiedCheckPoint(::ondisk::BlockID block,
		const CheckPoints& min_max) const;
public:
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
		std::unordered_map<::ondisk::CheckPointID, std::unordered_set<::ondisk::BlockID>> modified_;
	} blocks_;

	struct {
		/* Last successful ::ondisk::CheckPointID on VMDK */
		std::atomic<::ondisk::CheckPointID> last_checkpoint_{
			::ondisk::MetaData_constants::kInvalidCheckPointID()
		};
		mutable std::mutex mutex_;
		std::vector<std::unique_ptr<CheckPoint>> unflushed_;
		std::vector<std::unique_ptr<CheckPoint>> flushed_;
	} checkpoints_;

	std::unique_ptr<MetaDataKV> metad_kv_{nullptr};

	struct {
		std::atomic<uint64_t> writes_in_progress_{0};
		std::atomic<uint64_t> reads_in_progress_{0};
		std::atomic<uint64_t> flushes_in_progress_{0};
		std::atomic<uint64_t> moves_in_progress_{0};
	} stats_;

public:
	std::unique_ptr<RequestHandler> headp_{nullptr};
	std::unique_ptr<FlushAuxData> aux_info_{nullptr};

private:
	static constexpr uint32_t kDefaultBlockSize{4096};
};

class SnapshotVmdk : public Vmdk {
public:
	SnapshotVmdk(VmdkHandle handle, ::ondisk::VmdkID vmdk_id) :
			Vmdk(handle, std::move(vmdk_id)) {
	}
private:
	//Vmdk*               parentp_{nullptr};
	std::vector<Vmdk *> chidren_;
};

}
