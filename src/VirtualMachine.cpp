#include <chrono>
#include <algorithm>
#include <type_traits>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>

#include <aerospike/aerospike_info.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "VirtualMachine.h"
#include "Vmdk.h"
#include "VmConfig.h"
#include "ArmConfig.h"
#include "FlushManager.h"
#include "CkptMergeManager.h"
#include "Singleton.h"
#include "AeroOps.h"
#include "BlockTraceHandler.h"
#include "LockHandler.h"
#include "UnalignedHandler.h"
#include "CompressHandler.h"
#include "EncryptHandler.h"
#include "MultiTargetHandler.h"
#include "halib.h"

#include "VmSync.h"

#ifdef USE_NEP
#include "ArmSync.h"
#endif

using namespace ::hyc_thrift;
using namespace ::ondisk;

namespace pio {

using namespace std::chrono_literals;
constexpr auto kTickSeconds = 10s;
constexpr auto kL1Ticks = 10s / kTickSeconds;
constexpr auto kL2Ticks = 2min / kTickSeconds;
constexpr auto kL3Ticks = 2h / kTickSeconds;

std::ostream& operator << (std::ostream& os, const VirtualMachine& vm) {
	os << "VirtualMachine "
		<< "Handle " << vm.handle_
		<< "VmID " << vm.vm_id_
		<< "Analyzer " << vm.analyzer_
		<< std::endl;
	return os;
}

VirtualMachine::VirtualMachine(VmHandle handle, VmID vm_id,
		const std::string& config) : handle_(handle), vm_id_(std::move(vm_id)),
		config_(std::make_unique<config::VmConfig>(config)),
		analyzer_(vm_id_, kL1Ticks, kL2Ticks, kL3Ticks),
		timer_(kTickSeconds) {
	setname_ = config_->GetTargetName();
	if (not config_->GetVmUUID(vm_uuid_)) {
		throw std::invalid_argument("vm uuid is not set.");
	}
	analyzer_.SetVmUUID(vm_uuid_);
}

VirtualMachine::~VirtualMachine() {
}

const VmID& VirtualMachine::GetID() const noexcept {
	return vm_id_;
}

const VmUUID& VirtualMachine::GetUUID() const noexcept {
	return vm_uuid_;
}

VmHandle VirtualMachine::GetHandle() const noexcept {
	return handle_;
}

const config::VmConfig* VirtualMachine::GetJsonConfig() const noexcept {
	return config_.get();
}

int VirtualMachine::SetArmJsonConfig(const std::string& config) {
	std::lock_guard<std::mutex> lock1(vmdk_.mutex_);
	armconfig_ = std::make_unique<config::ArmConfig>(config);
	if (pio_unlikely(armconfig_ == nullptr)) {
		return -ENOMEM;
	}
	return 0;
}

void VirtualMachine::UnsetArmJsonConfig() {
	if (armconfig_ != nullptr) {
		armconfig_.reset();
	}
}

const config::ArmConfig* VirtualMachine::GetArmJsonConfig() const noexcept {
	return armconfig_.get();
}

Analyzer* VirtualMachine::GetAnalyzer() noexcept {
	return &analyzer_;
}

RequestID VirtualMachine::NextRequestID() {
	return ++request_id_;
}

folly::Future<RestResponse> VirtualMachine::RestCall(_ha_instance* instancep,
		std::string endpoint, std::string body) {
	auto ep = endpoint.c_str();
	auto bodyp = body.c_str();
	class RestCall rest(instancep);
	return rest.Post(ep, bodyp )
	.then([endpoint = std::move(endpoint), body = std::move(body)]
			(RestResponse& resp) mutable {
		if (pio_unlikely(resp.post_failed)) {
			return resp;
		} else if (pio_unlikely(resp.status != 200)) {
			LOG(ERROR) << "Posting analyzer stats to " << endpoint
				<< " failed with error " << resp.status;
			return resp;
		}
		VLOG(2) << "Posted rest call to " << endpoint;
		return resp;
	});
}

void VirtualMachine::PostIOStats(_ha_instance* instancep) {
	int32_t service_index = instancep->ha_svc_idx;
	auto body = analyzer_.GetIOStats(service_index);
	if (not body) {
		return;
	}
	std::string endpoint = EndPoint::kStats + GetUUID();
	this->RestCall(instancep, std::move(endpoint), std::move(body.value()));
}

void VirtualMachine::PostFingerPrintStats(_ha_instance* instancep) {
	auto body = analyzer_.GetFingerPrintStats();
	if (not body) {
		return;
	}
	std::string endpoint = EndPoint::kFingerPrint + GetUUID();
	this->RestCall(instancep, std::move(endpoint), std::move(body.value()));
}

int VirtualMachine::StartTimer(_ha_instance *instancep, folly::EventBase* basep) {
	timer_.AttachToEventBase(basep);
	timer_.ScheduleTimeout([this, instancep] () {
		analyzer_.SetTimerTicked();
		this->PostIOStats(instancep);
		this->PostFingerPrintStats(instancep);
		return true;
	});
	return 0;
}

ActiveVmdk* VirtualMachine::FindVmdk(const VmdkID& vmdk_id) const {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	auto eit = vmdk_.list_.end();
	auto it = std::find_if(vmdk_.list_.begin(), eit,
		[vmdk_id] (const auto& vmdkp) {
			return vmdkp->GetID() == vmdk_id;
		});
	if (pio_unlikely(it == eit)) {
		return nullptr;
	}
	return *it;
}

std::vector <::ondisk::VmdkID> VirtualMachine::GetVmdkIDs() {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	std::vector <::ondisk::VmdkID> vmdk_ids;
	for (const auto& vmdkp : vmdk_.list_) {
		vmdk_ids.emplace_back(vmdkp->GetID());
	}
	return vmdk_ids;
}

ActiveVmdk* VirtualMachine::FindVmdk(VmdkHandle vmdk_handle) const {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	auto eit = vmdk_.list_.end();
	auto it = std::find_if(vmdk_.list_.begin(), eit,
		[vmdk_handle] (const auto& vmdkp) {
			return vmdkp->GetHandle() == vmdk_handle;
		});
	if (pio_unlikely(it == eit)) {
		return nullptr;
	}
	return *it;
}

void VirtualMachine::AddVmdk(ActiveVmdk* vmdkp) {
	log_assert(vmdkp);
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	vmdk_.list_.emplace_back(vmdkp);
}

void VirtualMachine::NewVmdk(ActiveVmdk* vmdkp) {
	auto configp = vmdkp->GetJsonConfig();

	auto blktrace = std::make_unique<BlockTraceHandler>(vmdkp);
	auto unalingned = std::make_unique<UnalignedHandler>();
	auto compress = std::make_unique<CompressHandler>(configp);
	auto encrypt = std::make_unique<EncryptHandler>(vmdkp, configp);
	auto multi_target = std::make_unique<MultiTargetHandler>(vmdkp, configp);

	vmdkp->RegisterRequestHandler(std::move(blktrace));
	vmdkp->RegisterRequestHandler(std::move(unalingned));
	vmdkp->RegisterRequestHandler(std::move(compress));
	vmdkp->RegisterRequestHandler(std::move(encrypt));
	vmdkp->RegisterRequestHandler(std::move(multi_target));

	AddVmdk(vmdkp);
	StartPreload(vmdkp);
}

int VirtualMachine::VmdkCount() {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	return vmdk_.list_.size();
}

const std::vector<ActiveVmdk *>& VirtualMachine::GetAllVmdks() const noexcept {
	return vmdk_.list_;
}

int VirtualMachine::RemoveVmdk(ActiveVmdk* vmdkp) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	auto eit = vmdk_.list_.end();
	auto it = std::find(vmdk_.list_.begin(), eit, vmdkp);
	if (pio_unlikely(it == eit)) {
		LOG(ERROR) << __func__ << "Haven't found vmid entry, removing it";
		return 1;
	}

	LOG(ERROR) << __func__ << "Found vmid entry, removing it";
	vmdk_.list_.erase(it);
	return 0;
}

void VirtualMachine::CheckPointComplete(CheckPointID ckpt_id) {
	log_assert(ckpt_id != MetaData_constants::kInvalidCheckPointID());
	checkpoint_.in_progress_.clear();

	CheckPointID flushed = std::numeric_limits<CheckPointID>::max();
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		for (const ActiveVmdk* vmdkp : vmdk_.list_) {
			if (vmdkp->FlushedCheckpoints()) {
				flushed = std::min(flushed, vmdkp->GetFlushedCheckPointID());
			}
		}
	}

	std::lock_guard<std::mutex> lock(sync_.mutex_);
	for (auto& sync : sync_.list_) {
		auto thread = std::thread([ckpt_id, flushed, syncp = sync.second.get()] () mutable {
				syncp->SetCheckPoints(ckpt_id, flushed);
				syncp->SyncStart();
			}
		);
		thread.detach();
	}
}

bool VirtualMachine::AddVmSync(std::unique_ptr<VmSync> sync) {
	std::lock_guard<std::mutex> lock(sync_.mutex_);
	auto [it, inserted] = sync_.list_.emplace(sync->GetSyncType(), std::move(sync));
	(void) it;
	return inserted;
}

VmSync* VirtualMachine::GetVmSync(VmSync::Type type) noexcept {
	std::lock_guard<std::mutex> lock(sync_.mutex_);
	auto it = sync_.list_.find(type);
	if (it == sync_.list_.end()) {
		return nullptr;
	}
	return it->second.get();
}

void VirtualMachine::FlushComplete(CheckPointID ckpt_id) {
	log_assert(ckpt_id != MetaData_constants::kInvalidCheckPointID());
	flush_in_progress_.clear();
}

/*
 * Returns a vector of all the unflushed checkpoints for this VM. If no checkpoints
 * exist then create a new one and return that.
 */

int VirtualMachine::GetUnflushedCheckpoints(std::vector<CheckPointID>& unflushed_ckpts) {
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		for (const auto& vmdkp : vmdk_.list_) {
			vmdkp->GetUnflushedCheckpoints(unflushed_ckpts);
		}
	}

	if(pio_likely(unflushed_ckpts.empty())) {
		auto f = TakeCheckPoint();
		f.wait();
		auto [ckpt_id, rc] = f.value();
		if (pio_unlikely(rc)) {
			LOG(ERROR) << __func__ << ": CheckPoint create failed for VM: "
				<< vm_id_ << " error returned: " << rc;
			return rc;
		}
		unflushed_ckpts.push_back(ckpt_id);
	} else {
		sort(unflushed_ckpts.begin(), unflushed_ckpts.end());
		unflushed_ckpts.erase(unique(unflushed_ckpts.begin(),
			unflushed_ckpts.end()), unflushed_ckpts.end());
	}

	return 0;
}

int VirtualMachine::GetflushedCheckpoints(std::vector<CheckPointID>& flushed_ckpts) {
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		for (const auto& vmdkp : vmdk_.list_) {
			vmdkp->GetFlushedCheckpoints(flushed_ckpts);
		}
	}

	if(pio_likely(not flushed_ckpts.empty())) {
		sort(flushed_ckpts.begin(), flushed_ckpts.end());
		flushed_ckpts.erase(unique(flushed_ckpts.begin(),
			flushed_ckpts.end()), flushed_ckpts.end());
	}
	return 0;
}

/*
 * Takes a SnapshotID and a vector of CheckpointIDs to create a map of vmdkid:ckptid as key
 * and SnapshotID as the value.
 * Key is : <vmdkID>:<ckpt_id>, value is : <snap_id>
 * TBD: this map will be persisted as meta data into AS.
 */
int VirtualMachine::SerializeCheckpoints(int64_t snap_id, const std::vector<int64_t>& vec_ckpts) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (const auto& ckpt_id : vec_ckpts) {
		bool found_ckpt = false;
		for (const auto& vmdkp : vmdk_.list_) {
			if (pio_likely(vmdkp->GetCheckPoint(ckpt_id))) {
				std::string key = vmdkp->GetID() + ":" + std::to_string(ckpt_id);
				snap_ckpt_map_.emplace(std::make_pair(key, snap_id));
				found_ckpt = true;
			}
		}
		if(pio_unlikely(not found_ckpt)) {
			LOG(ERROR) << __func__ << "Failed to find CheckpointID: " << ckpt_id;
			return -EINVAL;
		}
	}
	return 0;
}

int VirtualMachine::DeSerializeCheckpoints(const std::vector<int64_t>& vec_ckpts) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (const auto& ckpt_id : vec_ckpts) {
		for (const auto& vmdkp : vmdk_.list_) {
			std::string key = vmdkp->GetID() + ":" + std::to_string(ckpt_id);
			snap_ckpt_map_.erase(key);
		}
	}
	return 0;
}

int VirtualMachine::DeSerializeCheckpoint(CheckPointID ckpt_id) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (const auto& vmdkp : vmdk_.list_) {
		std::string key = vmdkp->GetID() + ":" + std::to_string(ckpt_id);
		snap_ckpt_map_.erase(key);
	}
	return 0;
}

int64_t VirtualMachine::GetSnapID(ActiveVmdk* vmdkp, const uint64_t& ckpt_id) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	auto vmdk_id = vmdkp->GetID();
	std::string key = vmdk_id + ":" + std::to_string(ckpt_id);
	auto it = snap_ckpt_map_.find(key);
	if (it != snap_ckpt_map_.end()) {
		return it->second;
	}

	/* Display all elements of map */
	LOG(ERROR) << __func__ << "Not found snapid entry for ckpt_id " << ckpt_id
			<< ", printing all elements from map for debugging";
	LOG(ERROR) << "vmdkID:ckpt_id" << "=>snap_id";
	std::unordered_map <std::string, int64_t>::iterator it1 = snap_ckpt_map_.begin();
	while (it1 != snap_ckpt_map_.end()) {
		LOG(ERROR) << "Element:" << it1->first << " => " << it1->second;
		++it1;
	}

	return -1;
}

folly::Future<CheckPointResult> VirtualMachine::TakeCheckPoint() {
	if (checkpoint_.in_progress_.test_and_set()) {
		return std::make_pair(MetaData_constants::kInvalidCheckPointID(), -EAGAIN);
	}

	CheckPointID ckpt_id = (++checkpoint_.checkpoint_id_) - 1;
	return Stun(ckpt_id)
	.then([this, ckpt_id] (int rc) mutable -> folly::Future<CheckPointResult> {
		if (pio_unlikely(rc < 0)) {
			return std::make_pair(MetaData_constants::kInvalidCheckPointID(), rc);
		}

		std::vector<folly::Future<int>> futures;
		futures.reserve(vmdk_.list_.size());
		{
			std::lock_guard<std::mutex> lock(vmdk_.mutex_);
			for (const auto& vmdkp : vmdk_.list_) {
				auto fut = vmdkp->TakeCheckPoint(ckpt_id);
				futures.emplace_back(std::move(fut));
			}
		}

		return folly::collectAll(std::move(futures))
		.then([this, ckpt_id] (const std::vector<folly::Try<int>>& results)
				-> folly::Future<CheckPointResult> {
			for (auto& t : results) {
				if (pio_likely(t.hasValue() and t.value() == 0)) {
					continue;
				} else {
					return std::make_pair(MetaData_constants::kInvalidCheckPointID(), t.value());
				}
			}
			CheckPointComplete(ckpt_id);
			return std::make_pair(ckpt_id, 0);
		});
	});
}

folly::Future<int> VirtualMachine::CommitCheckPoint(CheckPointID ckpt_id) {

	std::vector<folly::Future<int>> futures;
	futures.reserve(vmdk_.list_.size());
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		for (const auto& vmdkp : vmdk_.list_) {
			auto fut = vmdkp->CommitCheckPoint(ckpt_id);
			futures.emplace_back(std::move(fut));
		}
	}

	return folly::collectAll(std::move(futures))
	.then([] (const std::vector<folly::Try<int>>& results)
			-> folly::Future<int> {
		for (auto& t : results) {
			if (pio_likely(t.hasValue() and t.value() == 0)) {
				continue;
			} else {
				return t.value();
			}
		}
		return (0);
	});
}

folly::Future<int> VirtualMachine::MoveUnflushedToFlushed(
		std::vector<::ondisk::CheckPointID>& vec_ckpts) {

	std::vector<folly::Future<int>> futures;
	futures.reserve(vmdk_.list_.size());

	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (const auto& vmdkp : vmdk_.list_) {
		auto fut = vmdkp->MoveUnflushedToFlushed(vec_ckpts);
		futures.emplace_back(std::move(fut));
	}

	return folly::collectAll(std::move(futures))
	.then([] (const std::vector<folly::Try<int>>& results)
				-> folly::Future<int> {
		for (auto& t : results) {
			if (pio_likely(t.hasValue() and t.value() == 0)) {
				continue;
			} else {
				return t.value();
			}
		}
		return 0;
	});

}

folly::Future<int> VirtualMachine::CreateNewVmDeltaContext(int64_t snap_id) {

	std::vector<folly::Future<int>> futures;
	futures.reserve(vmdk_.list_.size());

	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	for (const auto& vmdkp : vmdk_.list_) {
		auto fut = vmdkp->CreateNewVmdkDeltaContext(snap_id);
		futures.emplace_back(std::move(fut));
	}

	return folly::collectAll(std::move(futures))
	.then([] (const std::vector<folly::Try<int>>& results)
				-> folly::Future<int> {
		for (auto& t : results) {
			if (pio_likely(t.hasValue() and t.value() == 0)) {
				continue;
			} else {
				return t.value();
			}
		}
		return 0;
	});
}

int VirtualMachine::FlushStatus(FlushStats &flush_stat) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);

	uint64_t flush_time  = 0;
	uint64_t move_time   = 0;
	uint64_t flush_bytes = 0;
	uint64_t move_bytes  = 0;
	uint64_t flush_blks  = 0;
	uint64_t move_blks   = 0;
	uint64_t total_blks  = 0;

	VmID vmid;
	int stage = -1;

	per_disk_flush_stat disk_stats;
	for (const auto& vmdkp : vmdk_.list_) {
		vmid = vmdkp->GetVM()->GetID();
		stage = (unsigned int)vmdkp->aux_info_->GetStageInProgress();
		if (not vmdkp->aux_info_->FlushStageDuration_ &&
			vmdkp->aux_info_->FlushStartedAt_ < std::chrono::steady_clock::now()) {
			auto vmdk_flush_time = std::chrono::duration_cast<std::chrono::milliseconds>
				(std::chrono::steady_clock::now() - vmdkp->aux_info_->FlushStartedAt_);
			flush_time += vmdk_flush_time.count();
		} else {
			flush_time += vmdkp->aux_info_->FlushStageDuration_;
		}

		if (vmdkp->aux_info_->FlushStageDuration_ && not vmdkp->aux_info_->MoveStageDuration_) {
			auto vmdk_move_time = std::chrono::duration_cast<std::chrono::milliseconds>
				(std::chrono::steady_clock::now() - vmdkp->aux_info_->MoveStartedAt_);
			move_time += vmdk_move_time.count();
		} else {
			move_time += vmdkp->aux_info_->MoveStageDuration_;
		}

		flush_bytes += (vmdkp->aux_info_->GetFlushedBlksCnt() * vmdkp->BlockSize());
		move_bytes  += (vmdkp->aux_info_->GetMovedBlksCnt() * vmdkp->BlockSize());

		flush_blks += vmdkp->aux_info_->GetFlushedBlksCnt();
		move_blks  += vmdkp->aux_info_->GetMovedBlksCnt();
		total_blks += vmdkp->aux_info_->GetTotalFlushBlksCnt();
	}
	flush_stat.emplace("flush_data", std::make_pair(flush_time, flush_bytes));
	flush_stat.emplace("move_data", std::make_pair(move_time, move_bytes));
	flush_stat.emplace("op", std::make_pair(stage, total_blks));

	disk_stats = std::make_pair(flush_blks,	move_blks);
	flush_stat.emplace(vmid, disk_stats);
	return 0;
}

/*
 * Output :res::sets/CLEAN/tgt1	objects=132:tombstones=0:memory_data_bytes=0:truncate_lut=0:deleting=false:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;
 */

uint64_t GetObjectCount(char *res) {

	if (res == NULL || strlen(res) == 0 ) {
		return 0;
	}

	std::string temp = res;
	std::size_t first = temp.find_first_of("=");
	if (first == std::string::npos)
		return 0;

	std::size_t last = temp.find_first_of(":");
	if (last == std::string::npos)
		return 0;

	std::string strNew = temp.substr(first + 1, last - (first + 1));
	return stol(strNew);
}

int VirtualMachine::AeroCacheStats(AeroStats *aero_statsp, AeroSpikeConn *aerop) {

	std::ostringstream os_dirty;
	auto tgtname = GetJsonConfig()->GetTargetName();
	os_dirty << "sets/DIRTY/" << tgtname;

	std::ostringstream os_clean;
	os_clean << "sets/CLEAN/" << tgtname;

	auto aeroconf = aerop->GetJsonConfig();
	std::string node_ips = aeroconf->GetAeroIPs();

	std::vector<std::string> results;
	boost::split(results, node_ips, boost::is_any_of(","));
	uint32_t port;
	aeroconf->GetAeroPort(port);

	char* res = NULL;
	auto rc = 0;
	as_error err;
	std::ostringstream os_parent;
	for (auto node_ip : results) {
		res = NULL;
		if (aerospike_info_host(&aerop->as_, &err, NULL, node_ip.c_str(), port, os_dirty.str().c_str(), &res) != AEROSPIKE_OK) {
			LOG(ERROR) << __func__ << " aerospike_info_host failed";
			rc = -ENOMEM;
			break;
		} else if (res) {
			VLOG(5) << __func__ << " aerospike_info_host command completed successfully for DIRTY ns, res::" << res;
			VLOG(5) << __func__ << "Dirty block count ::" << GetObjectCount(res);
			aero_statsp->dirty_cnt_ += GetObjectCount(res);
			free(res);
		}

		res = NULL;
		if (aerospike_info_host(&aerop->as_, &err, NULL, node_ip.c_str(), port, os_clean.str().c_str(), &res) != AEROSPIKE_OK) {
			LOG(ERROR) << __func__ << " aerospike_info_host failed";
			rc = -ENOMEM;
			break;
		} else if (res) {
			VLOG(5) << __func__ << " aerospike_info_host command completed successfully for CLEAN ns, res::" << res;
			VLOG(5) << __func__ << "CLEAN block count ::" << GetObjectCount(res);
			aero_statsp->clean_cnt_ += GetObjectCount(res);
			free(res);
		}

		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		for (const auto& vmdkp : vmdk_.list_) {
			/* Loop on each disk for this disk to get the Parent Cache stats */
			auto parent_disk = vmdkp->GetParentDiskSet();
			if (pio_unlikely(!parent_disk.size())) {
				continue;
			}

			os_parent.str("");
			os_parent.clear();
			os_parent << "sets/CLEAN/" << parent_disk;
			if (aerospike_info_host(&aerop->as_, &err, NULL, node_ip.c_str(),
					port, os_parent.str().c_str(), &res) != AEROSPIKE_OK) {
				LOG(ERROR) << __func__ << " aerospike_info_host failed";
				rc = -ENOMEM;
				break;
			} else if (res) {
				VLOG(5) << __func__ << " aerospike_info_host command completed successfully for Parent ns, res::" << res;
				VLOG(5) << __func__ << "Parent block count ::" << GetObjectCount(res);
				aero_statsp->parent_cnt_ += GetObjectCount(res);
				free(res);
			}
		}

		if (rc) {
			break;
		}
	}

	return rc;
}

int VirtualMachine::GetVmdkParentStats(AeroSpikeConn *aerop, ActiveVmdk* vmdkp,
		VmdkCacheStats *vmdk_stats) {

	auto aeroconf = aerop->GetJsonConfig();
	std::string node_ips = aeroconf->GetAeroIPs();

	std::vector<std::string> results;
	boost::split(results, node_ips, boost::is_any_of(","));
	uint32_t port;
	aeroconf->GetAeroPort(port);

	auto rc = 0;
	vmdk_stats->parent_blks_ = 0;

	for (auto node_ip : results) {
		auto parent_disk = vmdkp->GetParentDiskSet( );
		if (pio_unlikely(!parent_disk.size())) {
			continue;
		}

		std::ostringstream os_parent;
		os_parent.str("");
		os_parent.clear();
		os_parent << "sets/CLEAN/" << parent_disk;

		as_error err;
		char* res = NULL;
		rc = aerospike_info_host(&aerop->as_, &err, NULL, node_ip.c_str(),
				port, os_parent.str().c_str(), &res);
		if (rc != AEROSPIKE_OK) {
			LOG(ERROR) << " aerospike_info_host failed";
			rc = -ENOMEM;
			break;
		} else if (res) {
			vmdk_stats->parent_blks_ += GetObjectCount(res);
			free(res);
		}
	}
	return rc;
}

int VirtualMachine::MergeStart(CheckPointID ckpt_id) {

	auto managerp = SingletonHolder<CkptMergeManager>::GetInstance();

	/* Start with Bitmap Merge stage */
	{
	std::vector<folly::Future<int>> futures;
	futures.reserve(vmdk_.list_.size());
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		for (const auto& vmdkp : vmdk_.list_) {
			folly::Promise<int> promise;
			auto fut = promise.getFuture();
			managerp->threadpool_.pool_->AddTask([vmdkp, ckpt_id,
				promise = std::move(promise)]() mutable {
				auto rc = vmdkp->MergeStages(ckpt_id,
					MergeStageType::kBitmapMergeStage);
				promise.setValue(rc);
			});

			futures.emplace_back(std::move(fut));
		}
	}

	int rc = 0;
	folly::collectAll(std::move(futures))
	.then([&rc] (const std::vector<folly::Try<int>>& results) {
		for (auto& t : results) {
			if (pio_likely(t.hasValue() and t.value() != 0)) {
				rc = t.value();
			}
		}
	})
	.wait();

	if(pio_unlikely(rc)) {
		LOG(ERROR) << __func__
			<< "Data Merge stage failed for Vmid:" << GetID()
				<< ", ckptid:" << ckpt_id;
		return rc;
	}
	}

	/* Now call stun on ckpt so that we can phase out already submitted IOs */
	LOG(INFO) << __func__ << " Trying for Stun";
	auto f = RStun(ckpt_id);
	f.wait();
	auto rc = f.value();
	if (pio_unlikely(rc)) {
		LOG(ERROR) << __func__ << "Stun failed for Vmid:"
			<< GetID() << ", ckptid:" << ckpt_id;
		return rc;
	}

	/* Start data merging/promotion stage now */
	{
	std::vector<folly::Future<int>> futures;
	futures.reserve(vmdk_.list_.size());
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		for (const auto& vmdkp : vmdk_.list_) {
			folly::Promise<int> promise;
			auto fut = promise.getFuture();
			managerp->threadpool_.pool_->AddTask([vmdkp, ckpt_id,
				promise = std::move(promise)]() mutable {
				auto rc = vmdkp->MergeStages(ckpt_id,
					MergeStageType::kDataMergeStage);
				promise.setValue(rc);
			});

			futures.emplace_back(std::move(fut));
		}
	}

	folly::collectAll(std::move(futures))
	.then([&rc] (const std::vector<folly::Try<int>>& results) {
		for (auto& t : results) {
			if (pio_likely(t.hasValue() and t.value() != 0)) {
				rc = t.value();
			}
		}
	})
	.wait();
	}

	if(pio_unlikely(rc)) {
		LOG(ERROR) << __func__ << "Data merge failed for Vmid:"
				<< GetID() << ", ckptid:" << ckpt_id;
		return rc;
	}

	DeSerializeCheckpoint(ckpt_id);
	return 0;
}

int VirtualMachine::FlushStart(CheckPointID ckpt_id, bool perform_flush,
		bool perform_move,
		uint32_t max_req_size, uint32_t max_pending_reqs) {

	if (flush_in_progress_.test_and_set()) {
		return -EAGAIN;
	}

	auto managerp = SingletonHolder<FlushManager>::GetInstance();
	std::vector<folly::Future<int>> futures;

	/*
	 * TBD : If flush for any disk is failing then
	 * it should halt the flush of all the disks
	 */

	futures.reserve(vmdk_.list_.size());
	{
		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		for (const auto& vmdkp : vmdk_.list_) {
			/* TBD : ckpt id as args */
			folly::Promise<int> promise;
			auto fut = promise.getFuture();
			managerp->threadpool_.pool_->AddTask([vmdkp, ckpt_id,
				promise = std::move(promise), perform_flush, perform_move,
					max_req_size, max_pending_reqs]() mutable {
				auto rc = vmdkp->FlushStages(ckpt_id, perform_flush,
					perform_move,
					max_req_size, max_pending_reqs);
				promise.setValue(rc);
			});

			futures.emplace_back(std::move(fut));
		}
	}

	int rc = 0;
	folly::collectAll(std::move(futures))
	.then([&rc] (const std::vector<folly::Try<int>>& results) {
		for (auto& t : results) {
			if (pio_likely(t.hasValue() and t.value() != 0)) {
				rc = t.value();
			}
		}
	})
	.wait();
	FlushComplete(ckpt_id);
	return rc;
}

folly::Future<int> VirtualMachine::Write(ActiveVmdk* vmdkp, Request* reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		auto ckpt_id = checkpoint_.checkpoint_id_.load();
		++checkpoint_.writes_per_checkpoint_[ckpt_id];
		return ckpt_id;
	} ();

	++stats_.writes_in_progress_;
	return vmdkp->Write(reqp, ckpt_id)
	.then([this, ckpt_id] (int rc) mutable {
		WriteComplete(ckpt_id);
		return rc;
	});
}

::ondisk::CheckPointID VirtualMachine::GetCurCkptID() const {
	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		auto ckpt_id = checkpoint_.checkpoint_id_.load();
		return ckpt_id;
	} ();

	return ckpt_id;
}

folly::Future<int> VirtualMachine::BulkWrite(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		auto ckpt_id = checkpoint_.checkpoint_id_.load();
		++checkpoint_.writes_per_checkpoint_[ckpt_id];
		return ckpt_id;
	} ();

	++stats_.writes_in_progress_;
	return vmdkp->BulkWrite(ckpt_id, requests, process)
	.then([this, ckpt_id] (int rc) mutable {
		WriteComplete(ckpt_id);
		return rc;
	});
}

void VirtualMachine::IncCheckPointRef(CheckPointID &ckpt_id) {
	std::lock_guard<std::mutex> guard(checkpoint_.r_mutex_);
	++checkpoint_.reads_per_checkpoint_[ckpt_id];
	VLOG(5) << __func__ << "Inc ckpt_id:" << ckpt_id << ", count:" <<
		checkpoint_.reads_per_checkpoint_[ckpt_id];
}

void VirtualMachine::DecCheckPointRef(CheckPointID &ckpt_id) {
	std::lock_guard<std::mutex> guard(checkpoint_.r_mutex_);
	if (!checkpoint_.reads_per_checkpoint_[ckpt_id]) {
		LOG(ERROR) << __func__ << "BUG!!!, ckpt_id:" << ckpt_id;
		log_assert(0);
		return;
	}

	auto c = --checkpoint_.reads_per_checkpoint_[ckpt_id];
	VLOG(5) << __func__ << "Dec ckpt_id:" << ckpt_id << ", count:" <<
		checkpoint_.reads_per_checkpoint_[ckpt_id];
	if (pio_unlikely(not c and ckpt_id != checkpoint_.checkpoint_id_)) {
		log_assert(ckpt_id < checkpoint_.checkpoint_id_);
		auto it = checkpoint_.r_stuns_.find(ckpt_id);
		if (pio_unlikely(it == checkpoint_.r_stuns_.end())) {
			return;
		}
		auto stun = std::move(it->second);
		checkpoint_.r_stuns_.erase(it);
		stun->SetPromise(0);
	}
}

folly::Future<int> VirtualMachine::Read(ActiveVmdk* vmdkp, Request* reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpts = std::make_pair(MetaData_constants::kInvalidCheckPointID() + 1,
		checkpoint_.checkpoint_id_.load());
	++stats_.reads_in_progress_;
	return vmdkp->Read(reqp, ckpts)
	.then([this] (int rc) {
		--stats_.reads_in_progress_;
		return rc;
	});
}

folly::Future<ReadResultVec> VirtualMachine::BulkRead(ActiveVmdk* vmdkp,
		std::unique_ptr<ReqVec> requests, std::unique_ptr<ReqBlockVec> process,
		std::unique_ptr<IOBufPtrVec> iobufs, size_t read_size) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		return folly::makeFuture<ReadResultVec>
			(std::invalid_argument("VMDK not attached to VM"));
	}

	auto ckpts = std::make_pair(MetaData_constants::kInvalidCheckPointID() + 1,
		checkpoint_.checkpoint_id_.load());
	++stats_.reads_in_progress_;

	return vmdkp->BulkRead(ckpts, *requests, *process)
	.then([this, requests = std::move(requests), process = std::move(process),
			iobufs = std::move(iobufs), read_size] (int rc) {
		--stats_.reads_in_progress_;

		size_t copied = 0;
		ReadResultVec res;
		res.reserve(requests->size());

		auto it = iobufs->begin();
		auto eit = iobufs->end();
		for (const auto& req : *requests) {
			log_assert(it != eit);
			size_t sz = (*it)->computeChainDataLength();
			log_assert(sz == req->GetTransferSize());
			copied += sz;

			res.emplace_back(apache::thrift::FragileConstructor(), req->GetID(),
				rc ? rc : req->GetResult(), std::move(*it));
			++it;
		}

		/* make sure CMAKE_BUILD_TYPE=Release does not report unused variables */
		(void) read_size;
		(void) copied;
		log_assert(copied == read_size);
		return res;
	});
}

folly::Future<std::unique_ptr<ReadResultVec>>
VirtualMachine::BulkRead(ActiveVmdk* vmdkp,
		std::vector<ReadRequest>::const_iterator it,
		std::vector<ReadRequest>::const_iterator eit,
		bool is_read_ahead_req = false) {
	static_assert(kBulkReadMaxSize >= 32*1024, "kBulkReadMaxSize too small");
	using ReturnType = std::unique_ptr<ReadResultVec>;

	auto AllocDS = [] (size_t nr) {
		auto process = std::make_unique<ReqBlockVec>();
		auto reqs = std::make_unique<ReqVec>();
		auto iobufs = std::make_unique<IOBufPtrVec>();
		reqs->reserve(nr);
		iobufs->reserve(nr);
		return std::make_tuple(std::move(reqs), std::move(process), std::move(iobufs));
	};

	auto FutureException = [] (auto&& exception) {
		return folly::makeFuture<ReturnType>
			(std::forward<decltype(exception)>(exception));
	};

	auto FutureExceptionBadAlloc = [&FutureException]
				(auto file, auto line, const std::string_view& msg) {
		std::ostringstream os;
		os << "HeapError file = " << file << " line = " << line;
		if (not msg.empty()) {
			os << " " << msg;
		}
		LOG(ERROR) << os.str();
		return FutureException(std::bad_alloc());
	};

	auto ExtractParams = [] (const ReadRequest& rd) {
		return std::make_tuple(rd.get_reqid(), rd.get_size(), rd.get_offset());
	};

	auto NewRequest = [&is_read_ahead_req] (RequestID reqid, ActiveVmdk* vmdkp, folly::IOBuf* bufp,
			size_t size, int64_t offset) {
		auto req = std::make_unique<Request>(reqid, vmdkp, Request::Type::kRead,
			bufp->writableData(), size, size, offset);
		if(is_read_ahead_req) { 
        	req->SetReadAheadRequest();
        }  	
		bufp->append(size);
		return req;
	};

	auto NewResult = [] (RequestID id, int32_t rc, IOBufPtr iobuf) {
		return ReadResult(apache::thrift::FragileConstructor(), id, rc,
			std::move(iobuf));
	};

	auto UnalignedRead = [&NewResult] (VirtualMachine* vmp, ActiveVmdk* vmdkp,
			size_t size, std::unique_ptr<Request> req, IOBufPtr iobuf) {
		return vmp->Read(vmdkp, req.get())
		.then([&NewResult, req = std::move(req), iobuf = std::move(iobuf), size]
				(const folly::Try<int>& tri) mutable {
			if (pio_unlikely(tri.hasException())) {
				return folly::makeFuture<ReadResultVec>(tri.exception());
			}
			log_assert(size == iobuf->computeChainDataLength() and
				size == req->GetTransferSize());

			auto rc = tri.value();
			ReadResultVec res;
			res.emplace_back(NewResult(req->GetID(),
				rc ? rc : req->GetResult(), std::move(iobuf)));
			return folly::makeFuture(std::move(res));
		});
	};

	std::vector<folly::Future<std::vector<ReadResult>>> futures;
	::ondisk::BlockID prev_start = 0;
	::ondisk::BlockID prev_end = 0;
	std::unique_ptr<ReqBlockVec> process;
	std::unique_ptr<ReqVec> requests;
	std::unique_ptr<IOBufPtrVec> iobufs;

	auto pending = std::distance(it, eit);
	std::tie(requests, process, iobufs) = AllocDS(pending);
	if (pio_unlikely(not requests or not process or not iobufs)) {
		return FutureExceptionBadAlloc(__FILE__, __LINE__, nullptr);
	}

	size_t read_size = 0;
	uint64_t total_read_size = 0;
	for (; it != eit; ++it) {
		auto [reqid, size, offset] = ExtractParams(*it);
		auto iobuf = folly::IOBuf::create(size);
		if (pio_unlikely(not iobuf)) {
			const char* msgp = "Allocating IOBuf for read failed";
			return FutureExceptionBadAlloc(__FILE__, __LINE__, msgp);
		}
		iobuf->unshare();
		iobuf->coalesce();
		log_assert(not iobuf->isChained());

		auto req = NewRequest(reqid, vmdkp, iobuf.get(), size, offset);
		if (pio_unlikely(not req)) {
			const char* msgp = "Allocating Request for read failed";
			return FutureExceptionBadAlloc(__FILE__, __LINE__, msgp);
		}
		log_assert(iobuf->computeChainDataLength() == static_cast<size_t>(size));

		auto reqp = req.get();
		if(is_read_ahead_req) { 
        	req->SetReadAheadRequest();
        }  	
		--pending;

		if (reqp->HasUnalignedIO()) {
			futures.emplace_back(UnalignedRead(this, vmdkp, size,
				std::move(req), std::move(iobuf)));
			continue;
		}

		auto [cur_start, cur_end] = reqp->Blocks();
		log_assert(prev_start <= cur_start);

		if (read_size >= kBulkReadMaxSize ||
				(prev_end >= cur_start && not requests->empty())) {
			futures.emplace_back(
				BulkRead(vmdkp, std::move(requests), std::move(process),
					std::move(iobufs), read_size));
			log_assert(not process and not requests);
			std::tie(requests, process, iobufs) = AllocDS(pending+1);
			read_size = 0;
			if (pio_unlikely(not requests or not process or not iobufs)) {
				return FutureExceptionBadAlloc(__FILE__, __LINE__, nullptr);
			}
		}

		process->reserve(process->size() + reqp->NumberOfRequestBlocks());
		reqp->ForEachRequestBlock([&] (RequestBlock* blockp) mutable {
			process->emplace_back(blockp);
			return true;
		});
		requests->emplace_back(std::move(req));
		iobufs->emplace_back(std::move(iobuf));

		read_size += size;
		prev_start = cur_start;
		prev_end = cur_end;
		total_read_size += size;
	}

	stats_.bulk_read_sz_ += total_read_size;
	stats_.bulk_reads_   += futures.size();

	if (pio_likely(not requests->empty())) {
		futures.emplace_back(
			BulkRead(vmdkp, std::move(requests), std::move(process),
				std::move(iobufs), read_size));
	}

	return folly::collectAll(std::move(futures))
	.then([&] (folly::Try<
				std::vector<
					folly::Try<
						ReadResultVec>>>& tries) mutable {
		if (pio_unlikely(tries.hasException())) {
			return FutureException(tries.exception());
		}

		auto results = std::make_unique<ReadResultVec>();
		auto v1 = std::move(tries.value());
		for (auto& tri : v1) {
			if (pio_unlikely(tri.hasException())) {
				return FutureException(tri.exception());
			}

			auto v2 = std::move(tri.value());
			results->reserve(results->size() + v2.size());
			std::move(v2.begin(), v2.end(), std::back_inserter(*results));
		}
		return folly::makeFuture(std::move(results));
	});
}

folly::Future<std::unique_ptr<ReadResultVec>>
VirtualMachine::BulkRead(ActiveVmdk* vmdkp,
		std::unique_ptr<std::vector<ReadRequest>> in_reqs) {
	return BulkRead(vmdkp, in_reqs->begin(), in_reqs->end())
	.then([in_reqs = std::move(in_reqs)]
			(std::unique_ptr<ReadResultVec> tri) mutable {
		return tri;
	});
}

folly::Future<int> VirtualMachine::TruncateBlocks(ActiveVmdk* vmdkp,
		RequestID reqid, const std::vector<TruncateReq>& requests) {
	return vmdkp->TruncateBlocks(reqid, checkpoint_.checkpoint_id_.load(), requests);
}

folly::Future<int> VirtualMachine::StartPreload(const ::ondisk::VmdkID& id) {
	auto vmdkp = FindVmdk(id);
	if (not vmdkp) {
		LOG(ERROR) << "VMDK (" << id << ") not attached to VM ("
			<< GetID() << ")";
		return -EINVAL;
	}

	return StartPreload(vmdkp);
}

folly::Future<int> VirtualMachine::StartPreload(ActiveVmdk* vmdkp) {
	auto IssueBulkRead = [this, vmdkp] (const auto& start, const auto& end) {
		return this->BulkRead(vmdkp, start, end)
		.then([] (const folly::Try<std::unique_ptr<ReadResultVec>>& tries) {
			if (pio_unlikely(tries.hasException())) {
				return -EIO;
			}
			return 0;
		});
	};
	auto RecursiveBulkRead = [IssueBulkRead] (auto start, const auto end, auto depth) mutable {
		auto Impl = [IssueBulkRead, depth] (auto start, const auto end, auto func) mutable {
			auto pending = std::distance(start, end);
			if (pending <= static_cast<int32_t>(depth)) {
				return IssueBulkRead(start, end);
			}
			return IssueBulkRead(start, std::next(start, depth))
			.then([s = start, e = end, depth, func] (int rc) mutable -> folly::Future<int> {
				if (rc < 0) {
					return rc;
				}
				std::advance(s, depth);
				return func(s, e, func);
			});
		};
		return Impl(start, end, Impl);
	};

	const auto& blocks = vmdkp->GetPreloadBlocks();
	if (blocks.empty()) {
		LOG(INFO) << "No blocks to preload for VmdkID " << vmdkp->GetID();
		return 0;
	}

	std::vector<ReadRequest> requests;
	try {
		requests.reserve(blocks.size());
	} catch (const std::bad_alloc& e) {
		LOG(ERROR) << "memory allocation failed";
		return -ENOMEM;
	}

	::hyc_thrift::RequestID reqid = 0;
	std::transform(blocks.begin(), blocks.end(), std::back_inserter(requests),
			[&reqid, bs = vmdkp->BlockShift()] (const auto& block)
			-> ReadRequest {
		return {apache::thrift::FragileConstructor(), ++reqid,
			block.second << bs, block.first << bs};
	});

	return RecursiveBulkRead(requests.begin(), requests.end(), kPreloadMaxIODepth)
	.then([vmdkp, requests = std::move(requests)] (int rc) mutable {
		if (pio_unlikely(rc < 0)) {
			LOG(ERROR) << "Preload for " << vmdkp->GetID() << " failed";
			return -EIO;
		}
		LOG(INFO) << "Preload for " << vmdkp->GetID() << " completed";
		return 0;
	});
}

#if 0
folly::Future<int> VirtualMachine::Flush(ActiveVmdk* vmdkp, Request* reqp, const CheckPoints& min_max) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	++stats_.flushs_in_progress_;
	return vmdkp->Flush(reqp, min_max)
	.then([this] (int rc) {
		--stats_.flushs_in_progress_;
		return rc;
	});
}
#endif


folly::Future<int> VirtualMachine::WriteSame(ActiveVmdk* vmdkp, Request* reqp) {
	if (pio_unlikely(not FindVmdk(vmdkp->GetHandle()))) {
		throw std::invalid_argument("VMDK not attached to VM");
	}

	auto ckpt_id = [this] () mutable -> CheckPointID {
		std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
		auto ckpt_id = checkpoint_.checkpoint_id_.load();
		++checkpoint_.writes_per_checkpoint_[ckpt_id];
		return ckpt_id;
	} ();

	++stats_.writes_in_progress_;
	return vmdkp->WriteSame(reqp, ckpt_id)
	.then([this, ckpt_id] (int rc) mutable {
		WriteComplete(ckpt_id);
		return rc;
	});
}

void VirtualMachine::WriteComplete(CheckPointID ckpt_id) {
	--stats_.writes_in_progress_;

	std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
	log_assert(checkpoint_.writes_per_checkpoint_[ckpt_id] > 0);
	auto c = --checkpoint_.writes_per_checkpoint_[ckpt_id];
	if (pio_unlikely(not c and ckpt_id != checkpoint_.checkpoint_id_)) {
		log_assert(ckpt_id < checkpoint_.checkpoint_id_);
		auto it = checkpoint_.stuns_.find(ckpt_id);
		if (pio_unlikely(it == checkpoint_.stuns_.end())) {
			return;
		}
		auto stun = std::move(it->second);
		checkpoint_.stuns_.erase(it);
		stun->SetPromise(0);
	}
}

folly::Future<int> VirtualMachine::Stun(CheckPointID ckpt_id) {
	std::lock_guard<std::mutex> guard(checkpoint_.mutex_);
	if (auto it = checkpoint_.writes_per_checkpoint_.find(ckpt_id);
			it == checkpoint_.writes_per_checkpoint_.end() ||
			it->second == 0) {
		return 0;
	} else {
		log_assert(it->second > 0);
	}

	if (auto it = checkpoint_.stuns_.find(ckpt_id);
			pio_likely(it == checkpoint_.stuns_.end())) {
		auto stun = std::make_unique<struct Stun>();
		auto fut = stun->GetFuture();
		checkpoint_.stuns_.emplace(ckpt_id, std::move(stun));
		return fut;
	} else {
		return it->second->GetFuture();
	}
}

folly::Future<int> VirtualMachine::RStun(CheckPointID ckpt_id) {

	std::lock_guard<std::mutex> guard(checkpoint_.r_mutex_);
	LOG(INFO) << __func__ << " Pending read count is (after lock)::"
		<< checkpoint_.reads_per_checkpoint_[ckpt_id]
		<< ", ckpt id::" << ckpt_id;

	if (auto it = checkpoint_.reads_per_checkpoint_.find(ckpt_id);
			it == checkpoint_.reads_per_checkpoint_.end() ||
			it->second == 0) {
		LOG(ERROR) << __func__ << " No pending read for ckpt::"
					<< ckpt_id;
		return 0;
	} else {
		log_assert(it->second > 0);
	}

	if (auto it = checkpoint_.r_stuns_.find(ckpt_id);
			pio_likely(it == checkpoint_.r_stuns_.end())) {
		auto stun = std::make_unique<struct Stun>();
		auto fut = stun->GetFuture();
		checkpoint_.r_stuns_.emplace(ckpt_id, std::move(stun));
		return fut;
	} else {
		return it->second->GetFuture();
	}
}

Stun::Stun() : promise(), futures(promise.getFuture()) {
}

folly::Future<int> Stun::GetFuture() {
	return futures.getFuture();
}

void Stun::SetPromise(int result) {
	promise.setValue(result);
}

}
