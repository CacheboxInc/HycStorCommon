#include <deque>
#include <string>
#include <atomic>
#include <mutex>
#include <boost/algorithm/string.hpp>

#include <folly/fibers/Fiber.h>
#include <folly/futures/Future.h>
#include <glog/logging.h>
#include <chrono>
#include <algorithm>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "DaemonTgtInterface.h"
#include "Request.h"
#include "ThreadPool.h"
#include "VirtualMachine.h"
#include "Vmdk.h"
#include "VmdkConfig.h"
#include "VmdkFactory.h"
#include "VmConfig.h"
#include "VmManager.h"
#include "AeroConn.h"
#include "Singleton.h"
#include "AeroFiberThreads.h"
#include "FlushManager.h"
#include "SetCkptBmapConfig.cpp"
#include "ScanManager.h"
#include "FlushInstance.h"
#include "ScanInstance.h"
#include "FlushConfig.h"
#include "AeroOps.h"
#include "TgtInterfaceImpl.h"

#include "BlockTraceHandler.h"
#include "LockHandler.h"
#include "UnalignedHandler.h"
#include "CompressHandler.h"
#include "EncryptHandler.h"
#include "MultiTargetHandler.h"

#ifdef USE_NEP
#include <NetworkTargetHandler.h>
#include "halib.h"
#include <TargetManager.hpp>
#endif

using namespace ::hyc_thrift;
using namespace ::ondisk;

namespace pio {
using namespace folly;

struct {
	std::mutex mutex_;
	/* Using handle to just keep track of how many entries are added */
	std::atomic<AeroClusterHandle> handle_{0};
	std::unordered_map<AeroClusterID, std::shared_ptr<AeroSpikeConn>> ids_;
} g_aero_clusters;

StorD::StorD() = default;
StorD::~StorD() = default;

int StorD::InitStordLib(void) {
	try {
		std::call_once(g_init_.initialized_, [=] () mutable {
			SingletonHolder<VmdkManager>::CreateInstance();
			SingletonHolder<pio::VmManager>::CreateInstance();
			SingletonHolder<AeroFiberThreads>::CreateInstance();
			SingletonHolder<FlushManager>::CreateInstance();
			SingletonHolder<ScanManager>::CreateInstance();
#ifdef USE_NEP
			SingletonHolder<TargetManager>::CreateInstance(false);
#endif
		});
	} catch (const std::exception& e) {
		LOG(ERROR) << "Hit exception" << e.what();
		return -1;
	}
	return 0;
}

int StorD::DeinitStordLib(void) {
	try {
		std::call_once(g_init_.deinitialized_, [=] () mutable {
			SingletonHolder<VmdkManager>::DestroyInstance();
			SingletonHolder<pio::VmManager>::DestroyInstance();
			SingletonHolder<AeroFiberThreads>::DestroyInstance();
			SingletonHolder<FlushManager>::DestroyInstance();
#ifdef USE_NEP
			SingletonHolder<TargetManager>::DestroyInstance();
#endif
		});
	} catch (const std::exception& e) {
		LOG(ERROR) << "Hit exception" << e.what();
		return -1;
	}

	return 0;
}

void AeroSpikeConnDisplay() {
	std::lock_guard<std::mutex> lock(g_aero_clusters.mutex_);
	LOG(ERROR) << __func__  <<  "ALL entries";
	for(auto it = g_aero_clusters.ids_.begin();
			it != g_aero_clusters.ids_.end(); ++it) {
		 LOG(ERROR) << __func__ << it->first << ":::" <<
			(it->second).get();
	}
	LOG(ERROR) << __func__  <<  "END";
}

std::shared_ptr<AeroSpikeConn> AeroSpikeConnFromClusterID (AeroClusterID cluster_id) {
	std::lock_guard<std::mutex> lock(g_aero_clusters.mutex_);
	auto it = g_aero_clusters.ids_.find(cluster_id);
	if (pio_unlikely(it == g_aero_clusters.ids_.end())) {
		return nullptr;
	}
	return (it->second);
}

void RemoveAeroClusterLocked(AeroClusterID cluster_id) {
	auto it = g_aero_clusters.ids_.find(cluster_id);
	if (pio_unlikely(it != g_aero_clusters.ids_.end())) {
		g_aero_clusters.ids_.erase(it);
		g_aero_clusters.handle_--;
	}
}

/*
void RemoveAeroCluster(AeroClusterID cluster_id) {
	std::lock_guard<std::mutex> lock(g_aero_clusters.mutex_);
	RemoveAeroClusterLocked(cluster_id);
}
*/

AeroClusterHandle NewAeroCluster(AeroClusterID cluster_id,
		const std::string& config) {

	std::lock_guard<std::mutex> lock(g_aero_clusters.mutex_);
	try {
		auto it = g_aero_clusters.ids_.find(cluster_id);
		if (pio_unlikely(it != g_aero_clusters.ids_.end())) {
			LOG(ERROR) << __func__ << "Aero new command failed. Cluster_id:: "
				<< cluster_id
				<< " is already present in configuration.";
			return kInvalidAeroClusterHandle;
		}

		auto handle = ++g_aero_clusters.handle_;
		auto aero_cluster_p =
			std::make_shared<AeroSpikeConn>(cluster_id, config);

		/*
		 * Establish Connection with aerospike cluster and add in global
		 * map in case of success (0 implies success).
		 */

		int ret = aero_cluster_p->Connect();
		if (!ret) {
			g_aero_clusters.ids_.insert(std::make_pair(cluster_id,
					aero_cluster_p));
			LOG(INFO) << __func__ <<  "Aero new for cluster_id:: " << cluster_id
				<< "... SUCCESS";

			return handle;
		}

		LOG(ERROR) << __func__ << "Failed to connect to AeroSpike cluster id::"
			<< cluster_id
			<< " please make sure aerospike server is up and running";
		return kInvalidAeroClusterHandle;
	} catch (const std::bad_alloc& e) {
		LOG(ERROR) << __func__ <<  " Aero new for cluster_id:: " << cluster_id
			<< " failed because of std::bad_alloc exception";
		RemoveAeroClusterLocked(cluster_id);
		return kInvalidAeroClusterHandle;
	}
}

AeroClusterHandle DelAeroCluster(AeroClusterID cluster_id,
	const std::string& config) {

	std::lock_guard<std::mutex> lock(g_aero_clusters.mutex_);
	auto it = g_aero_clusters.ids_.find(cluster_id);
	if (pio_unlikely(it == g_aero_clusters.ids_.end())) {
		LOG(ERROR) << __func__ << "Cluster id " << cluster_id <<
			" not found in connection map";
		return kInvalidAeroClusterHandle;
	}

	/*
	 * Destructor of shared pointer will take care of
	 * cleaning aerospike connection object when last
	 * reference will be dropped.
	 */

	g_aero_clusters.ids_.erase(it);
	LOG(INFO) << __func__ << "Removed successfully cluster id "
		<< cluster_id;
	return kValidAeroClusterHandle;
}

int NewFlushReq(VmID vmid, const std::string& config) {
	auto managerp = SingletonHolder<FlushManager>::GetInstance();

	std::lock_guard<std::mutex> flush_lock(managerp->lock_);
	auto ptr = managerp->GetInstance(vmid);
	if (ptr != nullptr) {
		LOG(ERROR) << "Flush is already running for given VM";
		return -EAGAIN;
	}

	try {
		auto rc = managerp->NewInstance(vmid, config);
		if (pio_unlikely(rc)) {
			return -ENOMEM;
		}

		auto fi = managerp->GetInstance(vmid);
		managerp->threadpool_.pool_->AddTask([managerp, vmid, fi]()
					mutable {
			auto rc = fi->StartFlush(vmid);
			/* Remove fi Instance */
			std::lock_guard<std::mutex> lock(managerp->lock_);
			managerp->FreeInstance(vmid);
			return rc;
		});
	} catch (...) {
		return -ENOMEM;
	}

	return 0;
}

int NewScanReq(VmID vmid, const std::string& config) {

	auto managerp = SingletonHolder<ScanManager>::GetInstance();
	auto vmp = SingletonHolder<pio::VmManager>::GetInstance()->GetInstance(vmid);
	if(pio_unlikely(vmp == nullptr)) {
		LOG(ERROR) << __func__ << " Given vmid is not valid";
		return 1;
	}

	/* Get Aerospike cluster ID */
	AeroClusterID cluster_id;
	vmp->GetJsonConfig()->GetAeroClusterID(cluster_id);

	/* Check if a scan task is already running for the given cluster ID */
	std::unique_lock<std::mutex> scan_lock(managerp->lock_);
	auto start_instance = false;
	auto si = managerp->GetInstance(cluster_id);
	if (pio_likely(si == nullptr)) {
		LOG(ERROR) << __func__ << " No instance found, creating a new one";
		auto rc = managerp->NewInstance(cluster_id);
		if (pio_unlikely(rc)) {
			return -ENOMEM;
		}

		si = managerp->GetInstance(cluster_id);
		start_instance = true;
	} else {
		LOG(ERROR) << __func__ << " Scan Instance found, scan is already running" <<
			" for given cluster";
	}

	/* Add VMDKIDs for given VM into the pending list */
	for (const auto& id : vmp->GetVmdkIDs()){
		LOG(INFO) << __func__ << "Adding ID in pending list::" << id.c_str();
		si->pending_list_.emplace_back(stol(id));
	}

	scan_lock.unlock();
	if (pio_unlikely(!start_instance)) {
#if 0
		/*
		 * TBD: Abort any already running scan session if it is processing
		 * less than 32 VmDKs. This will help scan to start with larger
		 * set of VmDKs instead of keep on working on narrow set of entries.
		 */

		auto size = si->working_list_.size();
		if (size && size < kMaxVmdkstoScan) {
			LOG(ERROR) << __func__ <<
				"Try to abort the session to start from begining";
		}
#endif
		return 0;
	}

	auto aero_conn = AeroSpikeConnFromClusterID(cluster_id);
	if (pio_unlikely(not aero_conn)) {
		LOG(ERROR) << __func__ <<
			" Unable to get the aerospike connection info";
		return -EINVAL;
	}

	/* Start a new scan thread for given aerospike cluster */
	si->aero_conn_ = aero_conn.get();
	return si->StartScanThread();
}

int NewFlushStatusReq(VmID vmid, FlushStats &flush_stat) {
	auto managerp = SingletonHolder<FlushManager>::GetInstance();
	/* Check if already a scan running for the given cluster ID */
	std::unique_lock<std::mutex> flush_lock(managerp->lock_);
	auto fi = managerp->GetInstance(vmid);
	if (fi == nullptr) {
		LOG(ERROR) << __func__ << " Flush is not running for given VM";
		return -EINVAL;
	}

	auto rc = fi->FlushStatus(vmid, flush_stat);
	return rc;
}

int FlushHistoryReq(VmID vmid, std::ostringstream& fh) {
	auto managerp = SingletonHolder<FlushManager>::GetInstance();
	std::unique_lock<std::mutex> flush_lock(managerp->lock_);
	auto t = managerp->GetHistory(vmid, fh);

	if (pio_unlikely(t)) {
		LOG(ERROR) << "Flush history not present for vmid " << vmid;
		return 1;
	}
	return t;
}

int NewScanStatusReq(AeroClusterID id, ScanStats &scan_stat) {
	auto managerp = SingletonHolder<ScanManager>::GetInstance();
	/* Check if already a scan running for the given cluster ID */
	std::unique_lock<std::mutex> scan_lock(managerp->lock_);

	auto si = managerp->GetInstance(id);
	if (si == nullptr) {
		LOG(ERROR) << __func__ << " Scan is not running for given AeroSpike cluster";
		return -EINVAL;
	}

	auto rc = si->ScanStatus(scan_stat);
	return rc;
}

std::shared_ptr<AeroSpikeConn> GetAeroConnUsingVmID(VmID vmid) {

	auto managerp = SingletonHolder<pio::VmManager>::GetInstance();
	auto vmp = managerp->GetInstance(vmid);
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << __func__ << " Given VmID is not present";
		return nullptr;
	}

	auto vm_confp = vmp->GetJsonConfig();
	AeroClusterID aero_cluster_id;
	auto ret = vm_confp->GetAeroClusterID(aero_cluster_id);
	if (pio_unlikely(ret == 0)) {
		LOG(ERROR) << __func__ << " Unable to find aerospike cluster "
			"id for given disk." " Please check JSON configuration "
			"with associated VM. Moving ahead without"
			" Aero connection object";
		return nullptr;
	}

	/* Get aero connection object*/
	return  pio::AeroSpikeConnFromClusterID(aero_cluster_id);
}

int NewAeroCacheStatReq(VmID vmid, AeroStats *aero_statsp) {

	LOG(ERROR) << __func__ << "START";
	auto managerp = SingletonHolder<pio::VmManager>::GetInstance();
	auto vmp = managerp->GetInstance(vmid);
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << " Given VmID is not present";
		return -EINVAL;
	}

	auto aero_conn = GetAeroConnUsingVmID(vmid);
	if (pio_unlikely(not aero_conn)) {
		LOG(ERROR) << "Given VM has not configured with AeroSpike cache";
		return -EINVAL;
	}

	auto rc = vmp->AeroCacheStats(aero_statsp, aero_conn.get());
	if (!rc) {
		LOG(ERROR) << __func__ << "Dirty cnt::" << aero_statsp->dirty_cnt_;
		LOG(ERROR) << __func__ << "Clean cnt::" << aero_statsp->clean_cnt_;
		LOG(ERROR) << __func__ << "Parent cnt::" << aero_statsp->parent_cnt_;
	} else {
		LOG(ERROR) << __func__ << "Failed to get the Aero Stats...";
		aero_statsp->dirty_cnt_ = 0;
		aero_statsp->clean_cnt_ = 0;
		aero_statsp->parent_cnt_ = 0;
	}

	return rc;
}

VmHandle NewVm(VmID vmid, const std::string& config) {
	auto managerp = SingletonHolder<pio::VmManager>::GetInstance();
	if (auto vmp = managerp->GetInstance(vmid); pio_unlikely(vmp)) {
		LOG(ERROR) << "VirtualMachine already present";
		return StorRpc_constants::kInvalidVmHandle();
	}

	try {
		return managerp->CreateInstance(std::move(vmid), config);
	} catch (...) {
		return StorRpc_constants::kInvalidVmHandle();
	}
}

int RemoveVmUsingVmID(VmID vmid) {
	LOG(ERROR) << __func__ << "START";
	auto managerp = SingletonHolder<pio::VmManager>::GetInstance();
	auto vmp = managerp->GetInstance(vmid);
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << "Given VmID is not present";
		return 1;
	}

	try {
		auto handle = vmp->GetHandle();
		if (pio_unlikely(not handle)) {
			LOG(ERROR) << __func__ << "Unable to get VM's handle";
			return 1;
		}

		/* No disk should be associated with VM */
		if (vmp->VmdkCount()) {
			LOG(ERROR) << __func__ << "Disks are still associated with this VM, can't remove";
			return 1;
		}

		#if 0
		/* Delete aerospike set first */
		if (AeroSetDelete(vmid)) {
			LOG(ERROR) << "Unable to Clean the aerospike set entries";
		}
		#endif

		LOG(INFO) << __func__ << " Calling FreeInstance";
		return managerp->FreeInstance(handle);
	} catch (...) {
		LOG(ERROR) << __func__ << " Returing because of exeception";
		return 1;
	}
}

int CommitCkpt(VmID vmid, std::string& ckpt_id) {
	auto managerp = SingletonHolder<pio::VmManager>::GetInstance();
	auto vmp = managerp->GetInstance(vmid);
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << "Given VmID:" << vmid << " is not present";
		return 1;
	}

	auto f = vmp->CommitCheckPoint(stol(ckpt_id));
	f.wait();
	auto rc = f.value();
	LOG(ERROR) << __func__ << " Done with commit for Ckpt ID:"
			<< ckpt_id << ", ret:" << rc;
	return rc;
}

int NewVmdkStatsReq(const std::string& vmdkid, VmdkCacheStats* vmdk_stats) {
	auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdkid);
	if (pio_unlikely(not p)) {
		LOG(ERROR) << "Given VmdkId is not present";
		return -EINVAL;
	}
	auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
	auto aero_conn = GetAeroConn(vmdkp);
	if (pio_unlikely(not aero_conn)) {
		LOG(ERROR) << "Given VM is not configured with AeroSpike cache";
		return -EINVAL;
	}

	auto vmp = vmdkp->GetVM();
	auto rc = vmp->GetVmdkParentStats(aero_conn.get(), vmdkp, vmdk_stats);
	if (pio_unlikely(rc)) {
		LOG(ERROR) << "Failed to get stats from Aerospike";
		return -EINVAL;
	}

	vmdkp->GetCacheStats(vmdk_stats);

	return 0;
}

std::shared_ptr<AeroSpikeConn> GetAeroConn(const ActiveVmdk *vmdkp) {
	auto vmp = vmdkp->GetVM();
	if (pio_unlikely(vmp == nullptr)) {
		return nullptr;
	}

	auto vm_confp = vmp->GetJsonConfig();
	AeroClusterID aero_cluster_id;
	auto ret = vm_confp->GetAeroClusterID(aero_cluster_id);
	if (pio_unlikely(ret == 0)) {
		LOG(ERROR) << __func__ << " Unable to find aerospike cluster "
			"id for given disk." " Please check JSON configuration "
			"with associated VM. Moving ahead without"
			" Aero connection object";
		return nullptr;
	}

	/* Get aero connection object*/
	return  pio::AeroSpikeConnFromClusterID(aero_cluster_id);
}

VmdkHandle NewActiveVmdk(VmHandle vm_handle, VmdkID vmdkid,
		const std::string& config) {
	auto managerp = SingletonHolder<VmdkManager>::GetInstance();
	auto local_vmdkid = vmdkid;

	if (auto vmdkp = managerp->GetInstance(vmdkid); pio_unlikely(vmdkp)) {
		/* VMDK already present */
		return StorRpc_constants::kInvalidVmdkHandle();
	}

	auto vmp = SingletonHolder<pio::VmManager>::GetInstance()->GetInstance(vm_handle);
	if (pio_unlikely(not vmp)) {
		return StorRpc_constants::kInvalidVmdkHandle();
	}

	auto handle = managerp->CreateInstance<ActiveVmdk>(std::move(vmdkid), vmp,
		config);
	if (pio_unlikely(handle == StorRpc_constants::kInvalidVmdkHandle())) {
		return handle;
	}

	try {
		auto vmdkp = dynamic_cast<ActiveVmdk*>(managerp->GetInstance(handle));
		if (pio_unlikely(vmdkp == nullptr)) {
			throw std::runtime_error("Fatal error");
		}
		auto configp = vmdkp->GetJsonConfig();

		auto blktrace = std::make_unique<BlockTraceHandler>();
		auto lock = std::make_unique<LockHandler>();
		auto unalingned = std::make_unique<UnalignedHandler>();
		auto compress = std::make_unique<CompressHandler>(configp);
		auto encrypt = std::make_unique<EncryptHandler>(configp);
		auto multi_target = std::make_unique<MultiTargetHandler>(vmdkp, configp);

		vmdkp->RegisterRequestHandler(std::move(blktrace));
		vmdkp->RegisterRequestHandler(std::move(lock));
		vmdkp->RegisterRequestHandler(std::move(unalingned));
		vmdkp->RegisterRequestHandler(std::move(compress));
		vmdkp->RegisterRequestHandler(std::move(encrypt));
		vmdkp->RegisterRequestHandler(std::move(multi_target));

		vmp->AddVmdk(vmdkp);
	} catch (const std::exception& e) {
		managerp->FreeVmdkInstance(handle);
		LOG(ERROR) << "Failed to add VMDK";
		handle = StorRpc_constants::kInvalidVmdkHandle();
	}
	return handle;
}

int RemoveActiveVmdk(VmHandle vm_handle, VmdkID vmdkid) {
	auto managerp = SingletonHolder<VmdkManager>::GetInstance();

	/* Check that given disk is added */
	auto vmdkp = managerp->GetInstance(vmdkid);
	if (pio_unlikely(not vmdkp)) {
		/* VMDK not present */
		return StorRpc_constants::kInvalidVmdkHandle();
	}

	auto vmp = SingletonHolder<pio::VmManager>::GetInstance()->GetInstance(vm_handle);
	if (pio_unlikely(not vmp)) {
		return StorRpc_constants::kInvalidVmdkHandle();
	}

	try {
		/* Send the Unregitser to handlers */
#if 0
		auto ptr = dynamic_cast<ActiveVmdk*> (vmdkp);
		ptr->Cleanup();
#endif
		vmp->RemoveVmdk(dynamic_cast<ActiveVmdk*> (vmdkp));
		managerp->FreeVmdkInstance(vmdkp->GetHandle());
	} catch (const std::exception& e) {
		LOG(ERROR) << "Failed to Remove VMDK";
		return 1;
	}

	return 0;
}

int PrepareCkpt(VmHandle vm_handle) {

	LOG(ERROR) << __func__ << "Start";
	auto managerp = SingletonHolder<VmdkManager>::GetInstance();

	auto vmp = SingletonHolder<pio::VmManager>::GetInstance()->GetInstance(vm_handle);
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << __func__ << " Invalid vm_handle:-" << vm_handle;
		return StorRpc_constants::kInvalidVmdkHandle();
	}

	/* Create very firt checkpoint if not present, we want a forzen copy to update bitmap */
	auto cur_ckptid = vmp->GetCurCkptID();
	if (pio_likely(cur_ckptid == MetaData_constants::kInvalidCheckPointID() + 1)) {

		LOG(ERROR) << __func__ << " Creating first checkpoint";
		auto f = vmp->TakeCheckPoint();
		f.wait();
		auto [id, rc] = f.value();
		if (pio_unlikely(rc)) {
			return rc;
		}
		log_assert (id == MetaData_constants::kInvalidCheckPointID() + 1);
		LOG(ERROR) << __func__ << "Checkpoint done";
	}

	return 0;
}

uint64_t GetBlocks(std::string& s, const char& delimiter,
		std::unordered_set<BlockID>& blocks,
		size_t block_size) {

	std::string token;
	std::istringstream tokenStream(s);
	while (std::getline(tokenStream, token, delimiter))
	{
		std::string temp = token;
		std::size_t first = temp.find_first_of(":");
		if (first == std::string::npos) {
			return 1;
		}

		std::string strNew = temp.substr(0, first);
		auto start = stol(strNew);

		strNew = temp.substr(first + 1, temp.length() - 1);
		auto length = stol(strNew);

		bool partial_ = not IsBlockSizeAlgined(start, block_size);
		if (partial_) {
			auto aligned_start = AlignDownToBlockSize(start, block_size);
			length += start - aligned_start;
			start = aligned_start;
		}

		while (length > 0)  {
			log_assert(start % block_size == 0);
			blocks.insert(start / block_size);
			start += block_size;
			length -= block_size;
		}
	}

	/* Print the blocks */
	for (auto a : blocks) {
		LOG(ERROR) << __func__ << "block ::" << a;
	}

	return 0;
}

int SetCkptBitmap(VmHandle vm_handle, VmdkID vmdkid, const std::string& config) {

	auto managerp = SingletonHolder<VmdkManager>::GetInstance();

	auto vmp = SingletonHolder<pio::VmManager>::GetInstance()->GetInstance(vm_handle);
	if (pio_unlikely(not vmp)) {
		LOG(ERROR) << __func__ << " Invalid vm_handle:-" << vm_handle;
		return StorRpc_constants::kInvalidVmdkHandle();
	}

	auto vmdkp = managerp->GetInstance(vmdkid);
	if (pio_unlikely(not vmdkp)) {
		LOG(ERROR) << __func__ << " Invalid vmdkid:-" << vmdkid;
		return StorRpc_constants::kInvalidVmdkHandle();
	}

	auto cur_ckptid = vmp->GetCurCkptID();
	if (cur_ckptid == MetaData_constants::kInvalidCheckPointID() + 1) {
		LOG(ERROR) << __func__ << " The very first checkpoint"
			" has not been created,"
			" please run prepare_ckpt REST call first.";
		return -EINVAL;
	}

	std::unique_ptr<config::SetCkptBmapConfig> configp
		= std::make_unique<config::SetCkptBmapConfig>(config);

	auto ckpt_id = MetaData_constants::kInvalidCheckPointID();
	if(pio_likely(!configp->GetCkptID(ckpt_id))) {
		LOG(ERROR) << __func__ << " Ckpt ID has not given as input, assuming ckpt id as 1";
		//ckpt_id = vmp->GetCurCkptID() - 1;
		ckpt_id = MetaData_constants::kInvalidCheckPointID() + 1;
	}

	if (pio_unlikely(ckpt_id != MetaData_constants::kInvalidCheckPointID() + 1)) {
		LOG(ERROR) << __func__ << " Invalid ckpt id as input, currently"
				" ckpt bitmap set is allowed only for 1st "
				" checkpoint only";
		return -EINVAL;
	}

	/*
	 * It's responsibility of caller to break extent list into smaller
	 * chunks and stream it to storD using multiple set ckpt bitmap calls
	 * Handle the cases of string overflow
	 */

	std::string extents;
	extents.clear();
	try {
		if (pio_likely(!configp->GetExtents(extents))) {
			LOG(ERROR) << __func__ << " Invalid configurtion, no extents input found";
			return -EINVAL;
		}
	} catch (const std::exception& e) {
		LOG(ERROR) << __func__ << " Exception " << e.what();
		return -ENOMEM;
	}

	if (pio_unlikely(extents.empty())) {
		LOG(ERROR) << "Invalid configurtion, empty extents list as input";
		return -EINVAL;
	}

	auto active_disk = dynamic_cast<ActiveVmdk*>(vmdkp);
	std::unordered_set<BlockID> blocks;
	if (GetBlocks(extents, ',', blocks, active_disk->BlockSize())) {
		LOG(ERROR) << "Unable to create block list from given extents";
		return -ENOMEM;
	}

	auto rc = active_disk->SetCkptBitmap(ckpt_id, blocks);
	if (pio_unlikely(rc)) {
		LOG(ERROR) << __func__ << " Failed to set checkpoint bitmap";
	}

	return rc;
}

VmHandle GetVmHandle(const std::string& vmid) {
	try {
		auto vmp = SingletonHolder<pio::VmManager>::GetInstance()->GetInstance(vmid);
		if (pio_unlikely(not vmp)) {
			return StorRpc_constants::kInvalidVmHandle();
		}
		return vmp->GetHandle();
	} catch (const std::exception& e) {
		return StorRpc_constants::kInvalidVmHandle();
	}
}

void RemoveVm(VmHandle handle) {
	try {
		SingletonHolder<pio::VmManager>::GetInstance()->FreeInstance(handle);
	} catch (...) {

	}
}


VmdkHandle GetVmdkHandle(const std::string& vmdkid) {
	try {
		auto vmdkp =
			SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdkid);
		if (pio_unlikely(not vmdkp)) {
			return StorRpc_constants::kInvalidVmdkHandle();
		}
		return vmdkp->GetHandle();
	} catch (const std::exception& e) {
		return StorRpc_constants::kInvalidVmdkHandle();
	}
}

void RemoveVmdk(VmdkHandle handle) {
	try {
		auto managerp = SingletonHolder<VmdkManager>::GetInstance();
		managerp->FreeVmdkInstance(handle);
	} catch (const std::exception& e) {

	}
}

int AeroSetTruncate(AeroClusterID cluster_id, const std::string& setp) {

	auto aero_conn = AeroSpikeConnFromClusterID(cluster_id);
	if (pio_unlikely(not aero_conn)) {
		LOG(ERROR) << __func__ << "Unable to get the aerospike connection info";
		return 1;
	}

	auto aero_conn_ = aero_conn.get();
	as_error err;

	LOG(ERROR) << __func__ << "Attempting to delete set ::" << setp.c_str()
		<< " from namespace ::" << pio::kAsNamespaceCacheClean.c_str();

	/*
	 * NOTE: 'aerospike_truncate' returns AEROSPIKE_OK even if
	 *        set does not exist in namespace.
	 */
	auto rc = aerospike_truncate(&aero_conn_->as_, &err, NULL,
			pio::kAsNamespaceCacheClean.c_str(), setp.c_str(), 0);
	if (rc != AEROSPIKE_OK) {
		LOG(ERROR) << __func__ << "Set delete NS: " << pio::kAsNamespaceCacheClean
			<< " Set:" << setp << " cleanup failed, may not exist. Error code::" << rc;
	}

	LOG(ERROR) << __func__ << "Attempting to delete set ::" << setp.c_str()
		<< " from namespace ::" << pio::kAsNamespaceCacheDirty.c_str();

	rc = aerospike_truncate(&aero_conn_->as_, &err, NULL,
			pio::kAsNamespaceCacheDirty.c_str(), setp.c_str(), 0);
	if (rc != AEROSPIKE_OK) {
		LOG(ERROR) << __func__ << "Set delete NS: " << pio::kAsNamespaceCacheDirty
			<< " Set:" << setp << " cleanup failed, may not exist. Error code:: " << rc;
	}
	return rc;
}

int AeroSetDelete(VmID vmid) {
	try {
		auto managerp = SingletonHolder<pio::VmManager>::GetInstance();
		auto vmp = managerp->GetInstance(vmid);
		if (pio_unlikely(not vmp)) {
			LOG(ERROR) << "Given VmID is not present";
			return 1;
		}

		auto vm_confp = vmp->GetJsonConfig();
		AeroClusterID aero_cluster_id;
		auto ret = vm_confp->GetAeroClusterID(aero_cluster_id);
		if (pio_unlikely(ret == 0)) {
			LOG(ERROR) << __func__ << "Unable to find aerospike cluster "
				"id for given disk." " Please check JSON configuration "
				"with associated VM. Moving ahead without"
				" Aero connection object";
			return 1;
		}

		LOG(ERROR) << __func__ << "Get all connection info";
		pio::AeroSpikeConnDisplay();

		auto setp = vm_confp->GetTargetName();
		auto rc = AeroSetTruncate(aero_cluster_id, setp);
		if (pio_unlikely(rc)) {
			LOG(ERROR) << __func__ << "Truncate failed.";
			return 1;
		}

		return rc;
	} catch (const std::exception& e) {
		return 1;
	}
}

int AeroSetCleanup(AeroClusterID cluster_id,
	const std::string& config) {
	try {
		namespace pt = boost::property_tree;
		pt::ptree config_params;
		config_params.clear();
		std::istringstream is(config);
		pt::read_json(is, config_params);

		auto vmid = config_params.get<std::string>("vmid");
		auto vm_handle = pio::GetVmHandle(vmid);
		if (pio_unlikely(vm_handle != StorRpc_constants::kInvalidVmHandle())) {
			LOG(ERROR) << "VM still exists, please issue"
			" vm_delete REST call for VMID:: " << vmid << " first";
			return 1;
		}

		auto setp = config_params.get<std::string>("TargetName");
		auto rc = AeroSetTruncate(cluster_id, setp);
		if (pio_unlikely(rc)) {
			LOG(ERROR) << __func__ << "Truncate failed.";
			return 1;
		}
		return rc;
	} catch (const std::exception& e) {
		LOG(ERROR) << "Failed to cleanup set. Received exception: "
			<< e.what();
		return 1;
	}
}

}
