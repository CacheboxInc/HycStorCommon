#include <deque>
#include <string>
#include <atomic>
#include <mutex>

#include <folly/fibers/Fiber.h>
#include <folly/futures/Future.h>
#include <glog/logging.h>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "DaemonTgtInterface.h"
#include "Request.h"
#include "ThreadPool.h"
#include "VirtualMachine.h"
#include "Vmdk.h"
#include "BlockTraceHandler.h"
#include "CacheHandler.h"
#include "VmdkConfig.h"
#include "VmdkFactory.h"
#include "VmConfig.h"
#include "VmManager.h"
#include "AeroConn.h"
#include "Singleton.h"
#include "AeroFiberThreads.h"
#include "FlushManager.h"
#include "FlushInstance.h"

using namespace ::hyc_thrift;
using namespace ::ondisk;

namespace pio {
using namespace folly;

struct {
	std::once_flag initialized_;
} g_init_;

struct {
	std::mutex mutex_;
	/* Using handle to just keep track of how many entries are added */
	std::atomic<AeroClusterHandle> handle_{0};
	std::unordered_map<AeroClusterID, std::shared_ptr<AeroSpikeConn>> ids_;
} g_aero_clusters;

int InitStordLib(void) {
	try {
		std::call_once(g_init_.initialized_, [=] () mutable {
			SingletonHolder<VmdkManager>::CreateInstance();
			SingletonHolder<VmManager>::CreateInstance();
			SingletonHolder<AeroFiberThreads>::CreateInstance();
			SingletonHolder<FlushManager>::CreateInstance();
		});
	} catch (const std::exception& e) {
		return -1;
	}
	return 0;
}

int DeinitStordLib(void) {
	return 0;
}


void AeroSpikeConnDisplay() {
	std::lock_guard<std::mutex> lock(g_aero_clusters.mutex_);

	LOG(INFO) << __func__  <<  "ALL entries";
	for(auto it = g_aero_clusters.ids_.begin();
			it != g_aero_clusters.ids_.end(); ++it) {
		 LOG(INFO) << __func__ << it->first << ":::" <<
			(it->second).get();
	}
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
			LOG(ERROR) << __func__ << "cluster_id " << cluster_id
				<< " already present.";
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
			LOG(INFO) << __func__ <<  " cluster_id " << cluster_id
				<< "... SUCCESS";
			return handle;
		}

		return kInvalidAeroClusterHandle;
	} catch (const std::bad_alloc& e) {
		LOG(ERROR) << __func__ <<  " cluster_id " << cluster_id
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

int NewFlushReq(VmID vmid) {
	auto managerp = SingletonHolder<FlushManager>::GetInstance();
	auto ptr = managerp->GetInstance(vmid);
	if (ptr != nullptr) {
		LOG(ERROR) << "Flush is already running for given VM";
		return -EAGAIN;
	}

	try {
		auto rc = managerp->NewInstance(vmid);
		if (pio_unlikely(rc)) {
			return -ENOMEM;
		}

		auto fi = managerp->GetInstance(vmid);
		managerp->threadpool_.pool_->AddTask([managerp, vmid, fi]()
					mutable {
			auto rc = fi->StartFlush(vmid);
			/* Remove fi Instance */
			managerp->FreeInstance(vmid);
			return rc;
		});
	} catch (...) {
		return -ENOMEM;
	}

	return 0;
}

VmHandle NewVm(VmID vmid, const std::string& config) {
	auto managerp = SingletonHolder<VmManager>::GetInstance();
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

std::shared_ptr<AeroSpikeConn> GetAeroConn(ActiveVmdk *vmdkp) {
	auto vmp = vmdkp->GetVM();
	if (pio_unlikely(vmp == nullptr)) {
		return nullptr;
	}

	auto vm_confp = vmp->GetJsonConfig();
	AeroClusterID aero_cluster_id;
	auto ret = vm_confp->GetAeroClusterID(aero_cluster_id);
	if (pio_unlikely(ret == 0)) {
		LOG(ERROR) << __func__ << "Unable to find aerospike cluster "
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

	if (auto vmdkp = managerp->GetInstance(vmdkid); pio_unlikely(vmdkp)) {
		/* VMDK already present */
		return StorRpc_constants::kInvalidVmdkHandle();
	}

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vm_handle);
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

		vmdkp->RegisterRequestHandler(std::make_unique<BlockTraceHandler>());
		auto ch = std::make_unique<CacheHandler>(vmdkp->GetJsonConfig());
		vmdkp->RegisterRequestHandler(std::move(ch));

		vmp->AddVmdk(vmdkp);
	} catch (const std::exception& e) {
		managerp->FreeVmdkInstance(handle);
		LOG(ERROR) << "Failed to add VMDK";
		handle = StorRpc_constants::kInvalidVmdkHandle();
	}
	return handle;
}

VmHandle GetVmHandle(const std::string& vmid) {
	try {
		auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vmid);
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
		SingletonHolder<VmManager>::GetInstance()->FreeInstance(handle);
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

}
