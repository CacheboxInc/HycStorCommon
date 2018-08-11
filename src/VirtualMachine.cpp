#include <algorithm>

#include <folly/futures/Future.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "VirtualMachine.h"
#include "Vmdk.h"
#include "VmConfig.h"
#include "FlushManager.h"
#include "Singleton.h"
#include <aerospike/aerospike_info.h>
#include <boost/algorithm/string.hpp>
#include "AeroOps.h"

using namespace ::hyc_thrift;
using namespace ::ondisk;

namespace pio {

VirtualMachine::VirtualMachine(VmHandle handle, VmID vm_id,
		const std::string& config) : handle_(handle), vm_id_(std::move(vm_id)),
		config_(std::make_unique<config::VmConfig>(config)) {
}

VirtualMachine::~VirtualMachine() = default;

const VmID& VirtualMachine::GetID() const noexcept {
	return vm_id_;
}

VmHandle VirtualMachine::GetHandle() const noexcept {
	return handle_;
}

const config::VmConfig* VirtualMachine::GetJsonConfig() const noexcept {
	return config_.get();
}

RequestID VirtualMachine::NextRequestID() {
	return ++request_id_;
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

int VirtualMachine::VmdkCount() {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	return vmdk_.list_.size();
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
}

void VirtualMachine::FlushComplete(CheckPointID ckpt_id) {
	log_assert(ckpt_id != MetaData_constants::kInvalidCheckPointID());
	flush_in_progress_.clear();
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
			return std::make_pair(ckpt_id, 0);;
		});
	});
}

int VirtualMachine::FlushStatus(flush_stats &flush_stat) {
	std::lock_guard<std::mutex> lock(vmdk_.mutex_);
	per_disk_flush_stat disk_stats;
	for (const auto& vmdkp : vmdk_.list_) {
		disk_stats = std::make_pair(vmdkp->aux_info_->GetFlushedBlksCnt(),
			vmdkp->aux_info_->GetMovedBlksCnt());
		flush_stat.emplace(vmdkp->GetID(), disk_stats);
	}
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
	LOG(ERROR) << __func__ << "strNew:::-" << strNew.c_str();
	return stol(strNew);
}

int VirtualMachine::AeroCacheStats(AeroStats *aero_statsp, AeroSpikeConn *aerop) {

	std::ostringstream os_dirty;
	auto tgtname = GetJsonConfig()->GetTargetName();
	os_dirty << "sets/DIRTY/" << tgtname;
	LOG(ERROR) << __func__ << "Dirty cmd is::" << os_dirty.str();

	std::ostringstream os_clean;
	os_clean << "sets/CLEAN/" << tgtname;
	LOG(ERROR) << __func__ << "Clean cmd is::" << os_clean.str();

	auto aeroconf = aerop->GetJsonConfig();
	std::string node_ips = aeroconf->GetAeroIPs();
	LOG(ERROR) << __func__ << "ips are::" << node_ips.c_str();

	std::vector<std::string> results;
	boost::split(results, node_ips, boost::is_any_of(","));
	uint32_t port;
	aeroconf->GetAeroPort(port);

	char* res = NULL;
	auto rc = 0;
	as_error err;
	std::ostringstream os_parent;
	for (auto node_ip : results) {
		LOG(ERROR) << __func__ << "Node ip is::" << node_ip.c_str();
		res = NULL;
		if (aerospike_info_host(&aerop->as_, &err, NULL, node_ip.c_str(), port, os_dirty.str().c_str(), &res) != AEROSPIKE_OK) {
			LOG(ERROR) << __func__ << " aerospike_info_host failed";
			rc = -ENOMEM;
			break;
		} else if (res) {
			LOG(ERROR) << __func__ << " aerospike_info_host command completed successfully for DIRTY ns, res::" << res;
			LOG(ERROR) << __func__ << "Dirty block count ::" << GetObjectCount(res);
			aero_statsp->dirty_cnt_ += GetObjectCount(res);
			free(res);
		}

		res = NULL;
		if (aerospike_info_host(&aerop->as_, &err, NULL, node_ip.c_str(), port, os_clean.str().c_str(), &res) != AEROSPIKE_OK) {
			LOG(ERROR) << __func__ << " aerospike_info_host failed";
			rc = -ENOMEM;
			break;
		} else if (res) {
			LOG(ERROR) << __func__ << " aerospike_info_host command completed successfully for CLEAN ns, res::" << res;
			LOG(ERROR) << __func__ << "CLEAN block count ::" << GetObjectCount(res);
			aero_statsp->clean_cnt_ += GetObjectCount(res);
			free(res);
		}

		std::lock_guard<std::mutex> lock(vmdk_.mutex_);
		for (const auto& vmdkp : vmdk_.list_) {
			/* Loop on each disk for this disk to get the Parent Cache stats */
			auto parent_disk = vmdkp->GetJsonConfig()->GetParentDisk();
			if (pio_unlikely(!parent_disk.size())) {
				continue;
			}

			os_parent.str("");
			os_parent.clear();
			os_parent << "sets/CLEAN/" << parent_disk;
			LOG(ERROR) << __func__ << "Parent set cmd is::" << os_parent.str();
			if (aerospike_info_host(&aerop->as_, &err, NULL, node_ip.c_str(),
					port, os_parent.str().c_str(), &res) != AEROSPIKE_OK) {
				LOG(ERROR) << __func__ << " aerospike_info_host failed";
				rc = -ENOMEM;
				break;
			} else if (res) {
				LOG(ERROR) << __func__ << " aerospike_info_host command completed successfully for Parent ns, res::" << res;
				LOG(ERROR) << __func__ << "Parent block count ::" << GetObjectCount(res);
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

int VirtualMachine::FlushStart(CheckPointID ckpt_id) {
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
				promise = std::move(promise)]() mutable {
				auto rc = vmdkp->FlushStages(ckpt_id);
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

Stun::Stun() : promise(), futures(promise.getFuture()) {
}

folly::Future<int> Stun::GetFuture() {
	return futures.getFuture();
}

void Stun::SetPromise(int result) {
	promise.setValue(result);
}

}
