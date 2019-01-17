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
#include "FlushManager.h"
#include "Singleton.h"
#include "AeroOps.h"
#include "BlockTraceHandler.h"
#include "LockHandler.h"
#include "UnalignedHandler.h"
#include "CompressHandler.h"
#include "EncryptHandler.h"
#include "MultiTargetHandler.h"

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
		timer_(kTickSeconds), analyzer_(vm_id_, kL1Ticks, kL2Ticks, kL3Ticks) {
	setname_ = config_->GetTargetName();
}

VirtualMachine::~VirtualMachine() {
}

const VmID& VirtualMachine::GetID() const noexcept {
	return vm_id_;
}

VmHandle VirtualMachine::GetHandle() const noexcept {
	return handle_;
}

const config::VmConfig* VirtualMachine::GetJsonConfig() const noexcept {
	return config_.get();
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
	auto body = analyzer_.GetIOStats();
	if (not body) {
		return;
	}
	std::string endpoint = EndPoint::kStats + GetID();
	this->RestCall(instancep, std::move(endpoint), std::move(body.value()));
}

void VirtualMachine::PostFingerPrintStats(_ha_instance* instancep) {
	auto body = analyzer_.GetFingerPrintStats();
	if (not body) {
		return;
	}
	std::string endpoint = EndPoint::kFingerPrint + GetID();
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

	AddVmdk(vmdkp);
	StartPreload(vmdkp);
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


int VirtualMachine::FlushStatus(FlushStats &flush_stat) {
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
			auto parent_disk = vmdkp->GetParentDiskSet();
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
		bool trigger_read_ahead = true) {
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

	auto NewRequest = [&trigger_read_ahead] (RequestID reqid, ActiveVmdk* vmdkp, folly::IOBuf* bufp,
			size_t size, int64_t offset) {
		auto req = std::make_unique<Request>(reqid, vmdkp, Request::Type::kRead,
			bufp->writableData(), size, size, offset);
		req->SetReadAheadRequired(trigger_read_ahead);
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
		req->SetReadAheadRequired(trigger_read_ahead);
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

Stun::Stun() : promise(), futures(promise.getFuture()) {
}

folly::Future<int> Stun::GetFuture() {
	return futures.getFuture();
}

void Stun::SetPromise(int result) {
	promise.setValue(result);
}

}
