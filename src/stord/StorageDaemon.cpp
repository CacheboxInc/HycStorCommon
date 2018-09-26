#include <cstdint>
#include <cstddef>
#include <chrono>
#include <thread>
#include <memory>
#include <unistd.h>
#include <csignal>

#include <glog/logging.h>

#include <folly/init/Init.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>
#include <chrono>

#include "gen-cpp2/StorRpc.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "DaemonTgtInterface.h"
#include "Request.h"
#include "Vmdk.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "halib.h"
#include "VmdkFactory.h"
#include "Singleton.h"
#include "AeroFiberThreads.h"
#include "FlushManager.h"
#include "ScanManager.h"
#include <boost/format.hpp>
#include <boost/property_tree/ini_parser.hpp>

#ifdef USE_NEP
#include <TargetManager.hpp>
#include <TargetManagerRest.hpp>
#endif

/*
 * Max number of pending REST call requests allowed at stord
 * at a time. This is mainly to handle the scenario where their
 * are multiple requests pending at stord and some of them are
 * timing out. Since stord rest handler will not be aware about
 * client timing out, it will continue processing the timeout
 * REST calls which can cause problem. That's why put a cap on
 * number of pending requests at stord which can be handled in
 * a timely fashion. More thinking is needed on this approach.
 * like ignore EEXIST on subsquent calls.
 */

#define MAX_PENDING_LIMIT 50
#define MAX_W_IOS_IN_HISTORY 100000
#define MAX_R_IOS_IN_HISTORY 100000

using namespace apache::thrift;
using namespace apache::thrift::async;
using namespace ::hyc_thrift;
using namespace pio;
#ifdef USE_NEP
using namespace hyc;
#endif

static constexpr int32_t kServerPort = 9876;
static const std::string kServerIp = "0.0.0.0";
static const std::string kNewVm = "new_vm";
static const std::string kNewVmdk = "new_vmdk";
static const std::string kNewAero = "new_aero";
static const std::string kDelAero = "del_aero";
static const std::string kFlushReq = "flush_req";
static const std::string kScanReq = "scan_del_req";
static const std::string kFlushStatus = "flush_status";
static const std::string kScanStatus = "scan_status";
static const std::string kAeroSetCleanup = "aero_set_cleanup";
static const std::string kRemoveVmdk = "vmdk_delete";
static const std::string kRemoveVm = "vm_delete";
static const std::string kAeroCacheStat = "aero_stat";
static const uint32_t kBulkWriteMaxSize = 256 * 1024;

class StorRpcSvImpl : public virtual StorRpcSvIf {
public:
	void async_tm_Ping(
			std::unique_ptr<HandlerCallback<std::unique_ptr<std::string>>> cb)
			override {
		std::string pong("pong");
		cb->result(std::move(pong));
	}

	void async_tm_PushVmdkStats(std::unique_ptr<HandlerCallbackBase> cb,
			VmdkHandle vmdk, std::unique_ptr<VmdkStats> stats) override {
		LOG(WARNING) << "read_requests " << stats->read_requests
			<< " read_bytes " << stats->read_bytes
			<< " read_latency " << stats->read_latency;
	}

	void async_tm_OpenVm(std::unique_ptr<HandlerCallback<VmHandle>> cb,
			std::unique_ptr<std::string> vmid) override {
		cb->result(GetVmHandle(*vmid.get()));
	}

	void async_tm_CloseVm(std::unique_ptr<HandlerCallback<void>> cb,
			VmHandle vm) override {
		cb->done();
	}

	void async_tm_OpenVmdk(std::unique_ptr<HandlerCallback<VmdkHandle>> cb,
			std::unique_ptr<std::string> vmid,
			std::unique_ptr<std::string> vmdkid) override {
		cb->result(GetVmdkHandle(*vmdkid.get()));
	}

	void async_tm_CloseVmdk(std::unique_ptr<HandlerCallback<int32_t>> cb,
			VmdkHandle vmdk) override {
		cb->result(0);
	}

	void async_tm_Read(
			std::unique_ptr<HandlerCallback<std::unique_ptr<ReadResult>>> cb,
			VmdkHandle vmdk, RequestID reqid, int32_t size, int64_t offset)
			override {
		auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdk);
		assert(pio_likely(p));
		auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
		assert(pio_likely(vmdkp));

		auto vmp = vmdkp->GetVM();
		assert(vmp != nullptr);

		auto iobuf = folly::IOBuf::create(size);
		iobuf->unshare();
		iobuf->coalesce();
		assert(pio_likely(not iobuf->isChained()));

		std::unique_lock<std::mutex> r_lock(vmdkp->r_stat_lock_);
		vmdkp->r_pending_count++;
		r_lock.unlock();

		//LOG(ERROR) << "Size ::" << size << "Offset::" << offset;
		auto reqp = std::make_unique<Request>(reqid, vmdkp, Request::Type::kRead,
			iobuf->writableData(), size, size, offset);

		vmp->Read(vmdkp, reqp.get())
		.then([iobuf = std::move(iobuf), cb = std::move(cb), size,
				reqp = std::move(reqp), vmdkp] (int rc) mutable {
			iobuf->append(size);
			auto read = std::make_unique<ReadResult>();
			read->set_reqid(reqp->GetID());
			read->set_data(std::move(iobuf));
			read->set_result(rc);
			auto finish = std::chrono::high_resolution_clock::now();
			long long duration =
				std::chrono::duration_cast<std::chrono::microseconds>
				(finish - reqp->start_time_).count();

			/* Under lock to avoid divide by zero error */
			/* Overflow wrap, hoping that wrap logic works with temp var too*/
			/* Track only MAX_R_IOS_IN_HISTORY previous IOs in histoty, after that reset */

			std::unique_lock<std::mutex> r_lock(vmdkp->r_stat_lock_);
			if ((vmdkp->r_total_latency + duration < vmdkp->r_total_latency) ||
					vmdkp->r_io_count >= MAX_R_IOS_IN_HISTORY) {
				vmdkp->r_total_latency = 0;
				vmdkp->r_io_count = 0;
				vmdkp->r_io_blks_count = 0;
			} else {
				vmdkp->r_total_latency += duration;
				vmdkp->r_io_blks_count += reqp->NumberOfRequestBlocks();
				vmdkp->r_io_count += 1;
			}

			/* We may not hit the modulo condition, keep the value somewhat agressive */
			if (((vmdkp->r_io_count % 100) == 0) && vmdkp->r_io_count && vmdkp->r_io_blks_count) {
				VLOG(5) << __func__ <<
					"[Read:VmdkID:" << vmdkp->GetID() <<
					", Total latency(microsecs) :" << vmdkp->r_total_latency <<
					", Total blks IO count (in blk size):" << vmdkp->r_io_blks_count <<
					", Total IO count:" << vmdkp->r_io_count <<
					", avg blk access latency:" << vmdkp->r_total_latency / vmdkp->r_io_blks_count <<
					", avg IO latency:" << vmdkp->r_total_latency / vmdkp->r_io_count <<
					", pending IOs:" << vmdkp->r_pending_count;
			}

			vmdkp->r_pending_count--;
			r_lock.unlock();
			cb->result(std::move(read));
		});
	}

	void async_tm_Write(
			std::unique_ptr<HandlerCallback<std::unique_ptr<WriteResult>>> cb,
			VmdkHandle vmdk, RequestID reqid, std::unique_ptr<IOBufPtr> data,
			int32_t size, int64_t offset) override {
		auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdk);
		assert(pio_likely(p));
		auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
		assert(pio_likely(vmdkp));

		auto vmp = vmdkp->GetVM();
		assert(vmp != nullptr);

		auto iobuf = std::move(*data);
		iobuf->unshare();
		iobuf->coalesce();
		assert(pio_likely(not iobuf->isChained()));

		std::unique_lock<std::mutex> w_lock(vmdkp->w_stat_lock_);
		vmdkp->w_pending_count++;
		w_lock.unlock();

		//LOG(ERROR) << "Size ::" << size << "Offset::" << offset;
		auto reqp = std::make_unique<Request>(reqid, vmdkp, Request::Type::kWrite,
			iobuf->writableData(), size, size, offset);

		vmp->Write(vmdkp, reqp.get())
		.then([iobuf = std::move(iobuf), cb = std::move(cb),
				reqp = std::move(reqp), vmdkp] (int rc) mutable {
			auto write = std::make_unique<WriteResult>();
			write->set_reqid(reqp->GetID());
			write->set_result(rc);
			auto finish = std::chrono::high_resolution_clock::now();
			long long duration =
				std::chrono::duration_cast<std::chrono::microseconds>
				(finish - reqp->start_time_).count();

			/* Under lock to avoid divide by zero error */
			/* Overflow wrap, hoping that wrap logic works with temp var too*/
			/* Track only MAX_W_IOS_IN_HISTORY previous IOs in histoty, after that reset */

			std::unique_lock<std::mutex> w_lock(vmdkp->w_stat_lock_);
			if ((vmdkp->w_total_latency + duration < vmdkp->w_total_latency)
					|| vmdkp->w_io_count >= MAX_W_IOS_IN_HISTORY) {
				vmdkp->w_total_latency = 0;
				vmdkp->w_io_count = 0;
				vmdkp->w_io_blks_count = 0;
			} else {
				vmdkp->w_total_latency += duration;
				vmdkp->w_io_blks_count += reqp->NumberOfRequestBlocks();
				vmdkp->w_io_count += 1;
			}

			/* We may not hit the modulo condition, keep the value somewhat agressive */
			if (((vmdkp->w_io_count % 100) == 0) && vmdkp->w_io_count && vmdkp->w_io_blks_count) {
				VLOG(5) << __func__ <<
					"[Write:VmdkID:" << vmdkp->GetID() <<
					", Total latency(microsecs) :" << vmdkp->w_total_latency <<
					", Total blks IO count (in blk size):" << vmdkp->w_io_blks_count <<
					", Total IO count:" << vmdkp->w_io_count <<
					", avg blk access latency:" << vmdkp->w_total_latency / vmdkp->w_io_blks_count <<
					", avg IO latency:" << vmdkp->w_total_latency / vmdkp->w_io_count <<
					", pending IOs:" << vmdkp->w_pending_count;
			}

			vmdkp->w_pending_count--;
			w_lock.unlock();

			cb->result(std::move(write));
		});
	}

	void async_tm_WriteSame(
			std::unique_ptr<HandlerCallback<std::unique_ptr<WriteResult>>> cb,
			VmdkHandle vmdk, RequestID reqid, std::unique_ptr<IOBufPtr> data,
			int32_t data_size, int32_t write_size, int64_t offset) override {
		auto write = std::make_unique<WriteResult>();
		write->set_reqid(reqid);
		write->set_result(0);
		cb->result(std::move(write));
	}

	folly::Future<std::unique_ptr<std::vector<::hyc_thrift::WriteResult>>>
	future_BulkWrite(::hyc_thrift::VmdkHandle vmdk,
			std::unique_ptr<std::vector<WriteRequest>> thrift_requests)
			override {
		static_assert(kBulkWriteMaxSize >= 32*1024, "kBulkWriteMaxSize too small");
		using ReqBlockVec = std::vector<RequestBlock*>;
		using ReqVec = std::vector<std::unique_ptr<Request>>;

		auto AllocDS = [] (size_t nr) {
			auto process = std::make_unique<ReqBlockVec>();
			auto reqs = std::make_unique<ReqVec>();
			reqs->reserve(nr);
			return std::make_pair(std::move(reqs), std::move(process));
		};

		auto NewRequest = [] (RequestID reqid, ActiveVmdk* vmdkp,
				folly::IOBuf* bufp, size_t size, int64_t offset) {
			bufp->unshare();
			bufp->coalesce();
			log_assert(not bufp->isChained());
			return std::make_unique<Request>(reqid, vmdkp,
				Request::Type::kWrite, bufp->writableData(), size, size,
				offset);
		};

		auto NewResult = [] (RequestID reqid, int rc) {
			return WriteResult(apache::thrift::FragileConstructor(), reqid, rc);
		};

		auto BulkWrite = [&NewResult] (VirtualMachine* vmp, ActiveVmdk* vmdkp,
				std::unique_ptr<ReqVec> reqs,
				std::unique_ptr<ReqBlockVec> process) mutable {
			return vmp->BulkWrite(vmdkp, *reqs, *process)
			.then([&NewResult, reqs = std::move(reqs),
					process = std::move(process)] (int rc) mutable {
				std::vector<::hyc_thrift::WriteResult> res;
				res.reserve(reqs->size());
				for (const auto& req : *reqs) {
					res.emplace_back(NewResult(req->GetID(), req->GetResult()));
				}
				return res;
			});
		};

		auto UnalignWrite = [&NewResult] (VirtualMachine* vmp, ActiveVmdk* vmdkp,
				std::unique_ptr<Request> req) mutable {
			auto reqp = req.get();
			return vmp->Write(vmdkp, reqp)
			.then([&NewResult, req = std::move(req)] (int rc) mutable {
				std::vector<::hyc_thrift::WriteResult> res;
				res.emplace_back(NewResult(req->GetID(), req->GetResult()));
				return res;
			});
		};

		auto ExtractParams = [] (WriteRequest& wr)
				-> std::tuple<RequestID, int32_t, int64_t> {
			return {wr.get_reqid(), wr.get_size(), wr.get_offset()};
		};

		struct {
			bool operator() (const WriteRequest& wr1, const WriteRequest& wr2) {
				return wr1.get_offset() < wr2.get_offset();
			}
		} CompareOffset;
		if (not std::is_sorted(thrift_requests->begin(), thrift_requests->end(),
				CompareOffset)) {
			std::sort(thrift_requests->begin(), thrift_requests->end(),
				CompareOffset);
		}

		auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdk);
		assert(pio_likely(p));
		auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
		assert(pio_likely(vmdkp));

		auto vmp = vmdkp->GetVM();
		assert(vmp != nullptr);

		std::vector<folly::Future<std::vector<WriteResult>>> futures;
		::ondisk::BlockID prev_start = 0;
		::ondisk::BlockID prev_end = 0;
		std::unique_ptr<ReqBlockVec> process;
		std::unique_ptr<ReqVec> write_requests;

		size_t pending = thrift_requests->size();
		std::tie(write_requests, process) = AllocDS(pending);
		uint32_t write_size = 0;

		for (auto& tr : *thrift_requests) {
			auto [reqid, size, offset] = ExtractParams(tr);
			auto& iobuf = tr.get_data();
			auto req = NewRequest(reqid, vmdkp, iobuf.get(), size, offset);
			auto reqp = req.get();
			--pending;

			if (reqp->HasUnalignedIO()) {
				futures.emplace_back(UnalignWrite(vmp, vmdkp, std::move(req)));
				continue;
			}

			auto [cur_start, cur_end] = reqp->Blocks();
			log_assert(prev_start <= cur_start);

			if (write_size >= kBulkWriteMaxSize &&  prev_end >= cur_start) {
				/* prev req and current req coincide - submit BulkWrite */
				futures.emplace_back(BulkWrite(vmp, vmdkp,
					std::move(write_requests), std::move(process)));

				/* create new data strucutures */
				log_assert(not process and not write_requests);
				std::tie(write_requests, process) = AllocDS(pending+1);
				write_size = 0;
			}

			process->reserve(process->size() + reqp->NumberOfRequestBlocks());
			reqp->ForEachRequestBlock([&] (RequestBlock *blockp) mutable {
				process->emplace_back(blockp);
				return true;
			});
			write_requests->emplace_back(std::move(req));

			write_size += size;
			std::tie(prev_start, prev_end) = {cur_start, cur_end};
		}

		if (pio_likely(not write_requests->empty())) {
			/* submit the last BulkWrite */
			futures.emplace_back(BulkWrite(vmp, vmdkp,
				std::move(write_requests), std::move(process)));
		}

		return folly::collectAll(std::move(futures))
		.then([thrift_requests = std::move(thrift_requests)]
				(const folly::Try<
					std::vector<
						folly::Try<
							std::vector<
								WriteResult>>>>& tries) mutable {
			using ResultType = std::unique_ptr<std::vector<WriteResult>>;
			auto results = std::make_unique<ResultType::element_type>();
			if (pio_unlikely(tries.hasException())) {
				return folly::makeFuture<ResultType>(tries.exception());
			}

			const auto& vec1 = tries.value();
			for (const auto& t1 : vec1) {
				if (pio_unlikely(t1.hasException())) {
					return folly::makeFuture<ResultType>(t1.exception());
				}
				auto& v2 = t1.value();
				results->reserve(results->size() + v2.size());
				std::move(v2.begin(), v2.end(), std::back_inserter(*results));
			}
			return folly::makeFuture(std::move(results));
		});
	}
private:
	std::atomic<VmHandle> vm_handle_{0};
	std::atomic<VmdkHandle> vmdk_handle_{0};
};

std::shared_ptr<ThriftServer> thirft_server;

static void Usr1SignalHandler(int signal) {
	thirft_server->stopListening();
	thirft_server->stop();
}

static bool ValidatePort(const char *flag, int port) {
	if (port > 0 && port < 32768) {
		return true;
	}
	LOG(ERROR) << "Invalid value for " << flag << ": " << port;
	return false;
}

DEFINE_string(etcd_ip, "", "etcd_ip supplied by HA");
DEFINE_string(svc_label, "", "represents service label for the service");
DEFINE_string(stord_version, "", "protocol version of tgt");
DEFINE_int32(ha_svc_port, 0, "ha service port number");
DEFINE_validator(ha_svc_port, &ValidatePort);

static struct {
	std::once_flag initialized_;
	std::unique_ptr<std::thread> thread_;
	std::condition_variable ha_hb_stop_cv_;
	bool stop_{false};
	std::mutex mutex_; // protects above cond var
	struct _ha_instance *ha_instance_;

	/* To Protect against concurrent REST accesses */
	std::mutex lock_;
	/* To Protect pending cnt */
	std::mutex p_lock_;
	/* Pending REST calls */
	std::atomic<int> p_cnt_{0};
} g_thread_;

enum StordSvcErr {
	STORD_ERR_TARGET_CREATE = 1,
	STORD_ERR_LUN_CREATE,
	STORD_ERR_INVALID_VM,
	STORD_ERR_INVALID_VMDK,
	STORD_ERR_INVALID_NO_DATA,
	STORD_ERR_INVALID_PARAM,
	STORD_ERR_INVALID_AERO,
	STORD_ERR_INVALID_FLUSH,
	STORD_ERR_FLUSH_NOT_STARTED,
	STORD_ERR_INVALID_SCAN,
	STORD_ERR_SCAN_NOT_STARTED,
	STORD_ERR_AERO_STAT,
	STORD_ERR_MAX_LIMIT,
};

void HaHeartbeat(void *userp) {
	struct _ha_instance *ha = (struct _ha_instance *) userp;

	while(1) {
		std::unique_lock<std::mutex> lck(g_thread_.mutex_);
		if (g_thread_.ha_hb_stop_cv_.wait_for(lck, std::chrono::seconds(60),
			 [] { return g_thread_.stop_; })) {
			LOG(INFO) << " Stop HA heartbeat thread";
			break;
		} else {
			LOG(INFO) << "After 60 seconds wait, update health status to ha";
			ha_healthupdate(ha);
		}
	}
	LOG(INFO) << "HB thread exiting";
}

int StordHaStartCallback(const _ha_request *reqp, _ha_response *resp,
		void *userp) {
	try {
		log_assert(userp != nullptr);

		std::call_once(g_thread_.initialized_, [=] () mutable {
			g_thread_.thread_ =
			std::make_unique<std::thread>(HaHeartbeat, userp);
			});
		return HA_CALLBACK_CONTINUE;
	} catch (const std::exception& e) {
		return HA_CALLBACK_ERROR;
	}
}

int StordHaStopCallback(const _ha_request *reqp, _ha_response *resp,
		void *userp) {
	{
		std::lock_guard<std::mutex> lk(g_thread_.mutex_);
		g_thread_.stop_ = true;
		g_thread_.ha_hb_stop_cv_.notify_one();
	}
	g_thread_.thread_->join();
	return HA_CALLBACK_CONTINUE;
}

static void SetErrMsg(_ha_response *resp, StordSvcErr err,
		const std::string& msg) {
	auto err_msg = ha_get_error_message(g_thread_.ha_instance_,
		err, msg.c_str());
	ha_set_response_body(resp, HTTP_STATUS_ERR, err_msg,
		strlen(err_msg));
	::free(err_msg);
}

static int GuardHandler() {
	std::unique_lock<std::mutex> p_lock(g_thread_.p_lock_);
	if (g_thread_.p_cnt_ >= MAX_PENDING_LIMIT) {
		p_lock.unlock();
		return 1;
	}

	g_thread_.p_cnt_++;
	p_lock.unlock();
	return 0;
}


static int NewVm(const _ha_request *reqp, _ha_response *resp, void *userp ) {
	auto param_valuep = ha_parameter_get(reqp, "vm-id");

	if (param_valuep  == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vmid param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	auto data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
			"VM config invalid");
		return HA_CALLBACK_CONTINUE;
	}

	std::string req_data(data);
	LOG(INFO) << "VM Configuration " << req_data;
	::free(data);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;
	LOG(INFO) << __func__ << "Start NewVM for vmid" << vmid;
	auto vm_handle = pio::NewVm(vmid, req_data);

	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		es << "Adding new VM failed, VM config: " << req_data;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Added successfully VmID " << vmid << ", VmHandle is " << vm_handle;
	const auto res = std::to_string(vm_handle);

	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

static int RemoveVm(const _ha_request *reqp, _ha_response *resp, void *userp ) {
	auto param_valuep = ha_parameter_get(reqp, "vm-id");

	LOG(INFO) << __func__ << "START";
	if (param_valuep  == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vmid param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;
	auto ret = pio::RemoveVmUsingVmID(vmid);
	if (ret) {
		std::ostringstream es;
		es << "Removing VM failed";
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Sucessfully Removed VmID::- " << vmid;
	const auto res = std::to_string(ret);

	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

static int NewAeroCluster(const _ha_request *reqp, _ha_response *resp,
	void *userp ) {
	auto param_valuep = ha_parameter_get(reqp, "aero-id");
	if (param_valuep  == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"aero-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string aeroid(param_valuep);

	auto data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
			"Aero config not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string req_data(data);
	LOG(INFO) << "Aerospike Configuration " << req_data;
	::free(data);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;
	auto aero_handle = pio::NewAeroCluster(aeroid, req_data);
	if (aero_handle == kInvalidAeroClusterHandle) {
		std::ostringstream es;
		es << "Adding new AeroSpike cluster failed, Aero config: "
			<< req_data;
		SetErrMsg(resp, STORD_ERR_INVALID_AERO, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Added AeroCluster successfully";
	const auto res = std::to_string(aero_handle);

	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

static int DelAeroCluster(const _ha_request *reqp, _ha_response *resp,
	void *userp ) {
	auto param_valuep = ha_parameter_get(reqp, "aero-id");

	if (param_valuep  == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"aero-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string aeroid(param_valuep);

	auto data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
			"Aero config invalid");
		return HA_CALLBACK_CONTINUE;
	}
	std::string req_data(data);
	LOG(INFO) << "Aerospike Configuration " << req_data;
	::free(data);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;
	auto aero_handle = pio::DelAeroCluster(aeroid, req_data);
	if (aero_handle == kInvalidAeroClusterHandle) {
		std::ostringstream es;
		es << "Adding new AeroSpike cluster failed, Aero config: "
			<< req_data;
		SetErrMsg(resp, STORD_ERR_INVALID_AERO, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Added AeroCluster successfully";
	const auto res = std::to_string(aero_handle);

	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

static int NewVmdk(const _ha_request *reqp, _ha_response *resp, void *userp ) {
	auto param_valuep = ha_parameter_get(reqp, "vm-id");

	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);
	param_valuep = ha_parameter_get(reqp, "vmdk-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vmdk-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmdkid(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;
	LOG(INFO) << "Started add of VMDKID::" << vmdkid << "in VmID:: " << vmid;
	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		es << "Adding new VMDK failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	char *data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
			"VMDK config invalid");
		return HA_CALLBACK_CONTINUE;
	}
	std::string req_data(data);
	LOG(INFO) << "VM Configuration " << req_data;
	::free(data);

	auto vmdk_handle = pio::NewActiveVmdk(vm_handle, vmdkid, req_data);
	if (vmdk_handle == StorRpc_constants::kInvalidVmdkHandle()) {
		std::ostringstream es;
		es << "Adding new VMDK failed."
			<< " VmID = " << vmid
			<< " VmHandle = " << vm_handle
			<< " VmdkID = " << vmdkid;
		SetErrMsg(resp, STORD_ERR_INVALID_VMDK, es.str());
		return HA_CALLBACK_CONTINUE;
	}
	LOG(INFO) << "Added Successfully VMDK VmID " << vmid
		<< " VmHandle " << vm_handle
		<< " VmdkID " << vmdkid
		<< " VmdkHandle " << vmdk_handle;


	const auto res = std::to_string(vmdk_handle);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

static int RemoveVmdk(const _ha_request *reqp, _ha_response *resp, void *userp ) {
	auto param_valuep = ha_parameter_get(reqp, "vm-id");

	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}

	std::string vmid(param_valuep);
	param_valuep = ha_parameter_get(reqp, "vmdk-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vmdk-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmdkid(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;
	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		es << "Removal of VMDK failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto ret = pio::RemoveActiveVmdk(vm_handle, vmdkid);
	if (ret) {
		std::ostringstream es;
		es << "Removing VMDK failed."
			<< " VmID = " << vmid
			<< " VmHandle = " << vm_handle
			<< " VmdkID = " << vmdkid;
		SetErrMsg(resp, STORD_ERR_INVALID_VMDK, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Removal of VMDK VmID = " << vmid
		<< " VmdkID = " << vmdkid
		<< " completed successfully ";

	const auto res = std::to_string(ret);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

static int NewScanReq(const _ha_request *reqp, _ha_response *resp, void *userp) {

	LOG(ERROR) << "NewScanReq start";
	auto param_valuep = ha_parameter_get(reqp, "vm-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	auto data = ha_get_data(reqp);
	std::string req_data;
	if (data != nullptr) {
		req_data.assign(data);
		::free(data);
	} else {
		req_data.clear();
	}

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;
	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto ret = pio::NewScanReq(vmid, req_data);
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Starting Scan request for VMID::"  << vmid << "Failed";
		es << "Starting Scan request for VMID::"  << vmid << " Failed";
		SetErrMsg(resp, STORD_ERR_INVALID_SCAN, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Scan for VM:" << vmid << "started successfully.";

	const auto res = std::to_string(ret);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}


static int NewScanStatusReq(const _ha_request *reqp, _ha_response *resp, void *userp) {
	auto param_valuep = ha_parameter_get(reqp, "aero-cluster-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"aero-cluter-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string id(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;

	ScanStats scan_stat;
	auto ret = pio::NewScanStatusReq(id, scan_stat);
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Scan status request for aero-cluster-id::"  << id << " Failed, errno:" << ret;
		if (ret == -EINVAL) {
			es << "Scan not running currently for aero-cluster-id::"  << id;
			SetErrMsg(resp, STORD_ERR_SCAN_NOT_STARTED, es.str());
		} else {
			es << "Failed to get scan status for aero-cluster-id::"  << id;
			SetErrMsg(resp, STORD_ERR_INVALID_SCAN, es.str());
		}
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Scan status for aero-cluster-id:" << id << "completed successfully";

	json_t *scan_params = json_object();
	if (pio_likely(scan_stat.progress_pct != 100)) {
		json_object_set(scan_params, "scan_running", json_boolean(true));
	} else {
		json_object_set(scan_params, "scan_running", json_boolean(false));
	}
	json_object_set(scan_params, "progress_pct", json_integer(scan_stat.progress_pct));
	json_object_set(scan_params, "records_scanned", json_integer(scan_stat.records_scanned));
	std::string scan_params_str = json_dumps(scan_params, JSON_ENCODE_ANY);
	json_object_clear(scan_params);
	json_decref(scan_params);
	ha_set_response_body(resp, HTTP_STATUS_OK, scan_params_str.c_str(),
			strlen(scan_params_str.c_str()));

	return HA_CALLBACK_CONTINUE;
}

static int NewFlushReq(const _ha_request *reqp, _ha_response *resp, void *userp) {

	auto param_valuep = ha_parameter_get(reqp, "vm-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	auto data = ha_get_data(reqp);
	std::string req_data;
	if (data != nullptr) {
		req_data.assign(data);
		::free(data);
	} else {
		req_data.clear();
	}

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;
	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto ret = pio::NewFlushReq(vmid, req_data);
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Starting flush request for VMID::"  << vmid << "Failed";
		es << "Starting flush request for VMID::"  << vmid << " Failed";
		SetErrMsg(resp, STORD_ERR_INVALID_FLUSH, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Flush for VM:" << vmid << "started successfully. Please run "
		"flush_status to get the progress";

	const auto res = std::to_string(ret);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}

static int NewFlushStatusReq(const _ha_request *reqp, _ha_response *resp, void *userp) {
	auto param_valuep = ha_parameter_get(reqp, "vm-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;

	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	FlushStats flush_stat;
	auto ret = pio::NewFlushStatusReq(vmid, flush_stat);
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Flush status request for VMID::"  << vmid << "Failed errno:" << ret;
		if (ret == -EINVAL) {
			es << "Flush not running currently for VMID::"  << vmid;
			SetErrMsg(resp, STORD_ERR_FLUSH_NOT_STARTED, es.str());
		} else {
			es << "Failed to get flush status for VMID::"  << vmid;
			SetErrMsg(resp, STORD_ERR_INVALID_FLUSH, es.str());
		}
		return HA_CALLBACK_CONTINUE;
	}

	bool first = true;
	FlushStats::iterator itr;

	uint64_t total_flushed_blks = 0;
	uint64_t total_moved_blks = 0;

	/*TODO: Get total number of blocks to be flushed from bitmap.
	 *      For now sending zero.
	 */
	uint64_t remaining_blks = 0;

	for (itr = flush_stat.begin(); itr != flush_stat.end(); ++itr) {
		if (pio_unlikely(first)) {
			LOG(ERROR) << boost::format("%1% %2% %3% %4%")
				% "Start time:-" % (itr->second).first
				% "Elapsed time:-" % (itr->second).second;
			first = false;
		} else {
			LOG(ERROR) << boost::format("[LUN:%1%] %2% %3% %|20t|%4% %5%")
				% itr->first % "Flushed Blks:-" % (itr->second).first
				% "Moved Blks:-" % (itr->second).second;
			total_flushed_blks += (itr->second).first;
			total_moved_blks   += (itr->second).second;
		}
	}

	LOG(INFO) << "Flush status for VM:" << vmid << "completed successfully";

	json_t *flush_params = json_object();

	json_object_set(flush_params, "flush_running", json_boolean(true));
	json_object_set(flush_params, "flushed_blks_cnt", json_integer(total_flushed_blks));
	json_object_set(flush_params, "moved_blks_cnt", json_integer(total_moved_blks));
	json_object_set(flush_params, "remaining_blks_cnt", json_integer(remaining_blks));

	std::string flush_params_str = json_dumps(flush_params, JSON_ENCODE_ANY);

	json_object_clear(flush_params);
	json_decref(flush_params);

	ha_set_response_body(resp, HTTP_STATUS_OK, flush_params_str.c_str(),
			strlen(flush_params_str.c_str()));

	return HA_CALLBACK_CONTINUE;
}

static int NewAeroCacheStatReq(const _ha_request *reqp, _ha_response *resp, void *userp) {
	auto param_valuep = ha_parameter_get(reqp, "vm-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}

	std::string vmid(param_valuep);
	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;

	auto aero_stats = std::make_shared<AeroStats>();
	auto aero_stats_p = aero_stats.get();
	auto ret = pio::NewAeroCacheStatReq(vmid, aero_stats_p);
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Cache status request for VMID::"  << vmid << "Failed errno:" << ret;
		if (ret == -EINVAL) {
			es << "Invalid VMID"  << vmid;
			SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		} else {
			es << "Failed to get cache status for VMID::"  << vmid;
			SetErrMsg(resp, STORD_ERR_AERO_STAT, es.str());
		}
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Successful...";

	json_t *aero_params = json_object();
	const auto res = std::to_string(ret);
	json_object_set(aero_params, "ret", json_string((char *) res.c_str()));
	json_object_set(aero_params, "dirty_blks_cnt", json_integer(aero_stats_p->dirty_cnt_));
	json_object_set(aero_params, "clean_blks_cnt", json_integer(aero_stats_p->clean_cnt_));
	json_object_set(aero_params, "parent_blks_cnt", json_integer(aero_stats_p->parent_cnt_));
	std::string aero_params_str = json_dumps(aero_params, JSON_ENCODE_ANY);

	json_object_clear(aero_params);
	json_decref(aero_params);

	ha_set_response_body(resp, HTTP_STATUS_OK, aero_params_str.c_str(),
			strlen(aero_params_str.c_str()));
	return HA_CALLBACK_CONTINUE;
}

static int AeroSetCleanup(const _ha_request *reqp, _ha_response *resp,
	void *userp ) {

	auto param_valuep = ha_parameter_get(reqp, "aero-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"aero-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string aeroid(param_valuep);

	auto data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
			"Aero set cleanup config not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string req_data(data);
	LOG(INFO) << "Aerospike cleanup Configuration " << req_data;
	::free(data);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.lock_);
	g_thread_.p_cnt_--;
	auto rc = pio::AeroSetCleanup(aeroid, req_data);
	if (rc) {
		std::ostringstream es;
		LOG(ERROR) << "Set delete request failed";
		es << "Deleting AeroSpike set failed";
		SetErrMsg(resp, STORD_ERR_INVALID_AERO, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Successfully deleted set.";
	const auto res = std::to_string(rc);

	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

int main(int argc, char* argv[])
{
	FLAGS_v = 2;
	folly::init(&argc, &argv, true);

#if 0
	auto pid = ::fork();
	if (pid < 0) {
		LOG(ERROR) << "Fork failed" << pid;
		exit(EXIT_FAILURE);
	}

	if (pid > 0) {
		LOG(INFO) << "Parent process exiting.";
		exit(EXIT_SUCCESS);
	}

	// Create a new SID for child process
	auto sid = ::setsid();
	if (sid < 0) {
		LOG(ERROR) << "Setsid for child process failed" << sid;
		exit(EXIT_FAILURE);
	}

	// Change current working dir to root
	if ((chdir("/")) < 0) {
		LOG(ERROR) << "Changing pwd to / failed";
		exit(EXIT_FAILURE);
	}

	// Close standard file descriptors
	::close(STDIN_FILENO);
	::close(STDOUT_FILENO);
	::close(STDERR_FILENO);

	LOG(INFO) << "stord daemon started successfully";
#else
	std::signal(SIGUSR1, Usr1SignalHandler);

#endif
	auto size = sizeof(struct ha_handlers) + 12 * sizeof(struct ha_endpoint_handlers);

	auto handlers =
		std::unique_ptr<struct ha_handlers,
		void(*)(void *)>(reinterpret_cast<struct ha_handlers*>(::malloc(size)),
			::free);
	if (not handlers) {
		LOG(ERROR) << "Memory allocation for ha handlers failed";
		return -ENOMEM;
	}

	handlers->ha_count = 0;

	/* new_vm handler */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kNewVm.c_str(),
		kNewVm.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = NewVm;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* new_vmdk handler */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kNewVmdk.c_str(),
		kNewVmdk.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = NewVmdk;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* new_aero Cluster */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kNewAero.c_str(),
			kNewAero.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = NewAeroCluster;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* del_aero cluster */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kDelAero.c_str(),
			kDelAero.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = DelAeroCluster;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* Flush Request */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kFlushReq.c_str(),
			kFlushReq.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = NewFlushReq;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* Scan Request */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kScanReq.c_str(),
			kScanReq.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = NewScanReq;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* Flush Status Request */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = GET;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kFlushStatus.c_str(),
			kFlushStatus.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = NewFlushStatusReq;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* Scan Status Request */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kScanStatus.c_str(),
			kScanStatus.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = NewScanStatusReq;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* Aero Stat Request */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = GET;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kAeroCacheStat.c_str(),
			kAeroCacheStat.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = NewAeroCacheStatReq;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* Aerospike Set Cleanup Request */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kAeroSetCleanup.c_str(),
			kAeroSetCleanup.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = AeroSetCleanup;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* Vmdk Delete Request */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kRemoveVmdk.c_str(),
		kRemoveVmdk.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = RemoveVmdk;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	/* remove_vm handler */
	handlers->ha_endpoints[handlers->ha_count].ha_http_method = POST;
	strncpy(handlers->ha_endpoints[handlers->ha_count].ha_url_endpoint, kRemoveVm.c_str(),
		kRemoveVm.size() + 1);
	handlers->ha_endpoints[handlers->ha_count].callback_function = RemoveVm;
	handlers->ha_endpoints[handlers->ha_count].ha_user_data = NULL;
	handlers->ha_count += 1;

	g_thread_.ha_instance_ = ::ha_initialize(FLAGS_ha_svc_port,
			FLAGS_etcd_ip.c_str(), FLAGS_svc_label.c_str(),
			FLAGS_stord_version.c_str(), 120,
			const_cast<const struct ha_handlers *> (handlers.get()),
			StordHaStartCallback, StordHaStopCallback, 0, NULL);
	handlers->ha_count += 1;

	if (g_thread_.ha_instance_ == nullptr) {
		LOG(ERROR) << "ha_initialize failed";
		return -EINVAL;
	}

	InitStordLib();

#ifdef USE_NEP
	/* Initialize threadpool for AeroSpike accesses */
	auto tmgr_rest = std::make_shared<TargetManagerRest>(
			SingletonHolder<TargetManager>::GetInstance().get(),
			g_thread_.ha_instance_);
#endif

	/* Initialize threadpool for AeroSpike accesses */
	auto rc = SingletonHolder<AeroFiberThreads>::GetInstance()
					->CreateInstance();
	log_assert(rc == 0);

	/* Initialize threadpool for Flush processing */
	rc = SingletonHolder<FlushManager>::GetInstance()
					->CreateInstance(g_thread_.ha_instance_);
	log_assert(rc == 0);

	auto si = std::make_shared<StorRpcSvImpl>();
	thirft_server = std::make_shared<ThriftServer>();

	thirft_server->setInterface(si);
	thirft_server->setAddress(kServerIp, kServerPort);
	LOG(INFO) << "Starting Thrift Server";
	google::FlushLogFiles(google::INFO);
	google::FlushLogFiles(google::ERROR);

	thirft_server->serve();

	DeinitStordLib();

	return 0;
}
