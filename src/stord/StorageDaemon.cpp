#include <cstdint>
#include <cstddef>
#include <chrono>
#include <thread>
#include <memory>
#include <chrono>
#include <unistd.h>
#include <csignal>

#include <glog/logging.h>

#include <folly/init/Init.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <boost/format.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "gen-cpp2/StorRpc.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "DaemonTgtInterface.h"
#include "Vmdk.h"
#include "halib.h"
#include "VmdkFactory.h"
#include "Singleton.h"
#include "AeroFiberThreads.h"
#include "FlushManager.h"
#include "CkptMergeManager.h"
#include "TgtInterfaceImpl.h"
#include "ScanManager.h"
#include "VmManager.h"
#include "VmConfig.h"
#include "ArmConfig.h"
#include "JsonHelper.h"

#ifdef USE_NEP
#include <TargetManager.hpp>
#include <TargetManagerRest.hpp>
#include "ArmSync.h"
#include "VddkLib.h"
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

static constexpr int32_t kServerPort = 9876;
static constexpr char kServerIp[] = "0.0.0.0";

[[maybe_unused]] static constexpr uint16_t kBatchSize = 4;

class StorRpcSvImpl : public virtual StorRpcSvIf {
public:
	void async_tm_Ping(
			std::unique_ptr<HandlerCallback<std::unique_ptr<std::string>>> cb)
			override {
		std::string pong("pong");
		cb->result(std::move(pong));
	}

	void async_tm_PushVmdkStats(std::unique_ptr<HandlerCallbackBase>,
			VmdkHandle, std::unique_ptr<VmdkStats> stats) override {
		LOG(WARNING) << "read_requests " << stats->read_requests
			<< " read_bytes " << stats->read_bytes
			<< " read_latency " << stats->read_latency;
	}

	void async_tm_OpenVm(std::unique_ptr<HandlerCallback<VmHandle>> cb,
			std::unique_ptr<std::string> vmid) override {
		cb->result(GetVmHandle(*vmid.get()));
	}

	void async_tm_CloseVm(std::unique_ptr<HandlerCallback<void>> cb,
			VmHandle) override {
		cb->done();
	}

	void async_tm_OpenVmdk(std::unique_ptr<HandlerCallback<VmdkHandle>> cb,
			std::unique_ptr<std::string>,
			std::unique_ptr<std::string> vmdkid) override {
		cb->result(GetVmdkHandle(*vmdkid.get()));
	}

	void async_tm_CloseVmdk(std::unique_ptr<HandlerCallback<int32_t>> cb,
			VmdkHandle) override {
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

		vmdkp->stats_->r_pending_count++;

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
			auto duration = reqp->GetLatency();

			/* Under lock to avoid divide by zero error */
			/* Overflow wrap, hoping that wrap logic works with temp var too*/
			/* Track only MAX_R_IOS_IN_HISTORY previous IOs in histoty, after that reset */

			if ((vmdkp->stats_->r_total_latency + duration < vmdkp->stats_->r_total_latency) ||
					vmdkp->stats_->r_io_count >= MAX_R_IOS_IN_HISTORY) {
				vmdkp->stats_->r_total_latency = 0;
				vmdkp->stats_->r_io_count = 0;
				vmdkp->stats_->r_io_blks_count = 0;
			} else {
				vmdkp->stats_->r_total_latency += duration;
				vmdkp->stats_->r_io_blks_count += reqp->NumberOfRequestBlocks();
				vmdkp->stats_->r_io_count += 1;
			}

			/* We may not hit the modulo condition, keep the value somewhat agressive */
			if (((vmdkp->stats_->r_io_count % 100) == 0) && vmdkp->stats_->r_io_count && vmdkp->stats_->r_io_blks_count) {
				VLOG(5) << __func__ <<
					"[Read:VmdkID:" << vmdkp->GetID() <<
					", Total latency(microsecs) :" << vmdkp->stats_->r_total_latency <<
					", Total blks IO count (in blk size):" << vmdkp->stats_->r_io_blks_count <<
					", Total IO count:" << vmdkp->stats_->r_io_count <<
					", avg blk access latency:" << vmdkp->stats_->r_total_latency / vmdkp->stats_->r_io_blks_count <<
					", avg IO latency:" << vmdkp->stats_->r_total_latency / vmdkp->stats_->r_io_count <<
					", pending IOs:" << vmdkp->stats_->r_pending_count;
			}

			vmdkp->stats_->r_pending_count--;
			cb->result(std::move(read));
		});
	}

	folly::Future<std::unique_ptr<std::vector<::hyc_thrift::ReadResult>>>
	future_BulkRead(::hyc_thrift::VmdkHandle vmdk,
			std::unique_ptr<std::vector<::hyc_thrift::ReadRequest>> rpc_reqs)
			override {
		struct {
			bool operator() (const ReadRequest& rd1, const ReadRequest& rd2) {
				return rd1.get_offset() < rd2.get_offset();
			}
		} CompareOffset;
		if (not std::is_sorted(rpc_reqs->begin(), rpc_reqs->end(), CompareOffset)) {
			std::sort(rpc_reqs->begin(), rpc_reqs->end(), CompareOffset);
		}

		auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdk);
		assert(pio_likely(p));
		auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
		assert(pio_likely(vmdkp));

		auto vmp = vmdkp->GetVM();
		assert(vmp != nullptr);

		return vmp->BulkRead(vmdkp, std::move(rpc_reqs));
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

		vmdkp->stats_->w_pending_count++;

		auto reqp = std::make_unique<Request>(reqid, vmdkp, Request::Type::kWrite,
			iobuf->writableData(), size, size, offset);

		vmp->Write(vmdkp, reqp.get())
		.then([iobuf = std::move(iobuf), cb = std::move(cb),
				reqp = std::move(reqp), vmdkp] (int rc) mutable {
			auto write = std::make_unique<WriteResult>();
			write->set_reqid(reqp->GetID());
			write->set_result(rc);
			auto duration = reqp->GetLatency();

			/* Under lock to avoid divide by zero error */
			/* Overflow wrap, hoping that wrap logic works with temp var too*/
			/* Track only MAX_W_IOS_IN_HISTORY previous IOs in histoty, after that reset */

			if ((vmdkp->stats_->w_total_latency + duration < vmdkp->stats_->w_total_latency)
					|| vmdkp->stats_->w_io_count >= MAX_W_IOS_IN_HISTORY) {
				vmdkp->stats_->w_total_latency = 0;
				vmdkp->stats_->w_io_count = 0;
				vmdkp->stats_->w_io_blks_count = 0;
			} else {
				vmdkp->stats_->w_total_latency += duration;
				vmdkp->stats_->w_io_blks_count += reqp->NumberOfRequestBlocks();
				vmdkp->stats_->w_io_count += 1;
			}

			/* We may not hit the modulo condition, keep the value somewhat agressive */
			if (((vmdkp->stats_->w_io_count % 100) == 0) && vmdkp->stats_->w_io_count && vmdkp->stats_->w_io_blks_count) {
				VLOG(5) << __func__ <<
					"[Write:VmdkID:" << vmdkp->GetID() <<
					", Total latency(microsecs) :" << vmdkp->stats_->w_total_latency <<
					", Total blks IO count (in blk size):" << vmdkp->stats_->w_io_blks_count <<
					", Total IO count:" << vmdkp->stats_->w_io_count <<
					", avg blk access latency:" << vmdkp->stats_->w_total_latency / vmdkp->stats_->w_io_blks_count <<
					", avg IO latency:" << vmdkp->stats_->w_total_latency / vmdkp->stats_->w_io_count <<
					", pending IOs:" << vmdkp->stats_->w_pending_count;
			}

			vmdkp->stats_->w_pending_count--;

			cb->result(std::move(write));
		});
	}

	void async_tm_WriteSame(
			std::unique_ptr<HandlerCallback<std::unique_ptr<WriteResult>>> cb,
			VmdkHandle, RequestID reqid, std::unique_ptr<IOBufPtr>,
			int32_t, int32_t, int64_t) override {
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
					process = std::move(process)] (int) mutable {
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
			.then([&NewResult, req = std::move(req)] (int) mutable {
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

			if (write_size >= kBulkWriteMaxSize ||
					(prev_end >= cur_start and not write_requests->empty())) {
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

	folly::Future<std::unique_ptr<TruncateResult>> future_Truncate(
			VmdkHandle vmdk, RequestID reqid,
			std::unique_ptr<std::vector<TruncateReq>> requests) override {
		auto p = SingletonHolder<VmdkManager>::GetInstance()->GetInstance(vmdk);
		if (pio_unlikely(not p)) {
			LOG(ERROR) << "Invalid VMDK handle " << vmdk;
			return folly::makeFuture<std::unique_ptr<TruncateResult>>(
				std::invalid_argument("Invalid VMDK handle")
			);
		}
		auto vmdkp = dynamic_cast<ActiveVmdk*>(p);
		assert(pio_likely(vmdkp));

		auto vmp = vmdkp->GetVM();
		assert(vmp != nullptr);

		return vmp->TruncateBlocks(vmdkp, reqid, *requests)
		.then([reqid, requests = std::move(requests)] (int rc) mutable {
			auto result = std::make_unique<TruncateResult>();
			result->set_reqid(reqid);
			result->set_result(rc);
			return result;
		});
}
private:
	std::atomic<VmHandle> vm_handle_{0};
	std::atomic<VmdkHandle> vmdk_handle_{0};
};

std::shared_ptr<ThriftServer> thrift_server;

static struct {
	std::unique_ptr<std::thread> ha_thread_;
	bool stop_{false};
	struct _ha_instance *ha_instance_;
	struct {
		std::condition_variable ha_hb_stop_cv_;
		std::mutex mutex_; // protects above cond var
	} ha_guard;

	struct {
		/* To Protect against concurrent REST accesses */
		std::mutex lock_;
		/* To Protect pending cnt */
		std::mutex p_lock_;
		/* Pending REST calls */
		std::atomic<int> p_cnt_{0};
	} rest_guard;
} g_thread_;

static void Usr1SignalHandler(int) {
	thrift_server->stopListening();
	thrift_server->stop();
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
	STORD_ERR_COMMIT_CKPT,
	STORD_ERR_CKPT_BMAP_SET,
	STORD_ERR_INVALID_PREPARE_CKPTID,
	STORD_ERR_FAILED_TO_START_PRELOAD,
	STORD_ERR_CHECKPOINTING_FAILED,
	STORD_ERR_NO_SUCH_CHECKPOINT,
	STORD_ERR_CKPT_MOVE_FAILED,
	STORD_ERR_ARM_MIGRATION_NOT_ENABLED,
	STORD_ERR_ARM_INVALID_INFO,
	STORD_ERR_ARM_SYNC_START_FAILED,
	STORD_ERR_ARM_SYNC_EXIST,
	STORD_ERR_ARM_VCENTER_CONN,
	STORD_ERR_CREATE_CKPT,
	STORD_ERR_ALLOC_FAILED,
	STORD_ERR_NOT_SUPPORTED
};

void HaHeartbeat(void *userp) {
	struct _ha_instance *ha = (struct _ha_instance *) userp;

	while(1) {
		std::unique_lock<std::mutex> lck(g_thread_.ha_guard.mutex_);
		if (g_thread_.ha_guard.ha_hb_stop_cv_.wait_for(lck, std::chrono::seconds(15),
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

int StordHaStartCallback(const _ha_request *, _ha_response *,
		void *userp) {
	try {
		log_assert(userp != nullptr);

		std::lock_guard<std::mutex> lk(g_thread_.ha_guard.mutex_);
		g_thread_.ha_thread_ =
			std::make_unique<std::thread>(HaHeartbeat, userp);
		g_thread_.stop_ = false;

		return HA_CALLBACK_CONTINUE;
	} catch (const std::exception& e) {
		return HA_CALLBACK_ERROR;
	}
}

int StordHaStopCallback(const _ha_request *, _ha_response *,
		void *) {
	{
		std::lock_guard<std::mutex> lk(g_thread_.ha_guard.mutex_);
		g_thread_.stop_ = true;
		g_thread_.ha_guard.ha_hb_stop_cv_.notify_one();
	}
	g_thread_.ha_thread_->join();

	try {
		Usr1SignalHandler(SIGINT);

	} catch (const std::exception& e) {
		return HA_CALLBACK_ERROR;
	}

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
	std::unique_lock<std::mutex> p_lock(g_thread_.rest_guard.p_lock_);
	if (g_thread_.rest_guard.p_cnt_ >= MAX_PENDING_LIMIT) {
		p_lock.unlock();
		return 1;
	}

	g_thread_.rest_guard.p_cnt_++;
	p_lock.unlock();
	return 0;
}


static int NewVm(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
	LOG(INFO) << __func__ << "Start NewVM for vmid" << vmid;
	auto vm_handle = pio::NewVm(vmid, req_data);

	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		es << "Adding new VM failed, VM config: " << req_data;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vm_handle);
	log_assert(vmp);

	auto basep = thrift_server->getServeEventBase();
	log_assert(basep);
	vmp->StartTimer(g_thread_.ha_instance_, basep);
	vmp->SetHaInstancePtr(g_thread_.ha_instance_);
	LOG(INFO) << "Added successfully VmID " << vmid << ", VmHandle is " << vm_handle;
	const auto res = std::to_string(vm_handle);

	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

static int RemoveVm(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
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
	void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
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
	void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
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

static int NewVmdk(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
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

static int RemoveVmdk(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
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

static int VmdkStartPreload(const _ha_request *reqp, _ha_response *resp, void *) {
	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT, "Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	auto valuep = ha_parameter_get(reqp, "vm-id");
	if (not valuep) {
		SetErrMsg(resp, STORD_ERR_INVALID_VMDK, "vm-id not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(valuep);

	valuep = ha_parameter_get(reqp, "vmdk-id");
	if (not valuep) {
		SetErrMsg(resp, STORD_ERR_INVALID_VMDK, "vmdk-id not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmdkid(valuep);

	auto rc = pio::StartPreload(vmid, vmdkid);
	if (rc < 0) {
		SetErrMsg(resp, STORD_ERR_FAILED_TO_START_PRELOAD, "preload failed");
		return HA_CALLBACK_CONTINUE;
	}

	const auto res = std::to_string(rc);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}

static int NewMergeReq(const _ha_request *reqp, _ha_response *resp, void *) {

	LOG(INFO) << __func__ << " NewMergeReq start..";
	auto param_valuep = ha_parameter_get(reqp, "vm-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	param_valuep = ha_parameter_get(reqp, "ckpt-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"ckpt-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string ckptid(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << __func__ << " vmid::" << vmid << " ckptid:" <<  stol(ckptid);
	auto ret = pio::NewMergeReq(vmid, stol(ckptid));
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Starting Merge request for VMID::"  << vmid << "Failed";
		es << "Starting Merge request for VMID::"  << vmid << " Failed";
		SetErrMsg(resp, STORD_ERR_INVALID_SCAN, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	LOG(INFO) << "Merge for VM:" << vmid << " started successfully.";
	const auto res = std::to_string(ret);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}

static int NewMergeStatusReq(const _ha_request *reqp, _ha_response *resp, void *) {

	auto param_valuep = ha_parameter_get(reqp, "vm-id");
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	CkptMergeStats merge_stat;
	auto ret = pio::NewMergeStatusReq(id, merge_stat);
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Merge status request for aero-cluster-id::"  << id << " Failed, errno:" << ret;
		if (ret == -EINVAL) {
			es << "Merge is not running currently for aero-cluster-id::"  << id;
			SetErrMsg(resp, STORD_ERR_SCAN_NOT_STARTED, es.str());
		} else {
			es << "Failed to get merge status for aero-cluster-id::"  << id;
			SetErrMsg(resp, STORD_ERR_INVALID_SCAN, es.str());
		}
		return HA_CALLBACK_CONTINUE;
	}

	json_t *merge_params = json_object();
	if (pio_likely(merge_stat.running)) {
		json_object_set_new(merge_params, "merge_running", json_boolean(true));
	} else {
		json_object_set_new(merge_params, "merge_running", json_boolean(false));
	}

	auto *merge_params_str = json_dumps(merge_params, JSON_ENCODE_ANY);
	json_object_clear(merge_params);
	json_decref(merge_params);
	ha_set_response_body(resp, HTTP_STATUS_OK, merge_params_str,
			strlen(merge_params_str));
	::free(merge_params_str);

	return HA_CALLBACK_CONTINUE;
}

static int NewScanReq(const _ha_request *reqp, _ha_response *resp, void *) {

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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto ret = pio::NewScanReq(vmid, MetaData_constants::kInvalidCheckPointID());
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Starting Scan request for VMID::"  << vmid << "Failed";
		es << "Starting Scan request for VMID::"  << vmid << " Failed";
		SetErrMsg(resp, STORD_ERR_INVALID_SCAN, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	const auto res = std::to_string(ret);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}

static int NewScanStatusReq(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	ScanStats scan_stat;
	auto ret = pio::NewScanStatusReq(id, scan_stat);
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Scan status request for aero-cluster-id::"  << id << " Failed, errno:" << ret;
		if (ret == -EINVAL) {
			es << "Scan is not running currently for aero-cluster-id::"  << id;
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
		json_object_set_new(scan_params, "scan_running", json_boolean(true));
	} else {
		json_object_set_new(scan_params, "scan_running", json_boolean(false));
	}
	json_object_set_new(scan_params, "progress_pct", json_integer(scan_stat.progress_pct));
	json_object_set_new(scan_params, "records_scanned", json_integer(scan_stat.records_scanned));
	auto *scan_params_str = json_dumps(scan_params, JSON_ENCODE_ANY);
	json_object_clear(scan_params);
	json_decref(scan_params);
	ha_set_response_body(resp, HTTP_STATUS_OK, scan_params_str,
			strlen(scan_params_str));
	::free(scan_params_str);

	return HA_CALLBACK_CONTINUE;
}

static int NewDeltaContextSet(const _ha_request *reqp, _ha_response *resp, void*) {

	auto param_valuep = ha_parameter_get(reqp, "vm-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);
	LOG(ERROR) << __func__ << " Vmid::" << vmid.c_str();

	param_valuep = ha_parameter_get(reqp, "snap-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"snap-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string snapid(param_valuep);
	LOG(ERROR) << __func__ << " snap-id::" << snapid.c_str();

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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto ret = pio::NewVmDeltaContextSet(vm_handle, snapid);
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "DeltaContextSet failed::"  << vmid << "Failed";
		es << "Delta context set failed for VMID::"  << vmid << " Failed";
		SetErrMsg(resp, STORD_ERR_INVALID_FLUSH, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	const auto res = std::to_string(ret);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}

static int NewFlushReq(const _ha_request *reqp, _ha_response *resp, void *) {

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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
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

static int
NewPrepareCkpt(const _ha_request *reqp, _ha_response *resp, void *) {

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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		es << "PrepareCkpt call failed, Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto ret = pio::PrepareCkpt(vm_handle);
	if (ret) {
		std::ostringstream es;
		es << "PrepareCkpt failed for "
			<< " VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_PREPARE_CKPTID, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	const auto res = std::to_string(ret);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}

static int
NewSetCkptBitMapReq(const _ha_request *reqp, _ha_response *resp, void *) {

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

	auto data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
			"SetCkptBitmap invalid config");
		return HA_CALLBACK_CONTINUE;
	}
	std::string req_data(data);
	::free(data);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		LOG(ERROR) << "Set Bitmap of VMDK failed. Invalid VmID = " << vmid;
		es << "Set Bitmap of VMDK failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto ret = pio::SetCkptBitmap(vm_handle, vmdkid, req_data);
	if (ret) {
		std::ostringstream es;
		es << "Settting CKPT bitmap for VMDK failed."
			<< " VmID = " << vmid
			<< " VmdkID = " << vmdkid;
		SetErrMsg(resp, STORD_ERR_CKPT_BMAP_SET, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	const auto res = std::to_string(ret);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}

static int
NewCommitCkpt(const _ha_request *reqp, _ha_response *resp, void *) {

	auto param_valuep = ha_parameter_get(reqp, "vm-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"vm-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	param_valuep = ha_parameter_get(reqp, "ckpt-id");
	if (param_valuep == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM,
			"ckpt-id param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string ckptid(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
	auto ret = pio::CommitCkpt(vmid, ckptid);
	if (ret) {
		std::ostringstream es;
		es << "Commit Ckpt failed, " << " VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_COMMIT_CKPT, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		es << "TakeCkpt call failed, Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	std::vector<::ondisk::CheckPointID> vec_ckpts;
	vec_ckpts.emplace_back(stol(ckptid));
	ret = pio::MoveUnflushedToFlushed(vm_handle, vec_ckpts);
	if (ret) {
		std::ostringstream es;
		es << "Moving checkpoints from unflushed to flushed failed."
			<< " VmID: " << vmid;
		SetErrMsg(resp, STORD_ERR_CKPT_MOVE_FAILED, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	const auto res = std::to_string(ret);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}


static int NewFlushStatusReq(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	FlushStats flush_stat;
	bool flush_not_running = false;
	auto ret = pio::NewFlushStatusReq(vmid, flush_stat);
	if (ret) {
		std::ostringstream es;
		LOG(ERROR) << "Flush status request for VMID::"  << vmid << "Failed errno:" << ret;
		if (ret == -EINVAL) {
			es << "Flush is not running currently for VMID::"  << vmid;
			SetErrMsg(resp, STORD_ERR_FLUSH_NOT_STARTED, es.str());
			// TODO: Return success for REST and check error in status.
			flush_not_running = true;
		} else {
			es << "Failed to get flush status for VMID::"  << vmid;
			LOG(ERROR) << "Failed to get flush status for VMID::"  << vmid;
			SetErrMsg(resp, STORD_ERR_INVALID_FLUSH, es.str());
			return HA_CALLBACK_CONTINUE;
		}
	}

	FlushStats::iterator itr;

	uint64_t total_flushed_blks = 0;
	uint64_t total_moved_blks = 0;
	uint64_t flush_duration = 0;
	uint64_t move_duration = 0;
	uint64_t flush_bytes = 0;
	uint64_t move_bytes = 0;
	int stage = -1;

	uint64_t total_blks_in_op = 0;
	uint64_t remaining_blks = 0;

	for (itr = flush_stat.begin(); itr != flush_stat.end(); ++itr) {
		if (pio_unlikely(flush_not_running)) {
			break;
		}
		if (itr->first == "time_data") {
			VLOG(10) << boost::format("%1% %2% %3% %4%")
				% "Start time:-" % (itr->second).first
				% "Elapsed time:-" % (itr->second).second;
		} else if (itr->first == "flush_data") {
			flush_duration = (itr->second).first;
			flush_bytes = (itr->second).second;
		} else if (itr->first == "move_data") {
			move_duration  = (itr->second).first;
			move_bytes  = (itr->second).second;
		} else if (itr->first == vmid) {
			VLOG(10) << boost::format("[LUN:%1%] %2% %3% %|20t|%4% %5%")
				% itr->first % "Flushed Blks:-" % (itr->second).first
				% "Moved Blks:-" % (itr->second).second;
			total_flushed_blks += (itr->second).first;
			total_moved_blks   += (itr->second).second;
		} else if (itr->first == "op") {
			stage = (itr->second).first;
			total_blks_in_op = (itr->second).second;
		}
	}

	LOG(INFO) << "Flush status for VM:" << vmid << "completed successfully";

	json_t *flush_params = json_object();

	if (flush_not_running) {
		json_object_set_new(flush_params, "flush_running", json_boolean(false));
	} else {
		json_object_set_new(flush_params, "flush_running", json_boolean(true));
	}

	if (stage == (int)FlushAuxData::FlushStageType::kFlushStage) {
		if (flush_duration && (flush_duration / 1000)) {
			LOG(INFO) << "flush_duration:" << flush_duration << " flush_bytes:" << flush_bytes
				<< " speed: " << ((flush_bytes / (flush_duration / 1000)) / 1024);
			auto flush_dur   = (flush_duration / 1000);
			auto flush_speed = ((flush_bytes / flush_dur) / 1024);
			json_object_set_new(flush_params, "curr_speed(KBps)", json_integer(flush_speed));
		}
		json_object_set_new(flush_params, "Operation", json_string("Flush"));
		json_object_set_new(flush_params, "blks_cnt", json_integer(total_flushed_blks));
		json_object_set_new(flush_params, "duration(ms)", json_integer(flush_duration));
		remaining_blks = total_blks_in_op - total_flushed_blks;
	} else if (stage == (int)FlushAuxData::FlushStageType::kMoveStage) {
		if (move_duration && (move_duration / 1000)) {
			LOG(INFO) << "move_duration:" << move_duration << " move_bytes:" << move_bytes
				<< " speed: " << ((move_bytes / (move_duration / 1000)) / 1024);
			auto move_dur   = (move_duration / 1000);
			auto move_speed = ((move_bytes / move_dur) / 1024);
			json_object_set_new(flush_params, "curr_speed(KBps)", json_integer(move_speed));
		}
		json_object_set_new(flush_params, "Operation", json_string("Move"));
		json_object_set_new(flush_params, "blks_cnt", json_integer(total_moved_blks));
		json_object_set_new(flush_params, "duration(ms)", json_integer(move_duration));
		remaining_blks = total_blks_in_op - total_moved_blks;
	}

	json_object_set_new(flush_params, "remaining_blks_cnt", json_integer(remaining_blks));

	param_valuep = ha_parameter_get(reqp, "get_history");
	if (param_valuep) {
		json_t *history_param = json_object();
		auto t = pio::FlushHistoryReq(vmid, history_param);
		if (pio_unlikely(t)) {
			LOG(INFO) << "History unavailable for vmid " << vmid;
		}

		auto *hist_str = json_dumps(history_param, JSON_ENCODE_ANY);
		json_object_clear(history_param);
		json_decref(history_param);

		json_object_set_new(flush_params, "history", json_string(hist_str));
		::free(hist_str);
	} else {
		LOG(INFO) << "History not queried";
	}

	auto *flush_params_str = json_dumps(flush_params, JSON_ENCODE_ANY);

	json_object_clear(flush_params);
	json_decref(flush_params);

	ha_set_response_body(resp, HTTP_STATUS_OK, flush_params_str,
			strlen(flush_params_str));
	::free(flush_params_str);

	return HA_CALLBACK_CONTINUE;
}

static int NewAeroCacheStatReq(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

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

	VLOG(10) << "Successful...";

	json_t *aero_params = json_object();
	const auto res = std::to_string(ret);
	json_object_set_new(aero_params, "ret", json_string((char *) res.c_str()));
	json_object_set_new(aero_params, "dirty_blks_cnt", json_integer(aero_stats_p->dirty_cnt_));
	json_object_set_new(aero_params, "clean_blks_cnt", json_integer(aero_stats_p->clean_cnt_));
	json_object_set_new(aero_params, "parent_blks_cnt", json_integer(aero_stats_p->parent_cnt_));
	auto *aero_params_str = json_dumps(aero_params, JSON_ENCODE_ANY);

	json_object_clear(aero_params);
	json_decref(aero_params);

	ha_set_response_body(resp, HTTP_STATUS_OK, aero_params_str,
			strlen(aero_params_str));
	::free(aero_params_str);

	return HA_CALLBACK_CONTINUE;
}

static int AeroSetCleanup(const _ha_request *reqp, _ha_response *resp,
	void *) {

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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;
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

json_t* GetElement(std::string key, uint64_t value, std::string desc) {
	json_t *stat_array_elem = json_array();
	json_array_append_new(stat_array_elem, json_string(key.c_str()));
	json_array_append_new(stat_array_elem, json_integer(value));
	json_array_append_new(stat_array_elem, json_string(desc.c_str()));
	return stat_array_elem;
}


static int GlobalStats([[maybe_unused]] const _ha_request *reqp, _ha_response *resp, void *) {

	LOG(INFO) << "inside Global stats";
	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
				"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	ComponentStats g_stats;
	auto rc = pio::GlobalStats(&g_stats);
	if (rc) {
		LOG(ERROR) << "Failed to get global stats";
		return HA_CALLBACK_CONTINUE;
	}

	json_t *stat_params = json_array();

	json_array_append_new(stat_params, GetElement("read_miss", 
		g_stats.vmdk_cache_stats_.read_miss_, "number of read miss across of all vmdks"));
	json_array_append_new(stat_params, GetElement("read_populates",
		g_stats.vmdk_cache_stats_.read_populates_, "number of read populated into cache"));
	json_array_append_new(stat_params, GetElement("total_blk_reads",
		g_stats.vmdk_cache_stats_.total_blk_reads_, "total blocks read"));

	json_array_append_new(stat_params, GetElement("read_hits", 
		g_stats.vmdk_cache_stats_.read_hits_, "number of read hits across of all vmdks"));
	json_array_append_new(stat_params, GetElement("nw_bytes_read",
		g_stats.vmdk_cache_stats_.nw_bytes_read_, "network read bytes"));
	json_array_append_new(stat_params, GetElement("nw_bytes_write",
		g_stats.vmdk_cache_stats_.nw_bytes_write_, "network write bytes"));
	
	json_array_append_new(stat_params, GetElement("total_reads",
		g_stats.vmdk_cache_stats_.total_reads_, "total number of reads across vmdks"));
	json_array_append_new(stat_params, GetElement("total_writes",
		g_stats.vmdk_cache_stats_.total_writes_, "total number of writes across vmdks"));
	json_array_append_new(stat_params, GetElement("total_reads_in_bytes",
		g_stats.vmdk_cache_stats_.total_bytes_reads_, "total bytes read across vmdks"));
	json_array_append_new(stat_params, GetElement("total_writes_in_bytes",
		g_stats.vmdk_cache_stats_.total_bytes_writes_, "total bytes written across vmdks"));
	

	json_array_append_new(stat_params, GetElement("read_failed", 
		g_stats.vmdk_cache_stats_.read_failed_, "number of read failed across all vmdks"));
	json_array_append_new(stat_params, GetElement("write_failed", 
		g_stats.vmdk_cache_stats_.write_failed_, "number of write failed across all vmdks"));
	json_array_append_new(stat_params, GetElement("reads_in_progress", 
		g_stats.vmdk_cache_stats_.reads_in_progress_, "reads in progress across of all vmdks"));
	json_array_append_new(stat_params, GetElement("writes_in_progress_", 
		g_stats.vmdk_cache_stats_.writes_in_progress_, "writes in progress across of all vmdks"));


	auto *stat_params_str = json_dumps(stat_params, JSON_ENCODE_ANY);
	size_t index;
	json_t *element;
	json_array_foreach(stat_params, index, element) {
		json_array_clear(element);
	}
	json_decref(stat_params);

	LOG(INFO) << "done collecting global stats, about to return";
	ha_set_response_body(resp, HTTP_STATUS_OK, stat_params_str,
			strlen(stat_params_str));
	::free(stat_params_str);

	return HA_CALLBACK_CONTINUE;
}


static int NewVmdkStatsReq(const _ha_request *reqp, _ha_response *resp, void *) {

	auto param_valuep = ha_parameter_get(reqp, "vmdk-id");
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	auto vmdk_stats   = std::make_shared<VmdkCacheStats>();
	auto vmdk_stats_p = vmdk_stats.get();

	auto rc = pio::NewVmdkStatsReq(vmdkid, vmdk_stats_p);
	if (rc == -EINVAL) {
		std::ostringstream es;
		LOG(ERROR) << "Vmdk Stats request for VmdkID " << vmdkid
		<< "failed. VmdkID invalid";
		es << "Invalid VmdkID: " << vmdkid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	json_t *stat_params = json_object();
	json_object_set_new(stat_params, "app_reads", json_integer(vmdk_stats_p->total_reads_));
	json_object_set_new(stat_params, "app_writes", json_integer(vmdk_stats_p->total_writes_));
	json_object_set_new(stat_params, "total_reads", json_integer(vmdk_stats_p->total_blk_reads_));
	json_object_set_new(stat_params, "total_writes", json_integer(vmdk_stats_p->cache_writes_));
	json_object_set_new(stat_params, "total_bytes_reads", json_integer(vmdk_stats_p->total_bytes_reads_));
	json_object_set_new(stat_params, "total_bytes_writes", json_integer(vmdk_stats_p->total_bytes_writes_));
	json_object_set_new(stat_params, "parent_blks", json_integer(vmdk_stats_p->parent_blks_));
	json_object_set_new(stat_params, "read_populates", json_integer(vmdk_stats_p->read_populates_));
	json_object_set_new(stat_params, "cache_writes", json_integer(vmdk_stats_p->cache_writes_));
	json_object_set_new(stat_params, "read_hits", json_integer(vmdk_stats_p->read_hits_));
	json_object_set_new(stat_params, "read_miss", json_integer(vmdk_stats_p->read_miss_));
	json_object_set_new(stat_params, "read_failed", json_integer(vmdk_stats_p->read_failed_));
	json_object_set_new(stat_params, "write_failed", json_integer(vmdk_stats_p->write_failed_));

	json_object_set_new(stat_params, "reads_in_progress", json_integer(vmdk_stats_p->reads_in_progress_));
	json_object_set_new(stat_params, "writes_in_progress", json_integer(vmdk_stats_p->writes_in_progress_));

	json_object_set_new(stat_params, "read_ahead_blks", json_integer(vmdk_stats_p->read_ahead_blks_));
	json_object_set_new(stat_params, "read_ahead_random_patterns", json_integer(vmdk_stats_p->rh_random_patterns_));
  	json_object_set_new(stat_params, "read_ahead_strided_patterns", json_integer(vmdk_stats_p->rh_strided_patterns_));
  	json_object_set_new(stat_params, "read_ahead_correlated_patterns", json_integer(vmdk_stats_p->rh_correlated_patterns_));
  	json_object_set_new(stat_params, "read_ahead_dropped_reads", json_integer(vmdk_stats_p->rh_dropped_reads_));
	
	json_object_set_new(stat_params, "flushes_in_progress", json_integer(vmdk_stats_p->flushes_in_progress_));
	json_object_set_new(stat_params, "moves_in_progress", json_integer(vmdk_stats_p->moves_in_progress_));
	json_object_set_new(stat_params, "block_size", json_integer(vmdk_stats_p->block_size_));
	json_object_set_new(stat_params, "flushed_chkpnts", json_integer(vmdk_stats_p->flushed_chkpnts_));
	json_object_set_new(stat_params, "unflushed_chkpnts", json_integer(vmdk_stats_p->unflushed_chkpnts_));
	json_object_set_new(stat_params, "flushed_blocks", json_integer(vmdk_stats_p->flushed_blocks_));
	json_object_set_new(stat_params, "moved_blocks", json_integer(vmdk_stats_p->moved_blocks_));
	json_object_set_new(stat_params, "pending_blocks", json_integer(vmdk_stats_p->pending_blocks_));
	json_object_set_new(stat_params, "dirty_blocks", json_integer(vmdk_stats_p->dirty_blocks_));
	json_object_set_new(stat_params, "clean_blocks", json_integer(vmdk_stats_p->clean_blocks_));

	json_object_set_new(stat_params, "nw_bytes_write", json_integer(vmdk_stats_p->nw_bytes_write_));
	json_object_set_new(stat_params, "nw_bytes_read", json_integer(vmdk_stats_p->nw_bytes_read_));
	json_object_set_new(stat_params, "aero_bytes_write", json_integer(vmdk_stats_p->aero_bytes_write_));
	json_object_set_new(stat_params, "aero_bytes_read", json_integer(vmdk_stats_p->aero_bytes_read_));

	json_object_set_new(stat_params, "bufsz_before_compress", json_integer(vmdk_stats_p->bufsz_before_compress));
	json_object_set_new(stat_params, "bufsz_after_compress", json_integer(vmdk_stats_p->bufsz_after_compress));
	json_object_set_new(stat_params, "bufsz_before_uncompress", json_integer(vmdk_stats_p->bufsz_before_uncompress));
	json_object_set_new(stat_params, "bufsz_after_uncompress", json_integer(vmdk_stats_p->bufsz_after_uncompress));

	auto *stat_params_str = json_dumps(stat_params, JSON_ENCODE_ANY);

	json_object_clear(stat_params);
	json_decref(stat_params);

	ha_set_response_body(resp, HTTP_STATUS_OK, stat_params_str,
			strlen(stat_params_str));
	::free(stat_params_str);

	return HA_CALLBACK_CONTINUE;
}

static int NewFlushHistoryReq(const _ha_request *reqp, _ha_response *resp, void *) {

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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	json_t *history_param = json_object();
	auto t = pio::FlushHistoryReq(vmid, history_param);
	if (pio_unlikely(t)) {
		std::ostringstream es;
		es << "History unavailable for vmid " << vmid
			<< ". First flush not triggered or still in progress";

		LOG(ERROR) << es.str();
		json_object_clear(history_param);
		json_decref(history_param);
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto *hist_str = json_dumps(history_param, JSON_ENCODE_ANY);
	json_object_clear(history_param);
	json_decref(history_param);

	json_t *flush_params = json_object();
	json_object_set_new(flush_params, "history", json_string(hist_str));
	::free(hist_str);

	auto *flush_params_str = json_dumps(flush_params, JSON_ENCODE_ANY);

	json_object_clear(flush_params);
	json_decref(flush_params);

	ha_set_response_body(resp, HTTP_STATUS_OK, flush_params_str,
			strlen(flush_params_str));
	::free(flush_params_str);

	return HA_CALLBACK_CONTINUE;
}

static int ReadAheadStatsReq(const _ha_request *reqp, _ha_response *resp, void *) {

	auto param_valuep = ha_parameter_get(reqp, "vmdk-id");
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	pio::ReadAhead::ReadAheadStats st_rh_stats = {0};
	auto rc = pio::ReadAheadStatsReq(vmdkid, st_rh_stats);
	if (rc == -EINVAL) {
		std::ostringstream es;
		LOG(ERROR) << "Vmdk Stats request for VmdkID " << vmdkid
		<< "failed. VmdkID invalid";
		es << "Invalid VmdkID: " << vmdkid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	json_t *stat_params = json_object();
	json_object_set_new(stat_params, "read_ahead_blks", json_integer(st_rh_stats.stats_rh_blocks_size_));
	json_object_set_new(stat_params, "read_ahead_misses", json_integer(st_rh_stats.stats_rh_read_misses_));

	auto *stat_params_str = json_dumps(stat_params, JSON_ENCODE_ANY);

	json_object_clear(stat_params);
	json_decref(stat_params);

	ha_set_response_body(resp, HTTP_STATUS_OK, stat_params_str,
			strlen(stat_params_str));
	::free(stat_params_str);

	return HA_CALLBACK_CONTINUE;
}

/************************************************************************
 		REST APIs to serve RTO/RPO HA workflow -- START
 ************************************************************************
*/

/************************************************************************
 * Returns all unflushed checkpoints for the given VmId, if no checkpoint
 * exists then create a new one and return that
 * Input Param: VmID
 * Output Param: Json array of unflushed checkpoints for the given VmID
 * Returns: Integer denoting success/failure
 * HTTP Response: 200(OK) if successful else a http error code
 ************************************************************************
*/
static int GetUnflushedCheckpoints(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	auto vmp = SingletonHolder<pio::VmManager>::GetInstance()->GetInstance(vmid);
	if (pio_likely(not vmp)) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	std::vector<::ondisk::CheckPointID> unflushed_ckpts;
	auto rc = vmp->GetUnflushedCheckpoints(unflushed_ckpts);
	if(pio_unlikely(rc)) {
		std::ostringstream es;
		LOG(ERROR) << "Failed to get unflushed checkpoints for VM: " << vmid;
		es << "Failed to get unflushed checkpoints for VM: " << vmid;
		SetErrMsg(resp, STORD_ERR_CHECKPOINTING_FAILED, es.str());
		return HA_CALLBACK_CONTINUE;
	}
	assert(unflushed_ckpts.size());

	json_t *json_params = json_object();
	json_t *array = json_array();
	for(auto const& ckpt_id : unflushed_ckpts) {
		json_array_append_new(array, json_integer(ckpt_id));
	}

	json_object_set_new(json_params, "unflushed_checkpoints", array);
	auto *json_params_str = json_dumps(json_params, JSON_ENCODE_ANY);
	json_object_clear(json_params);
	json_decref(json_params);
	json_decref(array);

	ha_set_response_body(resp, HTTP_STATUS_OK, json_params_str, strlen(json_params_str));
	::free(json_params_str);

	return HA_CALLBACK_CONTINUE;
}

/*****************************************************************************
 * Prepare for flush by going through the recovery protocol if any previous
 * flush has not completed gracefully
 * Input Param: VmID
 * Output Param: None
 * Returns: Integer denoting success/failure
 * HTTP Response: 200(OK) if successful else a http error code
 *****************************************************************************
*/
static int PrepareFlush(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	int rc = 0;
	const auto res = std::to_string(rc);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

/*****************************************************************************
 * Start writing to top most delta on prem, this API is async and hence the
 * status will be returned via another REST call which will be polled by HA
 * Input Param: VmID, Array of checkpoint Ids
 * Output Param: None
 * Returns: Integer denoting success/failure
 * HTTP Response: 202(Accepted) if successful else a http error code
 *****************************************************************************
*/

static int AsyncStartFlush(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	char *data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
				"Checkpoint IDs invalid");
		return HA_CALLBACK_CONTINUE;
	}
	assert(data);
	std::string req_data(data);
	::free(data);

	std::vector<int64_t> vec_ckpts;
	pio::JsonHelper json_helper(req_data);
	auto ret = json_helper.GetVector<int64_t>("checkpoint-ids", vec_ckpts);
	if(not ret) {
		std::ostringstream es;
		LOG(ERROR) << "Failed to extract checkpoint-ids from the supplied json: " << req_data;
		es << "Failed to extract checkpoint-ids from the supplied json: " << req_data;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	// ToDo: Need a method to know if flush is already running or not, if it's running then
	// do not start another flush instance and return proper message to the REST caller
	auto rc = pio::NewFlushReq(vmid, req_data);
	if (rc) {
		std::ostringstream es;
		LOG(ERROR) << "Starting flush request for VMID::"  << vmid << "Failed";
		es << "Starting flush request for VMID::"  << vmid << " Failed";
		SetErrMsg(resp, STORD_ERR_INVALID_FLUSH, es.str());
		return HA_CALLBACK_CONTINUE;
	}
	LOG(INFO) << "Flush for VM:" << vmid << "started successfully. Please run "
		"flush_status to get the progress";

	const auto res = std::to_string(rc);
	ha_set_response_body(resp, HTTP_STATUS_ACCEPTED, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

/*****************************************************************************
 * Serialize "checkpoints to snapshot mapping" and persist in Aerospike
 * Input Param: VmID, SnapshotID, Array of checkpointIds
 * Output Param: None
 * Returns: Integer denoting success/failure
 * HTTP Response: 200(OK) if successful else a http error code
 *****************************************************************************
 */

static int SerializeCheckpoints(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	char *data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
				"Checkpoint IDs invalid");
		return HA_CALLBACK_CONTINUE;
	}
	assert(data);

	std::string req_data(data);
	::free(data);

	int64_t snap_id = 0;
	std::vector<int64_t> vec_ckpts;
	pio::JsonHelper json_helper(req_data);
	auto r1 = json_helper.GetScalar<int64_t>("snapshot-id", snap_id);
	auto r2 = json_helper.GetVector<int64_t>("checkpoint-ids", vec_ckpts);
	if(pio_unlikely(not (r1 and r2))) {
		std::ostringstream es;
		LOG(ERROR) << "Failed to extract snapshot-id or/and checkpoint-ids from the supplied json: " << req_data;
		es << "Failed to extract snapshot-id or/and checkpoint-ids from the supplied json: " << req_data;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto vmp = SingletonHolder<pio::VmManager>::GetInstance()->GetInstance(vmid);
	if (pio_likely(not vmp)) {
		std::ostringstream es;
		LOG(ERROR) << "Retriving information related to given VM failed. Invalid VmID = " << vmid;
		es << "Retriving information related to given VM failed. Invalid VmID = " << vmid;
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	int rc = vmp->SerializeCheckpoints(snap_id, vec_ckpts);
	if (pio_unlikely(rc)) {
		std::ostringstream es;
		LOG(ERROR) << "Failed to locate one or few checkpoints for SnapshotID: " << snap_id << " for VM: " << vmid;
		es << "Failed to locate one or few checkpoints for SnapshotID: " << snap_id << " for VM: " << vmid;
		SetErrMsg(resp, STORD_ERR_NO_SUCH_CHECKPOINT, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	const auto res = std::to_string(rc);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

/*****************************************************************************
 * Start internal move stage for the given VmId
 * Input Param: VmID
 * Output Param: None
 * Returns: Integer denoting success/failure
 * HTTP Response: 202(Accepted) if successful else a http error code
 *****************************************************************************
*/
static int AsyncStartMoveStage(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	int rc = 0;
	const auto res = std::to_string(rc);
	ha_set_response_body(resp, HTTP_STATUS_ACCEPTED, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

/*****************************************************************************
 * Delete the given snapshots for the given VmId
 * Input Param: VmID, Array of checkpoint Ids
 * Output Param: None
 * Returns: Integer denoting success/failure
 * HTTP Response: 200(OK) if successful else a http error code
 *****************************************************************************
 */
static int DeleteSnapshots(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	char *data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
				"Snapshot IDs invalid");
		return HA_CALLBACK_CONTINUE;
	}
	assert(data);

	//A json_array of checkpoint ids, need to be converted to c++ array/list of CheckPointIDs
	//e.g; {"snapshot-ids": "[41, 42, 43]"}
	std::string snapshot_ids(data);
	::free(data);

	int rc = 0;
	const auto res = std::to_string(rc);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());

	return HA_CALLBACK_CONTINUE;
}

/*****************************************************************************
 * Start internal move stage which will move data from dirty to clean
 * Input Param: VmID
 * Output Param: move running status, count of moved blocks, remaining blocks
 * Returns: Integer denoting success/failure
 * HTTP Response: 200(OK) if successful else a http error code
 *****************************************************************************
*/
static int GetMoveStatus(const _ha_request *reqp, _ha_response *resp, void *) {
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

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	g_thread_.rest_guard.p_cnt_--;

	json_t *flush_params = json_object();
	json_object_set_new(flush_params, "move_running", json_boolean(false));
	json_object_set_new(flush_params, "moved_blks_cnt", json_integer(0));
	json_object_set_new(flush_params, "remaining_blks_cnt", json_integer(0));

	std::string flush_params_str = json_dumps(flush_params, JSON_ENCODE_ANY);
	json_object_clear(flush_params);
	json_decref(flush_params);

	ha_set_response_body(resp, HTTP_STATUS_OK, flush_params_str.c_str(),
			strlen(flush_params_str.c_str()));

	return HA_CALLBACK_CONTINUE;
}

static int
CreateCkpt(const _ha_request *reqp, _ha_response *resp, void *)
{
	auto param_valuep = ha_parameter_get(reqp, "vm-id");

	if (param_valuep  == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM, "vmid param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	// TODO: Check correctness of p_cnt_-- use here.
	g_thread_.rest_guard.p_cnt_--;

	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		std::ostringstream es;

		es << "Retriving information related to VM failed. Invalid vm-id = " << vmid;
		LOG(ERROR) << es.str();
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vm_handle);
	log_assert(vmp);

	auto f = vmp->TakeCheckPoint();
	f.wait();
	auto [id, rc] = f.value();
	if (pio_unlikely(rc)) {
		std::ostringstream es;

		es << "Take checkpoint failed, rc = " << rc << " vm-id = " << vmid;
		LOG(ERROR) << es.str();
		SetErrMsg(resp, STORD_ERR_CREATE_CKPT, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	json_t *ckpt_info = json_object();
	json_object_set_new(ckpt_info, "ckpt_id", json_integer(id));
	auto *ckpt_info_str = json_dumps(ckpt_info, JSON_ENCODE_ANY);
	json_object_clear(ckpt_info);
	json_decref(ckpt_info);

	ha_set_response_body(resp, HTTP_STATUS_OK, ckpt_info_str,
			strlen(ckpt_info_str));
	::free(ckpt_info_str);
	return HA_CALLBACK_CONTINUE;
}

static int AddVcenterDetails(const _ha_request *reqp, _ha_response *resp, void *)
{
	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	// TODO: Check correctness of p_cnt_-- use here.
	g_thread_.rest_guard.p_cnt_--;

	auto data = ha_get_data(reqp);
	if (data == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
			"Vcenter details not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string req_data(data);
	LOG(INFO) << "Vcenter details " << req_data;
	::free(data);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	namespace pt = boost::property_tree;
	pt::ptree vc_config;
	std::stringstream is(req_data);
	pt::read_json(is, vc_config);

	std::string vc_ip, vc_user, vc_passwd, vc_fprint1, vc_fprint256;

	try {
		vc_ip = vc_config.get<std::string>(vc_ns::kVcIp);
		vc_user = vc_config.get<std::string>(vc_ns::kVcUser);
		vc_passwd = vc_config.get<std::string>(vc_ns::kVcPasswd);
		vc_fprint1 = vc_config.get<std::string>(vc_ns::kVcFprint1);
		vc_fprint256 = vc_config.get<std::string>(vc_ns::kVcFprint256);
	} catch (boost::exception const&  ex) {
		SetErrMsg(resp, STORD_ERR_INVALID_NO_DATA,
				"Vcenter details are wrong");
		return HA_CALLBACK_CONTINUE;
	}

	pio::vc_ns::vc_info vcinfo = {
		{vc_ns::kVcUser, std::move(vc_user)},
		{vc_ns::kVcPasswd, std::move(vc_passwd)},
		{vc_ns::kVcFprint1, std::move(vc_fprint1)},
		{vc_ns::kVcFprint256, std::move(vc_fprint256)}
	};
	vc_ns::SetVcConfig(std::move(vc_ip), std::move(vcinfo));

	const auto res = std::to_string(0);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
}

static int ArmSyncStart(const _ha_request *reqp, _ha_response *resp, void *) {
#ifndef USE_NEP
	(void) reqp;
	SetErrMsg(resp, STORD_ERR_NOT_SUPPORTED, "ArmSync not supported.");
	return HA_CALLBACK_CONTINUE;
#else
	auto param_valuep = ha_parameter_get(reqp, "vm-id");
	if (param_valuep  == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM, "vmid param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	// TODO: Check correctness of p_cnt_-- use here.
	g_thread_.rest_guard.p_cnt_--;

	std::ostringstream es;

	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		es << "Retriving information related to VM failed. Invalid vm-id = " << vmid << std::endl;
		LOG(ERROR) << es.str();
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vm_handle);
	log_assert(vmp);

	auto vmsyncp = vmp->GetVmSync(VmSync::Type::kSyncArm);
	if (vmsyncp != nullptr) {
		es << "ARM sync is already in progress for vm-id = " << vmid << std::endl;
		LOG(ERROR) << es.str();
		SetErrMsg(resp, STORD_ERR_ARM_SYNC_EXIST, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto err_msg = [&vmp, &es, &resp] (bool unset, StordSvcErr err) {
		if (unset) {
			vmp->UnsetArmJsonConfig();
		}
		LOG(ERROR) << es.str();
		SetErrMsg(resp, err, es.str());
	};

	auto vm_config = vmp->GetJsonConfig();
	if (not vm_config->GetArmMigration()) {
		es << "ARM migration not enabled for vm-id = " << vmid << std::endl;
		err_msg(false, STORD_ERR_ARM_MIGRATION_NOT_ENABLED);
		return HA_CALLBACK_CONTINUE;
	}

	auto data = ha_get_data(reqp);
	std::string req_data(data);
	::free(data);
	LOG(INFO) << req_data;

	auto rc = vmp->SetArmJsonConfig(req_data);
	if (pio_unlikely(rc)) {
		es << "Stord allocation failure rc = " << rc << " for vm-id = " << vmid << std::endl;
		err_msg(false, STORD_ERR_ALLOC_FAILED);
		return HA_CALLBACK_CONTINUE;
	}

	auto arm_jsonconfig = const_cast<pio::config::ArmConfig *>(vmp->GetArmJsonConfig());

	std::string cookie;
	try {
		cookie = arm_jsonconfig->GetCookie();
	} catch (std::runtime_error const&  ex) {
		es << "No cookie present for vm-id = " << vmid << std::endl;
		LOG(INFO) << es.str();
		es.str("");
	}

	ondisk::CheckPointID ckpt_id = MetaData_constants::kInvalidCheckPointID();
	if (cookie.empty()) {
		try {
			ckpt_id = arm_jsonconfig->GetCkptId();
		} catch (boost::exception const&  ex) {
			es << "No ckpt id present for vm-id = " << vmid << std::endl;
			LOG(INFO) << es.str();
			es.str("");
		}

		if (pio_unlikely(ckpt_id == MetaData_constants::kInvalidCheckPointID())) {
			es << "Neither cookie nor ckpt id provided for vm-id = " << vmid << std::endl;
			err_msg(true, STORD_ERR_ARM_INVALID_INFO);
			return HA_CALLBACK_CONTINUE;
		}
	}

	auto vpath_info = arm_jsonconfig->GetVmdkPathInfo();
	auto vc_ip = arm_jsonconfig->GetVcIp();
	auto mo_id = arm_jsonconfig->GetMoId();

	auto vcinfo_map = pio::vc_ns::vc_info {
		{vc_ns::kVcIp, std::move(vc_ip)},
		{vc_ns::kVcUser, arm_jsonconfig->GetVcUser()},
		{vc_ns::kVcPasswd, arm_jsonconfig->GetVcPasswd()},
		{vc_ns::kVcFprint1, arm_jsonconfig->GetVcFprint1()},
		{vc_ns::kVcFprint256, arm_jsonconfig->GetVcFprint256()},
	};

	std::unique_ptr<ArmSync> armsync;
	if (ckpt_id != MetaData_constants::kInvalidCheckPointID()) {
		armsync = std::make_unique<ArmSync>(vmp, ckpt_id, kBatchSize);
		if (pio_unlikely(armsync == nullptr)) {
			es << "Stord allocation failures for vm-id = " << vmid << std::endl;
			err_msg(true, STORD_ERR_ALLOC_FAILED);
			return HA_CALLBACK_CONTINUE;
		}
	} else {
		// TODO: Need this version with cookie.
		//armsync = std::make_unique<ArmSync>(vmp, std::move(cookie));
	}

	rc = armsync->VCenterConnnect(std::move(mo_id),
			std::move(vcinfo_map));
	if (pio_unlikely(rc)) {
		es << "Cannot connect to vcenter rc = " << rc << " for vm-id = " << vmid << std::endl;
		err_msg(true, STORD_ERR_ARM_VCENTER_CONN);
		return HA_CALLBACK_CONTINUE;
	}

	rc = armsync->SyncStart(vpath_info);
	if (pio_unlikely(rc)) {
		es << "Stord ARM sync start failed rc = " << rc << " for vm-id = " << vmid << std::endl;
		err_msg(true, STORD_ERR_ARM_SYNC_START_FAILED);
		return HA_CALLBACK_CONTINUE;
	}
	vmp->AddVmSync(std::move(armsync));

	const auto res = std::to_string(0);
	ha_set_response_body(resp, HTTP_STATUS_OK, res.c_str(), res.size());
	return HA_CALLBACK_CONTINUE;
#endif
}


static int ArmSyncInfo(const _ha_request *reqp, _ha_response *resp, void *)
{
	auto param_valuep = ha_parameter_get(reqp, "vm-id");

	if (param_valuep  == NULL) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM, "vmid param not given");
		return HA_CALLBACK_CONTINUE;
	}
	std::string vmid(param_valuep);

	if (GuardHandler()) {
		SetErrMsg(resp, STORD_ERR_MAX_LIMIT,
			"Too many requests already pending");
		return HA_CALLBACK_CONTINUE;
	}

	std::lock_guard<std::mutex> lock(g_thread_.rest_guard.lock_);
	// TODO: Check correctness of p_cnt_-- use here.
	g_thread_.rest_guard.p_cnt_--;

	std::ostringstream es;

	auto vm_handle = pio::GetVmHandle(vmid);
	if (vm_handle == StorRpc_constants::kInvalidVmHandle()) {
		es << "Retriving information related to VM failed. Invalid vm-id = " << vmid << std::endl;
		LOG(ERROR) << es.str();
		SetErrMsg(resp, STORD_ERR_INVALID_VM, es.str());
		return HA_CALLBACK_CONTINUE;
	}

	auto vmp = SingletonHolder<VmManager>::GetInstance()->GetInstance(vm_handle);
	log_assert(vmp);

	// get the arm sync object
	auto syncp = vmp->GetVmSync(VmSync::Type::kSyncArm);
	if (syncp  == nullptr) {
		SetErrMsg(resp, STORD_ERR_INVALID_PARAM, "no arm_sync in progress");
		return HA_CALLBACK_CONTINUE;
	}

	// now that we have a valid arm_sync object we can extract the stats
	json_t *json_stats = json_array();
	for (auto& stats : syncp->GetStats()) {
		auto vmdk = json_object();
		json_object_set_new(vmdk, "sync_total", json_integer(stats.sync_total));
		json_object_set_new(vmdk, "sync_pending", json_integer(stats.sync_pending));
		json_object_set_new(vmdk, "sync_completed", json_integer(stats.sync_completed));
		json_object_set_new(vmdk, "sync_avoided", json_integer(stats.sync_avoided));
		json_array_append_new(json_stats, vmdk);
	}

	char* jsonp = json_dumps(json_stats, JSON_ENCODE_ANY);
	if (not jsonp) {
		SetErrMsg(resp, STORD_ERR_ARM_SYNC_START_FAILED,
			"creating json response failed");
		return HA_CALLBACK_CONTINUE;
	}

	std::string_view json(jsonp);
	ha_set_response_body(resp, HTTP_STATUS_OK, json.data(), json.length());
	json_decref(json_stats);
	std::free(reinterpret_cast<void*>(jsonp));
	return HA_CALLBACK_CONTINUE;
}

/************************************************************************ 
 		REST APIs to serve RTO/RPO HA workflow -- END 
 ************************************************************************
*/

using RestHandlers = std::unique_ptr<ha_handlers, void(*)(void*)>;
RestHandlers GetRestCallHandlers() {
	struct RestEndPoint {
		int method;
		const char* namep;
		int (*funcp) (const _ha_request* reqp, _ha_response* resp, void* datap);
		void* datap;
	};

	static constexpr std::array<RestEndPoint, 34> kHaEndPointHandlers = {{
		{POST, "new_vm", NewVm, nullptr},
		{POST, "vm_delete", RemoveVm, nullptr},
		{POST, "new_vmdk", NewVmdk, nullptr},
		{POST, "vmdk_delete", RemoveVmdk, nullptr},
		{GET, "vmdk_stats", NewVmdkStatsReq, nullptr},

		{POST, "start_preload", VmdkStartPreload, nullptr},

		{POST, "new_aero", NewAeroCluster, nullptr},
		{POST, "del_aero", DelAeroCluster, nullptr},
		{GET, "aero_stat", NewAeroCacheStatReq, nullptr},
		{POST, "scan_del_req", NewScanReq, nullptr},
		{POST, "scan_status", NewScanStatusReq, nullptr},
		{POST, "merge_req", NewMergeReq, nullptr},
		{GET, "merge_status", NewMergeStatusReq, nullptr},
		{POST, "aero_set_cleanup", AeroSetCleanup, nullptr},

		{POST, "flush_req", NewFlushReq, nullptr},
		{GET, "flush_status", NewFlushStatusReq, nullptr},
		{GET, "flush_history", NewFlushHistoryReq, nullptr},

		{POST, "prepare_ckpt", NewPrepareCkpt, nullptr},
		{POST, "commit_ckpt", NewCommitCkpt, nullptr},
		{POST, "set_bitmap", NewSetCkptBitMapReq, nullptr},

		{GET, "get_unflushed_checkpoints", GetUnflushedCheckpoints, nullptr},
		{POST, "prepare_flush", PrepareFlush, nullptr},
		{POST, "async_start_flush", AsyncStartFlush, nullptr},
		{POST, "serialize_checkpoints", SerializeCheckpoints, nullptr},
		{POST, "async_start_move_stage", AsyncStartMoveStage, nullptr},
		{POST, "delete_snapshots", DeleteSnapshots, nullptr},
		{GET, "move_status", GetMoveStatus, nullptr},
		{GET, "read_ahead_stats", ReadAheadStatsReq, nullptr},
		{GET, "get_stord_stats", GlobalStats, nullptr},
		{POST, "new_delta_context", NewDeltaContextSet, nullptr},

		{POST, "create_ckpt", CreateCkpt, nullptr},
		{POST, "add_vcenter_details", AddVcenterDetails, nullptr},
		{POST, "arm_sync_start", ArmSyncStart, nullptr},
		{POST, "arm_sync_info", ArmSyncInfo, nullptr}
	}};

	constexpr auto size = sizeof(ha_handlers) +
			kHaEndPointHandlers.size() * sizeof(ha_endpoint_handlers);
	auto handlers =
		RestHandlers(reinterpret_cast<ha_handlers*>(::malloc(size)), ::free);
	if (not handlers) {
		LOG(ERROR) << "Memory allocation for ha handlers failed";
		return handlers;
	}
	auto hp = handlers.get();
	std::memset(hp, 0, size);

	int count = 0;
	ha_endpoint_handlers* ep = hp->ha_endpoints;
	for (const auto& handler : kHaEndPointHandlers) {
		if (not handler.namep) {
			continue;
		}
		ep->ha_http_method = handler.method;
		std::strncpy(ep->ha_url_endpoint, handler.namep, sizeof(ep->ha_url_endpoint));
		ep->callback_function = handler.funcp;
		ep->ha_user_data = handler.datap;
		++ep;
		++count;
	}
	hp->ha_count = count;
	return handlers;
}

int main(int argc, char* argv[])
{
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
	std::signal(SIGQUIT, Usr1SignalHandler);
#endif

	auto handlers = GetRestCallHandlers();
	log_assert(handlers);

	g_thread_.ha_instance_ = ::ha_initialize(FLAGS_ha_svc_port,
			FLAGS_etcd_ip.c_str(), FLAGS_svc_label.c_str(),
			FLAGS_stord_version.c_str(), 120,
			const_cast<const struct ha_handlers *> (handlers.get()),
			StordHaStartCallback, StordHaStopCallback, 1, NULL);

	if (g_thread_.ha_instance_ == nullptr) {
		LOG(ERROR) << "ha_initialize failed";
		return -EINVAL;
	}

	auto stord_instance = std::make_unique<::StorD>();
	stord_instance->InitStordLib();

#ifdef USE_NEP
	/* Initialize threadpool for AeroSpike accesses */
	auto tmgr_rest = std::make_shared<pio::hyc::TargetManagerRest>(
		SingletonHolder<pio::hyc::TargetManager>::GetInstance().get(),
		g_thread_.ha_instance_);
#endif
	/* Initialize threadpool for Flush processing */
	auto rc = SingletonHolder<FlushManager>::GetInstance()
			->CreateInstance(g_thread_.ha_instance_);
	log_assert(rc == 0);

	/* Initialize threadpool for Merge processing */
	rc = SingletonHolder<CkptMergeManager>::GetInstance()
			->CreateInstance(g_thread_.ha_instance_);
	log_assert(rc == 0);

	LOG(INFO) << "Starting Thrift Server";
	google::FlushLogFiles(google::INFO);
	google::FlushLogFiles(google::ERROR);

	auto si = std::make_shared<StorRpcSvImpl>();
	thrift_server = std::make_shared<ThriftServer>();
	thrift_server->setInterface(si);
	thrift_server->setAddress(kServerIp, kServerPort);
	thrift_server->setNumIOWorkerThreads(3);
	thrift_server->serve();

	SingletonHolder<CkptMergeManager>::GetInstance()->DestroyInstance();
	SingletonHolder<FlushManager>::GetInstance()->DestroyInstance();

	stord_instance->DeinitStordLib();

	ha_deinitialize(g_thread_.ha_instance_);

	LOG(INFO) << "StorD Stopped";
	return 0;
}
