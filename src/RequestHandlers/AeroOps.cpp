#include <cerrno>
#include <iterator>
#include <chrono>
#include <string>

#include <folly/fibers/Fiber.h>
#include <folly/fibers/Baton.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_batch.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "WorkScheduler.h"
#include "Vmdk.h"
#include "Request.h"
#include "AeroOps.h"
#include "ThreadPool.h"
#include "DaemonTgtInterface.h"
#include "Singleton.h"

#if 0
#define INJECT_AERO_WRITE_ERROR 0
#define INJECT_AERO_DEL_ERROR 0
#define INJECT_AERO_READ_ERROR 0
#endif

#define MAX_W_IOS_IN_HISTORY 100000
#define MAX_R_IOS_IN_HISTORY 100000

using namespace ::ondisk;
using namespace std::chrono_literals;

namespace pio {
const static std::string kMetaSetName = "metaset";
const static uint32_t kAeroWriteBlockSize = 1024 * 1024;

AeroSpike::AeroSpike() {
	instance_ = SingletonHolder<AeroFiberThreads>::GetInstance();
}

AeroSpike::~AeroSpike() {}

int AeroSpike::CacheIoWriteKeySet(ActiveVmdk *, WriteRecord* wrecp,
		const std::string& ns,
		const std::string& setp) {
	auto kp  = &wrecp->key_;
	auto kp1 = as_key_init(kp, ns.c_str(), setp.c_str(),
		wrecp->key_val_.c_str());

	#if 0
	LOG(ERROR) << __func__ << "setp_::" << setp.c_str() <<
		"::" << ns.c_str() << "::" << wrecp->key_val_.c_str();
	#endif
	log_assert(kp1 == kp);

	auto rp = &wrecp->record_;
	as_record_init(rp, 1);

	auto srcp = wrecp->rq_block_->GetRequestBufferAtBack();
	auto s = as_record_set_raw(rp, kAsCacheBin.c_str(),
			(const uint8_t *)srcp->Payload(), srcp->PayloadSize());
	if (pio_unlikely(s == false)) {
		LOG(ERROR) << __func__<< "Error in setting record property";
		return -EINVAL;
	}

	return 0;
}

int AeroSpike::WriteBatchInit(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process, WriteBatch *w_batch_rec,
		const std::string& ns) {
	w_batch_rec->batch.recordsp_.reserve(process.size());

	/* Create Batch write records */
	for (auto block : process) {
		auto rec = std::make_unique<WriteRecord>(block, w_batch_rec, ns, vmdkp);
		if (pio_unlikely(not rec)) {
			return -ENOMEM;
		}

		w_batch_rec->batch.recordsp_.emplace_back(std::move(rec));
	}

	w_batch_rec->batch.nwrites_ = process.size();
	w_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

int AeroSpike::WriteBatchPrepare(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		WriteBatch *w_batch_rec, const std::string& ns) {
	WriteBatchInit(vmdkp, process, w_batch_rec, ns);
	for (auto& v_record : w_batch_rec->batch.recordsp_) {
		auto record  = v_record.get();
		auto rc = CacheIoWriteKeySet(vmdkp, record, ns,
					record->setp_);
		if (pio_unlikely(rc < 0)) {
			return rc;
		}

		auto srcp = record->rq_block_->GetRequestBufferAtBack();
		w_batch_rec->batch_write_size_ += srcp->PayloadSize();

		/*
		 * Set TTL -1 for DIRTY namespace writes, for CLEAN
		 * namespace it should be inherited from global
		 * aerospike conf file
		 */

		if (pio_likely(ns == kAsNamespaceCacheDirty)) {
			record->record_.ttl = -1;
		} else {
			record->record_.ttl = 0;
		}
	}
	return 0;
}

/*
 * This function is called from aerospike event loop threads - this is outside
 * Fiber context.
 * */

static void WriteListener(as_error *errp, void *datap, as_event_loop*) {

	auto wrp = (WriteRecord *) reinterpret_cast<WriteRecord*>(datap);
	log_assert(wrp && wrp->batchp_);
	auto batchp = (wrp->batchp_);

	bool stop_pipeline = false;
	wrp->status_ = AEROSPIKE_OK;

	batchp->batch.ncomplete_++;

	if ((batchp->batch.ncomplete_ == batchp->batch.nsent_) &&
		(batchp->batch.nsent_ == batchp->batch.nwrites_)) {
		stop_pipeline = true;
	} else if ((batchp->batch.ncomplete_ == batchp->batch.nsent_) &&
				(batchp->failed_ == true)) {
		stop_pipeline = true;
	}

	if (pio_unlikely(errp)) {
		wrp->status_ = errp->code;
		LOG(ERROR) << __func__ << "::error msg:"
			<< errp->message << ", err code:" << errp->code;
		batchp->failed_ = true;
		if (batchp->batch.ncomplete_ == batchp->batch.nsent_) {
			stop_pipeline = true;
		}
	}

#ifdef INJECT_AERO_WRITE_ERROR
	/* if not already failed */
	if (!wrp->status_) {
		if (batchp->retry_cnt_ > 1) {
			wrp->status_ = AEROSPIKE_ERR_TIMEOUT;
			LOG(ERROR) << __func__ << "::Injecting AEROSPIKE_ERR_TIMEOUT error, retry_cnt :: " << batchp->retry_cnt_;
		} else if (batchp->retry_cnt_ == 1) {
			wrp->status_ = AEROSPIKE_ERR_CLUSTER;
			LOG(ERROR) << __func__ << "::Injecting AEROSPIKE_ERR_CLUSTER error, retry_cnt :: " << batchp->retry_cnt_;
		}
	}
#endif

	if (stop_pipeline == true) {
			batchp->promise_->setValue(0);
	}
	return;
}

static void WritePipeListener(void *udatap, as_event_loop *lp) {

	auto wrp = reinterpret_cast<WriteRecord *>(udatap);
	log_assert(wrp && wrp->batchp_);
	auto batchp = wrp->batchp_;

	if (batchp->submitted_ == true || batchp->failed_ == true) {
		batchp->submitted_ = true;
		return;
	}
	batchp->batch.nsent_++;

	wrp = batchp->batch.rec_it_->get();
	++batchp->batch.rec_it_;
	as_pipe_listener fnp = nullptr;

	if(pio_unlikely(batchp->batch.nsent_ == batchp->batch.nwrites_)) {
		batchp->submitted_ = true;
	} else {
		fnp = WritePipeListener;
		log_assert(batchp->batch.nsent_ < batchp->batch.nwrites_);
	}

	auto kp = &wrp->key_;
	auto rp = &wrp->record_;
	as_error err;
	auto s = aerospike_key_put_async(&batchp->aero_conn_->as_, &err,
			NULL, kp, rp, WriteListener,
			reinterpret_cast<void*>(wrp), lp, fnp);

	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::request submit failed, code"
			<< err.code << "msg:" << err.message;
		batchp->batch.nsent_--;
		batchp->failed_ = true;
	}
}

int AeroSpike::ResetWriteBatchState(WriteBatch *batchp) {
	batchp->submitted_ = false;
	batchp->failed_ = false;
	batchp->retry_ = false;
	batchp->batch.nsent_ = 0;
	batchp->batch.ncomplete_ = 0;
	batchp->batch_write_size_ = 0;
	batchp->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

int AeroSpike::ResetDelBatchState(DelBatch *batchp) {
	batchp->submitted_ = false;
	batchp->failed_ = false;
	batchp->retry_ = false;
	batchp->batch.nsent_ = 0;
	batchp->batch.ncomplete_ = 0;
	batchp->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

int AeroSpike::ResetReadBatchState(ReadBatch *batchp) {
	batchp->failed_ = false;
	batchp->retry_ = false;
	batchp->as_result_ = AEROSPIKE_OK;
	batchp->ncomplete_ = 0;
	batchp->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}
folly::Future<int> AeroSpike::WriteBatchSubmit(WriteBatch *batchp) {

	as_event_loop    *lp = NULL;
	WriteRecord      *wrp;
	as_key           *kp;
	as_record        *rp;
	as_status        s;
	as_error         err;
	as_pipe_listener fnp;
	uint16_t         nwrites;

	nwrites = batchp->batch.nwrites_;
	log_assert(batchp && batchp->failed_ == false);
	log_assert(nwrites > 0);
	if (pio_unlikely(batchp->submitted_ != false)) {
		LOG(ERROR) << "Failed, batchp->submitted : " << batchp->submitted_;
		log_assert(0);
		return -1;
	}

	/* Get very first record from vector to start */
	batchp->batch.rec_it_ = (batchp->batch.recordsp_).begin();
	wrp = batchp->batch.rec_it_->get();
	++batchp->batch.rec_it_;

	kp  = &wrp->key_;
	rp  = &wrp->record_;
	if (nwrites == 1) {
		batchp->submitted_ = true;
		fnp = NULL;
	} else {
		batchp->submitted_ = false;
		fnp = WritePipeListener;
	}

	AeroSpikeConn::ConfigTag tag;
	batchp->aero_conn_->GetEventLoopAndTag(&lp, &tag);
	log_assert(lp != NULL);

	batchp->batch.nsent_ = 1;
	s = aerospike_key_put_async(&batchp->aero_conn_->as_,
			&err, NULL, kp, rp,
			WriteListener,
			reinterpret_cast<void*>(wrp), lp, fnp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::request submit failed, err code:"
			<< err.code << ", err msg:" << err.message;

		batchp->batch.nsent_--;
		wrp->status_ = s;
		batchp->failed_ = true;
		return -1;
	}

	return batchp->promise_->getFuture()
	.then([this, batchp, tag] (int) mutable {
		log_assert(batchp->submitted_ == true);
		batchp->submitted_ = 0;

		/*
		 * Check if any of the Request Blocks has been failed
		 * due to AERO TIMOUT or OVERLOAD error. We are retying
		 * at Request level and all Blocks would be retried again.
		 * TBD: Improve this behaviour.
		 */

		for (auto& v_rec : batchp->batch.recordsp_) {
			auto rec =  v_rec.get();
			switch(rec->status_) {
				default:
					LOG(ERROR) << __func__ << "::failed write request::"
						<< rec->status_ << "::ns::" << rec->batchp_->ns_;
					if (rec->batchp_->ns_ == kAsNamespaceCacheClean) {
						batchp->failed_ = false;
					}
					batchp->retry_ = false;
					break;
				case AEROSPIKE_ERR_TIMEOUT:
				case AEROSPIKE_ERR_DEVICE_OVERLOAD:
					batchp->aero_conn_->HandleServerOverload(tag);
					[[fallthrough]];
				case AEROSPIKE_ERR_ASYNC_CONNECTION:
				case AEROSPIKE_ERR_RECORD_BUSY:
				case AEROSPIKE_ERR_CLUSTER:
				case AEROSPIKE_ERR_SERVER:
					LOG(ERROR) << __func__ << rec->status_
						<< "::Retyring for failed write request"
						<< ", retry cnt ::"
						<< batchp->retry_cnt_;
					batchp->failed_ = true;
					batchp->retry_ = true;
					break;
				case AEROSPIKE_ERR_SERVER_FULL:
					if (rec->batchp_->ns_ == kAsNamespaceCacheDirty) {
						batchp->failed_ = true;
						batchp->retry_ = true;
						LOG(ERROR) << __func__ << rec->status_
							<< "::Retyring for failed write request"
							<< ", retry cnt ::"
							<< batchp->retry_cnt_;
						break;
					} else if (rec->batchp_->ns_ == kAsNamespaceCacheClean) {
						LOG(ERROR) << "Don't treat ENOSPACE as error during"
							<< " read populate";
						batchp->failed_ = false;
						batchp->retry_ = false;
						rec->status_ = AEROSPIKE_OK;
					}
				case AEROSPIKE_OK:
					break;
			}

			/* Break in case of nonretryable error */
			if (pio_unlikely(batchp->failed_ && !batchp->retry_)) {
				break;
			}
		}

		if (pio_unlikely(batchp->failed_ && batchp->retry_ && batchp->retry_cnt_)) {
			--batchp->retry_cnt_;
			ResetWriteBatchState(batchp);
			auto schedulerp = batchp->aero_conn_->GetWorkScheduler();
			return schedulerp->ScheduleAsync(
					(kMaxRetryCnt - batchp->retry_cnt_) * 128ms,
					[this, batchp] () mutable {
				return this->WriteBatchSubmit(batchp);
			});
		}

		return folly::makeFuture(int(batchp->failed_));
	});
}

folly::Future<int> AeroSpike::AeroWrite(ActiveVmdk *vmdkp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>&, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	auto batch = std::make_unique<WriteBatch>(vmdkp->GetID(), ns,
		vmdkp->GetVM()->GetSetName());
	if (pio_unlikely(batch == nullptr)) {
		LOG(ERROR) << "WriteBatch allocation failed";
		return -ENOMEM;
	}

	batch->batch.nwrites_ = process.size();
	batch->ckpt_ = ckpt;
	batch->aero_conn_ = aero_conn.get();
	log_assert(batch->aero_conn_ != nullptr);

	auto rc = WriteBatchPrepare(vmdkp, process, batch.get(), ns);
	if (pio_unlikely(rc < 0)) {
		return rc;
	}

	auto start_time = std::chrono::high_resolution_clock::now();
	return WriteBatchSubmit(batch.get())
	.then([vmdkp, batch = std::move(batch),
			start_time = std::move(start_time)]
			(int rc) mutable {
		if (pio_likely(!rc)) {
			auto end_time = std::chrono::high_resolution_clock::now();
			auto duration = std::chrono::duration_cast
						<std::chrono::microseconds>
						(end_time - start_time).count();

			std::unique_lock<std::mutex> w_lock(vmdkp->stats_->w_aero_stat_lock_);
			if (pio_unlikely(!batch->failed_)) {
				vmdkp->stats_->IncrAeroWriteBytes(batch->batch_write_size_);
				if ((vmdkp->stats_->w_aero_total_latency_ + duration)
					< vmdkp->stats_->w_aero_total_latency_ ||
					vmdkp->stats_->w_aero_io_blks_count_ > MAX_W_IOS_IN_HISTORY) {
					vmdkp->stats_->w_aero_total_latency_ = 0;
					vmdkp->stats_->w_aero_io_blks_count_ = 0;
				} else {
					vmdkp->stats_->w_aero_total_latency_ += duration;
					vmdkp->stats_->w_aero_io_blks_count_ += batch->batch.nwrites_;
				}
			}

			if (vmdkp->stats_->w_aero_io_blks_count_ &&
					(vmdkp->stats_->w_aero_io_blks_count_ % 100) == 0) {
				VLOG(5) << __func__ <<
				"[AeroWrite:VmdkID:" << vmdkp->GetID() <<
				", Total latency :" << vmdkp->stats_->w_aero_total_latency_ <<
				", Total blks IO count (in blk size):"
					<< vmdkp->stats_->w_aero_io_blks_count_ <<
				", avg blk access latency:"
					<< vmdkp->stats_->w_aero_total_latency_ / vmdkp->stats_->w_aero_io_blks_count_;
			}
			w_lock.unlock();
		} else {
			/* Mark records failed */
			for (auto& rec : batch->batch.recordsp_) {
				if (pio_likely(rec->status_ != AEROSPIKE_OK)) {
					rec->rq_block_->SetResult(-EIO, RequestStatus::kFailed);
				}
			}
		}

		return rc;
	});
}

folly::Future<int> AeroSpike::AeroWriteCmdProcess(ActiveVmdk *vmdkp,
		CheckPointID ckpt,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	if (pio_unlikely(process.empty())) {
		return 0;
	}

	failed.clear();
	return AeroWrite(vmdkp, ckpt, process, failed, ns, aero_conn);
}

int AeroSpike::CacheIoReadKeySet(ActiveVmdk *, ReadRecord* rrecp,
		const std::string& ns, ReadBatch* r_batch_rec) {

	auto recp = as_batch_read_reserve(r_batch_rec->aero_recordsp_);
	if (pio_unlikely(recp == nullptr)) {
		LOG(ERROR) << "ReadBatch allocation failed";
		return -EINVAL;
	}
	recp->read_all_bins = true;

	auto kp = as_key_init(&recp->key, ns.c_str(),
		rrecp->setp_.c_str(), rrecp->key_val_.c_str());
	if (pio_unlikely(kp == nullptr)) {
		return -ENOMEM;
	}
	log_assert(kp == &recp->key);

	rrecp->aero_recp_ = recp;
	return 0;
}

int AeroSpike::ReadBatchInit(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process, ReadBatch *r_batch_rec,
		const std::string& ns) {
	/* Allocate only to what is needed to serve the misses */
	unsigned int count = 0;
	for (auto block : process) {
		if (block->IsReadHit()) {
			continue;
		}
		++count;
	}

	r_batch_rec->aero_recordsp_ = as_batch_read_create(count);
	if (pio_unlikely(r_batch_rec->aero_recordsp_ == nullptr)) {
		return -ENOMEM;
	}

	r_batch_rec->recordsp_.reserve(count);
	count = 0;
	for (auto block : process) {

		/* Look only for misses. Useful when we are
		 * accessing CLEAN namespace for read misses */

		if (block->IsReadHit()) {
			continue;
		}

		auto rec = std::make_unique<ReadRecord>(block, r_batch_rec,
				ns, vmdkp, block->GetSetName());
		if (pio_unlikely(not rec)) {
			LOG(ERROR) << "ReadRecord allocation failed";
			return -ENOMEM;
		}

		auto rc = CacheIoReadKeySet(vmdkp, rec.get(), ns, r_batch_rec);
		if (pio_unlikely(rc < 0)) {
			return rc;
		}

		r_batch_rec->recordsp_.emplace_back(std::move(rec));
		++count;
	}
	log_assert(count <= process.size());
	r_batch_rec->nreads_ = count;
	r_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

int AeroSpike::ReadBatchPrepare(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		ReadBatch *r_batch_rec, const std::string& ns) {
	ReadBatchInit(vmdkp, process, r_batch_rec, ns);
	return 0;
}

/*
 * This function is called from aerospike event loop threads -
 * outside of Fiber context.
 */
static void ReadListener(as_error *errp, as_batch_read_records *,
		void *datap, as_event_loop *) {
	auto rc = 0;
	auto batchp = reinterpret_cast<ReadBatch *>(datap);
	log_assert(batchp);
	batchp->as_result_ = AEROSPIKE_OK;
	if (pio_unlikely(errp)) {
		batchp->as_result_ = errp->code;
		if (errp->code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			LOG(ERROR) << __func__ << " error msg:" <<
				errp->message << ", err code:" << errp->code;
			rc = 1;
		}
	}

#ifdef INJECT_AERO_READ_ERROR
	/* if not already failed */
	if (!batchp->as_result_) {
		if (batchp->retry_cnt_ > 1) {
			batchp->as_result_ = AEROSPIKE_ERR_TIMEOUT;
			LOG(ERROR) << __func__ << "::Injecting AEROSPIKE_ERR_TIMEOUT error, retry_cnt :: " << batchp->retry_cnt_;
		} else if (batchp->retry_cnt_ == 1) {
			batchp->as_result_ = AEROSPIKE_ERR_CLUSTER;
			LOG(ERROR) << __func__ << "::Injecting AEROSPIKE_ERR_CLUSTER error, retry_cnt :: " << batchp->retry_cnt_;
		}
		rc = 1;
	}
#endif

	batchp->promise_->setValue(rc);
}

folly::Future<int> AeroSpike::ReadBatchSubmit(ReadBatch *batchp) {

	as_event_loop    *loopp = NULL;
	as_status        s;
	as_error         err;
	uint16_t         nreads;

	nreads       = batchp->nreads_;
	log_assert(batchp && batchp->failed_ == false);
	log_assert(nreads > 0);

	AeroSpikeConn::ConfigTag tag;
	batchp->aero_conn_->GetEventLoopAndTag(&loopp, &tag);
	log_assert(loopp != NULL);

	s = aerospike_batch_read_async(&batchp->aero_conn_->as_, &err, NULL, batchp->aero_recordsp_,
			ReadListener, batchp, loopp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::aerospike_batch_read_async request submit failed, code"
			<< err.code << ", msg::" << err.message;
		batchp->as_result_ = s;
		batchp->failed_ = true;
		return -1;
	}

	return batchp->promise_->getFuture()
	.then([this, batchp, tag] (int) mutable {
		switch (batchp->as_result_) {
			/* Record not found is not an error case*/
			case AEROSPIKE_OK:
			case AEROSPIKE_ERR_RECORD_NOT_FOUND:
				break;
			case AEROSPIKE_ERR_TIMEOUT:
			case AEROSPIKE_ERR_DEVICE_OVERLOAD:
				batchp->aero_conn_->HandleServerOverload(tag);
				[[fallthrough]];
			case AEROSPIKE_ERR_ASYNC_CONNECTION:
			case AEROSPIKE_ERR_NO_MORE_CONNECTIONS:
			case AEROSPIKE_ERR_RECORD_BUSY:
			case AEROSPIKE_ERR_SERVER:
			case AEROSPIKE_ERR_CLUSTER:
				LOG(ERROR) << __func__ << "Retyring for failed aerospike_batch_read_async"
					<<  ", err code:" << batchp->as_result_;
				batchp->retry_ = true;
				batchp->failed_ = true;
				break;
			default:
				LOG(ERROR) << __func__ << "Unretryable error for failed aerospike_batch_read_async"
					<<  ", err code:" << batchp->as_result_;
				batchp->failed_ = true;
				batchp->retry_ = false;
				break;
		}

		if (pio_unlikely(batchp->failed_ && batchp->retry_ && batchp->retry_cnt_)) {
			--batchp->retry_cnt_;
			this->ResetReadBatchState(batchp);
			auto schedulerp = batchp->aero_conn_->GetWorkScheduler();
			return schedulerp->ScheduleAsync(
					(kMaxRetryCnt - batchp->retry_cnt_) * 128ms,
					[this, batchp] () mutable {
				return this->ReadBatchSubmit(batchp);
			});
		}

		as_batch_read_record *recp;
		for (auto& v_rec : batchp->recordsp_) {
			auto rec =  v_rec.get();
			recp = rec->aero_recp_;
			rec->status_ = recp->result;
		}
		return folly::makeFuture(int(batchp->failed_));
	});
}

folly::Future<int> AeroSpike::AeroRead(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	auto batch = std::make_unique<ReadBatch>(vmdkp->GetID(), ns,
			vmdkp->GetVM()->GetSetName());
	log_assert(batch != nullptr);

	batch->aero_conn_ = aero_conn.get();
	log_assert(batch->aero_conn_ != nullptr);

	auto rc = ReadBatchPrepare(vmdkp, process, batch.get(), ns);
	if (pio_unlikely(rc < 0)) {
		LOG(ERROR) <<__func__ << "::read_batch_prepare failed";
		return rc;
	}

	auto start_time = std::chrono::high_resolution_clock::now();
	return ReadBatchSubmit(batch.get())
	.then([batch = std::move(batch), vmdkp, &failed, start_time = std::move(start_time)]
			(int) mutable {
		auto rc = 0;
		if (pio_unlikely(batch->failed_)) {
			LOG(ERROR) <<__func__ << "read_batch_submit failed";
			rc = -EIO;
		} else {

			/* Latency cals */
			long long duration;
			auto end_time = std::chrono::high_resolution_clock::now();
			duration = std::chrono::duration_cast<std::chrono::microseconds>
					(end_time - start_time).count();

			std::unique_lock<std::mutex> r_lock(vmdkp->stats_->r_aero_stat_lock_);
			if ((vmdkp->stats_->r_aero_total_latency_ + duration)
					< vmdkp->stats_->r_aero_total_latency_ ||
					vmdkp->stats_->r_aero_io_blks_count_ > MAX_W_IOS_IN_HISTORY) {
					vmdkp->stats_->r_aero_total_latency_ = 0;
					vmdkp->stats_->r_aero_io_blks_count_ = 0;
			} else {
				vmdkp->stats_->r_aero_total_latency_ += duration;
				vmdkp->stats_->r_aero_io_blks_count_ += batch->nreads_;
			}

			if (vmdkp->stats_->r_aero_io_blks_count_ &&
					(vmdkp->stats_->r_aero_io_blks_count_ % 100) == 0) {
				VLOG(5) << __func__ <<
					"[AeroRead:VmdkID:" << vmdkp->GetID() <<
					", Total latency :" << vmdkp->stats_->r_aero_total_latency_ <<
					", Total blks IO count (in blk size):"
						<< vmdkp->stats_->r_aero_io_blks_count_ <<
					", avg blk access latency:"
						<< vmdkp->stats_->r_aero_total_latency_ / vmdkp->stats_->r_aero_io_blks_count_;
			}
			r_lock.unlock();

			/* Set hit Status for all the request blocks those has been found in cache */
			as_batch_read_record *recp;
			for (auto& rec : batch->recordsp_) {
				auto blockp = rec->rq_block_;
				recp = rec->aero_recp_;
				switch (recp->result) {
					default:
						rc = -EIO;
						break;
					case AEROSPIKE_OK:
						{
						as_bytes *bp = as_record_get_bytes(&recp->record, kAsCacheBin.c_str());
						if (pio_unlikely(bp == NULL)) {
							LOG(ERROR) << __func__ << "Access error, unable to get data from given rec";
							blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
							failed.emplace_back(blockp);
							rc = -ENOMEM;
							break;
						}

						auto destp = NewRequestBuffer(as_bytes_size(bp));
						if (pio_unlikely(not destp)) {
							blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
							failed.emplace_back(blockp);
							rc = -ENOMEM;
							break;
						}

						auto ret = as_bytes_copy(bp, 0, (uint8_t *) destp->Payload(),
							(uint32_t) destp->PayloadSize());
						log_assert(ret == destp->PayloadSize());
						if (pio_unlikely(ret != destp->PayloadSize())) {
							LOG(ERROR) << __func__ << "Access error, data found after"
								" cache read is less than expected size";
							blockp->SetResult(-EIO, RequestStatus::kFailed);
							failed.emplace_back(blockp);
							rc = -ENOMEM;
							break;
						}

						blockp->PushRequestBuffer(std::move(destp));
						blockp->SetResult(0, RequestStatus::kHit);
						vmdkp->stats_->IncrAeroReadBytes(ret);
						}
						break;

					case AEROSPIKE_ERR_RECORD_NOT_FOUND:
						blockp->SetResult(0, RequestStatus::kMiss);
						break;
				}

				/* If any of the RequestBlocks has been failed then break*/
				if (rc) {
					break;
				}
			}
		}
		return rc;
	});
}

folly::Future<int> AeroSpike::AeroReadCmdProcess(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	if (pio_unlikely(process.empty())) {
		return 0;
	}

	/* Allocate only whatever is needed to serve the misses */
	{
		unsigned int count = 0;
		for (auto blockp : process) {
			/* We need to look only for non hit cases */
			if (pio_unlikely(blockp->IsReadHit())) {
				continue;
			}
			++count;
		}

		if (!count) {
			/*
			 * Nothing to process, all hits (from DIRTY namespace)
			 * already
			 */
			return 0;
		}
	}

#if 0
	/* Use normal async interface (AeroSingleRead) instead of batch if we have
	 * to procsess just one record */

	if (pio_unlikely(process.size() == 1)) {
		return AeroSingleRead(vmdkp, process, failed, ns, aero_conn);
	} else {
		return AeroRead(vmdkp, process, failed, ns, aero_conn);
	}
#else
	return AeroRead(vmdkp, process, failed, ns, aero_conn);
#endif
}

int AeroSpike::CacheIoDelKeySet(ActiveVmdk *, DelRecord* drecp,
	const std::string& ns, const std::string& setp) {

	auto kp = as_key_init(&drecp->key_, ns.c_str(), setp.c_str(),
		drecp->key_val_.c_str());
	log_assert(kp == &drecp->key_);
	return 0;
}

int AeroSpike::DelBatchInit(ActiveVmdk *,
		const std::vector<RequestBlock*>& process, DelBatch *batchp,
		const std::string&) {
	batchp->batch.recordsp_.reserve(process.size());

	for (auto block : process) {
		auto offset = block->GetAlignedOffset();
		std::ostringstream os;
		os << batchp->pre_keyp_ << ":"
			<< batchp->ckpt_ << ":"
			<< (offset >> kSectorShift);
		batchp->batch.recordsp_.emplace_back(batchp, batchp->ns_,
			batchp->setp_, os.str());
	}
	batchp->batch.ndeletes_ = process.size();
	batchp->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

int AeroSpike::DelBatchPrepare(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		DelBatch *d_batch_rec, const std::string& ns) {
	return DelBatchInit(vmdkp, process, d_batch_rec, ns);
}

/*
 * This function is called from aerospike event loop threads - this is outside
 * of Fiber context.
 * */

static void DelListener(as_error *errp, void *datap, as_event_loop*) {

	auto drp = reinterpret_cast<DelRecord *>(datap);
	log_assert(drp && drp->batchp_);
	auto batchp = drp->batchp_;

	bool stop_pipeline = false;
	drp->status_ = AEROSPIKE_OK;

	batchp->batch.ncomplete_++;

	if ((batchp->batch.ncomplete_ >= batchp->batch.nsent_) &&
		(batchp->batch.nsent_ == batchp->batch.ndeletes_)) {
		stop_pipeline = true;
	} else if ((batchp->batch.ncomplete_ >= batchp->batch.nsent_) &&
				(batchp->failed_ == true)) {
		stop_pipeline = true;
	}

	if (pio_unlikely(errp and errp->code != AEROSPIKE_ERR_RECORD_NOT_FOUND)) {
		drp->status_ = errp->code;
		#if 0
		LOG(ERROR) << __func__ << "::error msg:" <<
			errp->message << ", err code:" << errp->code;
		#endif
		batchp->failed_ = true;
		if (batchp->batch.ncomplete_ >= batchp->batch.nsent_) {
			stop_pipeline = true;
		}
	}

#ifdef INJECT_AERO_DEL_ERROR
	/* if not already failed */
	if (!drp->status_) {
		if (batchp->retry_cnt_ > 1) {
			drp->status_ = AEROSPIKE_ERR_TIMEOUT;
			LOG(ERROR) << __func__ << "Injecting AEROSPIKE_ERR_TIMEOUT error, retry_cnt ::" << batchp->retry_cnt_;
		} else if (batchp->retry_cnt_ == 1) {
			drp->status_ = AEROSPIKE_ERR_CLUSTER;
			LOG(ERROR) << __func__ << "Injecting AEROSPIKE_ERR_CLUSTER error, retry_cnt ::" << batchp->retry_cnt_;
		}
	}
#endif

	if (stop_pipeline == true) {
			batchp->promise_->setValue(0);
	}
}

static void DelPipeListener(void *udatap, as_event_loop *lp) {

	DelRecord *drp = (DelRecord *) udatap;
	log_assert(drp && drp->batchp_);

	DelBatch *batchp = (drp->batchp_);

	as_pipe_listener fnp = DelPipeListener;

	if (batchp->submitted_ == true || batchp->failed_ == true) {
		batchp->submitted_ = true;
		return;
	}
	batchp->batch.nsent_++;

	drp = &(*batchp->batch.rec_it_);
	std::advance(batchp->batch.rec_it_, 1);
	if (batchp->batch.rec_it_ == (batchp->batch.recordsp_).end()) {
		batchp->submitted_ = true;
		fnp                = NULL;
		log_assert(batchp->batch.nsent_ == batchp->batch.ndeletes_);
	} else {
		log_assert(batchp->batch.nsent_ < batchp->batch.ndeletes_);
	}

	as_key *kp  = &drp->key_;
	as_error  err;
	as_status s;
	s = aerospike_key_remove_async(&batchp->aero_conn_->as_, &err, NULL, kp,
			DelListener, (void *) drp, lp, fnp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::Submit failed, err code:"
			<< err.code << ", err msg:" << err.message;
		batchp->batch.nsent_--;
		batchp->failed_ = true;
	}
}

folly::Future<int> AeroSpike::DelBatchSubmit(DelBatch *batchp) {

	as_event_loop    *loopp = NULL;
	DelRecord        *drp;
	as_key           *kp;
	as_status        s;
	as_error         err;
	as_pipe_listener fnp;
	uint16_t         ndeletes;

	ndeletes       = batchp->batch.ndeletes_;

	log_assert(batchp && batchp->failed_ == false);
	log_assert(ndeletes > 0);

	if (pio_unlikely(batchp->submitted_ != false)) {
		LOG(ERROR) << "Failed, batchp->submitted : " << batchp->submitted_;
		log_assert(0);
		return -1;
	}

	/* Get very first record from vector to process */
	batchp->batch.rec_it_ = (batchp->batch.recordsp_).begin();
	drp = &(*batchp->batch.rec_it_);
	std::advance(batchp->batch.rec_it_, 1);

	batchp->batch.nsent_     = 1;
	kp  = &drp->key_;
	if (ndeletes == 1) {
		batchp->submitted_ = true;
		fnp = NULL;
	} else {
		batchp->submitted_ = false;
		fnp = DelPipeListener;
	}

	AeroSpikeConn::ConfigTag tag;
	batchp->aero_conn_->GetEventLoopAndTag(&loopp, &tag);
	log_assert(loopp != NULL);

	s = aerospike_key_remove_async(&batchp->aero_conn_->as_, &err, NULL, kp,
			DelListener, (void *) drp, loopp, fnp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::aerospike_key_remove_async"
			<< " request submit failed, code"
			<< err.code << "msg:" << err.message;

		batchp->batch.nsent_--;
		drp->status_ = s;
		batchp->failed_ = true;
		return -1;
	}

	return batchp->promise_->getFuture()
	.then([this, batchp, tag] (int) mutable {
		log_assert(batchp->submitted_ == true);
		batchp->submitted_ = 0;

		/*
		 * Check if any of the Request has failed because of
		 * AERO TIMOUT or OVERLOAD error. We are retying at
		 * Request level and so all the RequestBlocks would
		 * be retried again. TBD: Improve this behaviour,
		 * retry only failed request blocks
		 */

		for (auto& rec : batchp->batch.recordsp_) {
			switch (rec.status_) {
				default:
					LOG(ERROR) << __func__ << rec.status_
						<< "::failed delete request::"
						<< rec.status_;
					batchp->failed_ = true;
					batchp->retry_ = false;
					break;
				case AEROSPIKE_ERR_TIMEOUT:
				case AEROSPIKE_ERR_DEVICE_OVERLOAD:
					batchp->aero_conn_->HandleServerOverload(tag);
					[[fallthrough]];
				case AEROSPIKE_ERR_ASYNC_CONNECTION:
				case AEROSPIKE_ERR_RECORD_BUSY:
				case AEROSPIKE_ERR_CLUSTER:
				case AEROSPIKE_ERR_SERVER:
					LOG(ERROR) << __func__ <<
					"::Retrying for failed delete request, retry cnt :"
					<< batchp->retry_cnt_;
					batchp->failed_ = true;
					batchp->retry_ = true;
					break;
				case AEROSPIKE_OK:
				case AEROSPIKE_ERR_RECORD_NOT_FOUND:
					break;
			}

			/* Break in case of nonretryable error */
			if (pio_unlikely(batchp->failed_ && !batchp->retry_)) {
				break;
			}
		}

		if (pio_unlikely(batchp->failed_ && batchp->retry_ && batchp->retry_cnt_)) {
			--batchp->retry_cnt_;
			ResetDelBatchState(batchp);
			auto schedulerp = batchp->aero_conn_->GetWorkScheduler();
			return schedulerp->ScheduleAsync(
					(kMaxRetryCnt - batchp->retry_cnt_) * 128ms,
					[this, batchp] () mutable {
				return this->DelBatchSubmit(batchp);
			});
		}
		return folly::makeFuture(int(batchp->failed_));
	});
}

folly::Future<int> AeroSpike::AeroDel(ActiveVmdk *vmdkp, CheckPointID ckpt,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>&, const std::string& ns,
		const std::string& set,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	/* Create Batch write records */
	auto batch = std::make_unique<DelBatch>(vmdkp->GetID(), ns, set);
	if (pio_unlikely(batch == nullptr)) {
		LOG(ERROR) << "DelBatch allocation failed";
		return -ENOMEM;
	}

	batch->batch.ndeletes_ = process.size();
	batch->ckpt_ = ckpt;
	batch->aero_conn_ = aero_conn.get();
	log_assert(batch->aero_conn_ != nullptr);
	auto rc = DelBatchPrepare(vmdkp, process, batch.get(), ns);
	if (pio_unlikely(rc < 0)) {
		return rc;
	}

	return DelBatchSubmit(batch.get())
	.then([batch = std::move(batch)] (int rc) mutable {
		return rc;
	});
}

folly::Future<int> AeroSpike::AeroDelCmdProcess(ActiveVmdk *vmdkp,
		CheckPointID ckpt,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		const std::string& set,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	if (pio_unlikely(process.empty())) {
		return 0;
	}

	return AeroDel(vmdkp, ckpt, process, failed, ns, set, aero_conn);
}

WriteRecord::WriteRecord(RequestBlock* blockp, WriteBatch* batchp,
	const std::string& ns, ActiveVmdk *vmdkp) :
	rq_block_(blockp), batchp_(batchp) {

	std::ostringstream os;
	if (ns == kAsNamespaceCacheClean) {

		/* Writes in Clean namespace is due to following */
		/* 1. Read Populate for aligned Read IOs, blockp will be set */
		/* 3. Read Populate for unaligned Read IOs, blockp will be set */
		/* 2. Read Populate for unaligned Write IOs, blockp would be set */
		/* 2. Moving data from DIRTY to CLEAN namespace: blockp would have setname set */

		/* Set the setname in reqblock where we want to write */
		std::string bset = blockp->GetSetName();
		log_assert(!bset.empty());
		std::string pset = vmdkp->GetParentDiskSet();
		if (pio_likely(bset != pset)) {
			os << (batchp->pre_keyp_) << ":"
			<< std::to_string(blockp->GetReadCheckPointId()) << ":" <<
			std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
		} else {
			auto pvmdkid = vmdkp->GetParentDiskVmdkId();
			os << pvmdkid << ":" << std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
		}
		setp_ = bset;
	} else {
		setp_ = batchp->setp_;
		os << (batchp->pre_keyp_) << ":"
		<< std::to_string(batchp->ckpt_) << ":"
		<< std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
	}

	key_val_ = os.str();
}

WriteRecord::~WriteRecord() {
	as_record_destroy(&record_);
	as_key_destroy(&key_);
}

WriteBatch::WriteBatch(const VmdkID& vmdkid, const std::string& ns, const std::string set)
		: pre_keyp_(vmdkid), ns_(ns), setp_(set) {
}

ReadRecord::ReadRecord(RequestBlock* blockp, ReadBatch* batchp, const std::string& ns,
		ActiveVmdk *vmdkp, const std::string& setp) :
		rq_block_(blockp), batchp_(batchp), setp_(setp) {
	std::ostringstream os;
	if (ns == kAsNamespaceCacheClean) {

		/* Read from Clean namespace can be due to following */
		/* 1. Cache read for aligned IO */
		/* 2. Cache read for unaligned Write IOs,
		 * blockp would be set approiately */

		/* Get the setname from where we want to write */
		std::string bset = blockp->GetSetName();
		log_assert(!bset.empty());
		std::string pset = vmdkp->GetParentDiskSet();
		if (pio_likely(bset != pset)) {
			os << (batchp->pre_keyp_) << ":"
			<< std::to_string(blockp->GetReadCheckPointId()) << ":" <<
			std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
		} else {
			auto pvmdkid = vmdkp->GetParentDiskVmdkId();
			os << pvmdkid << ":" << std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
		}
		setp_ = bset;
	} else {
		setp_ = batchp->setp_;
		os << (batchp->pre_keyp_) << ":"
		<< std::to_string(blockp->GetReadCheckPointId()) << ":" <<
		std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
	}

	key_val_ = os.str();
}

ReadBatch::ReadBatch(const VmdkID& vmdkid, const std::string& ns, const std::string set) :
		ns_(ns), pre_keyp_(vmdkid), setp_(set) {
}

ReadBatch::~ReadBatch() {
	as_batch_read_destroy(aero_recordsp_);
}

DelBatch::DelBatch(const VmdkID& vmdkid, const std::string& ns, const std::string& set)
		: pre_keyp_(vmdkid), ns_(ns), setp_(set) {
}

int DelBatch::Prepare(AeroSpikeConn* connp,
		const ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const BlockID start,
		const BlockID end) {
	const size_t ndeletes = end - start + 1;
	batch.recordsp_.reserve(ndeletes);

	for (auto it = start; it <= end; ++it) {
		std::ostringstream os;
		os << pre_keyp_ << ":"
			<< ckpt_id << ":"
			<< ((it << vmdkp->BlockShift()) >> kSectorShift);

		batch.recordsp_.emplace_back(this, ns_, setp_, os.str());
	}
	log_assert(ndeletes == batch.recordsp_.size());

	batch.ndeletes_ = ndeletes;
	promise_ = std::make_unique<folly::Promise<int>>();
	aero_conn_ = connp;
	ckpt_ = ckpt_id;
	return 0;
}

DelRecord::DelRecord(DelBatch* batchp, const std::string& ns,
			const std::string& set,
			std::string&& key) noexcept :
		batchp_(batchp), ns_(ns), set_(set),
		key_val_(std::forward<std::string>(key)) {
	[[maybe_unused]] auto kp = as_key_init(&key_, ns_.c_str(), set_.c_str(),
		key_val_.c_str());
	log_assert(kp == &key_);
}

DelRecord::~DelRecord() noexcept {
	as_key_destroy(&key_);
}

DelRecord::DelRecord(DelRecord&& rhs) noexcept :
		batchp_(rhs.batchp_), ns_(rhs.ns_), set_(rhs.set_),
		key_val_(std::move(rhs.key_val_)) {
	[[maybe_unused]] auto kp = as_key_init(&key_, ns_.c_str(), set_.c_str(),
		key_val_.c_str());
	log_assert(kp == &key_);
}

folly::Future<int> AeroSpike::Delete(AeroSpikeConn* connp,
		const ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const std::pair<::ondisk::BlockID, ::ondisk::BlockID> range,
		const std::string& ns,
		const std::string& set) {
	auto batch = std::make_unique<DelBatch>(vmdkp->GetID(), ns, set);
	auto rc = batch->Prepare(connp, vmdkp, ckpt_id, range.first, range.second);
	if (pio_unlikely(rc < 0)) {
		return rc;
	}

	auto batchp = batch.get();
	return DelBatchSubmit(batchp)
	.then([batch = std::move(batch)] (int rc) mutable {
		return rc;
	});
}

/* META KV related functions */
int AeroSpike::MetaWriteKeySet(WriteRecord* wrecp,
		const std::string& ns, const std::string& setp,
		const MetaDataKey& key, const std::string& value,
		const uint32_t& start_offset, const uint32_t& length,
		const bool add_bin_val) {

	log_assert(key.size() > 0);
	auto kp  = &wrecp->key_;
	auto kp1 = as_key_init(kp, ns.c_str(), setp.c_str(), wrecp->key_val_.c_str());
	log_assert(kp1 == kp);

	LOG(INFO) << __func__ << "Key ::" << wrecp->key_val_ << ", setp ::" << setp;
	if (kp && kp->valuep) {
		char *key_val_str = as_val_tostring(kp->valuep);
		LOG(INFO) << __func__ << "Stage11 : key is :" << key_val_str;
	}

	auto rp = &wrecp->record_;
	if (add_bin_val) {
		LOG(ERROR) << __func__ << "Value is:: "
			<< (const uint8_t *) value.c_str() + start_offset;
		as_record_init(rp, 2);
	} else {
		as_record_init(rp, 1);
	}

	auto s = as_record_set_raw(rp, kAsMetaBin.c_str(),
			((const uint8_t *) value.c_str()) + start_offset,
			length);
	if (pio_unlikely(s == false)) {
		LOG(ERROR) << __func__<< "Error in setting record property";
		return -EINVAL;
	}

	if (add_bin_val) {
		LOG(INFO) << __func__ << "Adding additional Bin record";
		auto s = as_record_set_raw(rp, kAsMetaBinExt.c_str(),
			((const uint8_t *) &add_bin_val), sizeof(add_bin_val));
		if (pio_unlikely(s == false)) {
			LOG(ERROR) << __func__<< "Error in setting record property";
			return -EINVAL;
		}
	}

	return 0;
}

uint64_t GetSize(char *res) {

	if (res == NULL || strlen(res) == 0 ) {
		return 0;
	}

	std::string temp = res;
	std::size_t first = temp.find_first_of("=");
	if (first == std::string::npos)
		return 0;

	std::size_t last = temp.find_first_of(";");
	if (last == std::string::npos)
		return 0;

	std::string strNew = temp.substr(first + 1, last - (first + 1));
	LOG(ERROR) << __func__ << "strNew:::-" << strNew.c_str();
	return stol(strNew);
}

folly::Future<int> AeroSpike::AeroMetaWrite(ActiveVmdk *vmdkp,
		const std::string& ns, const MetaDataKey& key,
		const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	/* Find that record can fit in one aerospike record entry or not */
	auto value_sz = value.size();
	auto count = value_sz / kAeroWriteBlockSize;
	if (value_sz % kAeroWriteBlockSize) {
		count++;
	}

	LOG(INFO) << __func__ << "count:" << count
		<< ", value.size:" << value_sz;

	std::string m_value;
	m_value.clear();
	if (pio_unlikely(count > 1)) {
		auto w_batch_rec = std::make_unique<WriteBatch>(vmdkp->GetID(),
			ns, kMetaSetName);
		if (pio_unlikely(w_batch_rec == nullptr)) {
			LOG(ERROR) << "WriteBatch allocation failed";
			return -ENOMEM;
		}

		w_batch_rec->aero_conn_ = aero_conn.get();
		log_assert(w_batch_rec->aero_conn_ != nullptr);
		w_batch_rec->batch.recordsp_.reserve(count);

		/* Input value is greater than Aero Write Block size */
		uint32_t start_offset = 0;
		uint32_t remaining_len = value_sz;

		/* format is "size=<value_sz>;map=<comma separate entries>" */
		m_value = "size=" + std::to_string(value_sz) + ";map=";
		std::ostringstream new_key;
		for (uint16_t i = 0; i < count; i++) {
			auto rec = std::make_unique<WriteRecord>();
			if (pio_unlikely(not rec)) {
				return -ENOMEM;
			}
			rec->batchp_ = w_batch_rec.get();
			auto len = remaining_len > kAeroWriteBlockSize ?
				kAeroWriteBlockSize : remaining_len;

			/* New key which represents the partial value worth
			 * AeroSpike Write Block size of data */

			new_key.str("");
			new_key.clear();
			new_key << key << ":" << std::to_string(i);
			rec->key_val_ = new_key.str();

			LOG(INFO) << __func__ << "Start offset:"
					<< start_offset <<", len:"
					<< len << ", key:" << new_key.str();
			MetaWriteKeySet(rec.get(), ns, kAsMetaSet, new_key.str().c_str(),
					value, start_offset, len, false);

			remaining_len -= len;
			start_offset += len;

			w_batch_rec->batch.recordsp_.emplace_back(std::move(rec));
			if (pio_unlikely(i)) {
				m_value += ",";
			}
			m_value += new_key.str();
		}

		for (auto& rec_val : w_batch_rec->batch.recordsp_) {
			auto kp  = &rec_val.get()->key_;
			if (kp && kp->valuep) {
				char *key_val_str = as_val_tostring(kp->valuep);
				LOG(INFO) << __func__ << "Stage33 : key is :"
					<< key_val_str << ", rec_val:"
					<< rec_val.get();
			}
		}

		w_batch_rec->batch.nwrites_ = count;
		w_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();
		return WriteBatchSubmit(w_batch_rec.get())
		.then([this, w_batch_rec = std::move(w_batch_rec),
			key, vmdkp, ns, aero_conn, m_value = std::move(m_value)] (int rc) mutable {
			if (rc) {
				LOG(ERROR) << __func__ << "Error";
			} else {
				LOG(INFO) << __func__ << "Success...";
			}

			if (w_batch_rec->failed_) {
				LOG(ERROR) << __func__ << "Error";
				return folly::makeFuture(-EIO);
			}

			/* Now write the final record */
			auto w_batch_rec = std::make_unique<WriteBatch>(vmdkp->GetID(),
					ns, kMetaSetName);
			if (pio_unlikely(w_batch_rec == nullptr)) {
				LOG(ERROR) << "WriteBatch allocation failed";
				return folly::makeFuture(-ENOMEM);
			}

			w_batch_rec->aero_conn_ = aero_conn.get();
			log_assert(w_batch_rec->aero_conn_ != nullptr);
			w_batch_rec->batch.recordsp_.reserve(1);
			auto rec = std::make_unique<WriteRecord>();
			if (pio_unlikely(not rec)) {
				return folly::makeFuture(-ENOMEM);
			}

			rec->batchp_ = w_batch_rec.get();
			rec->key_val_ = key;
			LOG(INFO) << __func__ << "Writting map: " << m_value.c_str();
			MetaWriteKeySet(rec.get(), ns, kAsMetaSet, key,
					m_value, 0, m_value.size(), true);
			w_batch_rec->batch.recordsp_.emplace_back(std::move(rec));
			w_batch_rec->batch.nwrites_ = 1;
			w_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();
			return WriteBatchSubmit(w_batch_rec.get())
			.then([w_batch_rec = std::move(w_batch_rec)] (int rc) mutable {
				return folly::makeFuture(rc);
			});
		});
	} else {

		LOG(INFO) << __func__ << "Next stage..";
		/* Now write the final record */
		auto w_batch_rec = std::make_unique<WriteBatch>(vmdkp->GetID(),
				ns, kMetaSetName);
		if (pio_unlikely(w_batch_rec == nullptr)) {
			LOG(ERROR) << "WriteBatch allocation failed";
			return -ENOMEM;
		}

		w_batch_rec->aero_conn_ = aero_conn.get();
		log_assert(w_batch_rec->aero_conn_ != nullptr);
		w_batch_rec->batch.recordsp_.reserve(1);
		auto rec = std::make_unique<WriteRecord>();
		if (pio_unlikely(not rec)) {
			return -ENOMEM;
		}

		rec->batchp_ = w_batch_rec.get();
		rec->key_val_ = key;
		LOG(INFO) << __func__ << "Writting usual record";
		MetaWriteKeySet(rec.get(), ns, kAsMetaSet, key,
					value, 0, value.size(), false);
		w_batch_rec->batch.recordsp_.emplace_back(std::move(rec));
		w_batch_rec->batch.nwrites_ = 1;
		w_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();
		return WriteBatchSubmit(w_batch_rec.get())
		.then([w_batch_rec = std::move(w_batch_rec)] (int rc) mutable {
			return rc;
		});
	}
}

folly::Future<int> AeroSpike::AeroMetaWriteCmd(ActiveVmdk *vmdkp,
		const MetaDataKey& key, const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn) {
	return AeroMetaWrite(vmdkp, kAsNamespaceCacheDirty, key,
				value, aero_conn);
}

int AeroSpike::MetaReadKeySet(ReadRecord* rrecp,
		const std::string& ns, const std::string& setp,
		const MetaDataKey& key, ReadBatch* r_batch_rec) {

	log_assert(key.size() > 0);
	auto recp = as_batch_read_reserve(r_batch_rec->aero_recordsp_);
	if (pio_unlikely(recp == nullptr)) {
		LOG(ERROR) << "ReadBatch allocation failed";
		return -EINVAL;
	}
	recp->read_all_bins = true;
	auto kp = as_key_init(&recp->key, ns.c_str(), setp.c_str(),
					rrecp->key_val_.c_str());
	if (pio_unlikely(kp == nullptr)) {
		return -ENOMEM;
	}
	log_assert(kp == &recp->key);

	rrecp->aero_recp_ = recp;
	return 0;
}

folly::Future<int> AeroSpike::AeroMetaRead(ActiveVmdk *vmdkp,
	const std::string& ns, const MetaDataKey& key,
	std::string& value,
	std::shared_ptr<AeroSpikeConn> aero_conn) {

	/* First read the Metadata portion to figure out how big is the
	 * record to read */

	auto r_batch_rec = std::make_unique<ReadBatch>(vmdkp->GetID(),
		ns, kMetaSetName);
	if (pio_unlikely(r_batch_rec == nullptr)) {
		LOG(ERROR) << "ReadBatch allocation failed";
		return -ENOMEM;
	}

	r_batch_rec->aero_conn_ = aero_conn.get();
	log_assert(r_batch_rec->aero_conn_ != nullptr);

	r_batch_rec->aero_recordsp_ = as_batch_read_create(1);
	if (pio_unlikely(r_batch_rec->aero_recordsp_ == nullptr)) {
		LOG(ERROR) <<__func__ << "ReadBatch create failed";
		return -ENOMEM;
	}

	r_batch_rec->recordsp_.reserve(1);
	auto rec = std::make_unique<ReadRecord>();
	if (pio_unlikely(not rec)) {
		LOG(ERROR) <<__func__ << "ReadRecord create failed";
		return -ENOMEM;
	}

	rec->key_val_ = key;
	LOG(ERROR) << __func__ << "Initial key is:" << key;
	rec->batchp_ = r_batch_rec.get();
	MetaReadKeySet(rec.get(), ns, kAsMetaSet, key, r_batch_rec.get());
	r_batch_rec->recordsp_.emplace_back(std::move(rec));
	r_batch_rec->nreads_ = 1;
	r_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();

	int rc = 0;
	ReadBatchSubmit(r_batch_rec.get()).wait();
	if (pio_unlikely(r_batch_rec->failed_)) {
		rc = -EIO;
		LOG(ERROR) <<__func__ << "read_batch_submit failed";
		return rc;
	}

	auto it = r_batch_rec->recordsp_.begin();
	as_batch_read_record *recp = it->get()->aero_recp_;
	auto read_more = false;
	char *destp = NULL;
	uint32_t size;
	as_bytes *bp = NULL;
	switch (recp->result) {
		default:
			rc = -EIO;
			break;
		case AEROSPIKE_OK:
			/* Check whether EXT bin is avaliable */
			bp = as_record_get_bytes(&recp->record,
					kAsMetaBinExt.c_str());
			if (pio_unlikely(bp != NULL)) {
				/* Extended Bin exist, implies that value
				 * is more than 1 record */
				LOG(INFO) << __func__
					<< "kAsMetaBinEx exists";
				read_more = true;
			} else {
				LOG(INFO) << __func__
					<< "kAsMetaBinEx Bin does not exists";
			}

			bp = as_record_get_bytes(&recp->record,
					kAsMetaBin.c_str());
			if (pio_unlikely(bp == NULL)) {
				LOG(ERROR) << __func__
					<< "Access error, unable to get data from given rec";
				return -ENOMEM;
			}

			size = as_bytes_size(bp);
			destp = new char[size + 1];
			memset(destp, 0, size + 1);
#if 0
			auto destp = NewRequestBuffer(size);
			if (pio_unlikely(not destp)) {
				return -ENOMEM;
			}
#endif
			if (as_bytes_copy(bp, 0, (uint8_t *) destp, (uint32_t) size) != size) {
				delete destp;
				LOG(ERROR) << __func__ << "Error in reading buffer";
				return -EIO;
			}

			LOG(INFO) << __func__ << "size is:" << GetSize(destp);
			if(pio_unlikely(read_more)) {
				LOG(INFO) << __func__ << "Read_more is set";
			}

			break;

		case AEROSPIKE_ERR_RECORD_NOT_FOUND:
			rc = -ENOENT;
			break;
	}

	if (pio_unlikely(rc || destp == NULL)) {
		LOG(ERROR) <<__func__ << "Read failed, rc:" << rc;
		return rc;
	}

	if (pio_likely(!read_more)) {
		/* Return the value from here */
		value = destp;
		return 0;
	}

	/* Format is size=<val>;map=<entries> */
	auto total_size = GetSize(destp);
	uint16_t count = total_size / kAeroWriteBlockSize;
	if (total_size % kAeroWriteBlockSize) {
		count++;
	}

	LOG(INFO) << __func__ << "Count value:" << count;
	if (pio_unlikely(!count)) {
		LOG(ERROR) << __func__ << "Count value should be non zero";
		log_assert(0);
		return -EIO;
	}

	/* Destp has the values which needs to be read */
	r_batch_rec = std::make_unique<ReadBatch>(vmdkp->GetID(),
		ns, kMetaSetName);
	if (pio_unlikely(r_batch_rec == nullptr)) {
		LOG(ERROR) << "ReadBatch allocation failed";
		return -ENOMEM;
	}

	r_batch_rec->aero_conn_ = aero_conn.get();
	log_assert(r_batch_rec->aero_conn_ != nullptr);
	r_batch_rec->aero_recordsp_ = as_batch_read_create(count);
	if (pio_unlikely(r_batch_rec->aero_recordsp_ == nullptr)) {
		LOG(ERROR) << "ReadBatch allocation failed";
		return -ENOMEM;
	}

	r_batch_rec->recordsp_.reserve(count);
	std::ostringstream new_key;
	for (uint16_t i = 0; i < count ; i++) {
		auto rec = std::make_unique<ReadRecord>();
		if (pio_unlikely(not rec)) {
			LOG(ERROR) << "ReadRecord allocation failed";
			return -ENOMEM;
		}

		/* Create Keys to Read */
		new_key.str("");
		new_key.clear();
		new_key << key << ":" << std::to_string(i);
		rec->key_val_ = new_key.str();
		LOG(INFO) << __func__ << "Addition key:" << new_key.str();
		rec->batchp_ = r_batch_rec.get();
		MetaReadKeySet(rec.get(), ns, kAsMetaSet, key, r_batch_rec.get());
		r_batch_rec->recordsp_.emplace_back(std::move(rec));
	}

	r_batch_rec->nreads_ = count;
	r_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();
	ReadBatchSubmit(r_batch_rec.get()).wait();
	rc = 0;
	if (pio_unlikely(r_batch_rec->failed_)) {
		rc = -EIO;
		LOG(ERROR) <<__func__ << "read_batch_submit failed";
		return rc;
	}

	if (pio_likely(destp)) {
		delete destp;
	}

	/* Create result buffer */
	destp = new char[total_size];
	if (pio_unlikely(not destp)) {
		return -ENOMEM;
	}

	auto start_offset = 0;
	size = 0;
	bp = NULL;
	rc = 0;
	for (auto& rec : r_batch_rec->recordsp_) {
		recp = rec->aero_recp_;
		switch (recp->result) {
			default:
				rc = -EIO;
				break;
			case AEROSPIKE_OK:
				bp = as_record_get_bytes(&recp->record,
						kAsMetaBin.c_str());
				if (pio_unlikely(bp == NULL)) {
					LOG(ERROR) << __func__
						<< "Access error, unable to get data from given rec";
					rc = -ENOMEM;
					break;
				}

				size = as_bytes_size(bp);
				LOG(INFO) << "start offset::" << start_offset << "size::" << size;
				if (as_bytes_copy(bp, 0, (uint8_t *) destp + start_offset,
						(uint32_t) size) != size) {
					rc = -EIO;
					break;
				}
				{
					int c = *(destp + start_offset);
					LOG(ERROR) << "Initial byte is::" << c;
				}
				start_offset += size;
				break;
			case AEROSPIKE_ERR_RECORD_NOT_FOUND:
				LOG(ERROR) << __func__ << "AEROSPIKE_ERR_RECORD_NOT_FOUND not found";
				rc = -ENOENT;
				break;
		}

		if (pio_unlikely(rc)) {
			break;
		}
	}

	if (pio_unlikely(rc)) {
		delete destp;
	} else {
		value = destp;
	}

	return rc;
}

int AeroSpike::AeroMetaReadCmd(ActiveVmdk *vmdkp,
		const MetaDataKey& key, std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn) {
	auto f = AeroMetaRead(vmdkp, kAsNamespaceCacheDirty, key,
				value, aero_conn);
	f.wait();
	return 0;
}


/*
 * This function is called from aerospike event loop threads -
 * outside of Fiber context.
 */

static void ReadListenerSingle(as_error *errp, as_record *record,
		void *datap, as_event_loop *) {
	auto rc = 0;
	auto batchp = reinterpret_cast<ReadSingle *>(datap);
	log_assert(batchp);
	batchp->as_result_ = AEROSPIKE_OK;
	if (pio_unlikely(errp)) {
		batchp->as_result_ = errp->code;
	}

	if (pio_unlikely(errp) && errp->code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		batchp->as_result_ = errp->code;
		LOG(ERROR) << __func__ << "error msg:" <<
			errp->message << ", err code:" << errp->code;
		rc = 1;
	} else if (!errp && record) {
		as_bytes *bp = as_record_get_bytes(record, kAsCacheBin.c_str());
		if (pio_unlikely(bp == NULL)) {
			LOG(ERROR) << __func__ << "Access error, unable to get data from given rec";
			batchp->as_result_ = AEROSPIKE_ERR_CLIENT_ABORT;
			rc = 1;
		} else {
			if(as_bytes_size(bp) != batchp->blk_size_) {
				LOG(ERROR) << __func__ << "Received less than block size data from cache";
				batchp->as_result_ = AEROSPIKE_ERR_CLIENT_ABORT;
				rc = 1;
			} else if(as_bytes_copy(bp, 0,
				(uint8_t *) batchp->destp_->Payload(), (uint32_t) batchp->blk_size_)
					!= batchp->blk_size_) {
				LOG(ERROR) << __func__ << "Copy error, got less than block size worth data";
				batchp->as_result_ = AEROSPIKE_ERR_CLIENT_ABORT;
				rc = 1;
			}
		}
	}

	batchp->promise_->setValue(rc);
}

int AeroSpike::ResetReadSingleState(ReadSingle *batchp) {
	batchp->failed_ = false;
	batchp->retry_ = false;
	batchp->as_result_ = AEROSPIKE_OK;
	batchp->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

folly::Future<int> AeroSpike::ReadSingleSubmit(ReadSingle *batchp) {

	as_event_loop    *lp = NULL;
	as_status        s;
	as_error         err;
	uint16_t         nreads;

	nreads       = batchp->nreads_;
	log_assert(batchp && batchp->failed_ == false);
	log_assert(nreads > 0);
	log_assert(nreads == 1);

	AeroSpikeConn::ConfigTag tag;
	batchp->aero_conn_->GetEventLoopAndTag(&lp, &tag);
	log_assert(lp != NULL);

	auto kp = &batchp->key_;
	s = aerospike_key_get_async(&batchp->aero_conn_->as_, &err,
		NULL, kp, ReadListenerSingle, (void *) batchp, lp, NULL);

	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "aerospike_batch_read_async request submit failed, code"
		<< err.code << "msg:" << err.message;
		batchp->as_result_ = s;
		batchp->failed_ = true;
		return -1;
	}

	return batchp->promise_->getFuture()
	.then([this, batchp, tag] (int) mutable {
		switch (batchp->as_result_) {
			/* Record not found is not an error case*/
			case AEROSPIKE_OK:
			case AEROSPIKE_ERR_RECORD_NOT_FOUND:
				batchp->failed_ = false;
				batchp->retry_ = false;
				break;
			case AEROSPIKE_ERR_TIMEOUT:
			case AEROSPIKE_ERR_DEVICE_OVERLOAD:
				batchp->aero_conn_->HandleServerOverload(tag);
				[[fallthrough]];
			case AEROSPIKE_ERR_RECORD_BUSY:
			case AEROSPIKE_ERR_SERVER:
			case AEROSPIKE_ERR_CLUSTER:
				LOG(ERROR) << __func__ << "retyring for failed Aerospike_batch_read_async"
					<<  ", err code:" << batchp->as_result_;
				batchp->retry_ = true;
				batchp->failed_ = true;
				break;
			default:
				batchp->failed_ = true;
				batchp->retry_ = false;
				break;
		}

		if (pio_unlikely(batchp->failed_ && batchp->retry_ && batchp->retry_cnt_)) {
			--batchp->retry_cnt_;
			this->ResetReadSingleState(batchp);
			auto schedulerp = batchp->aero_conn_->GetWorkScheduler();
			return schedulerp->ScheduleAsync(
					(kMaxRetryCnt - batchp->retry_cnt_) * 128ms,
					[this, batchp] () mutable {
				return this->ReadSingleSubmit(batchp);
			});
		}
		return folly::makeFuture(int(batchp->failed_));
	});
}

ReadSingle::ReadSingle(const VmdkID& vmdkid, const std::string& ns,
	const std::string set, const std::vector<RequestBlock*>& process, ActiveVmdk *vmdkp) :
	ns_(ns), pre_keyp_(vmdkid), setp_(set) {
	auto count = 0;
	for (auto blockp : process) {
		rq_block_ = blockp;
		count++;
	}
	log_assert(count == 1);

	blk_size_ = vmdkp->BlockSize();
	destp_= NewRequestBuffer(blk_size_);
	if (pio_unlikely(not destp_)) {
		throw std::runtime_error("Fatal error, Unable to allocate buffer");
	}
}

ReadSingle::~ReadSingle() {
	as_key_destroy(&key_);
}

folly::Future<int> AeroSpike::AeroSingleRead(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	auto batch = std::make_unique<ReadSingle>(vmdkp->GetID(),
		ns, vmdkp->GetVM()->GetJsonConfig()->GetTargetName(), process, vmdkp);
	if (pio_unlikely(batch == nullptr)) {
		LOG(ERROR) << "ReadSingle allocation failed";
		return -ENOMEM;
	}

	batch->aero_conn_ = aero_conn.get();
	log_assert(batch->aero_conn_ != nullptr);

	batch->nreads_ = 1;
	batch->promise_ = std::make_unique<folly::Promise<int>>();
	batch->ckpt_ = MetaData_constants::kInvalidCheckPointID() + 1;

	log_assert(batch->ckpt_ == 1);
	log_assert(batch->nreads_ == 1);
	log_assert(process.size() == 1);

	std::ostringstream os;
	if (ns == kAsNamespaceCacheClean) {
		os << (batch->pre_keyp_) << ":"
		<< std::to_string(batch->rq_block_->GetReadCheckPointId()) << ":"
		<< std::to_string(batch->rq_block_->GetAlignedOffset() >> kSectorShift);
	} else {
		os << (batch->pre_keyp_) << ":"
		<< std::to_string(batch->ckpt_) << ":"
		<< std::to_string(batch->rq_block_->GetAlignedOffset() >> kSectorShift);
	}

	auto key_val_ = os.str();
	//LOG(ERROR) << __func__ << "NS::" << ns.c_str() << "::Key val :: " << key_val_.c_str();
	auto kp = as_key_init(&batch->key_, ns.c_str(),
		batch->setp_.c_str(), key_val_.c_str());

	if (pio_unlikely(kp == nullptr)) {
		return -ENOMEM;
	}

	return ReadSingleSubmit(batch.get())
	.then([batch = std::move(batch), &failed]
			(int) mutable {
		auto rc = 0;
		if (pio_unlikely(batch->failed_)) {
			LOG(ERROR) <<__func__ << "read_batch_submit failed";
			rc = -EIO;
		} else {
			/* Set hit Status for all the request blocks those has been found in cache */
			auto blockp = batch->rq_block_;
			switch (batch->as_result_) {
				default:
					rc = -EIO;
					break;
				case AEROSPIKE_OK:
					{
					if (pio_unlikely(not batch->destp_)) {
						blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
						failed.emplace_back(blockp);
						rc = -ENOMEM;
						break;
					}

					blockp->PushRequestBuffer(std::move(batch->destp_));
					blockp->SetResult(0, RequestStatus::kHit);
					}
					break;

				case AEROSPIKE_ERR_RECORD_NOT_FOUND:
					break;
			}
		}
		return rc;
	});
}

}
