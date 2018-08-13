#include <cerrno>
#include <iterator>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "Vmdk.h"
#include "Request.h"
#include "AeroOps.h"
#include "ThreadPool.h"
#include <folly/fibers/Fiber.h>
#include <folly/fibers/Baton.h>
#include "DaemonTgtInterface.h"
#include "Singleton.h"
#include "MetaDataKV.h"
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>

#if 0
#define INJECT_AERO_WRITE_ERROR 0
#define INJECT_AERO_DEL_ERROR 0
#define INJECT_AERO_READ_ERROR 0
#endif

#define MAX_W_IOS_IN_HISTORY 100000
#define MAX_R_IOS_IN_HISTORY 100000

using namespace ::ondisk;

namespace pio {

AeroSpike::AeroSpike() {
	instance_ = SingletonHolder<AeroFiberThreads>::GetInstance();
}

AeroSpike::~AeroSpike() {}

void add_delay(uint64_t ms) {
	auto baton = std::make_unique<folly::fibers::Baton>();
	(baton.get())->try_wait_for(std::chrono::milliseconds(100));
	return;
}

int AeroSpike::CacheIoWriteKeySet(ActiveVmdk *vmdkp, WriteRecord* wrecp,
		Request *reqp, const std::string& ns,
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
	log_assert(srcp->Size() == vmdkp->BlockSize());

	auto s = as_record_set_raw(rp, kAsCacheBin.c_str(),
			(const uint8_t *)srcp->Payload(), srcp->Size());
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
		const std::vector<RequestBlock*>& process, Request *reqp,
		WriteBatch *w_batch_rec, const std::string& ns) {
	WriteBatchInit(vmdkp, process, w_batch_rec, ns);
	for (auto& v_record : w_batch_rec->batch.recordsp_) {
		auto record  = v_record.get();
		auto rc = CacheIoWriteKeySet(vmdkp, record, reqp, ns,
					record->setp_);
		if (pio_unlikely(rc < 0)) {
			return rc;
		}

		/*
		 * Set TTL -1 for DIRTY namespace writes, for CLEAN
		 * namespace it should be inherited from global
		 * aerospike conf file
		 */

		if (ns == kAsNamespaceCacheDirty || 1) {
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

static void WriteListener(as_error *errp, void *datap, as_event_loop* loopp) {
	auto wrp = (WriteRecord *) reinterpret_cast<WriteRecord*>(datap);
	log_assert(wrp && wrp->batchp_);
	auto batchp = (wrp->batchp_);

	wrp->status_ = AEROSPIKE_OK;
	if (pio_unlikely(errp)) {
		wrp->status_ = errp->code;
		as_monitor_notify(&batchp->aero_conn_->as_mon_);
		LOG(ERROR) << __func__ << "::error msg::"
			<< errp->message << "::err code::" << errp->code;
	}

#ifdef INJECT_AERO_WRITE_ERROR
	/* if not already failed */
	if (!wrp->status_) {
		if (batchp->retry_cnt_ > 2) {
			wrp->status_ = AEROSPIKE_ERR_TIMEOUT;
			LOG(ERROR) << __func__ << "::Injecting AEROSPIKE_ERR_TIMEOUT error, retry_cnt :: " << batchp->retry_cnt_;
			as_monitor_notify(&batchp->aero_conn_->as_mon_);
		} else if (batchp->retry_cnt_ == 2) {
			wrp->status_ = AEROSPIKE_ERR_CLUSTER;
			LOG(ERROR) << __func__ << "::Injecting AEROSPIKE_ERR_CLUSTER error, retry_cnt :: " << batchp->retry_cnt_;
			as_monitor_notify(&batchp->aero_conn_->as_mon_);
		}
	}
#endif

	auto complete = false;
	std::unique_lock<std::mutex> b_lock(batchp->batch.lock_);
	batchp->batch.ncomplete_++;
	if (batchp->submitted_ == true &&
			batchp->batch.ncomplete_ == batchp->batch.nsent_) {
		complete = true;
	}
	b_lock.unlock();

	if (complete == true) {
		batchp->promise_->setValue(0);
	}

	return;
}

static void WritePipeListener(void *udatap, as_event_loop *lp) {
	auto wrp = reinterpret_cast<WriteRecord *>(udatap);
	log_assert(wrp && wrp->batchp_ && wrp->batchp_->req_);

	auto batchp = wrp->batchp_;
	std::unique_lock<std::mutex> b_lock(batchp->batch.lock_);
	if (batchp->batch.nsent_ >= batchp->batch.nwrites_) {
		LOG(ERROR) << __func__ << "nsent::" << batchp->batch.nsent_
			<< "nwrites::" << batchp->batch.nwrites_;
		log_assert(batchp->batch.nsent_ < batchp->batch.nwrites_);
	}

	log_assert(batchp->submitted_ == false);
	batchp->batch.nsent_++;

	wrp = batchp->batch.rec_it_->get();
	std::advance(batchp->batch.rec_it_, 1);
	as_pipe_listener fnp = nullptr;
	if (batchp->batch.rec_it_ == batchp->batch.recordsp_.end()) {
		/* complete batch is submitted */
		batchp->submitted_ = true;
	} else {
		fnp = WritePipeListener;
	}
	b_lock.unlock();

	auto kp = &wrp->key_;
	auto rp = &wrp->record_;
	auto complete = false;
	as_error err;
	auto s = aerospike_key_put_async(&batchp->aero_conn_->as_, &err,
			NULL, kp, rp, WriteListener, (void *) wrp, lp, fnp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::request submit failed, code"
			<< err.code << "msg:" << err.message;

		/*
		 * Error in submitting no more keys from this batch will be put --
		 * mark batch submitted
		 */

		std::unique_lock<std::mutex> b_lock(batchp->batch.lock_);
		batchp->submitted_ = true;
		batchp->batch.nsent_--;
		if (batchp->batch.nsent_ == batchp->batch.ncomplete_) {

			/*
			 * Sending new put request failed and all sent requests
			 * have been responded
			 */

			complete = true;
		}

		wrp->status_ = s;
		batchp->failed_ = true;
		b_lock.unlock();
	}

	if (complete == true) {
		log_assert(batchp->submitted_ == true);
		batchp->promise_->setValue(0);
	}
}

int AeroSpike::ResetWriteBatchState(WriteBatch *batchp, uint16_t nwrites) {
	batchp->batch.nwrites_ = nwrites;
	batchp->submitted_ = false;
	batchp->failed_ = false;
	batchp->retry_ = false;
	batchp->batch.nsent_ = 0;
	batchp->batch.ncomplete_ = 0;
	batchp->promise_ = nullptr;
	batchp->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

int AeroSpike::ResetDelBatchState(DelBatch *batchp, uint16_t ndeletes) {
	batchp->batch.ndeletes_ = ndeletes;
	batchp->submitted_ = false;
	batchp->failed_ = false;
	batchp->retry_ = false;
	batchp->batch.nsent_ = 0;
	batchp->batch.ncomplete_ = 0;
	batchp->promise_ = nullptr;
	batchp->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

int AeroSpike::ResetReadBatchState(ReadBatch *batchp, uint16_t nreads) {
	batchp->nreads_ = nreads;
	batchp->failed_ = false;
	batchp->retry_ = false;
	batchp->as_result_ = AEROSPIKE_OK;
	batchp->ncomplete_ = 0;
	batchp->promise_ = nullptr;
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
	log_assert(nwrites > 0 && batchp->req_);
	if (pio_unlikely(batchp->submitted_ != false)) {
		LOG(ERROR) << "Failed, batchp->submitted : " << batchp->submitted_;
		log_assert(0);
		return -1;
	}

	/* Get very first record from vector to start */
	batchp->batch.rec_it_ = (batchp->batch.recordsp_).begin();
	wrp = batchp->batch.rec_it_->get();
	std::advance(batchp->batch.rec_it_, 1);

	kp  = &wrp->key_;
	rp  = &wrp->record_;
	if (nwrites == 1) {
		batchp->submitted_ = true;
		fnp = NULL;
	} else {
		batchp->submitted_ = false;
		fnp = WritePipeListener;
	}

	lp = as_event_loop_get();
	log_assert(lp != NULL);

	batchp->q_lock_.lock();
	batchp->batch.nsent_ = 1;
	s = aerospike_key_put_async(&batchp->aero_conn_->as_,
			&err, NULL, kp, rp,
			WriteListener, (void *) wrp, lp, fnp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::request submit failed, err code:-"
			<< err.code << "msg:" << err.message;

		batchp->batch.nsent_--;
		wrp->status_ = s;
		batchp->failed_ = true;
		batchp->q_lock_.unlock();
		return -1;
	}

	batchp->q_lock_.unlock();
	return batchp->promise_->getFuture()
	.then([this, batchp, lp] (int rc) mutable {
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
					#if 0
					LOG(ERROR) << __func__ << rec->status_
						<< "::failed write request::"
						<< batchp->retry_cnt_;
					#endif
					batchp->failed_ = true;
					break;
				case AEROSPIKE_ERR_TIMEOUT:
				case AEROSPIKE_ERR_RECORD_BUSY:
				case AEROSPIKE_ERR_DEVICE_OVERLOAD:
				case AEROSPIKE_ERR_CLUSTER:
				case AEROSPIKE_ERR_SERVER:
					#if 0
					LOG(ERROR) << __func__ << rec->status_
						<< "::Retyring for failed write request"
						<< ", retry cnt ::-"
						<< batchp->retry_cnt_;
					#endif
					batchp->failed_ = true;
					batchp->retry_ = true;
					break;

				case AEROSPIKE_OK:
					break;
			}
		}

		return folly::makeFuture(0);
	});
}

folly::Future<int> AeroSpike::AeroWrite(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	auto w_batch_rec = std::make_unique<WriteBatch>(reqp, vmdkp->GetID(), ns,
		vmdkp->GetVM()->GetJsonConfig()->GetTargetName());
	if (pio_unlikely(w_batch_rec == nullptr)) {
		LOG(ERROR) << "WriteBatch allocation failed";
		return -ENOMEM;
	}

	w_batch_rec->batch.nwrites_ = process.size();
	w_batch_rec->ckpt_ = ckpt;
	w_batch_rec->aero_conn_ = aero_conn.get();
	log_assert(w_batch_rec->aero_conn_ != nullptr);

	auto rc = WriteBatchPrepare(vmdkp, process, reqp, w_batch_rec.get(), ns);
	if (pio_unlikely(rc < 0)) {
		return rc;
	}

	folly::Promise<int> promise;
	auto fut = promise.getFuture();
	instance_->threadpool_.pool_->AddTask([this,
			vmdkp, reqp, &process, &failed,
			w_batch_rec = std::move(w_batch_rec),
			promise = std::move(promise)] () mutable {
		uint16_t nwrites = w_batch_rec->batch.nwrites_;
		long long duration;
		while(true) {
			auto start_time = std::chrono::high_resolution_clock::now();
			WriteBatchSubmit(w_batch_rec.get()).wait();
			auto end_time = std::chrono::high_resolution_clock::now();
			/* Error case and retyr_cnt is still non zero */
			if (w_batch_rec->retry_ && w_batch_rec->retry_cnt_) {
				--w_batch_rec->retry_cnt_;
				ResetWriteBatchState(w_batch_rec.get(), nwrites);

				/* Wait for 100ms before retry */
				add_delay(100);
			} else {
				duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
				break;
			}
		}

		std::unique_lock<std::mutex> w_lock(vmdkp->w_aero_stat_lock_);
		if (pio_unlikely(!w_batch_rec->failed_)) {
			if ((vmdkp->w_aero_total_latency_ + duration) < vmdkp->w_aero_total_latency_ ||
					vmdkp->w_aero_io_blks_count_ > MAX_W_IOS_IN_HISTORY) {
					vmdkp->w_aero_total_latency_ = 0;
					vmdkp->w_aero_io_blks_count_ = 0;
			} else {
				vmdkp->w_aero_total_latency_ += duration;
				vmdkp->w_aero_io_blks_count_ += nwrites;
			}
		}
		if (vmdkp->w_aero_io_blks_count_ && (vmdkp->w_aero_io_blks_count_ % 5) == 0) {
			LOG(ERROR) << __func__ <<
				"[AeroWrite:VmdkID:" << vmdkp->GetID() <<
				", Total latency :" << vmdkp->w_aero_total_latency_ <<
				", Total blks IO count (in blk size):" << vmdkp->w_aero_io_blks_count_ <<
				", avg blk access latency:" << vmdkp->w_aero_total_latency_ / vmdkp->w_aero_io_blks_count_;
		}
		w_lock.unlock();
		promise.setValue(0);
	});
	return fut;
}

folly::Future<int> AeroSpike::AeroWriteCmdProcess(ActiveVmdk *vmdkp,
		Request *reqp, CheckPointID ckpt,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	if (pio_unlikely(process.empty())) {
		return 0;
	}

	failed.clear();
	return AeroWrite(vmdkp, reqp, ckpt, process, failed, ns, aero_conn);
}

int AeroSpike::CacheIoReadKeySet(ActiveVmdk *vmdkp, ReadRecord* rrecp,
		Request *reqp, const std::string& ns, ReadBatch* r_batch_rec) {

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
		Request *reqp, const std::string& ns) {
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
				ns, vmdkp, r_batch_rec->setp_);
		if (pio_unlikely(not rec)) {
			LOG(ERROR) << "ReadRecord allocation failed";
			return -ENOMEM;
		}

		auto rc = CacheIoReadKeySet(vmdkp, rec.get(), reqp, ns, r_batch_rec);
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
		const std::vector<RequestBlock*>& process, Request *reqp,
		ReadBatch *r_batch_rec, const std::string& ns) {
	ReadBatchInit(vmdkp, process, r_batch_rec, reqp, ns);
	return 0;
}

/*
 * This function is called from aerospike event loop threads -
 * outside of Fiber context.
 */
static void ReadListener(as_error *errp, as_batch_read_records *records,
		void *datap, as_event_loop *lp) {
	auto rc = 0;
	auto batchp = reinterpret_cast<ReadBatch *>(datap);
	log_assert(batchp);
	batchp->as_result_ = AEROSPIKE_OK;
	if (pio_unlikely(errp)) {
		batchp->as_result_ = errp->code;
		#if 0
		LOG(ERROR) << __func__ << "error msg:-" <<
			errp->message << "err code:-" << errp->code;
		#endif
		rc = 1;
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
	log_assert(nreads > 0 && batchp->req_);

	loopp = as_event_loop_get();
	log_assert(loopp != NULL);

	batchp->q_lock_.lock();
	s = aerospike_batch_read_async(&batchp->aero_conn_->as_, &err, NULL, batchp->aero_recordsp_,
			ReadListener, batchp, loopp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::aerospike_batch_read_async request submit failed, code"
			<< err.code << ", msg::" << err.message;
		batchp->q_lock_.unlock();
		batchp->as_result_ = s;
		batchp->failed_ = true;
		return -1;
	}

	batchp->q_lock_.unlock();
	return batchp->promise_->getFuture()
	.then([this, batchp, loopp] (int rc) mutable {
		as_monitor_notify(&batchp->aero_conn_->as_mon_);
		switch (batchp->as_result_) {
			case AEROSPIKE_OK:
			case AEROSPIKE_ERR_RECORD_NOT_FOUND:
				break;

			case AEROSPIKE_ERR_TIMEOUT:
			case AEROSPIKE_ERR_RECORD_BUSY:
			case AEROSPIKE_ERR_DEVICE_OVERLOAD:
			case AEROSPIKE_ERR_SERVER:
			case AEROSPIKE_ERR_CLUSTER:
				#if 0
				LOG(ERROR) << __func__ << "retyring for failed Aerospike_batch_read_async"
					<<  ", err code::-" << batchp->as_result_;
				#endif
				batchp->retry_ = true;
				batchp->failed_ = true;
				break;
			default:
				batchp->failed_ = true;
				break;
		}

		as_batch_read_record *recp;
		for (auto& v_rec : batchp->recordsp_) {
			auto rec =  v_rec.get();
			recp = rec->aero_recp_;
			rec->status_ = recp->result;
		}
		return folly::makeFuture(0);
	});
}

folly::Future<int> AeroSpike::AeroRead(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	auto r_batch_rec = std::make_unique<ReadBatch>(reqp, vmdkp->GetID(), ns,
			vmdkp->GetVM()->GetJsonConfig()->GetTargetName());
	log_assert(r_batch_rec != nullptr);

	r_batch_rec->aero_conn_ = aero_conn.get();
	log_assert(r_batch_rec->aero_conn_ != nullptr);

	auto rc = ReadBatchPrepare(vmdkp, process, reqp, r_batch_rec.get(), ns);
	if (pio_unlikely(rc < 0)) {
		LOG(ERROR) <<__func__ << "::read_batch_prepare failed";
		return rc;
	}

	folly::Promise<int> promise;
	auto fut = promise.getFuture();
	instance_->threadpool_.pool_->AddTask([this, vmdkp, &failed,
			r_batch_rec = std::move(r_batch_rec),
			promise = std::move(promise)] () mutable {
		auto batchp = r_batch_rec.get();
		const auto nreads = batchp->nreads_;
		long long duration;
		while (true) {
			auto start_time = std::chrono::high_resolution_clock::now();
			ReadBatchSubmit(batchp).wait();
			auto end_time = std::chrono::high_resolution_clock::now();
			/* Error case and retyr_cnt is still non zero */
			if (batchp->retry_ && batchp->retry_cnt_) {
				--batchp->retry_cnt_;
				ResetReadBatchState(batchp, nreads);
				/* Wait for 100ms before retry */
				add_delay(100);
			} else {
				duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
				break;
			}
		}

		std::unique_lock<std::mutex> r_lock(vmdkp->r_aero_stat_lock_);
		if (pio_unlikely(!batchp->failed_)) {
			if ((vmdkp->r_aero_total_latency_ + duration) < vmdkp->r_aero_total_latency_ ||
					vmdkp->r_aero_io_blks_count_ > MAX_W_IOS_IN_HISTORY) {
					vmdkp->r_aero_total_latency_ = 0;
					vmdkp->r_aero_io_blks_count_ = 0;
			} else {
				vmdkp->r_aero_total_latency_ += duration;
				vmdkp->r_aero_io_blks_count_ += nreads;
			}

			if (vmdkp->r_aero_io_blks_count_ && (vmdkp->r_aero_io_blks_count_ % 100) == 0) {
				LOG(ERROR) << __func__ <<
					"[AeroRead:VmdkID:" << vmdkp->GetID() <<
					", Total latency :" << vmdkp->r_aero_total_latency_ <<
					", Total blks IO count (in blk size):" << vmdkp->r_aero_io_blks_count_ <<
					", avg blk access latency:" << vmdkp->r_aero_total_latency_ / vmdkp->r_aero_io_blks_count_;
			}
		}
		r_lock.unlock();

		auto rc = 0;
		if (pio_unlikely(batchp->failed_)) {
			LOG(ERROR) <<__func__ << "read_batch_submit failed";
			rc = -EIO;
		} else {

			/* Now create process and failed list. If rc is non zero
			 * then move in failed list */

			as_batch_read_record *recp;
			for (auto& rec : r_batch_rec->recordsp_) {
				auto blockp = rec->rq_block_;
				recp = rec->aero_recp_;
				switch (recp->result) {
					default:
						rc = -EIO;
						break;
					case AEROSPIKE_OK:
						{
						auto destp = NewRequestBuffer(vmdkp->BlockSize());
						if (pio_unlikely(not destp)) {
							blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
							failed.emplace_back(blockp);
							rc = -ENOMEM;
							break;
						}

						as_bytes *bp = as_record_get_bytes(&recp->record, kAsCacheBin.c_str());
						if (pio_unlikely(bp == NULL)) {
							LOG(ERROR) << __func__ << "Access error, unable to get data from given rec";
							blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
							failed.emplace_back(blockp);
							rc = -ENOMEM;
							break;
						}

						log_assert(as_bytes_size(bp) == vmdkp->BlockSize());
						log_assert(as_bytes_copy(bp, 0,
							(uint8_t *) destp->Payload(), (uint32_t) vmdkp->BlockSize())
								== vmdkp->BlockSize());

						blockp->PushRequestBuffer(std::move(destp));
						blockp->SetResult(0, RequestStatus::kHit);
						}
						break;

					case AEROSPIKE_ERR_RECORD_NOT_FOUND:
						break;
				}

				/* If any of the RequestBlocks failed then retutn from here*/
				if (rc) {
					break;
				}
			}
		}
		promise.setValue(rc);
	});
	return fut;
}

folly::Future<int> AeroSpike::AeroReadCmdProcess(ActiveVmdk *vmdkp,
		Request *reqp, const std::vector<RequestBlock*>& process,
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

	return AeroRead(vmdkp, reqp, process, failed, ns, aero_conn);
}

int AeroSpike::CacheIoDelKeySet(ActiveVmdk *vmdkp, DelRecord* drecp,
	Request *reqp, const std::string& ns, const std::string& setp) {

	auto kp = as_key_init(&drecp->key_, ns.c_str(), setp.c_str(),
		drecp->key_val_.c_str());
	log_assert(kp == &drecp->key_);
	return 0;
}

int AeroSpike::DelBatchInit(ActiveVmdk *vmdkp,
	const std::vector<RequestBlock*>& process, DelBatch *d_batch_rec,
	const std::string& ns) {

	d_batch_rec->batch.recordsp_.reserve(process.size());
	for (auto block : process) {
		auto rec = std::make_unique<DelRecord>(block, d_batch_rec);
		if (pio_unlikely(not rec)) {
			LOG(ERROR) << "DelRecord allocation failed";
			return -ENOMEM;
		}

		d_batch_rec->batch.recordsp_.emplace_back(std::move(rec));
	}
	d_batch_rec->batch.ndeletes_ = process.size();
	d_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();
	return 0;
}

int AeroSpike::DelBatchPrepare(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		Request *reqp, DelBatch *d_batch_rec, const std::string& ns) {
	auto rc = DelBatchInit(vmdkp, process, d_batch_rec, ns);
	if (pio_unlikely(rc < 0)) {
		return rc;
	}
	for (auto& record : d_batch_rec->batch.recordsp_) {
		auto rc = CacheIoDelKeySet(vmdkp, record.get(), reqp, ns,
				d_batch_rec->setp_);
		if (pio_unlikely(rc < 0)) {
			return rc;
		}
	}
	return 0;
}

/*
 * This function is called from aerospike event loop threads - this is outside
 * of Fiber context.
 * */

static void DelListener(as_error *errp, void *datap, as_event_loop* loopp) {
	auto drp = reinterpret_cast<DelRecord *>(datap);
	log_assert(drp && drp->batchp_);
	auto batchp = drp->batchp_;
	drp->status_ = AEROSPIKE_OK;
	if (pio_unlikely(errp)) {
		drp->status_ = errp->code;
		as_monitor_notify(&batchp->aero_conn_->as_mon_);
		#if 0
		LOG(ERROR) << __func__ << "::error msg:-" <<
			errp->message << ",err code:-" << errp->code;
		#endif
	}

#ifdef INJECT_AERO_DEL_ERROR
	/* if not already failed */
	if (!drp->status_) {
		if (batchp->retry_cnt_ > 2) {
			drp->status_ = AEROSPIKE_ERR_TIMEOUT;
			as_monitor_notify(&batchp->aero_conn_->as_mon_);
			LOG(ERROR) << __func__ << "Injecting AEROSPIKE_ERR_TIMEOUT error, retry_cnt ::" << batchp->retry_cnt_;
		} else if (batchp->retry_cnt_ == 2) {
			drp->status_ = AEROSPIKE_ERR_CLUSTER;
			as_monitor_notify(&batchp->aero_conn_->as_mon_);
			LOG(ERROR) << __func__ << "Injecting AEROSPIKE_ERR_CLUSTER error, retry_cnt ::" << batchp->retry_cnt_;
		}
	}
#endif
	auto complete = [&] () mutable {
		std::lock_guard<std::mutex> lock(batchp->batch.lock_);
		batchp->batch.ncomplete_++;
		return batchp->submitted_ and batchp->batch.ncomplete_
						== batchp->batch.nsent_;
	} ();

	if (complete == true) {
		batchp->promise_->setValue(0);
	}
}

static void DelPipeListener(void *udatap, as_event_loop *lp) {

	DelRecord *drp = (DelRecord *) udatap;
	log_assert(drp && drp->batchp_ && drp->batchp_->req_);

	DelBatch *batchp = (drp->batchp_);
	as_pipe_listener fnp = DelPipeListener;

	std::unique_lock<std::mutex> b_lock(batchp->batch.lock_);
	log_assert(batchp->batch.nsent_ < batchp->batch.ndeletes_);
	log_assert(batchp->submitted_ == false);
	batchp->batch.nsent_++;

	drp = (*(batchp->batch.rec_it_)).get();
	std::advance(batchp->batch.rec_it_, 1);
	if (batchp->batch.rec_it_ == (batchp->batch.recordsp_).end()) {
		/* complete batch is submitted */
		batchp->submitted_ = true;
		fnp                = NULL;
	}

	b_lock.unlock();
	as_key *kp  = &drp->key_;
	bool complete = false;

	as_error  err;
	as_status s;
	s = aerospike_key_remove_async(&batchp->aero_conn_->as_, &err, NULL, kp,
			DelListener, (void *) drp, lp, fnp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {

		/* Sending failed */
		LOG(ERROR) << __func__ << "::Submit failed, err code::"
			<< err.code << "err msg::" << err.message;

		std::unique_lock<std::mutex> b_lock(batchp->batch.lock_);

		/* No more keys from this batch will be removed --
		 * mark batch submitted
		 */

		batchp->submitted_ = true;
		batchp->batch.nsent_--;
		if (batchp->batch.nsent_ == batchp->batch.ncomplete_) {
			/* All sent requests has been responded */
			complete = true;
		}

		drp->status_ = s;
		batchp->failed_ = true;
		b_lock.unlock();
	}

	if (complete == true) {
		log_assert(batchp->submitted_ == true);
		batchp->promise_->setValue(0);
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
	log_assert(ndeletes > 0 && batchp->req_);

	if (pio_unlikely(batchp->submitted_ != false)) {
		LOG(ERROR) << "Failed, batchp->submitted : " << batchp->submitted_;
		log_assert(0);
		return -1;
	}

	/* Get very first record from vector to process */
	batchp->batch.rec_it_ = (batchp->batch.recordsp_).begin();
	drp = (*(batchp->batch.rec_it_)).get();
	std::advance(batchp->batch.rec_it_, 1);

	kp  = &drp->key_;
	if (ndeletes == 1) {
		batchp->submitted_ = true;
		batchp->batch.nsent_     = 1;
		fnp = NULL;
	} else {
		batchp->submitted_ = false;
		batchp->batch.nsent_     = 1;
		fnp = DelPipeListener;
	}

	loopp = as_event_loop_get();
	log_assert(loopp != NULL);

	batchp->q_lock_.lock();
	s = aerospike_key_remove_async(&batchp->aero_conn_->as_, &err, NULL, kp,
			DelListener, (void *) drp, loopp, fnp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::aerospike_key_remove_async"
			<< " request submit failed, code"
			<< err.code << "msg:" << err.message;

		batchp->q_lock_.unlock();
		drp->status_ = s;
		batchp->failed_ = true;
		return -1;
	}

	batchp->q_lock_.unlock();
	return batchp->promise_->getFuture()
	.then([this, batchp, ndeletes, loopp] (int rc) mutable {
		log_assert(batchp->submitted_ == true);
		batchp->submitted_ = 0;

		/*
		 * Check if any of the Request has failed because of
		 * AERO TIMOUT or OVERLOAD error. We are retying at
		 * Request level and so all the RequestBlocks would
		 * be retried again. TBD: Improve this behaviour,
		 * retry only failed request blocks
		 */

		for (auto& v_rec : batchp->batch.recordsp_) {
			auto rec =  v_rec.get();
			switch (rec->status_) {
				default:
					batchp->failed_ = true;
					break;
				case AEROSPIKE_ERR_TIMEOUT:
				case AEROSPIKE_ERR_RECORD_BUSY:
				case AEROSPIKE_ERR_DEVICE_OVERLOAD:
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
		}

		return folly::makeFuture(0);
	});
}

folly::Future<int> AeroSpike::AeroDel(ActiveVmdk *vmdkp, Request *reqp, CheckPointID ckpt,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	/* Create Batch write records */
	auto d_batch_rec = std::make_unique<DelBatch>(reqp, vmdkp->GetID(),
			ns, (vmdkp->GetVM()->GetJsonConfig())->GetTargetName());
	if (pio_unlikely(d_batch_rec == nullptr)) {
		LOG(ERROR) << "DelBatch allocation failed";
		return -ENOMEM;
	}

	d_batch_rec->batch.ndeletes_ = process.size();
	d_batch_rec->ckpt_ = ckpt;
	d_batch_rec->aero_conn_ = aero_conn.get();
	log_assert(d_batch_rec->aero_conn_ != nullptr);
	auto rc = DelBatchPrepare(vmdkp, process, reqp, d_batch_rec.get(), ns);
	if (pio_unlikely(rc < 0)) {
		return rc;
	}

	folly::Promise<int> promise;
	auto fut = promise.getFuture();
	instance_->threadpool_.pool_->AddTask([this,
			vmdkp, reqp, &process, &failed,
			d_batch_rec = std::move(d_batch_rec),
			promise = std::move(promise)] () mutable {
		uint16_t ndeletes = d_batch_rec->batch.ndeletes_;
		while(true) {
			DelBatchSubmit(d_batch_rec.get()).wait();
			/* Error case and retyr_cnt is still non zero */
			if (d_batch_rec->retry_ && d_batch_rec->retry_cnt_) {
				--d_batch_rec->retry_cnt_;
				ResetDelBatchState(d_batch_rec.get(), ndeletes);
				/* Wait for 100ms before retry */
				add_delay(100);
			} else {
				break;
			}
		}
		promise.setValue(0);
	});
	return fut;
}

folly::Future<int> AeroSpike::AeroDelCmdProcess(ActiveVmdk *vmdkp,
		Request *reqp, CheckPointID ckpt,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	if (pio_unlikely(process.empty())) {
		return 0;
	}

	return AeroDel(vmdkp, reqp, ckpt, process, failed, ns, aero_conn);
}

WriteRecord::WriteRecord(RequestBlock* blockp, WriteBatch* batchp, const std::string& ns, ActiveVmdk *vmdkp) :
	rq_block_(blockp), batchp_(batchp), setp_(batchp->setp_) {
	std::ostringstream os;

	if (ns == kAsNamespaceCacheClean) {
		os << (batchp->pre_keyp_) << ":"
		<< std::to_string(blockp->GetReadCheckPointId()) << ":"
		<< std::to_string(blockp->GetAlignedOffset() >> kSectorShift);

		/* Set the setname from where we want to write */
		if (blockp->GetReadCheckPointId() == MetaData_constants::kInvalidCheckPointID() + 1){
			std::string parent_disk = vmdkp->GetJsonConfig()->GetParentDisk();
			if (parent_disk.size()) {
				setp_ = parent_disk;
				os.str("");
				os.clear();
				os << std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
			}
		}
	} else {
		os << (batchp->pre_keyp_) << ":"
		<< std::to_string(batchp->ckpt_) << ":"
		<< std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
	}

	key_val_ = os.str();
//	LOG(ERROR) << __func__ << "::Key is::" << key_val_;
}

WriteRecord::~WriteRecord() {
	as_key_destroy(&key_);
}

WriteBatch::WriteBatch(Request* reqp, const VmdkID& vmdkid,
		const std::string& ns, const std::string set)
		: req_(reqp), pre_keyp_(vmdkid), ns_(ns), setp_(set) {
}

ReadRecord::ReadRecord(RequestBlock* blockp, ReadBatch* batchp, const std::string& ns,
		ActiveVmdk *vmdkp, const std::string& setp) :
		rq_block_(blockp), batchp_(batchp), setp_(setp) {
	std::ostringstream os;
	os << batchp->pre_keyp_ << ":"
		<< std::to_string(blockp->GetReadCheckPointId()) << ":"
		<< std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
	if (ns == kAsNamespaceCacheClean) {
		if (blockp->GetReadCheckPointId() ==
			MetaData_constants::kInvalidCheckPointID() + 1) {
			std::string parent_disk;
			parent_disk = vmdkp->GetJsonConfig()->GetParentDisk();
			if (parent_disk.size()) {
				setp_ = parent_disk;
				os.str("");
				os.clear();
				os << std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
			}
		}
	}
	key_val_ = os.str();
}

ReadBatch::ReadBatch(Request* reqp, const VmdkID& vmdkid, const std::string& ns,
		const std::string set) :
		req_(reqp), ns_(ns), pre_keyp_(vmdkid), setp_(set) {
}

ReadBatch::~ReadBatch() {
	as_batch_read_destroy(aero_recordsp_);
}

DelBatch::DelBatch(Request* reqp, const VmdkID& vmdkid, const std::string& ns,
		const std::string set) : req_(reqp), pre_keyp_(vmdkid),
		ns_(ns), setp_(set) {
}

DelRecord::DelRecord(RequestBlock* blockp, DelBatch* batchp) :
		rq_block_(blockp), batchp_(batchp) {
	std::ostringstream os;
	os << batchp_->pre_keyp_ << ":"
		<< std::to_string(batchp_->ckpt_) << ":"
		<< std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
	key_val_ = os.str();
}

DelRecord::~DelRecord() {
	as_key_destroy(&key_);
}

/* META KV related functions */
int AeroSpike::MetaWriteKeySet(WriteRecord* wrecp,
		const std::string& ns, const std::string& setp,
		const MetaDataKey& key, const std::string& value) {

	auto kp  = &wrecp->key_;
	auto kp1 = as_key_init(kp, ns.c_str(), setp.c_str(), key.c_str());
	log_assert(kp1 == kp);

	auto rp = &wrecp->record_;
	as_record_init(rp, 1);
	auto s = as_record_set_raw(rp, kAsMetaBin.c_str(),
			(const uint8_t *)value.c_str(), value.size());
	if (pio_unlikely(s == false)) {
		LOG(ERROR) << __func__<< "Error in setting record property";
		return -EINVAL;
	}

	return 0;
}

folly::Future<int> AeroSpike::AeroMetaWrite(ActiveVmdk *vmdkp,
		const std::string& ns, const MetaDataKey& key,
		const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	auto w_batch_rec = std::make_unique<WriteBatch>(nullptr, vmdkp->GetID(),
		ns, vmdkp->GetVM()->GetJsonConfig()->GetTargetName());
	if (pio_unlikely(w_batch_rec == nullptr)) {
		LOG(ERROR) << "WriteBatch allocation failed";
		return -ENOMEM;
	}

	w_batch_rec->ckpt_ = MetaData_constants::kInvalidCheckPointID();
	w_batch_rec->aero_conn_ = aero_conn.get();
	log_assert(w_batch_rec->aero_conn_ != nullptr);
	auto rec = std::make_unique<WriteRecord>();
	if (pio_unlikely(not rec)) {
		return -ENOMEM;
	}

	MetaWriteKeySet(rec.get(), ns, kAsMetaBin, key, value);
	w_batch_rec->batch.recordsp_.emplace_back(std::move(rec));
	w_batch_rec->batch.nwrites_ = 1;
	w_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();

	return WriteBatchSubmit(w_batch_rec.get())
	.then([w_batch_rec = std::move(w_batch_rec),
		vmdkp, key, value, aero_conn] (int rc) mutable {
		return rc;
	}).wait();
}

int AeroSpike::AeroMetaWriteCmd(ActiveVmdk *vmdkp,
		const MetaDataKey& key, const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn) {
	auto f = AeroMetaWrite(vmdkp, kAsNamespaceMeta, key,
				value, aero_conn);
	f.wait();
	return 0;
}

int AeroSpike::MetaReadKeySet(ReadRecord* rrecp,
		const std::string& ns, const std::string& setp,
		const MetaDataKey& key, ReadBatch* r_batch_rec) {

	auto recp = as_batch_read_reserve(r_batch_rec->aero_recordsp_);
	if (pio_unlikely(recp == nullptr)) {
		LOG(ERROR) << "ReadBatch allocation failed";
		return -EINVAL;
	}
	recp->read_all_bins = true;
	auto kp = as_key_init(&recp->key, ns.c_str(), setp.c_str(),
					key.c_str());
	if (pio_unlikely(kp == nullptr)) {
		return -ENOMEM;
	}
	log_assert(kp == &recp->key);

	rrecp->aero_recp_ = recp;
	return 0;
}

folly::Future<int> AeroSpike::AeroMetaRead(ActiveVmdk *vmdkp,
		const std::string& ns, const MetaDataKey& key,
		const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	auto r_batch_rec = std::make_unique<ReadBatch>(nullptr, vmdkp->GetID(),
		ns, vmdkp->GetVM()->GetJsonConfig()->GetTargetName());
	if (pio_unlikely(r_batch_rec == nullptr)) {
		LOG(ERROR) << "ReadBatch allocation failed";
		return -ENOMEM;
	}

	r_batch_rec->aero_conn_ = aero_conn.get();
	log_assert(r_batch_rec->aero_conn_ != nullptr);

	r_batch_rec->aero_recordsp_ = as_batch_read_create(1);
	if (pio_unlikely(r_batch_rec->aero_recordsp_ == nullptr)) {
		return -ENOMEM;
	}

	r_batch_rec->recordsp_.reserve(1);
	auto rec = std::make_unique<ReadRecord>();
	if (pio_unlikely(not rec)) {
		return -ENOMEM;
	}

	MetaReadKeySet(rec.get(), ns, kAsMetaBin, key, r_batch_rec.get());
	r_batch_rec->recordsp_.emplace_back(std::move(rec));
	r_batch_rec->nreads_ = 1;
	r_batch_rec->promise_ = std::make_unique<folly::Promise<int>>();

	return ReadBatchSubmit(r_batch_rec.get())
	.then([r_batch_rec = std::move(r_batch_rec),
		vmdkp, key, value, aero_conn] (int rc) mutable {

		if (pio_unlikely(rc < 0)) {
			LOG(ERROR) <<__func__ << "read_batch_submit failed";
			return rc;
		}

		as_batch_read_record *recp;
		for (auto& rec : r_batch_rec->recordsp_) {
			recp = rec->aero_recp_;
			switch (recp->result) {
				default:
					break;
				case AEROSPIKE_OK:
					{
					as_bytes *bp = as_record_get_bytes(&recp->record,
							kAsMetaBin.c_str());
					if (pio_unlikely(bp == NULL)) {
						LOG(ERROR) << __func__
							<< "Access error, unable to get data from given rec";
						return -ENOMEM;
					}

					uint32_t size = as_bytes_size(bp);
					auto destp = NewRequestBuffer(size);
					if (pio_unlikely(not destp)) {
						return -ENOMEM;
					}

					log_assert(as_bytes_copy(bp, 0,
							(uint8_t *) destp->Payload(),
							(uint32_t) size) == size);
					}
					break;

				case AEROSPIKE_ERR_RECORD_NOT_FOUND:
					break;
				}
		}
		return 0;
	});
}

int AeroSpike::AeroMetaReadCmd(ActiveVmdk *vmdkp,
		const MetaDataKey& key, const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn) {
	auto f = AeroMetaRead(vmdkp, kAsNamespaceMeta, key,
				value, aero_conn);
	f.wait();
	return 0;
}
}
