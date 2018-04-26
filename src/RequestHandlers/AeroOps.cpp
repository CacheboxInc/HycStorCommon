#include <cerrno>
#include <iterator>
#include "gen-cpp2/StorRpc_types.h"
#include "Vmdk.h"
#include "Request.h"
#include "AeroOps.h"
#include "ThreadPool.h"
#include <folly/fibers/Fiber.h>
#include "DaemonTgtInterface.h"
#include "Singleton.h"

namespace pio {

AeroSpike::AeroSpike() {
	instance_ = SingletonHolder<AeroFiberThreads>::GetInstance();
}

AeroSpike::~AeroSpike() {}

int AeroSpike::CacheIoWriteKeySet(ActiveVmdk *vmdkp, WriteRecord* wrecp,
		Request *reqp, const std::string& ns,
		const std::string& setp) {
	auto kp  = &wrecp->key_;
	auto kp1 = as_key_init(kp, ns.c_str(), setp.c_str(),
		wrecp->key_val_.c_str());
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
		std::vector<RequestBlock*>& process, WriteBatch *w_batch_rec,
		const std::string& ns) {
	w_batch_rec->batch.recordsp_.reserve(process.size());

	/* Create Batch write records */
	for (auto block : process) {
		auto rec = std::make_unique<WriteRecord>(block, w_batch_rec);
		if (pio_unlikely(not rec)) {
			return -ENOMEM;
		}

		w_batch_rec->batch.recordsp_.emplace_back(std::move(rec));
	}

	w_batch_rec->batch.nwrites_ = process.size();
	return 0;
}

int AeroSpike::WriteBatchPrepare(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process, Request *reqp,
		WriteBatch *w_batch_rec, const std::string& ns) {
	WriteBatchInit(vmdkp, process, w_batch_rec, ns);
	for (auto& v_record : w_batch_rec->batch.recordsp_) {
		auto record  = v_record.get();
		auto rc = CacheIoWriteKeySet(vmdkp, record, reqp, ns,
					w_batch_rec->setp_);
		if (pio_unlikely(rc < 0)) {
			return rc;
		}

		/*
		 * Set TTL 0 for DIRTY namespace writes, for CLEAN
		 * namespace it will be inherited from aerospike
		 * conf file
		 */

		if (ns == kAsNamespaceCacheDirty) {
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
		LOG(ERROR) << __func__ << "error msg:-"
			<< errp->message << "err code:-" << errp->code;
	}

	auto complete = false;
	std::unique_lock<std::mutex> b_lock(batchp->batch.lock_);
	batchp->batch.ncomplete_++;
	if (batchp->submitted_ == true && batchp->batch.ncomplete_ == batchp->batch.nsent_) {
		complete = true;
	}
	b_lock.unlock();

	if (complete == true) {
		batchp->q_lock_.lock();
		int ret = batchp->rendez_.TaskWakeUp();
		log_assert(ret > 0);
		batchp->q_lock_.unlock();
	}

	return;
}

static void WritePipeListener(void *udatap, as_event_loop *lp) {
	auto wrp = reinterpret_cast<WriteRecord *>(udatap);
	log_assert(wrp && wrp->batchp_ && wrp->batchp_->req_);

	auto batchp = wrp->batchp_;

	std::unique_lock<std::mutex> b_lock(batchp->batch.lock_);
	log_assert(batchp->batch.nsent_ < batchp->batch.nwrites_);
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
		 * no more keys from this batch will be put --
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
		batchp->q_lock_.lock();
		auto ret = batchp->rendez_.TaskWakeUp();
		log_assert(ret > 0);
		batchp->q_lock_.unlock();
	}
}

int AeroSpike::WriteBatchSubmit(ActiveVmdk *vmdkp,
	std::vector<RequestBlock*>& process,
	Request *reqp, WriteBatch *batchp, const std::string& ns) {

	as_event_loop    *lp = NULL;
	WriteRecord      *wrp;
	as_key           *kp;
	as_record        *rp;
	as_status        s;
	as_error         err;
	as_pipe_listener fnp;
	uint16_t         nwrites, retry_cnt = 5;

retry:
	nwrites = batchp->batch.nwrites_;
	log_assert(batchp && batchp->failed_ == false);
	log_assert(nwrites > 0 && batchp->req_ && reqp);
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
		batchp->batch.nsent_     = 1;
		fnp = NULL;
	} else {
		batchp->submitted_ = false;
		batchp->batch.nsent_     = 1;
		fnp = WritePipeListener;
	}

	lp = as_event_loop_get();
	log_assert(lp != NULL);

	batchp->q_lock_.lock();
	s = aerospike_key_put_async(&batchp->aero_conn_->as_,
			&err, NULL, kp, rp,
			WriteListener, (void *) wrp, lp, fnp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "::request submit failed, code"
			<< err.code << "msg:" << err.message;

		batchp->q_lock_.unlock();
		wrp->status_ = s;
		batchp->failed_ = true;
		return -1;
	}

	(batchp->rendez_).TaskSleep(&batchp->q_lock_);
	batchp->q_lock_.unlock();
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
				break;
			case AEROSPIKE_ERR_TIMEOUT:
			case AEROSPIKE_ERR_RECORD_BUSY:
			case AEROSPIKE_ERR_DEVICE_OVERLOAD:
			case AEROSPIKE_ERR_CLUSTER:
			case AEROSPIKE_ERR_SERVER:
				if (!retry_cnt) {
					break;
				}

				VLOG(1) << __func__ << rec->status_
					<< "Retyring for failed write request"
					<< ", retry cnt :"
					<< retry_cnt;

				/* TBD: May want to clear aero_status from every record */
				retry_cnt--;
				batchp->batch.nwrites_ = nwrites;
				batchp->submitted_ = false;
				batchp->failed_ = false;
				batchp->batch.nsent_ = 0;
				batchp->batch.ncomplete_ = 0;
				/* Sleep for 300 ms before retry */
				usleep(1000 * 300);
				goto retry;
		}
	}

	for (auto& v_rec : batchp->batch.recordsp_) {
		auto rec = v_rec.get();
		switch (rec->status_) {
			default:
				LOG(ERROR) << __func__
					<< "aerospike_key_put_async failed :"
					<< rec->status_ << "Offset"
					<< rec->rq_block_->GetAlignedOffset();
				batchp->failed_ = true;
				break;
			case AEROSPIKE_OK:
				break;
		}
	}

	return 0;
}

int AeroSpike::AeroWrite(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
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

	return WriteBatchSubmit(vmdkp, process, reqp, w_batch_rec.get(), ns);
}

folly::Future<int> AeroSpike::AeroWriteCmdProcess(ActiveVmdk *vmdkp,
		Request *reqp, CheckPointID ckpt,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	if (pio_unlikely(process.empty())) {
		return 0;
	}

	folly::Promise<int> promise;
	auto fut = promise.getFuture();
	instance_->threadpool_.pool_->AddTask([this, vmdkp, reqp,
		&process, &failed, ckpt, ns,
		promise = std::move(promise), aero_conn] () mutable {

		failed.clear();
		auto rc = this->AeroWrite(vmdkp, reqp, ckpt, process, failed,
					ns, aero_conn);
		promise.setValue(rc);
	});
	return fut;
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
		r_batch_rec->setp_.c_str(), rrecp->key_val_.c_str());
	if (pio_unlikely(kp == nullptr)) {
		return -ENOMEM;
	}
	log_assert(kp == &recp->key);

	rrecp->aero_recp_ = recp;
	return 0;
}

int AeroSpike::ReadBatchInit(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process, ReadBatch *r_batch_rec,
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

		auto rec = std::make_unique<ReadRecord>(block, r_batch_rec);
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
	return 0;
}

int AeroSpike::ReadBatchPrepare(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process, Request *reqp,
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
	auto batchp = reinterpret_cast<ReadBatch *>(datap);
	log_assert(batchp);
	batchp->as_result_ = AEROSPIKE_OK;
	if (pio_unlikely(errp)) {
		batchp->as_result_ = errp->code;
		LOG(ERROR) << __func__ << "error msg:-" <<
			errp->message << "err code:-" << errp->code;
	}

	batchp->q_lock_.lock();
	auto rc = batchp->rendez_.TaskWakeUp();
	log_assert(rc > 0);
	batchp->q_lock_.unlock();
}

int AeroSpike::ReadBatchSubmit(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process,
		Request *reqp, ReadBatch *batchp, const std::string& ns) {

	as_event_loop    *loopp = NULL;
	as_status        s;
	as_error         err;
	uint16_t         nreads, retry_cnt = 5;

retry:
	nreads       = batchp->nreads_;

	log_assert(batchp && batchp->failed_ == false);
	log_assert(nreads > 0 && batchp->req_ && reqp);

	loopp = as_event_loop_get();
	log_assert(loopp != NULL);

	batchp->q_lock_.lock();
	s = aerospike_batch_read_async(&batchp->aero_conn_->as_, &err, NULL, batchp->aero_recordsp_,
			ReadListener, batchp, loopp);
	if (pio_unlikely(s != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "aerospike_batch_read_async request submit failed, code"
			<< err.code << "msg:" << err.message;
		batchp->q_lock_.unlock();
		batchp->as_result_ = s;
		batchp->failed_ = true;
		return -1;
	}

	(batchp->rendez_).TaskSleep(&batchp->q_lock_);
	batchp->q_lock_.unlock();
	as_monitor_notify(&batchp->aero_conn_->as_mon_);

	/* batch failed */
	if (pio_unlikely(batchp->as_result_ != AEROSPIKE_OK)) {
		LOG(ERROR) << __func__ << "Aerospike_batch_read_async failed"
			<<  ", err code" << batchp->as_result_;
		if (batchp->as_result_ == AEROSPIKE_ERR_TIMEOUT
		|| batchp->as_result_ == AEROSPIKE_ERR_RECORD_BUSY
		|| batchp->as_result_ == AEROSPIKE_ERR_DEVICE_OVERLOAD
		|| batchp->as_result_ == AEROSPIKE_ERR_SERVER) {
			if (retry_cnt) {
				VLOG(1) << __func__
					<< "Failed read request, retry cnt :"
					<< retry_cnt;
				retry_cnt--;
				batchp->nreads_ = nreads;
				batchp->failed_ = false;
				batchp->as_result_ = AEROSPIKE_OK;
				batchp->ncomplete_ = 0;
				/* Sleep for 300 ms before retry */
				usleep(1000 * 300);
				goto retry;
			}
		}

		batchp->failed_ = true;
		return -1;
	}

	/* reset temporary counters */
	as_batch_read_record *recp;
	for (auto& v_rec : batchp->recordsp_) {
		auto rec =  v_rec.get();
		recp = rec->aero_recp_;
		switch (recp->result) {
			default:
				/* TODO: handle failure */
				LOG(ERROR) <<__func__
					<< "Error in reading record, result : "
					<< recp->result;
				batchp->failed_ = true;
				break;
			case AEROSPIKE_OK:
			case AEROSPIKE_ERR_RECORD_NOT_FOUND:
				break;
		}

		rec->status_ = recp->result;
	}

	return 0;
}

int AeroSpike::AeroRead(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	auto r_batch_rec = std::make_unique<ReadBatch>(reqp, vmdkp->GetID(), ns,
			vmdkp->GetVM()->GetJsonConfig()->GetTargetName());
	log_assert(r_batch_rec != nullptr);

	r_batch_rec->ckpt_ = ckpt;
	r_batch_rec->aero_conn_ = aero_conn.get();
	log_assert(r_batch_rec->aero_conn_ != nullptr);

	auto rc = ReadBatchPrepare(vmdkp, process, reqp, r_batch_rec.get(), ns);
	if (pio_unlikely(rc < 0)) {
		VLOG(1) <<__func__ << "read_batch_prepare failed";
		return rc;
	}

	rc = ReadBatchSubmit(vmdkp, process, reqp, r_batch_rec.get(), ns);
	if (pio_unlikely(rc < 0)) {
		VLOG(1) <<__func__ << "read_batch_submit failed 1";
		return rc;
	}

	rc = 0;

	/*
	 * Now create process and failed list. If rc is non zero
	 * then move in failed list
	 */

	as_batch_read_record *recp;
	for (auto& rec : r_batch_rec->recordsp_) {
		auto blockp = rec->rq_block_;
		recp = rec->aero_recp_;
		switch (recp->result) {
			default:
				break;
			case AEROSPIKE_OK:
				{
				auto destp = NewRequestBuffer(vmdkp->BlockSize());
				if (pio_unlikely(not destp)) {
					blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
					failed.emplace_back(blockp);
					return -ENOMEM;
				}

				as_bytes *bp = as_record_get_bytes(&recp->record, kAsCacheBin.c_str());
				if (pio_unlikely(bp == NULL)) {
					VLOG(1) << __func__ << "Access error, unable to get data from given rec";
					blockp->SetResult(-ENOMEM, RequestStatus::kFailed);
					failed.emplace_back(blockp);
					return -ENOMEM;
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
				//blockp->SetResult(0, RequestStatus::kMiss);
				break;
		}
	}

	return rc;
}

folly::Future<int> AeroSpike::AeroReadCmdProcess(ActiveVmdk *vmdkp,
		Request *reqp, CheckPointID ckpt,
		std::vector<RequestBlock*>& process,
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

	folly::Promise<int> promise;
	auto fut = promise.getFuture();
	instance_->threadpool_.pool_->AddTask([this, vmdkp, reqp, &process,
		&failed, ckpt, ns, promise = std::move(promise),
		aero_conn] () mutable {

		failed.clear();
		int rc = this->AeroRead(vmdkp, reqp, ckpt, process, failed,
				ns, aero_conn);
		promise.setValue(rc);
	});

	return fut;
}

int AeroSpike::CacheIoDelKeySet(ActiveVmdk *vmdkp, DelRecord* drecp,
	Request *reqp, const std::string& ns, const std::string& setp) {

	auto kp = as_key_init(&drecp->key_, ns.c_str(), setp.c_str(),
		drecp->key_val_.c_str());
	log_assert(kp == &drecp->key_);
	return 0;
}

int AeroSpike::DelBatchInit(ActiveVmdk *vmdkp,
	std::vector<RequestBlock*>& process, DelBatch *d_batch_rec,
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
	return 0;
}

int AeroSpike::DelBatchPrepare(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process,
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
		LOG(ERROR) << __func__ << "error msg:-" <<
			errp->message << "err code:-";
		#endif
	}

	auto complete = [&] () mutable {
		std::lock_guard<std::mutex> lock(batchp->batch.lock_);
		batchp->batch.ncomplete_++;
		return batchp->submitted_ and batchp->batch.ncomplete_
						== batchp->batch.nsent_;
	} ();

	if (complete == true) {
		batchp->q_lock_.lock();
		auto rc = batchp->rendez_.TaskWakeUp();
		log_assert(rc > 0);
		batchp->q_lock_.unlock();
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
		batchp->q_lock_.lock();
		log_assert((batchp->rendez_).TaskWakeUp() > 0);
		batchp->q_lock_.unlock();
	}
}

int AeroSpike::DelBatchSubmit(ActiveVmdk *vmdkp,
	std::vector<RequestBlock*>& process,
	Request *reqp, DelBatch *batchp, const std::string& ns) {

	as_event_loop    *loopp = NULL;
	DelRecord        *drp;
	as_key           *kp;
	as_status        s;
	as_error         err;
	as_pipe_listener fnp;
	uint16_t         ndeletes, retry_cnt = 5;

retry:
	ndeletes       = batchp->batch.ndeletes_;

	log_assert(batchp && batchp->failed_ == false);
	log_assert(ndeletes > 0 && batchp->req_ && reqp);

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
		LOG(ERROR) << __func__ << "aerospike_key_remove_async"
			<< " request submit failed, code"
			<< err.code << "msg:" << err.message;

		batchp->q_lock_.unlock();
		drp->status_ = s;
		batchp->failed_ = true;
		return -1;
	}

	(batchp->rendez_).TaskSleep(&batchp->q_lock_);
	batchp->q_lock_.unlock();
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
				break;

			case AEROSPIKE_ERR_TIMEOUT:
			case AEROSPIKE_ERR_RECORD_BUSY:
			case AEROSPIKE_ERR_DEVICE_OVERLOAD:
			case AEROSPIKE_ERR_CLUSTER:
			case AEROSPIKE_ERR_SERVER:
				if (!retry_cnt) {
					break;
				}

				VLOG(1) << __func__ << rec->status_ <<
					"::Retyring for failed remove request, retry cnt :"
					<< retry_cnt;

				retry_cnt--;
				batchp->batch.ndeletes_ = ndeletes;
				batchp->submitted_ = false;
				batchp->failed_ = false;
				batchp->batch.nsent_ = 0;
				batchp->batch.ncomplete_ = 0;
				/* Sleep for 300 ms before retry */
				usleep(1000 * 300);
				goto retry;
		}
	}

	for (auto& v_rec : batchp->batch.recordsp_) {
		auto rec = v_rec.get();
		switch (rec->status_) {
		default:
			LOG(ERROR) << __func__
				<< "aerospike_key_remove_async failed:"
				<< rec->status_ << "::Offset::"
				<< rec->rq_block_->GetAlignedOffset();
			break;
		case AEROSPIKE_OK:
		case AEROSPIKE_ERR_RECORD_NOT_FOUND:
			break;
		}
	}

	return 0;
}

int AeroSpike::AeroDel(ActiveVmdk *vmdkp, Request *reqp, CheckPointID ckpt,
		std::vector<RequestBlock*>& process,
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

	return DelBatchSubmit(vmdkp, process, reqp, d_batch_rec.get(), ns);
}

folly::Future<int> AeroSpike::AeroDelCmdProcess(ActiveVmdk *vmdkp,
		Request *reqp, CheckPointID ckpt,
		std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn) {

	if (pio_unlikely(process.empty())) {
		return 0;
	}

	folly::Promise<int> promise;
	auto fut = promise.getFuture();
	instance_->threadpool_.pool_->AddTask([this, vmdkp, reqp, &process,
		&failed, ckpt, ns, promise = std::move(promise),
		aero_conn] () mutable {

		failed.clear();
		auto rc = this->AeroDel(vmdkp, reqp, ckpt, process, failed,
					ns, aero_conn);
		promise.setValue(rc);
	});
	return fut;
}

WriteRecord::WriteRecord(RequestBlock* blockp, WriteBatch* batchp) :
	rq_block_(blockp), batchp_(batchp) {
	std::ostringstream os;
	os << (batchp->pre_keyp_) << ":"
		<< std::to_string(batchp->ckpt_) << ":"
		<< std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
	key_val_ = os.str();
}

WriteRecord::~WriteRecord() {
	as_key_destroy(&key_);
}

WriteBatch::WriteBatch(Request* reqp, const VmdkID& vmdkid,
		const std::string& ns, const std::string& set)
		: req_(reqp), pre_keyp_(vmdkid), ns_(ns), setp_(set) {
}

ReadRecord::ReadRecord(RequestBlock* blockp, ReadBatch* batchp) :
		rq_block_(blockp), batchp_(batchp) {
	std::ostringstream os;
	os << batchp->pre_keyp_ << ":"
		<< std::to_string(batchp->ckpt_) << ":"
		<< std::to_string(blockp->GetAlignedOffset() >> kSectorShift);
	key_val_ = os.str();
}

ReadBatch::ReadBatch(Request* reqp, const VmdkID& vmdkid, const std::string& ns,
		const std::string& set) :
		req_(reqp), ns_(ns), pre_keyp_(vmdkid), setp_(set) {
}

ReadBatch::~ReadBatch() {
	as_batch_read_destroy(aero_recordsp_);
}

DelBatch::DelBatch(Request* reqp, const VmdkID& vmdkid, const std::string& ns,
		const std::string& set) : req_(reqp), pre_keyp_(vmdkid),
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

}
