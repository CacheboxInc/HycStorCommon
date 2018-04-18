#pragma once

#include <vector>
#include <memory>

#include <folly/futures/Future.h>

#include "DaemonCommon.h"
#include "QLock.h"
#include "Rendez.h"
#include "AeroConn.h"

namespace pio {

const std::string kAsNamespaceCacheDirty = "DIRTY";
const std::string kAsNamespaceCacheClean = "CLEAN";
const std::string kAsCacheBin = "data_map";

struct WriteBatch;
struct WriteRecord {
public:
	WriteRecord(RequestBlock* blockp, WriteBatch* batchp);
	~WriteRecord();

	RequestBlock *rq_block_{nullptr};
	WriteBatch *batchp_{nullptr};
	std::string key_val_;
	as_key key_;
	as_record record_;
	as_status status_{AEROSPIKE_ERR};
};

struct WriteBatch {
	WriteBatch(Request* reqp, const VmdkID& vmdkid, const std::string& ns);

	Request* req_{nullptr};
	const VmdkID& pre_keyp_;
	const std::string& ns_;

	struct {
		std::mutex lock_;
		std::vector<std::unique_ptr<WriteRecord>> recordsp_;
		std::vector<std::unique_ptr<WriteRecord>>::iterator rec_it_;
		uint16_t nwrites_{0};
		uint16_t nsent_{0};
		uint16_t ncomplete_{0};
	} batch;

	QLock q_lock_;
	Rendez rendez_;

	CheckPointID  ckpt_{kInvalidCheckPointID};
	AeroSpikeConn *aero_conn_{nullptr};

	bool failed_{false};
	bool submitted_{false};
	as_status as_result_{AEROSPIKE_OK};
};

struct ReadBatch;
struct ReadRecord {
public:
	ReadRecord(RequestBlock* blockp, ReadBatch* batchp);
	RequestBlock* rq_block_;
	ReadBatch* batchp_;
	as_status status_{AEROSPIKE_ERR};
	std::string key_val_;
	as_batch_read_record *aero_recp_;
};

struct ReadBatch {
	ReadBatch(Request* reqp, const VmdkID& vmdkid, const std::string& ns);
	~ReadBatch();

	Request *req_{nullptr};
	const std::string& ns_;
	const VmdkID& pre_keyp_;

	as_batch_read_records *aero_recordsp_{nullptr};
	std::vector<std::unique_ptr<ReadRecord>> recordsp_;
	QLock q_lock_;
	Rendez rendez_;
	uint16_t nreads_{0};
	uint16_t ncomplete_{0};
	as_status as_result_{AEROSPIKE_OK};
	bool failed_{false};

	CheckPointID ckpt_{kInvalidCheckPointID};
	AeroSpikeConn *aero_conn_{nullptr};
};

struct DelBatch;
struct DelRecord {
	DelRecord(RequestBlock* blockp, DelBatch* batchp);
	~DelRecord();

	RequestBlock  *rq_block_{nullptr};
	DelBatch *batchp_{nullptr};
	std::string key_val_;
	as_key key_;
	as_status status_{AEROSPIKE_ERR};
};

struct DelBatch {
	DelBatch(Request* reqp, const VmdkID& vmdkid, const std::string& ns);

	struct {
		std::mutex lock_;
		std::vector<std::unique_ptr<DelRecord>> recordsp_;
		std::vector<std::unique_ptr<DelRecord>>::iterator rec_it_;
		uint16_t ndeletes_{0};
		uint16_t nsent_{0};
		uint16_t ncomplete_{0};
	} batch;

	Request *req_{nullptr};
	const VmdkID& pre_keyp_;
	const std::string& ns_;

	CheckPointID ckpt_{kInvalidCheckPointID};
	AeroSpikeConn *aero_conn_{nullptr};

	QLock q_lock_;
	Rendez rendez_;
	bool failed_{false};
	bool submitted_{false};
};

class AeroSpike {
public:
	AeroSpike();
	~AeroSpike();
public:

	int AeroReadCmdProcess(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns);

	int AeroRead(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns);

	int CacheIoReadKeySet(ActiveVmdk *vmdkp, ReadRecord* rrecp,
		Request *reqp, const std::string& ns, ReadBatch* r_batch_rec);

	int ReadBatchInit(ActiveVmdk *vmdkp, std::vector<RequestBlock*>& process,
		ReadBatch *r_batch_rec,	Request *reqp, const std::string& ns);

	int ReadBatchPrepare(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process,
		Request *reqp, ReadBatch *r_batch_rec, const std::string& ns);

 	int ReadBatchSubmit(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process, Request *reqp,
		ReadBatch *batchp, const std::string& ns);

	int CacheIoWriteKeySet(ActiveVmdk *vmdkp,
		WriteRecord *wrecp, Request *reqp, const std::string& nsp);

	int AeroWriteCmdProcess(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns);

	int AeroWrite(ActiveVmdk *vmdkp, Request *reqp,
                CheckPointID ckpt, std::vector<RequestBlock*>& process,
                std::vector<RequestBlock *>& failed, const std::string& ns);

	int WriteBatchInit(ActiveVmdk *vmdkp,
			std::vector<RequestBlock*>& process,
			WriteBatch *w_batch_rec, const std::string& ns);

	int WriteBatchPrepare(ActiveVmdk *vmdkp,
			std::vector<RequestBlock*>& process,
			Request *reqp, WriteBatch *w_batch_rec,
			const std::string& ns);

	int WriteBatchSubmit(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process,
		Request *reqp, WriteBatch *batchp, const std::string& ns);

	int AeroDelCmdProcess(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns);

	int AeroDel(ActiveVmdk *vmdkp, Request *reqp,
		CheckPointID ckpt, std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns);

	int DelBatchSubmit(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process,
		Request *reqp, DelBatch *batchp, const std::string& ns);

	int DelBatchPrepare(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process,
		Request *reqp, DelBatch *d_batch_rec, const std::string& ns);

	int DelBatchInit(ActiveVmdk *vmdkp,
		std::vector<RequestBlock*>& process,
		DelBatch *d_batch_rec, const std::string& ns);

	int CacheIoDelKeySet(ActiveVmdk *vmdkp, DelRecord* drecp,
		Request *reqp, const std::string& ns);

};
}
