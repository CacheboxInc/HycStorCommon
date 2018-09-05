#pragma once

#include <vector>
#include <memory>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>
#include <folly/fibers/Fiber.h>
#include <folly/fibers/FiberManager.h>
#include <folly/fibers/GenericBaton.h>
#include <folly/io/async/EventBase.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/MetaData_constants.h"
#include "DaemonCommon.h"
#include "AeroConn.h"
#include "AeroFiberThreads.h"
#include "VmConfig.h"
#include "VmdkConfig.h"
#include "MetaDataKV.h"
#include <aerospike/aerospike_info.h>

namespace pio {

const std::string kAsNamespaceCacheDirty = "DIRTY";
const std::string kAsNamespaceCacheClean = "CLEAN";
const std::string kAsNamespaceMeta = "META";
const std::string kAsCacheBin = "data_map";
const std::string kAsMetaBin = "meta_map";
const auto kMaxRetryCnt = 5;

struct WriteBatch;
struct WriteRecord {
public:
	WriteRecord(RequestBlock* blockp, WriteBatch* batchp, const std::string& ns, ActiveVmdk *vmdkp);
	WriteRecord(){};
	~WriteRecord();

	RequestBlock *rq_block_{nullptr};
	WriteBatch *batchp_{nullptr};
	std::string key_val_;
	as_key key_;
	as_record record_;
	as_status status_{AEROSPIKE_ERR};
	std::string setp_;
};

struct WriteBatch {
	WriteBatch(Request* reqp, const ::ondisk::VmdkID& vmdkid, const std::string& ns,
			const std::string set);
	Request* req_{nullptr};
	const ::ondisk::VmdkID& pre_keyp_;
	const std::string& ns_;
	std::string setp_;

	struct {
		std::mutex lock_;
		std::vector<std::unique_ptr<WriteRecord>> recordsp_;
		std::vector<std::unique_ptr<WriteRecord>>::iterator rec_it_;
		uint16_t nwrites_{0};
		uint16_t nsent_{0};
		uint16_t ncomplete_{0};
	} batch;

	bool retry_{false};
	std::atomic <int> retry_cnt_{kMaxRetryCnt};
	std::unique_ptr<folly::Promise<int>> promise_{nullptr};
	::ondisk::CheckPointID ckpt_{
		::ondisk::MetaData_constants::kInvalidCheckPointID()
	};
	AeroSpikeConn *aero_conn_{nullptr};

	bool failed_{false};
	bool submitted_{false};
	as_status as_result_{AEROSPIKE_OK};
};

struct ReadBatch;
struct ReadRecord {
public:
	ReadRecord(RequestBlock* blockp, ReadBatch* batchp,
		const std::string& ns, ActiveVmdk *vmdkp,
		const std::string& setp);
	ReadRecord() {};
	RequestBlock* rq_block_;
	ReadBatch* batchp_;
	as_status status_{AEROSPIKE_ERR};
	std::string key_val_;
	as_batch_read_record *aero_recp_;
	std::string setp_;
};

struct ReadBatch {
	ReadBatch(Request* reqp, const ::ondisk::VmdkID& vmdkid, const std::string& ns,
			const std::string set);
	~ReadBatch();

	Request *req_{nullptr};
	const std::string& ns_;
	const ::ondisk::VmdkID& pre_keyp_;
	const std::string setp_;

	as_batch_read_records *aero_recordsp_{nullptr};
	std::vector<std::unique_ptr<ReadRecord>> recordsp_;
	uint16_t nreads_{0};
	uint16_t ncomplete_{0};
	as_status as_result_{AEROSPIKE_OK};
	bool failed_{false};

	bool retry_{false};
	std::atomic <int> retry_cnt_{kMaxRetryCnt};
	std::unique_ptr<folly::Promise<int>> promise_{nullptr};

	AeroSpikeConn *aero_conn_{nullptr};
};


struct ReadSingle {
	ReadSingle(Request* reqp, const ::ondisk::VmdkID& vmdkid, const std::string& ns,
			const std::string set,  const std::vector<RequestBlock*>& process,
			ActiveVmdk *vmdkp);
	~ReadSingle();

	Request *req_{nullptr};
	const std::string& ns_;
	const ::ondisk::VmdkID& pre_keyp_;
	const std::string setp_;

	as_status as_result_{AEROSPIKE_OK};
	bool failed_{false};

	bool retry_{false};
	std::atomic <int> retry_cnt_{kMaxRetryCnt};
	std::unique_ptr<folly::Promise<int>> promise_{nullptr};
	uint16_t nreads_{0};

	as_key key_;
	AeroSpikeConn *aero_conn_{nullptr};
	RequestBlock  *rq_block_{nullptr};
	::ondisk::CheckPointID ckpt_{
		::ondisk::MetaData_constants::kInvalidCheckPointID()
	};

	size_t blk_size_{4096};
	std::unique_ptr<RequestBuffer> destp_{nullptr};
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
	DelBatch(Request* reqp, const ::ondisk::VmdkID& vmdkid, const std::string& ns,
			const std::string set);

	struct {
		std::mutex lock_;
		std::vector<std::unique_ptr<DelRecord>> recordsp_;
		std::vector<std::unique_ptr<DelRecord>>::iterator rec_it_;
		uint16_t ndeletes_{0};
		uint16_t nsent_{0};
		uint16_t ncomplete_{0};
	} batch;

	Request *req_{nullptr};
	const ::ondisk::VmdkID& pre_keyp_;
	const std::string& ns_;
	const std::string setp_;

	::ondisk::CheckPointID ckpt_{
		::ondisk::MetaData_constants::kInvalidCheckPointID()
	};
	AeroSpikeConn *aero_conn_{nullptr};

	bool retry_{false};
	std::atomic<int> retry_cnt_{kMaxRetryCnt};
	std::unique_ptr<folly::Promise<int>> promise_{nullptr};

	bool failed_{false};
	bool submitted_{false};
};

class AeroSpike {
public:
	AeroSpike();
	~AeroSpike();
	std::shared_ptr<AeroFiberThreads> instance_{nullptr};
public:

	folly::Future<int> AeroSingleRead(ActiveVmdk *vmdkp,
		Request *reqp, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> ReadSingleSubmit(ReadSingle *batchp);


	folly::Future<int> AeroReadCmdProcess(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> AeroRead(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	int CacheIoReadKeySet(ActiveVmdk *vmdkp, ReadRecord* rrecp,
		Request *reqp, const std::string& ns, ReadBatch* r_batch_rec);

	int ReadBatchInit(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process, ReadBatch *r_batch_rec,
		Request *reqp, const std::string& ns);

	int ReadBatchPrepare(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process, Request *reqp,
		ReadBatch *r_batch_rec, const std::string& ns);

	folly::Future<int> ReadBatchSubmit(ReadBatch *batchp);

	int CacheIoWriteKeySet(ActiveVmdk *vmdkp,
		WriteRecord *wrecp, Request *reqp, const std::string& nsp,
		const std::string& setp);

	folly::Future<int> AeroWriteCmdProcess(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> AeroWrite(ActiveVmdk *vmdkp, Request *reqp,
                ::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
                std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	int WriteBatchInit(ActiveVmdk *vmdkp,
			const std::vector<RequestBlock*>& process,
			WriteBatch *w_batch_rec, const std::string& ns);

	int WriteBatchPrepare(ActiveVmdk *vmdkp,
			const std::vector<RequestBlock*>& process,
			Request *reqp, WriteBatch *w_batch_rec,
			const std::string& ns);

	folly::Future<int> WriteBatchSubmit(WriteBatch *batchp);

	folly::Future<int> AeroDelCmdProcess(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> AeroDel(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> DelBatchSubmit(DelBatch *batchp);

	int DelBatchPrepare(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		Request *reqp, DelBatch *d_batch_rec, const std::string& ns);

	int DelBatchInit(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		DelBatch *d_batch_rec, const std::string& ns);

	int CacheIoDelKeySet(ActiveVmdk *vmdkp, DelRecord* drecp,
		Request *reqp, const std::string& ns,
		const std::string& setp);

	int ResetWriteBatchState(WriteBatch *batchp);
	int ResetDelBatchState(DelBatch *batchp);
	int ResetReadBatchState(ReadBatch *batchp);
	int ResetReadSingleState(ReadSingle *batchp);

	folly::Future<int> AeroMetaWrite(ActiveVmdk *vmdkp,
		const std::string& ns,
		const MetaDataKey& key, const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	int MetaWriteKeySet(WriteRecord* wrecp,
		const std::string& ns, const std::string& setp,
		const MetaDataKey& key, const std::string& value);

	int AeroMetaWriteCmd(ActiveVmdk *vmdkp, const MetaDataKey& key,
		const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	int AeroMetaReadCmd(ActiveVmdk *vmdkp,
		const MetaDataKey& key, const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> AeroMetaRead(ActiveVmdk *vmdkp,
		const std::string& ns, const MetaDataKey& key,
		const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	int MetaReadKeySet(ReadRecord* rrecp,
		const std::string& ns, const std::string& setp,
		const MetaDataKey& key, ReadBatch* r_batch_rec);
};
}
