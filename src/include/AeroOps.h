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
#include <aerospike/aerospike_info.h>

typedef struct as_batch_read_records_s as_batch_read_records;
typedef struct as_batch_read_record_s as_batch_read_record;

namespace pio {
const std::string kAsNamespaceMeta = "META";
const std::string kAsCacheBin = "data_map";
const std::string kAsMetaBin = "meta_bin";
const std::string kAsMetaBinExt = "meta_bin_ext";
const std::string kAsMetaSet = "metaset";
const auto kMaxRetryCnt = 3;

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
	WriteBatch(const ::ondisk::VmdkID& vmdkid, const std::string& ns,
			const std::string set);
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
	bool merge_context_{false};
	as_status as_result_{AEROSPIKE_OK};
	std::atomic<size_t> batch_write_size_{0};
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
	ReadBatch(const ::ondisk::VmdkID& vmdkid, const std::string& ns,
			const std::string set);
	~ReadBatch();

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
	ReadSingle(const ::ondisk::VmdkID& vmdkid, const std::string& ns,
			const std::string set,  const std::vector<RequestBlock*>& process,
			ActiveVmdk *vmdkp);
	~ReadSingle();

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
	DelRecord(DelBatch* batchp, const std::string& ns,
			const std::string& set,
			std::string&& key) noexcept;
	~DelRecord() noexcept;
	DelRecord(DelRecord&& rhs) noexcept;

	DelRecord(const DelRecord& rhs) = delete;
	DelRecord& operator = (const DelRecord& rhs) = delete;
	DelRecord& operator = (DelRecord&& rhs) = delete;

	DelBatch *batchp_{nullptr};
	const std::string& ns_;
	const std::string& set_;
	std::string key_val_;
	as_key key_;
	as_status status_{AEROSPIKE_ERR};
};

struct DelBatch {
	DelBatch(const ::ondisk::VmdkID& vmdkid, const std::string& ns,
			const std::string& set);
	int Prepare(AeroSpikeConn* connp,
		const ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const ::ondisk::BlockID start, const ::ondisk::BlockID end);

	struct {
		std::mutex lock_;
		std::vector<DelRecord> recordsp_;
		std::vector<DelRecord>::iterator rec_it_;
		uint16_t ndeletes_{0};
		uint16_t nsent_{0};
		uint16_t ncomplete_{0};
	} batch;

	const ::ondisk::VmdkID& pre_keyp_;
	const std::string& ns_;
	const std::string& setp_;

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
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> ReadSingleSubmit(ReadSingle *batchp);


	folly::Future<int> AeroReadCmdProcess(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> AeroRead(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	int CacheIoReadKeySet(ActiveVmdk *vmdkp, ReadRecord* rrecp,
		const std::string& ns, ReadBatch* r_batch_rec);

	int ReadBatchInit(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process, ReadBatch *r_batch_rec,
		const std::string& ns);

	int ReadBatchPrepare(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		ReadBatch *r_batch_rec, const std::string& ns);

	folly::Future<int> ReadBatchSubmit(ReadBatch *batchp);

	int CacheIoWriteKeySet(ActiveVmdk *vmdkp,
		WriteRecord *wrecp, const std::string& nsp,
		const std::string& setp);

	folly::Future<int> AeroWriteCmdProcess(ActiveVmdk *vmdkp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn, bool);

	folly::Future<int> AeroWrite(ActiveVmdk *vmdkp,
                ::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
                std::vector<RequestBlock *>& failed, const std::string& ns,
		std::shared_ptr<AeroSpikeConn> aero_conn, bool);

	int WriteBatchInit(ActiveVmdk *vmdkp,
			const std::vector<RequestBlock*>& process,
			WriteBatch *w_batch_rec, const std::string& ns);

	int WriteBatchPrepare(ActiveVmdk *vmdkp,
			const std::vector<RequestBlock*>& process,
			WriteBatch *w_batch_rec,
			const std::string& ns);

	folly::Future<int> WriteBatchSubmit(WriteBatch *batchp);

	folly::Future<int> AeroDelCmdProcess(ActiveVmdk *vmdkp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		const std::string& set,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> AeroDel(ActiveVmdk *vmdkp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed, const std::string& ns,
		const std::string& set,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> DelBatchSubmit(DelBatch *batchp);

	int DelBatchPrepare(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		DelBatch *d_batch_rec, const std::string& ns);

	int DelBatchInit(ActiveVmdk *vmdkp,
		const std::vector<RequestBlock*>& process,
		DelBatch *d_batch_rec, const std::string& ns);

	folly::Future<int> Delete(AeroSpikeConn* connp,
		const ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const std::pair<::ondisk::BlockID, ::ondisk::BlockID> range,
		const std::string& ns,
		const std::string& set);

	int CacheIoDelKeySet(ActiveVmdk *vmdkp, DelRecord* drecp,
		const std::string& ns,
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
		const MetaDataKey& key, const std::string& value,
		const uint32_t& start_offset, const uint32_t& length,
		bool);

	folly::Future<int> AeroMetaWriteCmd(ActiveVmdk *vmdkp,
		const MetaDataKey& key,
		const std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	int AeroMetaReadCmd(ActiveVmdk *vmdkp,
		const MetaDataKey& key, std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	int AeroMetaDelCmd(ActiveVmdk *vmdkp,
		const MetaDataKey& key,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	folly::Future<int> AeroMetaRead(ActiveVmdk *vmdkp,
		const std::string& ns, const MetaDataKey& key,
		std::string& value,
		std::shared_ptr<AeroSpikeConn> aero_conn,
		bool delete_context = false);

	folly::Future<int> AeroMetaDel(ActiveVmdk *vmdkp,
		const MetaDataKey& key,
		std::shared_ptr<AeroSpikeConn> aero_conn);

	int MetaReadKeySet(ReadRecord* rrecp,
		const std::string& ns, const std::string& setp,
		const MetaDataKey& key, ReadBatch* r_batch_rec);
};
}
