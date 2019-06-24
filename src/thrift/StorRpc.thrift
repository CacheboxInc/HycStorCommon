namespace cpp2 hyc_thrift

typedef i64 RequestID
typedef i64 VmHandle
typedef i64 VmdkHandle
typedef i64 ShmHandle
typedef binary (cpp.type = "std::unique_ptr<folly::IOBuf>") IOBufPtr

const VmHandle kInvalidVmHandle = 0;
const VmdkHandle kInvalidVmdkHandle = 0;
const RequestID kInvalidRequestID = 0;

struct OpenResult {
	1: required VmdkHandle handle;
	2: required string shm_id;
}

struct ReadRequest {
	1: required ShmHandle shm;
	2: required RequestID reqid;
	3: required i32 size;
	4: required i64 offset;
}

struct ReadResult {
	1: required RequestID reqid;
	2: required i32 result;
	3: required IOBufPtr data;
}

struct WriteRequest {
	1: required ShmHandle shm;
	2: required RequestID reqid;
	3: required IOBufPtr data;
	4: required i32 size;
	5: required i64 offset;
}

struct WriteResult {
	1: required RequestID reqid;
	2: required i32 result;
}

struct AbortResult {
	1: required RequestID reqid;
	2: required i32 result;
}

exception ServiceException {
	1: string message;
	2: i32 error_number;
}

struct VmdkStats {
	1: i64 read_requests;
	2: i64 read_failed;
	3: i64 read_bytes;
	4: i64 read_latency;
	5: i64 write_requests;
	6: i64 write_failed;
	7: i64 write_same_requests;
	8: i64 write_same_failed;
	9: i64 write_bytes;
	10: i64 write_latency;
}

struct TruncateReq {
	1: i64 offset;
	2: i64 length;
}

struct TruncateResult {
	1: required RequestID reqid;
	2: required i32 result;
}

service StorRpc {
	string Ping() throws (1: ServiceException e);

	VmHandle OpenVm(1: string vmid);
	void CloseVm(1: VmHandle vm);

	OpenResult OpenVmdk(1: string vmid, 2: string vmdkid, 3: bool is_local, 4: i64 max_io_size);
	i32 CloseVmdk(1: VmdkHandle vmdk);

	oneway void PushVmdkStats(1: VmdkHandle vmdk, 2: VmdkStats stats);

	ReadResult Read(1: VmdkHandle vmdk, 2: ShmHandle shm, 3: RequestID reqid,
		4: i32 size, 5: i64 offset);
	WriteResult Write(1: VmdkHandle vmdk, 2: ShmHandle shm, 3: RequestID reqid,
		4: IOBufPtr data, 5: i32 size, 6: i64 offset);
	WriteResult WriteSame(1: VmdkHandle vmdk, 2: ShmHandle shm,
		3: RequestID reqid, 4: IOBufPtr data, 5: i32 data_size,
		6: i32 write_size, 7: i64 offset);
	AbortResult Abort(1: VmdkHandle vmdk, 2: RequestID reqid);

	list<WriteResult> BulkWrite(1: VmdkHandle vmdk, 2: list<WriteRequest> requests);
	list<ReadResult> BulkRead(1: VmdkHandle vmdk, 2: list<ReadRequest> requests);

	TruncateResult Truncate(1: VmdkHandle handle, 2: RequestID reqid, 3: list<TruncateReq> requests);
}
