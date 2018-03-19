namespace cpp2 hyc_thrift

typedef i64 RequestId
typedef i64 VmHandle
typedef i64 VmdkHandle

struct ReadResult {
	1: required RequestId reqid;
	2: required i32 result;
	3: required binary data;
}

struct WriteResult {
	1: required RequestId reqid;
	2: required i32 result;
}

struct AbortResult {
	1: required RequestId reqid;
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

service StorRpc {
	string Ping() throws (1: ServiceException e);

	VmHandle OpenVm(1: string vmid);
	void CloseVm(1: VmHandle vm);

	VmdkHandle OpenVmdk(1: string vmid, 2: string vmdkid);
	i32 CloseVmdk(1: VmdkHandle vmdk);

	oneway void PushVmdkStats(1: VmdkHandle vmdk, 2: VmdkStats stats);

	ReadResult Read(1: VmdkHandle vmdk, 2: RequestId reqid, 3: i32 size,
		4: i64 offset);
	WriteResult Write(1: VmdkHandle vmdk, 2: RequestId reqid, 3: binary data,
		4: i32 size, 5: i64 offset);
	WriteResult WriteSame(1: VmdkHandle vmdk, 2: RequestId reqid, 3: binary data,
		4: i32 data_size, 5: i32 write_size, 6: i64 offset);
	AbortResult Abort(1: VmdkHandle vmdk, 2: RequestId reqid);
}