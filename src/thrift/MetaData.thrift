namespace cpp2 ondisk

typedef i64 CheckPointID
typedef string VmdkID
typedef string VmID
typedef string VmdkUUID
typedef string VmUUID
typedef i64 SnapshotID
typedef i64 BlockID
typedef binary (cpp.type = "std::unique_ptr<folly::IOBuf>") IOBufPtr

const CheckPointID kInvalidCheckPointID = 0;
const SnapshotID kInvalidSnapshotID = 0;

struct CheckPointOnDisk {
	1: CheckPointID id;
	2: VmdkID vmdk_id;
	3: binary bitmap;
	4: BlockID start;
	5: BlockID end;
	6: bool flushed;
}

struct Preload {
	1: BlockID block;
	2: i16 count;
}

struct ActiveVmdkOnDisk {
	1: VmID vm_id;
	2: VmdkID vmdk_id;
	3: i64 disk_size_in_sectors;
	4: i32 block_size_in_sectors;
	5: CheckPointID unflushed_begin;
	6: CheckPointID unflushed_end;
	7: list<Preload> preload_blocks;
}

struct SnapshotVmdkOnDisk {
	1: VmID vm_id;
	2: VmdkID vmdk_id;
	3: SnapshotID snapshot_id;
	4: list<CheckPointID> checkpoints;
}

struct VirtualMachineOnDisk {
	1: VmID vm_id;
	2: list<VmdkID> vmdks;
	3: i64 checkpoint_id;
	4: i64 snapshot_id;
}

struct IOAVmdkStats {
	1: i32 service_index;

	30: i64 total_reads;
	31: i64 total_writes;
	32: i64 nw_bytes_read;
	33: i64 nw_bytes_write;
	34: i64 aero_bytes_read;
	35: i64 aero_bytes_write;
	36: i64 flushed_chkpnts;
	37: i64 unflushed_chkpnts;
	38: i64 clean_blocks;
	39: i64 dirty_blocks;
	40: i64 read_ahead_blks;
	41: i64 read_hits;
	42: i64 read_miss;
	43: i64 read_populates;
	44: i64 read_failed;
	45: i64 write_failed;
	46: i64 cache_writes;
	47: i64 reads_in_progress;
	48: i64 writes_in_progress;
	49: i64 flushes_in_progress;
	50: i64 moves_in_progress;
	51: i64 pending_blocks;
	52: i64 parent_blks;
	53: i64 flushed_blocks;
	54: i64 moved_blocks;
	55: i64 block_size;
	56: i64 total_bytes_reads;
	57: i64 total_bytes_writes;
	58: i64 bufsz_before_compress;
	59: i64 bufsz_after_compress;
	60: i64 bufsz_before_uncompress;
	61: i64 bufsz_after_uncompress;
	62: i64 total_blk_reads;
	63: i64 app_reads;
	64: i64 app_writes;
}

struct IOAVmStats {
	1: map<string, string> data;
}

struct IOAVmFPrintStats {
	1:  map<string, string> data;
}
