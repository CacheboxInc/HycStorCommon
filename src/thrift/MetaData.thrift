namespace cpp2 ondisk

typedef i64 CheckPointID
typedef string VmdkID
typedef string VmID
typedef i64 SnapshotID
typedef i64 BlockID

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

struct ActiveVmdkOnDisk {
	1: VmID vm_id;
	2: VmdkID vmdk_id;
	3: i64 disk_size_in_sectors;
	4: i32 block_size_in_sectors;
	5: CheckPointID unflushed_begin;
	6: CheckPointID unflushed_end;
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