# PREREQUISITES,
# Need stord to be started with ha port 9000
#./src/stord/./stord -etcd_ip="http://127.0.0.1:2379" -stord_version="v1.0" -svc_label="stord_svc" -ha_svc_port=9000
# Need tgtd to be started with ha port 9001
#./usr/tgtd -f -e "http://127.0.0.1:2379" -s "tgt_svc" -v "v1.0" -p 9001 -D "127.0.0.1" -P 9876

import json
import requests
import time
import sys
import os

from collections import OrderedDict
from urllib.parse import urlencode

h = "http"
cert = None

AeroClusterIPs="192.168.6.58"
AeroClusterPort="3000"
AeroClusterID="1"
TargetName="tgt1"
TargetIp = "192.168.4.212"

VmId="1"
VmdkID="1"
TargetID="%s" %VmId
LunID="%s" %VmdkID
DevTarget="/dev/sdd"

size_in_gb="20" #Size in GB
size_in_bytes=int(size_in_gb) * int(1024) * int(1024) * int(1024)

DevName="iscsi-%s-disk_%s" %(TargetName, LunID)
DevPath="/var/hyc/%s" %(DevName)
cmd="truncate --size=%sG %s" %(size_in_gb, DevPath)

headers = {'Content-type': 'application/json'}
params = OrderedDict([('first', 1), ('second', 2), ('third', 3)])
data = { "service_type": "test_server", "service_instance" : 0, "etcd_ips" : ["3213213", "213213"]}

print("******Using default values********")
print("Aero IP   : %s" %AeroClusterIPs)
print("TGT IP    : %s" %TargetIp)
print("DevTarget : %s" %DevTarget)
print("Dev Size  : %sG" %size_in_gb)
a = input("\nChange them at top of script to use custom. Do you want to change?(y/n)")
if a in("y", "Y"):
    sys.exit(1)

print("\nSetup Started")
# Start component for stord_svc on port 9000
r = requests.post("%s://127.0.0.1:9000/ha_svc/v1.0/component_start" % h, data=json.dumps(data), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 1 to stord_svc, Add new aero cluster at StorD
data1 = {"aeroid": "%s" %AeroClusterID, "AeroClusterIPs":"%s" %AeroClusterIPs,"AeroClusterPort":"%s" %AeroClusterPort,"AeroClusterID":"%s" %AeroClusterID}
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_aero/?aero-id=1" % h, data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 2 to stord_svc
data1 = { "vmid": "%s" %VmId, "TargetID": "%s" %TargetID, "TargetName": "%s" %TargetName, "AeroClusterID":"%s" %AeroClusterID}
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vm/?vm-id=%s" %(h, VmId), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 3 to stord_svc
parent = False
if parent == True:
	data2 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID,"BlockSize":"4096", "ParentDiskName":"set10", "ParentDiskVmdkID" : "12", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"}, "ReadAhead":{"Enabled":"true"}, "SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"false", "TargetFilePath":"%s" %DevTarget,"TargetFileSize":"%s" %size_in_bytes}, "CleanupOnWrite":"true", "DeltaTargetFilePath" :"/mnt", "VmUUID":"1", "VmdkUUID":"2" }
else:
	data2 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID,"BlockSize":"4096", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"false", "TargetFilePath":"%s" %DevTarget,"TargetFileSize":"%s" %size_in_bytes}, "CleanupOnWrite":"true", "DeltaTargetFilePath" :"/mnt", "VmUUID":"1", "VmdkUUID":"2" }

r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h,VmId,VmdkID), data=json.dumps(data2), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# Start component for tgt_svc on port 9001
r = requests.post("%s://127.0.0.1:9001/ha_svc/v1.0/component_start" % h, data=json.dumps(data), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call to tgt_svc adding stord to tgt
data3 = { "StordIp": "127.0.0.1", "StordPort": "9876"}
r = requests.post("%s://127.0.0.1:9001/tgt_svc/v1.0/new_stord" % (h), data=json.dumps(data3), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 4 to tgt_svc
data1 = {"TargetName": "%s" %TargetName}
r = requests.post("%s://127.0.0.1:9001/tgt_svc/v1.0/target_create/?tid=%s" % (h, TargetID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 5 to tgt_svc
data2 = {"DevName": "%s" %(DevName), "VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID, "LunSize":"%s" %size_in_bytes}
r = requests.post("%s://127.0.0.1:9001/tgt_svc/v1.0/lun_create/?tid=%s&lid=%s" % (h, TargetID, LunID), data=json.dumps(data2), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 4 to stord_svc
ckptID="1"
r = requests.post("http://127.0.0.1:9000/stord_svc/v1.0/prepare_ckpt/?vm-id=%s" %(VmId), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 5 to stord_svc
r = requests.post("http://127.0.0.1:9000/stord_svc/v1.0/commit_ckpt/?vm-id=%s&ckpt-id=%s" % (VmId, ckptID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 6 to stord_svc
ckpt_ids = [1]
snapshot_id = 1
data = {"checkpoint-ids": "%s" %ckpt_ids, "snapshot-id": "%s" %snapshot_id}
r = requests.post("http://127.0.0.1:9000/stord_svc/v1.0/serialize_checkpoints/?vm-id=%s" %VmId, data=json.dumps(data), headers=headers, cert=None, verify=False)
assert (r.status_code == 200)

cmd = "iscsiadm -m node --logout"
os.system(cmd);

lsblk_cmd = "lsblk -o name -n -l"
tmp_str   = os.popen(lsblk_cmd).read()
old_disks = tmp_str.split("\n")

cmd = "iscsiadm --mode discovery --type sendtargets --portal %s" %TargetIp
os.system(cmd);

print("\nISCSI logging in")
cmd = "iscsiadm -m node --login"
os.system(cmd);

time.sleep(5)
lsblk_cmd = "lsblk -o name -n -l"
tmp_str   = os.popen(lsblk_cmd).read()
new_disks = tmp_str.split("\n")
new_disk  = list(set(new_disks) - set(old_disks))[0]
new_dev_name = "/dev/%s" %new_disk

mkfs_cmd  = "mkfs.ext4 %s" %new_dev_name
os.system(mkfs_cmd)

mount_cmd = "mount %s /mnt1" %new_dev_name
os.system(mount_cmd)

print("\nDoing IOs")
cmd = "fio --name=random --ioengine=libaio --iodepth=16 --norandommap --group_reporting --gtod_reduce=1 --stonewall --rw=randrw --bs=16384 --direct=1 --size=100M --numjobs=1 --randrepeat=0 --filename=/mnt1/1"
os.system(cmd)

mdsum_cmd = "md5sum /mnt1/1"
tmp_str   = os.popen(mdsum_cmd).read()
old_mdsum = tmp_str.split()[0]

print("\nFlushing data")
data1 = {"vmid": "%s" %VmId , "MoveAllowed" : "true", "FlushAllowed": "true"}
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=%s" % (h, VmId), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

def flush_running():
    data1 = {"vmid": "%s" %VmId}
    r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/flush_status/?vm-id=%s" % (h, VmId))
    a = r.json()
    if (a["flush_running"]):
        return True
    else:
        return False

while(flush_running()):
    print("Waiting for flush to complete")
    time.sleep(5)

print("\nCleanup started")
mount_cmd = "umount /mnt1"
os.system(mount_cmd);

r = requests.post("%s://127.0.0.1:9001/tgt_svc/v1.0/lun_delete/?tid=%s&lid=%s" % (h, TargetID, LunID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/vmdk_delete/?vm-id=%s&vmdk-id=%s" % (h, VmId, VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

force_delete = 1
r = requests.post("%s://127.0.0.1:9001/tgt_svc/v1.0/target_delete/?tid=%s&force=%s" % (h, TargetID, force_delete), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/vm_delete/?vm-id=%s" %(h, VmId))
assert (r.status_code == 200)

# TODO: The below assert won't work until checkpoint merging code is present in StorD.
# Commenting it for now.
'''
mount_cmd = "mount %s /mnt1" %DevTarget
os.system(mount_cmd);

mdsum_cmd = "md5sum /mnt1/1"
tmp_str   = os.popen(mdsum_cmd).read()
new_mdsum = tmp_str.split()[0]

assert (old_mdsum == new_mdsum)
print("\nData verified successfully!!")
'''

mount_cmd = "umount /mnt1"
os.system(mount_cmd);

# Stop component for tgt_svc on port 9001
r = requests.post("%s://127.0.0.1:9001/ha_svc/v1.0/component_stop" % h, data=json.dumps(data), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# Stop component for stord_svc on port 9000
r = requests.post("%s://127.0.0.1:9000/ha_svc/v1.0/component_stop" % h, data=json.dumps(data), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

print("\nTest complete")
