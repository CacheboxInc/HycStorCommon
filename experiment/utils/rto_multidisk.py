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
from config import *

def init_components():
    data = { "service_type": "test_server", "service_instance" : 0, "etcd_ips" : "%s" %EtcdIps}

    # Start component for stord_svc
    r = requests.post("%s://%s/ha_svc/v1.0/component_start" %(h, StordUrl), data=json.dumps(data), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("Stord: start component done")

    # Start component for tgt_svc
    r = requests.post("%s://%s/ha_svc/v1.0/component_start" %(h, TgtUrl), data=json.dumps(data), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("TGT: start component done")

    # Add new aero cluster at StorD
    aero_data = {"aeroid": "%s" %AeroClusterID, "AeroClusterIPs":"%s" %AeroClusterIPs,"AeroClusterPort":"%s" %AeroClusterPort,"AeroClusterID":"%s" %AeroClusterID}
    r = requests.post("%s://%s/stord_svc/v1.0/new_aero/?aero-id=%s" %(h, StordUrl, AeroClusterID), data=json.dumps(aero_data), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("Stord: Aero added")

    # Add new stord to tgt
    stord_data = { "StordIp": StordIp, "StordPort": TgtToStordPort}
    r = requests.post("%s://%s/tgt_svc/v1.0/new_stord" % (h, TgtUrl), data=json.dumps(stord_data), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("TGT: new stord added")


def new_vm(VmId, TargetName):

    TargetID = VmId

    vm_data = { "vmid": "%s" %VmId, "TargetID": "%s" %TargetID, "TargetName": "%s" %TargetName, "AeroClusterID":"%s" %AeroClusterID, "VmUUID": "1"}
    r = requests.post("%s://%s/stord_svc/v1.0/new_vm/?vm-id=%s" %(h, StordUrl, VmId), data=json.dumps(vm_data), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("STORD: New VM added: %s" %VmId)

    vm_data1 = {"TargetName": "%s" %TargetName}
    r = requests.post("%s://%s/tgt_svc/v1.0/target_create/?tid=%s" % (h, TgtUrl, TargetID), data=json.dumps(vm_data1), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("TGT: New VM added: %s" %VmId)


def create_vmdk(VmId, LunID, DevName, DevPath, VmdkID, target, createfile = "false"):
    TargetID = VmId
    blk_sz   = 4096
    #blk_sz   = 16384


    if LunID == 1:
        #my data
        #vmdk_data = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID,"BlockSize":"%s" %blk_sz,"Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true", "CreateFile":"%s" %createfile, "TargetFilePath":"%s" %target, "TargetFileSize":"%s" %FileSize}, "ReadAhead":{"Enabled":"True"}, "VmUUID":"1", "VmdkUUID":"2" ,"DiskSizeBytes": 20 * 1024*1024*1024, "ReadCacheRecommendation" : 100, "WriteCacheRecommendation": 100}


        vmdk_data = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID,"BlockSize":"%s" %blk_sz,"Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true", "CreateFile":"%s" %createfile, "TargetFilePath":"%s" %target, "TargetFileSize":"%s" %FileSize, "DeltaTargetFilePath" :"/mnt"}, "ReadAhead":{"Enabled":"True"}, "VmUUID":"1", "VmdkUUID":"2" ,"DiskSizeBytes": 20 * 1024*1024*1024}
        #vmdk_data = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID,"BlockSize":"%s" %blk_sz,"Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true", "CreateFile":"%s" %createfile, "TargetFilePath":"%s" %target, "TargetFileSize":"%s" %FileSize}, "ReadCacheRecommendation":"10","WriteCacheRecommendation":"20"}
    else:
        #vmdk_data = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID,"BlockSize":"%s" %blk_sz, "ParentDiskName":"test-1", "ParentDiskVmdkID" : "1", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"}, "ReadAhead":{"Enabled":"true"}, "SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"%s" %createfile, "TargetFilePath":"%s" %target,"TargetFileSize":"%s" %FileSize}, "CleanupOnWrite":"true"}
        pass
    print(vmdk_data)
    r = requests.post("%s://%s/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h, StordUrl, VmId, VmdkID), data=json.dumps(vmdk_data), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("STORD: New VMDK: %s added for VM: %s" %(VmdkID, VmId))

    data2 = {"DevName": "%s" %(DevName), "VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID, "LunSize":"%s" %size_in_gb}
    r = requests.post("%s://%s/tgt_svc/v1.0/lun_create/?tid=%s&lid=%s" % (h, TgtUrl, TargetID, LunID), data=json.dumps(data2), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("TGT: New VMDK: %s added for VM: %s" %(VmdkID, VmId))


def truncate_disk(i, j):
    Name="iscsi-disk_%s_%s" %(i, j)
    Path="/var/hyc/%s" %(Name)
    #cmd="truncate --size=%sG %s" %(size_in_gb, Path)
    cmd="truncate --size=%s %s" %(size_in_gb, Path)
    os.system(cmd);

    return Name, Path

def delete_vmdk(VmId, LunID, VmdkID):

    TargetID = VmId

    r = requests.post("%s://%s/tgt_svc/v1.0/lun_delete/?tid=%s&lid=%s" % (h, TgtUrl, TargetID, LunID), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("TGT: LUN %s deleted for VM: %s" %(LunID, TargetID))

    r = requests.post("%s://%s/stord_svc/v1.0/vmdk_delete/?vm-id=%s&vmdk-id=%s" % (h, StordUrl, VmId, VmdkID), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("STORD: VMDK %s deleted(vmdk_delete) for VM: %s" %(VmdkID, VmId))

def delete_vm(VmId):

    TargetID = VmId

    force_delete = 1
    r = requests.post("%s://%s/tgt_svc/v1.0/target_delete/?tid=%s&force=%s" % (h, TgtUrl, TargetID, force_delete), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)
    print ("TGT: target %s deleted" %TargetID)

    r = requests.post("%s://%s/stord_svc/v1.0/vm_delete/?vm-id=%s" %(h, StordUrl, VmId))
    assert (r.status_code == 200)
    print ("STORD: target %s deleted" %VmId)


def remove_aero():

    r = requests.post("%s://%s/stord_svc/v1.0/del_aero/?aero-id=%s" %(h, StordUrl, AeroClusterID), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)

def deinit_components():

    data = { "service_type": "test_server", "service_instance" : 0, "etcd_ips" : "%s" %EtcdIps}
    r = requests.post("%s://%s/ha_svc/v1.0/component_stop" %(h, TgtUrl), data=json.dumps(data), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)

    r = requests.post("%s://%s/ha_svc/v1.0/component_stop" %(h, StordUrl), data=json.dumps(data), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)


def do_setup(no_of_vms, no_of_vmdks):
    disk_no = 0
    for i in range(1, (no_of_vms + 1)):
        TargetName = "%s-%s" %(TargetNameStr, i)
        new_vm(i, TargetName)

        for j in range(1, (no_of_vmdks + 1)):
            DevName, DevPath = truncate_disk(i, j)
            if TargetType == "dev":
                create_vmdk(i, j, DevName, DevPath, (disk_no + 1), DevTarget[disk_no])
            elif TargetType == "file":
                create_vmdk(i, j, DevName, DevPath, (disk_no + 1), FileTarget, "true")

            disk_no += 1

    VmID = i

    #r = requests.post("%s://%s/stord_svc/v1.0/prepare_ckpt/?vm-id=%s" % (h, StordUrl, VmID), headers=headers, cert=cert, verify=False)

    ckptID="1"
    r = requests.post("http://%s/stord_svc/v1.0/prepare_ckpt/?vm-id=%s" %(StordUrl, VmID), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)

    r = requests.post("http://%s/stord_svc/v1.0/commit_ckpt/?vm-id=%s&ckpt-id=%s" % (StordUrl, VmID, ckptID), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)

    ckpt_ids = [1]
    snapshot_id = 1
    data = {"checkpoint-ids": "%s" %ckpt_ids, "snapshot-id": "%s" %snapshot_id}
    r = requests.post("http://%s/stord_svc/v1.0/serialize_checkpoints/?vm-id=1" %StordUrl,
            data=json.dumps(data), headers=headers, cert=None, verify=False)
    if r.status_code == 200:
            print("moving ahead")
    else:
            print("error")

    assert (r.status_code == 200)

    cmd = "iscsiadm --mode discovery --type sendtargets --portal %s" %TargetIp
    os.system(cmd);

    cmd = "iscsiadm -m node --login"
    os.system(cmd);

    time.sleep(1)
    os.system("lsblk")

def do_cleanup():
    cmd = "iscsiadm -m node --logout"
    os.system(cmd);

    cmd = "iscsiadm -m node -o delete"
    os.system(cmd);

    time.sleep(5)
    os.system("lsblk")

    disk_no = 0
    for i in range(1, (no_of_vms + 1)):
        for j in range(1, (no_of_vmdks + 1)):
            disk_no += 1
            delete_vmdk(i, j, disk_no)
        delete_vm(i)

def do_io():
    #cmd = "fio ~/fio_sdf.conf"
    cmd = "dd if=/dev/urandom of=/dev/sdf bs=4k count=1 oflag=direct"
    os.system(cmd)
    return


def do_restcalls():
    '''
    data1 = {"vmid": 1}
    print ("Send stord_svc aero_stat")
    r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/aero_stat/?vm-id=1" % h)
    assert (r.status_code == 200)

    print ("Send stord_svc vmdk_stats")
    r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/vmdk_stats/?vmdk-id=1" % h)
    assert (r.status_code == 200)

    print ("Send stord_svc flush_req")
    data1 = {"vmid": "1", "MoveAllowed" : "true"}
    r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=1" %h, data=json.dumps(data1), headers=headers, cert=cert, verify=False)
    assert (r.status_code == 200)

    data1 = {"vmid": 1}
    print ("Send GET stord_svc flush_status 1")
    r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/flush_status/?vm-id=1" % h)
    assert (r.status_code == 200)

    print ("Send GET stord_svc flush_history 1")
    r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/flush_history/?vm-id=1&vmdk-id=1" % h)
    assert (r.status_code == 200)

    my_list=range(0,10)
    for i in my_list:
        data = {"aero-cluster-id": "%s" %AeroClusterID}
        r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/scan_status/?aero-cluster-id=%s" %(h, AeroClusterID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

    data1 = {"aero-cluster-id": "%s" %AeroClusterID}
    r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/scan_status/?aero-cluster-id=%s" % (h, AeroClusterID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
    '''
    data1 = {"aero-cluster-id": "%s" %AeroClusterID}
    r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/scan_status/?aero-cluster-id=%s" %(h, AeroClusterID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

if __name__ == '__main__':

    cleanup = False
    #cleanup = True

    if len(sys.argv) < 3:
        print (len(sys.argv))
        print("Usage: python3 test_multidisk.py <#VM> <#VMDK> <#Iters>\n")
        sys.exit(1)


    no_of_vms   = int(sys.argv[1])
    no_of_vmdks = int(sys.argv[2])

    if ((TargetType == "dev") and
            (len(DevTarget) < (no_of_vms * no_of_vmdks))):

        print("DevTarget: %s" %DevTarget)
        print("Required disks: %d" %(no_of_vms * no_of_vmdks))
        print("Insufficient TargetHandler disks provided.")
        sys.exit(1)

    if sys.argv[3]:
        no_iters = int(sys.argv[3])

    cert = None
    if h == "https" :
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        h = "https"
        cert=('./cert/cert.pem', './cert/key.pem')

    init_done = False
    init_components()

    for m in range(1, (no_iters + 1)):
        print("#################Iter %s #################" %m)
        do_setup(no_of_vms, no_of_vmdks)
        #do_io()
        #do_restcalls()
        if cleanup:
            print("Setup and Discovery complete will continue cleanup!!\n\n")

            do_cleanup()

            print("Cleanup complete!!\n\n")

    if cleanup:
        remove_aero()
        deinit_components()
