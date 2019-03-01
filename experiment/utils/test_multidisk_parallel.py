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
import re
import random

from collections import OrderedDict
from urllib.parse import urlencode
from config import *

import threading

class Counter:
	def __init__(self):
		self.count_ = 0
		self.lock_ = threading.Lock()

	def Next(self):
		with self.lock_:
			self.count_ += 1
			return self.count_

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

	"""
	# Add new aero cluster at StorD
	aero_data = {"aeroid": "%s" %AeroClusterID, "AeroClusterIPs":"%s" %AeroClusterIPs,"AeroClusterPort":"%s" %AeroClusterPort,"AeroClusterID":"%s" %AeroClusterID}
	r = requests.post("%s://%s/stord_svc/v1.0/new_aero/?aero-id=%s" %(h, StordUrl, AeroClusterID), data=json.dumps(aero_data), headers=headers, cert=cert, verify=False)
	assert (r.status_code == 200)
	print ("Stord: Aero added")
	"""

	# Add new stord to tgt
	stord_data = { "StordIp": StordIp, "StordPort": TgtToStordPort}
	r = requests.post("%s://%s/tgt_svc/v1.0/new_stord" % (h, TgtUrl), data=json.dumps(stord_data), headers=headers, cert=cert, verify=False)
	assert (r.status_code == 200)
	print ("TGT: new stord added")


def new_vm(VmId, TargetName):

	TargetID = VmId

	vm_data = { "vmid": "%s" %VmId, "TargetID": "%s" %TargetID, "TargetName": "%s" %TargetName, "AeroClusterID":"%s" %AeroClusterID, "VmUUID" : "%s" % VmId } 
	r = requests.post("%s://%s/stord_svc/v1.0/new_vm/?vm-id=%s" %(h, StordUrl, VmId), data=json.dumps(vm_data), headers=headers, cert=cert, verify=False)
	assert (r.status_code == 200)
	print ("STORD: New VM added: %s" %VmId)

	vm_data1 = {"TargetName": "%s" %TargetName}
	r = requests.post("%s://%s/tgt_svc/v1.0/target_create/?tid=%s" % (h, TgtUrl, TargetID), data=json.dumps(vm_data1), headers=headers, cert=cert, verify=False)
	assert (r.status_code == 200)
	print ("TGT: New VM added: %s" %VmId)

def create_vmdk(VmId, LunID, DevName, DevPath, VmdkID, target, createfile = "false"):
	TargetID = VmId

	vmdk_data = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID,"BlockSize":"4096","Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"true","MemoryInMB":"1024"},"FileCache":{"Enabled":"false", "Path":"/dev/shm/fil1.txt"},"SuccessHandler":{"Enabled":"true"}, "FileTarget":{"Enabled":"false", "CreateFile":"%s" %createfile, "TargetFilePath":"%s" %target, "TargetFileSize":"%s" %FileSize}, "VmUUID": "%s" % VmId, "VmdkUUID" : "%s" % VmdkID, "ReadAhead":"false", "AeroSpikeCache" : { "Enabled" : "false" } }

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
	cmd="truncate --size=%sG %s" %(size_in_gb, Path)
	print (cmd)
	os.system(cmd);

	return Name, Path

def delete_vmdk(VmId, LunID, VmdkID):

	TargetID = VmId

	# r = requests.post("%s://%s/tgt_svc/v1.0/lun_delete/?tid=%s&lid=%s" % (h, TgtUrl, TargetID, LunID), headers=headers, cert=cert, verify=False)
	# assert (r.status_code == 200)
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
	# r = requests.post("%s://%s/stord_svc/v1.0/del_aero/?aero-id=%s" %(h, StordUrl, AeroClusterID), headers=headers, cert=cert, verify=False)
	# assert (r.status_code == 200)
	return

def deinit_components():

	data = { "service_type": "test_server", "service_instance" : 0, "etcd_ips" : "%s" %EtcdIps}
	r = requests.post("%s://%s/ha_svc/v1.0/component_stop" %(h, TgtUrl), data=json.dumps(data), headers=headers, cert=cert, verify=False)
	assert (r.status_code == 200)

	r = requests.post("%s://%s/ha_svc/v1.0/component_stop" %(h, StordUrl), data=json.dumps(data), headers=headers, cert=cert, verify=False)
	assert (r.status_code == 200)

def getAllDevices(targetIP):
	source_path = "/dev/disk/by-path/"
	devices = []
	for roots, dirs, files in os.walk(source_path):
		for file in files:
			if targetIP in file:
				disk = os.path.realpath(os.path.join(source_path, file))
				devices.append(disk)
	return devices


'''
[0:0:0:0]    cd/dvd  ata:                            /dev/sr0 
[2:0:0:0]    storage test-1,t,0x1                    -        
[2:0:0:1]    disk    test-1,t,0x1                    -        
[2:0:0:2]    disk    test-1,t,0x1                    -        
'''


def find_devices_by_iqn(search_iqn):
	lsscsi_output = "/tmp/lsscsi.txt"
	cmd = "lsscsi -t > {0}".format(lsscsi_output)
	print (cmd)
	ret = os.system(cmd)
	if ret:
		return None

	print("Search IQN %s" % search_iqn)
	with open(lsscsi_output) as fh:
		for line in fh.readlines():
			print(line)

	iqn_re = re.compile("\[\d+:\d+:\d+:\d+\]\s+(\w+)\s+([\w\.:\-,]+)\s+([\w\/]+)");
	matched_disks = []
	for line in open(lsscsi_output):
		line = line.rstrip()
		print(line)
		m = iqn_re.match(line)
		if not m:
			print("not matched")
			continue
		lun_type, iqn_fields, disk = m.groups()
		if lun_type != "disk":
			print("not disk")
			continue
		iqn = iqn_fields.split(',')[0]
		if iqn != search_iqn:
			print("iqn = ", iqn)
			continue

		matched_disks.append(disk)
	print("Matched disks %s" % matched_disks)
	return matched_disks

def CreateVm(counter):
	vmid = counter.Next()
	TargetName = "%s-%s" %(TargetNameStr, vmid)
	new_vm(vmid, TargetName)
	return (vmid, TargetName)

def CreateVmdks(counter, vmid, nvmdks):
	vmdkids = []
	for j in range(1, nvmdks+1):
		dev_name, dev_path = truncate_disk(vmid, j)
		vmdkid = counter.Next()
		create_vmdk(vmid, j, dev_name, dev_path, vmdkid, FileTarget, "true")
		vmdkids.append(vmdkid)
	return vmdkids

def LoginToVmdksAndGetDeviceNames(target_ip, iqn):
	for repeat in range(0, 5):
		cmd = "iscsiadm --mode discovery --type sendtargets --portal %s" % target_ip
		os.system(cmd);

		cmd = "iscsiadm -m node --target %s --portal %s --login " % (iqn, target_ip)
		os.system(cmd);

		time.sleep(5)
		os.system("lsblk")

		devices = find_devices_by_iqn(iqn)
		if devices and len(devices) > 0:
			break
	return find_devices_by_iqn(iqn)

def ConcatDeviceNames(devices, delim):
	s = devices[0]
	for i in range(1, len(devices)):
		s += delim + devices[i]
	return s

def RunFio(iqn, devices):
	fio_device_names = ConcatDeviceNames(devices, ':')
	assert(fio_device_names)
	print ("fio device names ", fio_device_names)
	run_time = random.randint(10, 60)
	fio_cmd = "fio --name=hycPerf --ioengine=libaio --iodepth=1  --group_reporting --rw=randread --bs=16k --direct=1 --numjobs=1 --rwmixread=100 --randrepeat=0 --filename=%s --runtime=%ss --time_based=1 --norandommap --group_reporting --randrepeat=0 --output=%s.fio.log" % (fio_device_names, run_time, iqn)
	status = os.system(fio_cmd)
	if status is not 0:
		print ("Fio run failed")
		os.system("cat %s.fio.log" % fio_device_names)

def LogoutTargets(iqn, target_ip):
	cmd = "iscsiadm -m node --target %s --portal %s --logout" % (iqn, target_ip)
	print (cmd)
	os.system(cmd);

	cmd = "iscsiadm -m node -o delete --target %s" % iqn
	print (cmd)
	os.system(cmd);

def DeleteVmdks(vmid, vmdkids):
	disk_no = 0
	for vmdkid in vmdkids:
		disk_no += 1
		delete_vmdk(vmid, disk_no, vmdkid)

def worker(tid, iterations, counter, no_of_vmdks):
	for it in range(0, iterations):
		print("################# TID = %d Iter %s #################" % (tid, it))
		print("%d: Creating VM " % (tid))
		(vmid, iqn) = CreateVm(counter);
		print("%d VMID=%s Creating VMDKs " % (tid, vmid))
		vmdkids = CreateVmdks(counter, vmid, no_of_vmdks)
		print("%d VMID=%s Login to VMDKs " % (tid, vmid))
		devices = LoginToVmdksAndGetDeviceNames(TargetIp, iqn)
		if devices is None or len(devices) == 0:
			print ("Not able to find device")
			return None

		print("%d VMID=%s Running FIO on VMDKs " % (tid, vmid))
		RunFio(iqn, devices)

		print("%d VMID=%s Logout VMDKs " % (tid, vmid))
		LogoutTargets(iqn, TargetIp)

		print("%d VMID=%s Delete VMDKs " % (tid, vmid))
		DeleteVmdks(vmid, vmdkids)

		print("%d VMID=%s Delete VM " % (tid, vmid))
		delete_vm(vmid)

if __name__ == '__main__':
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

	counter = Counter()
	threads = []
	for n in range(1, (no_of_vms + 1)):
		t = threading.Thread(target=worker, args=(n, no_iters, counter, no_of_vmdks))
		threads.append(t)
		t.start()
	for t in threads:
		t.join()

	remove_aero()
	deinit_components()
