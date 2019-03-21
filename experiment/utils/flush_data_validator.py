import json
import requests
import time
import sys
import os
import subprocess

from collections import OrderedDict
from urllib.parse import urlencode

h = "http"
cert = None
VmId="1"
headers = {'Content-type': 'application/json'}
if len(sys.argv) != 4:
	print("USAGE: python3 flush_data_validator.py <target_device_path> <mount_dir> <data_dir>")
	sys.exit()

target = sys.argv[1].lower() 
mount_dir = sys.argv[2].lower()	
data_dir = sys.argv[3].lower()

print("************************FLUSH-SETUP-START***********************")
os.system("mkfs %s" %target)
os.system("e2fsck %s" %target)
print("***Created filesystem for device path %s***" %target)
os.system("mount %s %s" %(target, mount_dir))
print("***Mounted %s to directory %s" %(target, mount_dir))
os.system("cp %s/* %s > /dev/null 2>&1" %(data_dir, mount_dir))
print("***Copied %s/* to directory %s" %(data_dir, mount_dir))
os.system("rm -rf ./source.txt")
os.system("find %s -type f -exec md5sum {} + | sort -k 2 > ./source.txt" %mount_dir)
print("Wrote md5sum of %s to ./source.txt" %mount_dir)
os.system("umount %s" %mount_dir)
print("Unmounted %s" %mount_dir)
print("************************FLUSH-SETUP-END***********************")

print("\n")

#AsyncStartFlush
print("************************FLUSH-START***********************")
print ("Send POST stord_svc/async_start_flush/?vm-id=%s" %VmId)
ckpt_ids = [0, 1]
data = {"checkpoint-ids": "%s" %ckpt_ids}
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/async_start_flush/?vm-id=%s" 
	%(h, VmId), data=json.dumps(data), headers=headers, cert=cert, verify=False)
print("HTTP Response: %s" %r.status_code)
print ("Result: %s" %r.json())
assert (r.status_code == 202)

print("\n")

#GetFlushStatus
print("Flush Running.....")
blocks_flushed = 0
while True:
	time.sleep(1)
	r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/flush_status/?vm-id=%s" % (h, VmId))
	flush_running = r.json()["flush_running"]
	num_flushed = r.json()["flushed_blks_cnt"]
	if num_flushed:
		blocks_flushed = num_flushed
	if not flush_running:
		print("Flush Completed")
		break;

print("Total blocks flushed = %s" %blocks_flushed)
print("************************FLUSH-END***********************")

print("\n")

print("************************FLUSH-VALIDATION-START***********************")
os.system("e2fsck %s" %target)
os.system("mount %s %s" %(target, mount_dir))
print("***Mounted device path %s to directory %s" %(target, mount_dir))
os.system("find %s -type f -exec md5sum {} + | sort -k 2 > ./destination.txt" %mount_dir)
print("Wrote md5sum of %s to ./destination.txt" %mount_dir)
os.system("rm -rf %s/*" %mount_dir)
os.system("umount %s" %mount_dir)
print("Unmounted %s" %mount_dir)
os.system("wipefs -f %s" %target)
print("Validating results...")
diff_result =  subprocess.Popen("diff ./source.txt ./destination.txt", shell=True, stdout=subprocess.PIPE).stdout
ret =  diff_result.read()
os.system("rm -f ./source.txt ./destination.txt")
if not ret:
	print("Data validation SUCCESSFUL-----BINGO!")
else:
	print("Data validation FAILED-----OOPS!")
print("************************FLUSH-VALIDATION-END***********************")
print("Done!!!")
