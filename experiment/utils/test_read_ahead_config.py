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
VmdkID="1"
VmId="1"
TargetID="%s" %VmId
LunID="%s" %VmdkID
FileTarget="/tmp/hyc"
createfile="true"
size_in_gb="5" #Size in GB
size_in_bytes=int(size_in_gb) * int(1024) * int(1024) * int(1024)

TargetName="tgt1"
DevName="iscsi-%s-disk_%s" %(TargetName, LunID)
DevPath="/var/hyc/%s" %(DevName)

if len(sys.argv) > 1:
    if sys.argv[1].lower() == "https" :
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        h = "https"
        cert=('./cert/cert.pem', './cert/key.pem')

headers = {'Content-type': 'application/json'}
params = OrderedDict([('first', 1), ('second', 2), ('third', 3)])
data = { "service_type": "test_server", "service_instance" : 0, "etcd_ips" : ["3213213", "213213"]}

print("##########################")
print("NEW VMDK CONFIG TESTS")
print("##########################")
print ("Send POST new_vmdk with config & enabled=true")
VmdkID = VmdkID + "1"
data1 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %(VmdkID),"BlockSize":"16384", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"%s" %createfile, "TargetFilePath":"%s" %FileTarget,"TargetFileSize":"%s" %size_in_bytes, "DeltaTargetFilePath" :"/mnt"}, "CleanupOnWrite":"true", "ReadAhead":{"Enabled":"true" , "AggRandomPatternOccurrences":"20", "MaxPatternStability":"30", "IoMissWindow":"10", "IoMissThresholdPercent":"60", "PatternStabilityPercent":"60", "GhbHistoryLength":"4096", "MaxPredictionSize":1 << 20, "MinPredictionSize":1 << 17, "MaxPacketSizeBytes":"262144", "MaxIoSizeBytes":1 << 20}, 'DiskSizeBytes': "%d" %size_in_bytes, 'VmUUID': 'hycvc43primaryiolan_Lin-cho-LINKED_CLONE-19', 'VmdkUUID': 'hycvc43primaryiolan_6000c2936a9b98fd962c959e4f509b91_Lin-cho-LINKED_CLONE-19', "DeltaTargetFilePath" :"/mnt"}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h,VmId,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

print ("Send POST new_vmdk with no config & enabled=true")
VmdkID = VmdkID + "2"
data1 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %(VmdkID),"BlockSize":"16384", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"%s" %createfile, "TargetFilePath":"%s" %FileTarget,"TargetFileSize":"%s" %size_in_bytes, "DeltaTargetFilePath" :"/mnt"}, "CleanupOnWrite":"true", "ReadAhead":{"Enabled":"true"}, 'DiskSizeBytes': "%d" %size_in_bytes, 'VmUUID': 'hycvc43primaryiolan_Lin-cho-LINKED_CLONE-19', 'VmdkUUID': 'hycvc43primaryiolan_6000c2936a9b98fd962c959e4f509b91_Lin-cho-LINKED_CLONE-19', "DeltaTargetFilePath" :"/mnt"}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h,VmId,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

print ("Send POST new_vmdk with no config & enabled=false")
VmdkID = VmdkID + "3"
data1 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %(VmdkID),"BlockSize":"16384", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"%s" %createfile, "TargetFilePath":"%s" %FileTarget,"TargetFileSize":"%s" %size_in_bytes, "DeltaTargetFilePath" :"/mnt"}, "CleanupOnWrite":"true", "ReadAhead":{"Enabled":"false"}, 'DiskSizeBytes': "%d" %size_in_bytes, 'VmUUID': 'hycvc43primaryiolan_Lin-cho-LINKED_CLONE-19', 'VmdkUUID': 'hycvc43primaryiolan_6000c2936a9b98fd962c959e4f509b91_Lin-cho-LINKED_CLONE-19', "DeltaTargetFilePath" :"/mnt"}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h,VmId,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

print ("Send POST new_vmdk with no config at all")
VmdkID = VmdkID + "4"
data1 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %(VmdkID),"BlockSize":"16384", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"%s" %createfile, "TargetFilePath":"%s" %FileTarget,"TargetFileSize":"%s" %size_in_bytes, "DeltaTargetFilePath" :"/mnt"}, "CleanupOnWrite":"true", 'DiskSizeBytes': "%d" %size_in_bytes, 'VmUUID': 'hycvc43primaryiolan_Lin-cho-LINKED_CLONE-19', 'VmdkUUID': 'hycvc43primaryiolan_6000c2936a9b98fd962c959e4f509b91_Lin-cho-LINKED_CLONE-19', "DeltaTargetFilePath" :"/mnt"}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h,VmId,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

print("#########################################")
print("GLOBAL-LOCAL NEW VMDK CONFIG TESTS")
print("#########################################")
print ("Send POST set_read_ahead_global_config with config & enabled=true")
data1 = {"ReadAhead":{"Enabled":"true", "AggRandomPatternOccurrences":"12", "MaxPatternStability":"10", "IoMissWindow":"12", "IoMissThresholdPercent":"70", "PatternStabilityPercent":"30", "GhbHistoryLength":"1024", "MaxPredictionSize":"%s" %(1 << 18), "MinPredictionSize":1 << 16, "MaxPacketSizeBytes":"262144", "MinDiskSizeSupportedBytes":"%s" %(2 << 30), "MaxIoSizeBytes":"%s" %(1 << 20)}}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/set_read_ahead_global_config" %h, data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_global_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_global_config" %h, headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

print ("Send POST new_vmdk with no config & enabled=true")
VmdkID = VmdkID + "5"
data1 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %(VmdkID),"BlockSize":"16384", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"%s" %createfile, "TargetFilePath":"%s" %FileTarget,"TargetFileSize":"%s" %size_in_bytes, "DeltaTargetFilePath" :"/mnt"}, "CleanupOnWrite":"true", "ReadAhead":{"Enabled":"true"}, 'DiskSizeBytes': "%d" %size_in_bytes, 'VmUUID': 'hycvc43primaryiolan_Lin-cho-LINKED_CLONE-19', 'VmdkUUID': 'hycvc43primaryiolan_6000c2936a9b98fd962c959e4f509b91_Lin-cho-LINKED_CLONE-19', "DeltaTargetFilePath" :"/mnt"}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h,VmId,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

print ("Send POST new_vmdk with new full config & enabled=true")
VmdkID = VmdkID + "5"
data1 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %(VmdkID),"BlockSize":"16384", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"%s" %createfile, "TargetFilePath":"%s" %FileTarget,"TargetFileSize":"%s" %size_in_bytes, "DeltaTargetFilePath" :"/mnt"}, "CleanupOnWrite":"true", "ReadAhead":{"Enabled":"true", "AggRandomPatternOccurrences":"20", "MaxPatternStability":"30", "IoMissWindow":"50", "IoMissThresholdPercent":"40", "PatternStabilityPercent":"50", "GhbHistoryLength":"2048", "MaxPredictionSize":"%s" %(1 << 20), "MinPredictionSize":1 << 17, "MaxPacketSizeBytes":"262144", "MinDiskSizeSupportedBytes":"%s" %(1 << 30), "MaxIoSizeBytes":"%s" %(1 << 20)}, 'DiskSizeBytes': "%d" %size_in_bytes, 'VmUUID': 'hycvc43primaryiolan_Lin-cho-LINKED_CLONE-19', 'VmdkUUID': 'hycvc43primaryiolan_6000c2936a9b98fd962c959e4f509b91_Lin-cho-LINKED_CLONE-19', "DeltaTargetFilePath" :"/mnt"}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h,VmId,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

print ("Send POST new_vmdk with new partial config & enabled=true")
VmdkID = VmdkID + "6"
data1 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %(VmdkID),"BlockSize":"16384", "Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"false"}, "FileTarget":{"Enabled":"true","CreateFile":"%s" %createfile, "TargetFilePath":"%s" %FileTarget,"TargetFileSize":"%s" %size_in_bytes, "DeltaTargetFilePath" :"/mnt"}, "CleanupOnWrite":"true", "ReadAhead":{"Enabled":"true", "AggRandomPatternOccurrences":"20", "MaxPatternStability":"30", "IoMissWindow":"50", "IoMissThresholdPercent":"40", "PatternStabilityPercent":"50"}, 'DiskSizeBytes': "%d" %size_in_bytes, 'VmUUID': 'hycvc43primaryiolan_Lin-cho-LINKED_CLONE-19', 'VmdkUUID': 'hycvc43primaryiolan_6000c2936a9b98fd962c959e4f509b91_Lin-cho-LINKED_CLONE-19', "DeltaTargetFilePath" :"/mnt"}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h,VmId,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

print("##########################")
print("GLOBAL CONFIG TESTS")
print("##########################")
#Expected Output : Should return default immutable ReadAhead config
print ("Send GET get_read_ahead_default_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_default_config" %h, headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

# Expected Output : Should enable ReadAhead for all vmdks across all vms and apply this global config
# Note: It first deletes each ReadAhead object for each vmdk and reinstate them with this config
print ("Send POST set_read_ahead_global_config with config & enabled=true")
data1 = {"ReadAhead":{"Enabled":"true", "AggRandomPatternOccurrences":"12", "MaxPatternStability":"8", "IoMissWindow":"16", "IoMissThresholdPercent":"65", "PatternStabilityPercent":"50", "GhbHistoryLength":"2048", "MaxPredictionSize":"%s" %(1 << 20), "MinPredictionSize":1 << 17, "MaxPacketSizeBytes":"262144", "MinDiskSizeSupportedBytes":"%s" %(1 << 30), "MaxIoSizeBytes":"%s" %(1 << 20)}}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/set_read_ahead_global_config" %h, data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_global_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_global_config" %h, headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

# Expected Output : Should disable ReadAhead for all vmdks across all vms
# Note: This deletes each ReadAhead object for each vmdk 
print ("Send POST set_read_ahead_global_config with no config & enable=false")
data1 = {"ReadAhead":{"Enabled":"false"}}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/set_read_ahead_global_config" %h, data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_global_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_global_config" %h, headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

# Expected Output : Should enable ReadAhead for all vmdks across all vms and apply the current global config in memory
# Note: It first deletes each ReadAhead object for each vmdk and reinstate them with current global config
print ("Send POST set_read_ahead_global_config with no config & enable=true")
data1 = {"ReadAhead":{"Enabled":"true"}}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/set_read_ahead_global_config" %h, data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_global_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_global_config" %h, headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

print("##########################")
print("LOCAL CONFIG TESTS")
print("##########################")
# Expected Output : Should enable ReadAhead for this vmdk and apply this config
# Note: It first deletes the ReadAhead object for this vmdk and reinstate it with this config
VmdkID = "1"
print ("Send POST set_read_ahead_local_config with config & enable=true")
data1 = {"ReadAhead":{"Enabled":"true", "AggRandomPatternOccurrences":"16", "MaxPatternStability":"10", "IoMissWindow":"10", "IoMissThresholdPercent":"40", "PatternStabilityPercent":"35", "GhbHistoryLength":"1024", "MaxPredictionSize":"%s" %(1 << 20), "MinPredictionSize":1 << 17, "MaxPacketSizeBytes":"262144", "MaxIoSizeBytes":"%s" %(1 << 20)}}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/set_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

# Expected Output : Should disable ReadAhead for this vmdk
# Note: This deletes the ReadAhead object for this vmdk 
print ("Send POST set_read_ahead_local_config with no config & enable=false")
data1 = {"ReadAhead":{"Enabled":"false"}}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/set_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

# Expected Output : Should enable ReadAhead for this vmdk and apply the current global config in memory
# Note: It first deletes the ReadAhead object this vmdk and reinstate it with current global config
print ("Send POST set_read_ahead_local_config with no config & enable=true")
data1 = {"ReadAhead":{"Enabled":"true"}}

print(data1)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/set_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.status_code)
print()

print ("Send GET get_read_ahead_local_config")
r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_read_ahead_local_config/?vmdk-id=%s" %(h,VmdkID), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
print(r.json())
print()

