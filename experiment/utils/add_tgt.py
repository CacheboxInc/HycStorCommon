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

AeroClusterID="1"
TargetName="tgt2"

VmId="2"
VmdkID="2"
TargetID="%s" %VmId
LunID="%s" %VmdkID

size_in_gb="1" #Size in GB
DevName="iscsi-%s-disk_%s" %(TargetName, LunID)
DevPath="/var/hyc/%s" %(DevName)
cmd="truncate --size=%sG %s" %(size_in_gb, DevPath)

print ("DevPath: %s" %DevPath)
print ("Cmd: %s" %cmd)
os.system(cmd);

if len(sys.argv) > 1:
    if sys.argv[1].lower() == "https" :
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        h = "https"
        cert=('./cert/cert.pem', './cert/key.pem')

headers = {'Content-type': 'application/json'}
params = OrderedDict([('first', 1), ('second', 2), ('third', 3)])
data = { "service_type": "test_server", "service_instance" : 0, "etcd_ips" : ["3213213", "213213"]}

# POST call 2 to stord_svc
data1 = { "vmid": "%s" %VmId, "TargetID": "%s" %TargetID, "TargetName": "%s" %TargetName, "AeroClusterID":"%s" %AeroClusterID}
print ("Send POST stord_svc new_vm %s" %VmId)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vm/?vm-id=%s" %(h, VmId), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 3 to stord_svc
data2 = {"TargetID":"%s" %TargetID,"LunID":"%s" %LunID,"DevPath":"%s" %DevPath,"VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID,"BlockSize":"4096","Compression":{"Enabled":"false"},"Encryption":{"Enabled":"false"},"RamCache":{"Enabled":"false","MemoryInMB":"1024"},"FileCache":{"Enabled":"false"},"SuccessHandler":{"Enabled":"true"}}
print ("Send POST stord_svc new_vmdk %s" %VmdkID)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/new_vmdk/?vm-id=%s&vmdk-id=%s" % (h, VmId, VmdkID), data=json.dumps(data2), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 4 to tgt_svc
print ("Send POST tgt_svc target_create %s" %TargetName)
data1 = {"TargetName": "%s" %TargetName}
r = requests.post("%s://127.0.0.1:9001/tgt_svc/v1.0/target_create/?tid=%s" % (h, TargetID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

# POST call 5 to tgt_svc
print ("Send POST tgt_svc lun_create %s" %LunID)
data2 = {"DevName": "%s" %(DevName), "VmID":"%s" %VmId, "VmdkID":"%s" %VmdkID, "LunSize":"%s" %size_in_gb}
r = requests.post("%s://127.0.0.1:9001/tgt_svc/v1.0/lun_create/?tid=%s&lid=%s" % (h, TargetID, LunID), data=json.dumps(data2), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)
