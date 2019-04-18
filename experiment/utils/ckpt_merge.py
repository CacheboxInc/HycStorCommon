# PREREQUISITES,
# Need stord to be started with ha port 9000
#./src/stord/./stord -etcd_ip="http://127.0.0.1:2379" -stord_version="v1.0" -svc_label="stord_svc" -ha_svc_port=9000
# Need tgtd to be started with ha port 9001
#./usr/tgtd -f -e "http://127.0.0.1:2379" -s "tgt_svc" -v "v1.0" -p 9001 -D "127.0.0.1" -P 9876

import json
import requests
import time
import sys

from collections import OrderedDict
from urllib.parse import urlencode

h = "http"
cert = None

if len(sys.argv) != 3:
	print ("Please provide vmid and ckptid, <VmID> <CkptID>");
	sys.exit(1)

VmID=sys.argv[1]
ckptid=sys.argv[2]
headers = {'Content-type': 'application/json'}
params = OrderedDict([('first', 1), ('second', 2), ('third', 3)])
data = { "service_type": "test_server", "service_instance" : 0, "etcd_ips" : ["3213213", "213213"]}

# POST call 1 to stord_svc
print ("Send POST stord_svc merge_req %s" %VmID)
r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/merge_req/?vm-id=%s&ckpt-id=%s" %(h, VmID, ckptid), headers=headers, cert=cert, verify=False)
assert (r.status_code == 200)

def merge_running():
    r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/merge_status/?vm-id=%s" % (h, VmID))
    a = r.json()
    print(a)
    if (a["merge_running"]):
        "returning true"
        return True
    else:
        "returning false"
        return False

while(merge_running()):
    print ("Merge is still running for vmid:%s" %VmID)
    time.sleep(3)
