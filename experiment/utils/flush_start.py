# PREREQUISITES,
# Need stord to be started with ha port 9000
#./src/stord/./stord -etcd_ip="http://127.0.0.1:2379" -stord_version="v1.0" -svc_label="stord_svc" -ha_svc_port=9000
# Need tgtd to be started with ha port 9001
#./usr/tgtd -f -e "http://127.0.0.1:2379" -s "tgt_svc" -v "v1.0" -p 9001 -D "127.0.0.1" -P 9876

import json
import requests
import time
import sys, os

from collections import OrderedDict
from urllib.parse import urlencode

h = "http"
cert = None
VmID="1"

RTO=True
#RTO=False

if len(sys.argv) > 1:
	VmID=sys.argv[1]

headers = {'Content-type': 'application/json'}
params = OrderedDict([('first', 1), ('second', 2), ('third', 3)])
data = { "service_type": "test_server", "service_instance" : 0, "etcd_ips" : ["3213213", "213213"]}

def flush_running():
    data1 = {"vmid": "%s" %VmID}
    print ("Send GET stord_svc flush_status 1")
    r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/flush_status/?vm-id=1" % h)
    a = r.json()
    print(a)

    if (a["flush_running"]):
        "returning true"
        return True
    else:
        "returning false"
        return False


# POST call 1 to stord_svc
if not RTO:

     r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_unflushed_checkpoints/?vm-id=%s" %(h, VmID))
     assert (r.status_code == 200)
     ckpt_ids = r.json()["unflushed_checkpoints"]

     data1 = {"vmid": "%s" %VmID , "FlushAllowed" : "true", "MoveAllowed" : "false"}
     print ("Send POST stord_svc flush_req %s" %VmID)
     r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=%s" % (h, VmID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

     while(flush_running()):
        time.sleep(3)

     snapshot_id = 0
     print ("Serialize : %s" %ckpt_ids)
     data = {"checkpoint-ids": "%s" %ckpt_ids, "snapshot-id": "%s" %snapshot_id}
     r = requests.post("http://127.0.0.1:9000/stord_svc/v1.0/serialize_checkpoints/?vm-id=1",
               data=json.dumps(data), headers=headers, cert=None, verify=False)
     if r.status_code == 200:
           print("moving ahead")
     else:
           print("error")

     data1 = {"vmid": "%s" %VmID , "FlushAllowed" : "false", "MoveAllowed" : "true"}
     print ("Send POST stord_svc flush_req %s" %VmID)
     r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=%s" % (h, VmID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

     while(flush_running()):
        time.sleep(3)

else:
    merge_no = 2
    for i in range(2, 30):
        print ("Sleeping............")
        time.sleep(30)

        r = requests.get("%s://127.0.0.1:9000/stord_svc/v1.0/get_unflushed_checkpoints/?vm-id=%s" %(h, VmID))
        assert (r.status_code == 200)
        ckpt_ids = r.json()["unflushed_checkpoints"]

        data1 = {"vmid": "%s" %VmID , "FlushAllowed" : "true", "MoveAllowed" : "false"}
        print ("Send POST stord_svc flush_req %s" %VmID)
        r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=%s" % (h, VmID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

        while(flush_running()):
            time.sleep(3)

        snapshot_id = 0
        print ("Serialize : %s" %ckpt_ids)
        data = {"checkpoint-ids": "%s" %ckpt_ids, "snapshot-id": "%s" %snapshot_id}
        r = requests.post("http://127.0.0.1:9000/stord_svc/v1.0/serialize_checkpoints/?vm-id=1",
                data=json.dumps(data), headers=headers, cert=None, verify=False)
        if r.status_code == 200:
            print("moving ahead")
        else:
            print("error")

        data1 = {"vmid": "%s" %VmID , "FlushAllowed" : "false", "MoveAllowed" : "true"}
        print ("Send POST stord_svc flush_req %s" %VmID)
        r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=%s" % (h, VmID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

        while(flush_running()):
            time.sleep(3)

        if i >= 5:
            print ("Starting merge for ckpt_id : %s..." %merge_no)
            cmd = "python3 ckpt_merge.py %s %s" %(VmID, merge_no)
            print ("Cmd: %s" %cmd)
            os.system(cmd)
            merge_no = merge_no + 1
