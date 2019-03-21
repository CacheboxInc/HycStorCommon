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

     data1 = {"vmid": "%s" %VmID , "FlushAllowed" : "true", "MoveAllowed" : "false"}
     print ("Send POST stord_svc flush_req %s" %VmID)
     r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=%s" % (h, VmID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

     while(flush_running()):
        time.sleep(3)

     data1 = {"vmid": "%s" %VmID , "FlushAllowed" : "false", "MoveAllowed" : "true"}
     print ("Send POST stord_svc flush_req %s" %VmID)
     r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=%s" % (h, VmID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

     while(flush_running()):
        time.sleep(3)

else:
    for i in range(2, 10):
        seek_offset = 10* i*1024
        seek_offset = 0

	# Create input file with pattern
        filename = "/tmp/input_%s" %i
        print (filename)
        f = open(filename, "w")
        for j in range(0, 16384):
              f.write(str(i))
        f.flush()
        f.close()

        cmd = "dd if=%s of=/dev/sdf bs=16k count=1 oflag=direct seek=%s" %(filename, seek_offset)
        print (cmd)
        os.system(cmd)

        data1 = {"vmid": "%s" %VmID , "FlushAllowed" : "true", "MoveAllowed" : "false"}
        print ("Send POST stord_svc flush_req %s" %VmID)
        r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=%s" % (h, VmID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

        while(flush_running()):
            time.sleep(3)

        data1 = {"vmid": "%s" %VmID , "FlushAllowed" : "false", "MoveAllowed" : "true"}
        print ("Send POST stord_svc flush_req %s" %VmID)
        r = requests.post("%s://127.0.0.1:9000/stord_svc/v1.0/flush_req/?vm-id=%s" % (h, VmID), data=json.dumps(data1), headers=headers, cert=cert, verify=False)

        while(flush_running()):
            time.sleep(3)

        ckpt_ids = [i]
        snapshot_id = i
        data = {"checkpoint-ids": "%s" %ckpt_ids, "snapshot-id": "%s" %snapshot_id}
        r = requests.post("http://127.0.0.1:9000/stord_svc/v1.0/serialize_checkpoints/?vm-id=1",
                data=json.dumps(data), headers=headers, cert=None, verify=False)
        if r.status_code == 200:
            print("moving ahead")
        else:
            print("error")

        si = i + 1
        url = "http://127.0.0.1:9000/stord_svc/v1.0/new_delta_context/?vm-id=1&snap-id=%s" %si
        r = requests.post(url, headers=headers, cert=None, verify=False)
