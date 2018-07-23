#!/bin/bash
TargetName="tgt2"
TargetIP="192.168.5.138"
#iscsiadm -m node --logout
#iscsiadm -m node -o delete

tgtadm --lld iscsi --op show --mode target | grep "Target" | grep $TargetName
if [ $? -eq 0 ]; then
	echo "Targetname $TargetName already exists.."
	exit 0
fi

python3 add_tgt.py

iscsiadm --mode discovery --type sendtargets --portal $TargetIP
iscsiadm -m node -T $TargetName --login
lsblk
