#!/bin/bash
ID=2
if [ "$#" -ne 0 ];then
	ID=$1
fi

TargetName="tgt${ID}"
TargetIP="192.168.5.138"
#iscsiadm -m node --logout
#iscsiadm -m node -o delete

tgtadm --lld iscsi --op show --mode target | grep "Target" | grep $TargetName
if [ $? -eq 0 ]; then
	echo "Targetname $TargetName already exists.."
	exit 0
fi

python3 add_tgt.py $ID

#iscsiadm --mode discovery --type sendtargets --portal $TargetIP
#iscsiadm -m node -T $TargetName --login
#lsblk
#for ((i=2;i<8;i++)) do echo "i::$i" ; bash add_tgt.sh $i & done
