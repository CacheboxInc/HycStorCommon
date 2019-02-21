#!/usr/bin/env bash

#this script will give us /tmp/checksum.txt
#some things to consider-
#1. photon vm doesn't have iscsiadm, python. dont know how many 
#   more restrictions in future 

#design:
#1. assume offsets are given in single file
#   one offset on one line
#2. do login with tgt
#3. dd read for given offset
#4. calculate checksum of above block
#5. write offset:chksum in output file


#set -x #enable to debug

asd_ip="$1"
asd_set="$2" #set90
checksum_type="$3"
target_name="$4" #vmid like vm3471
namespace="$5"
set_type="$6" #parent if parent set else child in case of full clone
chkpt_id="$7" #2 for DIRTY


tgt_target_ip="192.168.7.13"
offsets_file="/tmp/aero_scan_output" #depends on hard-coded value in aero-scan
output_file="/tmp/checksum.txt"
block_size="16" #default:16k, if 16k then only give 16
asd_port=3000 #default 3000, as per asd docker
bs="${block_size}k"

#for hyc_run script
vmdk_id=1486

usage() {
	echo "args:"
	echo "1.asd_ip"
	echo "2.asd_set, lookout in app.py.log"
	echo "3.checksum, md5sum/crc10/crc32"
	echo "4.target_name, lookout in app.py.log" 
	echo "5.namespace" 
	echo "6.set_type, lookout in app.py.log" 
	echo "7.chkpt id"
	exit 1 
}

if [ "$#" -lt 7 ]; then 
	usage
fi


get_offsets_using_aero_scan_binary() {
		echo "do nothing for now"
		#first do make to get aero_scan binary
		#make
		./aero_scan -h $asd_ip -p $asd_port -n $namespace -s $asd_set -v $vmdk_id -c $chkpt_id
		#for hyc_run asd host ip is different
		#./aero_scan -h 172.18.0.5 -p 3000 -n CLEAN -s vm116 -v 1 -c 1
}


get_file_containing_offsets() {
		> /tmp/aql.script
		echo "set TIMEOUT 3600000" >> /tmp/aql.script
		echo "set output json" >> /tmp/aql.script
		echo "select * from ${namespace}.${asd_set}" >> /tmp/aql.script

		#FIXME it takes huge space, make sure /tmp has at least 1GB free

		> /tmp/aql.output.1
		aql -h ${asd_ip} -p ${asd_port} -f /tmp/aql.script >> /tmp/aql.output.1
		> /tmp/aql.output.2
		cat /tmp/aql.output.1 | grep "PK" >> /tmp/aql.output.2
		> ${offsets_file}
		if [ $set_type == "parent" ] ; then
				cat /tmp/aql.output.2 | cut -d"\"" -f 4  | cut -d":" -f2 >> ${offsets_file}
		else 
				cat /tmp/aql.output.2 | cut -d"\"" -f 4  | cut -d":" -f3 >> ${offsets_file}
		fi 
}


get_boot_device_path() {
		lsblk >> /tmp/1
		#FIXME verify asd_set is valid by checking if its part of output of below command
		iscsiadm --mode discovery --type sendtargets --portal $tgt_target_ip
		iscsiadm -m node -T $target_name --login
		echo "sleeping for 5 sec for new devices to appear"
		sleep 5
		lsblk >> /tmp/2
		device=`diff /tmp/1 /tmp/2 | awk "NR==2"  | cut -d" " -f2`
		dev_path="/dev/${device}"
		size=`diff /tmp/1 /tmp/2 | awk "NR==2"  | awk '{print $5}'`
		echo "we found out ${dev_path} as boot device for vm ${target_name} having size $size"
		#comment below line once u r sure we always find boot disk correctly
		read -p "Continue? (Y/N): " confirm && [[ $confirm == [nN] || $confirm == [nN][oO] ]] && exit 1
}



calculate_checksum() {
		which ${checksum_type}
		[[ $? -ne 0 ]] && echo "${checksum_type} not found under \$PATH" && exit 1
		> /tmp/output_file
		while IFS= read -r offset
		do
				offset_in_byte=`echo "$offset*512" | bc` 
				skip_blocks=`echo "$offset_in_byte/($block_size*1024)" | bc`
				dd if=$dev_path of=/tmp/block_data bs=$bs skip=$skip_blocks count=1 iflag=direct &> /dev/null 
				if [ $checksum_type == "md5sum" ] ; then
					chksum=`cat /tmp/block_data | md5sum`
				fi 
				if [ $checksum_type == "crc10" ] ; then
					chksum=`crc10 /tmp/block_data`
				fi 
				if [ $checksum_type == "crc16" ] ; then
					chksum=`crc16 /tmp/block_data`
				fi 
				echo -e "$offset_in_byte:$chksum" >> /tmp/output_file

		done < "$offsets_file"

		cat /tmp/output_file | cut -d" " -f1 > ${output_file} 
}



cleanup() {
		iscsiadm -m node  --logout
		iscsiadm -m node -o delete
		[[ -e /tmp/block_data ]] && rm /tmp/block_data
		[[ -e /tmp/output_file ]] && rm /tmp/output_file
		[[ -e /tmp/${offsets_file} ]] && rm /tmp/${offsets_file}
		[[ -e /tmp/1 ]] && rm /tmp/1
		[[ -e /tmp/2 ]] && rm /tmp/2
		[[ -e /tmp/aql.script ]] && rm /tmp/aql.script
		[[ -e /tmp/aql.output.1 ]] && rm /tmp/aql.output.1
		[[ -e /tmp/aql.output.2 ]] && rm /tmp/aql.output.2
		[[ -e /tmp/aql.output.3 ]] && rm /tmp/aql.output.3
}


check_free_space() {
		size_in_kb=`df -k /tmp | awk "NR==2"  | awk '{print $4}'`
		size_in_gb=`echo "$size_in_kb/(1024*1024)" | bc`
		if [ $size_in_gb -ge 1 ] ;then 
			echo "/tmp has ${size_in_gb}GB free space"
		else 
			echo "/tmp needs 1-2GB free space, run this script from different machine"
			exit 1
		fi 
}

cleanup
check_free_space
get_boot_device_path
get_file_containing_offsets
#get_offsets_using_aero_scan_binary
calculate_checksum
[[ -e ${output_file} ]] && echo "plz find ${output_file}"
