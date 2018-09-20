#include "Singleton.h"
#include "ScanManager.h"
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/aerospike_info.h>

namespace pio {

const auto kMaxVmdkstoScan = 32;
const std::string kUdfModuleName = "hyc_delete_rec_module";
const std::string kUdffnName = "hyc_delete_rec";

ScanInstance::ScanInstance(AeroClusterID cluster_id):cluster_id_(cluster_id) {
	start_time_ = std::chrono::steady_clock::now();
}

ScanInstance::~ScanInstance() {
}

/*
 * Input : \"vmdkid:ckptid:offset\"
 */

uint8_t GetObjects(const char *res, uint32_t& vmdkid, uint32_t& ckpt_id, uint64_t& offset) {

	if (res == NULL || strlen(res) == 0 ) {
		return 1;
	}

	std::string temp = res;

	std::size_t first = temp.find_first_of(":");
	if (first == std::string::npos)
		return 1;

	/* First char is "\""(double quotes), start copying from 2nd char */
	std::string strNew = temp.substr(1, first);
	vmdkid = stoi(strNew);

	std::size_t second = temp.find_first_of(":", first + 1);
	if (second == std::string::npos)
		return 1;

	strNew = temp.substr(first + 1, second);
	ckpt_id = stoi(strNew);

	/* Last char is "\""(double quotes) */
	strNew = temp.substr(second + 1, temp.length() - 1);
	offset = stol(strNew);
	return 0;
}

/* Callback of following scan call.
 * aerospike_scan_foreach(&aero_conn->as_, &err, &policy, scan, scan_cb, (void *) NULL) !=
 *                       AEROSPIKE_OK)
 */

bool scan_cb(const as_val* p_val, void* udata)
{
	bool ret = true;
	if (!p_val) {
		LOG(ERROR) << __func__ << "Scan callback returned null - scan is complete";
		return false;
	}

	as_record* p_rec = as_record_fromval(p_val);
	if (! p_rec) {
		LOG(ERROR) << __func__ << " AeroSpike Scan callback returned non-as_record object";
		return false;
	}

	if (p_rec->key.valuep) {
		/* Extract the key from record */
		char *key_val_str = as_val_tostring(p_rec->key.valuep);
		uint32_t vmdkid, ckpt_id;
		uint64_t offset;
		if (!GetObjects(key_val_str, vmdkid, ckpt_id, offset)) {
			LOG(ERROR) << __func__ << " VMDK ID:"  << vmdkid << ",ckpt:" << ckpt_id << ",offset:" << offset;
		} else {
			LOG(ERROR) << __func__ << " Error, Invalid format";
			ret = false;
		}
		free(key_val_str);
	} else {
		LOG(ERROR) << __func__ << " Error, Key is not part of record";
		ret = false;
	}

	as_record_destroy(p_rec);
	return ret;
}

int ScanInstance::ScanTask() {
	LOG(ERROR) << __func__ << " Scan task start";
	as_scan *scan;
	as_error err;
	as_monitor monitor;
	scan_id_ = 0;

	/* initialise scan policy */
	as_policy_scan policy;
	as_policy_scan_init(&policy);
	policy.fail_on_cluster_change = false;

	/* Scanning whole namespace */
	scan = as_scan_new(kAsNamespaceCacheClean.c_str(), NULL);
	as_monitor_init(&monitor);

	/* We don't require any bin data */
	as_scan_set_nobins(scan, true);

	/* Create argument list (VMDKids) for UDF */
	as_arraylist arglist;
	as_arraylist_init(&arglist, working_list_.size(), 0);
	for (auto entry : working_list_) {
		LOG(ERROR) << __func__ << " Adding entry in UDF arglist::" << entry;
		as_arraylist_append_int64(&arglist, entry);
	}

	/* Lua has number of arguments limitations (< 50), encapsulate all the
	 * args in a single map */
	as_arraylist arg_c;
	as_arraylist_inita(&arg_c, 1);
	as_arraylist_append_list(&arg_c, (as_list *) &arglist);

	/* Apply UDF so that record will be deleted on server itself */
	as_scan_apply_each(scan, kUdfModuleName.c_str(), kUdffnName.c_str(),
					(as_list *) &arg_c);

	auto ret = 0;
	if (aerospike_scan_background(&aero_conn_->as_, &err,
			NULL, scan, &scan_id_) != AEROSPIKE_OK) {
		LOG(ERROR) << __func__ << " aerospike_scan start failed, err code::"
			<<  err.code << ", err msg::" << err.message;
		ret = err.code;
	} else {
		/* Wait for scan to finish */
		LOG(INFO) << __func__ << " aerospike scan started sucessfully.";
		aerospike_scan_wait(&aero_conn_->as_, &err, NULL, scan_id_, 0);
		if (err.code != AEROSPIKE_OK) {
			LOG(ERROR) << " aerospike scan  run failed,"
				" err code::" <<  err.code
				<< ", err msg::" << err.message;
			ret = err.code;
		} else {
			LOG(ERROR) << __func__ << " aerospike scan completed sucessfully.";
		}
	}

	LOG(INFO) << __func__ << " : ... Scan destory";
	as_scan_destroy(scan);
	as_monitor_destroy(&monitor);
	as_arraylist_destroy(&arglist);
	as_arraylist_destroy(&arg_c);
	return ret;
}

int ScanInstance::ScanStatus(ScanStats &scan_stats) {
	scan_stats.start_time_ = start_time_;
	as_scan_info scan_info;
	as_error err;
	if (aerospike_scan_info(&aero_conn_->as_, &err, NULL,
			scan_id_, &scan_info) != AEROSPIKE_OK) {
		LOG(ERROR) << __func__ << " aerospike scan info error, err code::"
				<<  err.code << ", err msg::" << err.message;
		return 1;
	}

	scan_stats.records_scanned = scan_info.records_scanned;
	scan_stats.progress_pct = scan_info.progress_pct;
	LOG(ERROR) << __func__ << " Scan id::" << scan_id_ << ", status::"
		<< scan_info.status << ", percent::" << scan_info.progress_pct;
	return 0;
}

int ScanInstance::StartScanThread() {
	LOG(ERROR) << __func__ << " Creating new scan thread";
	std::thread thread = std::thread(&ScanInstance::Scanfn, this);

	/* Mark thread detached */
	thread.detach();
	return 0;
}

void ScanInstance::Scanfn(void) {
	auto managerp = SingletonHolder<ScanManager>::GetInstance();
	auto rc = 0;

	std::unique_lock<std::mutex> scan_lock(managerp->lock_);
	while(true) {

		/* Create a working list for this instance,
		 * max 32 VMDKs to process in one go */
		auto count = kMaxVmdkstoScan - working_list_.size();
		if (count > pending_list_.size()) {
			count = pending_list_.size();
		}

		/* Get entries to process from pending list */
		auto eit = pending_list_.begin();
		std::advance(eit, count);
		std::copy(pending_list_.begin(), eit, std::back_inserter(working_list_));

		scan_lock.unlock();
		rc = ScanTask();
		scan_lock.lock();
		if (pio_unlikely(!rc)) {
			/* SUCCESS, remove working_list entries from pending_list */
			std::sort(pending_list_.begin(), pending_list_.end());
			std::sort(working_list_.begin(), working_list_.end());
			std::vector<uint64_t> tmp_list;
			std::set_symmetric_difference(pending_list_.begin(),
				pending_list_.end(), working_list_.begin(),
				working_list_.end(), std::back_inserter(tmp_list));
			pending_list_.clear();
			pending_list_ = tmp_list;
		}

		working_list_.clear();
		/* Do we have remaining entry to process or previous scan failed
		 * then start all over again */
		if (pio_unlikely(!pending_list_.size())) {
			LOG(ERROR) << __func__ << " Calling FreeInstance...";
			managerp->FreeInstance(cluster_id_);
			scan_lock.unlock();
			break;
		}
	}
}
}
