#include <iterator>
#include <vector>
#include <map>
#include <algorithm>
#include "Request.h"
#include "Vmdk.h"
#include "ReadAhead.h"
#include "prefetch/ghb.h"
#include "DaemonUtils.h"
#include "limits.h"

using namespace pio;

std::mutex ReadAhead::prediction_mutex_;
bool ReadAhead::initialized_ = false;
ghb_params_t ReadAhead::ghb_params_ = {};
ghb_t ReadAhead::ghb_ = {};

ReadAhead::ReadAhead(ActiveVmdk* vmdkp)
		: force_disable_read_ahead_(false), prefetch_depth_(0), start_index_(32),
		loopback_(8), n_history_(MAX_PENDING_IOS_), max_offset_(0), vmdkp_(vmdkp) {
	if(not vmdkp_) {
		LOG(ERROR) <<  __func__  << "vmdkp is passed as nullptr, cannot construct ReadAhead object";
		throw std::runtime_error("At __func__ vmdkp is passed as nullptr, cannot construct ReadAhead object");
	}
	InitializeEssentials();
}

ReadAhead::~ReadAhead() {}

void ReadAhead::RefreshGHB() {
	if(initialized_) {
		ghb_finalize(&ghb_);
	}
	InitializeGHB();
}

void ReadAhead::InitializeGHB() {
	ghb_params_ = {};
	ghb_params_.n_index = start_index_;
	ghb_params_.n_history = n_history_;
	ghb_params_.n_lookback = loopback_;
	ghb_params_.prefetch_depth = prefetch_depth_;
	ghb_ = {};
	ghb_init(&ghb_, &ghb_params_);
	initialized_ = true;
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Run(ReqBlockVec& offsets, Request* request) {
	std::vector<int64_t> rh_offsets;
	auto results = std::make_unique<ReadResultVec>();
	
	// Filter out read ahead missed blocks if any
	if(not request->IsReadAheadRequired()) {
		return folly::makeFuture(std::move(results));
	}
	
	// Update stats
	UpdateTotalReadMissBlocks(offsets.size());

	for(const auto& offset : offsets) {
		int64_t an_offset = offset->GetOffset();
		if(an_offset < max_offset_) {
			rh_offsets.push_back(an_offset);
		}	
	}
	
	return RunPredictions(rh_offsets);
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Run(ReqBlockVec& offsets, const std::vector<std::unique_ptr<Request>>& requests) {
	std::vector<int64_t> rh_offsets;
	auto results = std::make_unique<ReadResultVec>();
	
	// Filter out read ahead missed blocks if any
	bool contains_rh_blocks = false, contains_app_blocks = false;
	for(const auto& req : requests) {
		if(not req->IsReadAheadRequired()) {
			contains_rh_blocks = true;
			continue;
		}
		contains_app_blocks = true;
	}
	assert(contains_rh_blocks != contains_app_blocks);
	(void)contains_app_blocks;
	
	if(contains_rh_blocks) {
		return folly::makeFuture(std::move(results));	
	}
	
	// Update stats
	UpdateTotalReadMissBlocks(offsets.size());
	
	for(const auto& offset : offsets) {
		int64_t an_offset = offset->GetOffset();
		if(an_offset < max_offset_) {
			rh_offsets.push_back(an_offset);
		}	
	}
	
	return (rh_offsets.empty() ? folly::makeFuture(std::move(results)) : RunPredictions(rh_offsets));
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::RunPredictions(std::vector<int64_t>& offsets) {
	auto block_size = vmdkp_->BlockSize();
	std::map<int64_t, bool> predictions;
	auto results = std::make_unique<ReadResultVec>();

	std::unique_lock<std::mutex> io_lock(pending_ios_mutex_);
	if(pending_ios_.size() + offsets.size() >= MAX_PENDING_IOS_) {
		// Should rarely occur
		LOG(WARNING) << "ReadAhead queue is full cannot serve this time. Should be very rare though.";
		return folly::makeFuture(std::move(results));
	}

	std::for_each(offsets.begin(), offsets.end(), [&](int64_t offset){
		pending_ios_.insert(std::pair<int64_t, bool>(offset, true));
	});
	
	std::unique_lock<std::mutex> prediction_lock(prediction_mutex_, std::defer_lock);
	if(!prediction_lock.try_lock()) {
		return folly::makeFuture(std::move(results));
	}
	std::map<int64_t, bool> local_offsets;
	if(pending_ios_.size() >= PENDING_IOS_SERVE_SIZE) {
		local_offsets = std::move(pending_ios_);
		pending_ios_.clear();
	}
	io_lock.unlock();
	
	RefreshGHB();
	
	uint64_t* prefetch_lbas = new uint64_t[prefetch_depth_];
	for(auto it = local_offsets.begin(); it != local_offsets.end(); ++it) {
		int n_prefetch = ghb_update_and_query(&ghb_, 1, it->first, prefetch_lbas);
		if(n_prefetch) {
			for(int i = 0; i < n_prefetch; ++i) {
				auto offset = prefetch_lbas[i];
				if(not IsBlockSizeAlgined(offset, block_size)) {
					offset = AlignDownToBlockSize(offset, block_size);
				}
				if(((int64_t)(offset + block_size) < max_offset_) 
					and (offset >= block_size)) {
					predictions.insert(std::pair<int64_t, bool>(offset, true));
				}
			}
		}
	}
	prediction_lock.unlock();
	
	delete[] prefetch_lbas;
	if(predictions.size() > 0) {
		for(auto it = offsets.begin(); it != offsets.end(); ++it) {
			auto it_predictions = predictions.find(*it);
			if(it_predictions != predictions.end()) {
				predictions.erase(it_predictions);
			}
		}
		auto pred_size = predictions.size();
		if(pred_size > (size_t)prefetch_depth_) {
			// Should rarely occur
        	auto extra = pred_size - prefetch_depth_;
        	auto rit = predictions.rbegin();
        	while(extra--) {
            	predictions.erase(--rit++.base());
        	}
    	}
		pred_size = predictions.size();
		assert((pred_size * block_size) <= MAX_PREDICTION_SIZE);
		
		// Update stats
		UpdateTotalReadAheadBlocks(pred_size);	
		
		return Read(predictions);
	}
	
	return folly::makeFuture(std::move(results));
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Read(std::map<int64_t, bool>& predictions) {
	assert(not predictions.empty());
	ReadRequestVec requests;
	
	CoalesceRequests(predictions, requests);

	auto vmp = vmdkp_->GetVM();
	assert(vmp);
	
	return vmp->BulkRead(vmdkp_, requests.begin(), requests.end(), false);
}

void ReadAhead::CoalesceRequests(/*[In]*/std::map<int64_t, bool>& predictions, 
				/*[Out]*/ReadRequestVec& requests) {
    int32_t req_id = 0;
    float block_size = (float)vmdkp_->BlockSize();
    int32_t num_blocks = 1;
	auto predictions_len = predictions.size();
	ReadRequest a_request = {};
	
	if(predictions_len < 1) {
		return;
	}
	if(predictions_len == 1) {
		a_request.reqid = ++req_id;
		a_request.size = block_size;
		a_request.offset = predictions.begin()->first;
		requests.emplace_back(a_request);
		return;
	}
	predictions.insert(std::pair<int64_t, bool>(LONG_MAX, true));
    auto start_offset = predictions.begin();
    int64_t total_size = 0;
    for(auto it = ++predictions.begin(); it != predictions.end(); ++it) {
        float size = (float)(it->first - start_offset->first) / (float)num_blocks;
        if(it->first == LONG_MAX) {
            total_size += block_size;
        }
        else {
            total_size += size;
        }
        if((size == block_size) and (total_size < MAX_PACKET_SIZE)) {
            ++num_blocks;
        }
        else {
            if(num_blocks > 1) {
                a_request.size = num_blocks * block_size;
            }
            else {
                a_request.size = block_size;
            }
            a_request.reqid = ++req_id;
            a_request.offset = start_offset->first;
            requests.emplace_back(a_request);
            start_offset = it;
            num_blocks = 1;
            total_size = 0;
            a_request = {};
        }
    }
    predictions.erase(LONG_MAX);
}

void ReadAhead::InitializeEssentials() {
	auto disk_size = vmdkp_->GetDiskSize();
	if(disk_size < MAX_DISK_SIZE_SUPPORTED) {
    	force_disable_read_ahead_ = true;
    	LOG(WARNING) << "For VmdkID = " << vmdkp_->GetID() << ", Disk Size = " << disk_size <<
    			" is too small to participate in ReadAhead. ReadAhead disabled for this vmdk";
		return;
	}
	// Initialize prefetch_depth_ for prediction
    auto block_size = vmdkp_->BlockSize();
    force_disable_read_ahead_ = false;
    prefetch_depth_ = MAX_PREDICTION_SIZE / block_size;
    if(prefetch_depth_ < 1) {
		force_disable_read_ahead_ = true;
        LOG(WARNING) << "For VmdkID = " << vmdkp_->GetID()  <<
                 ", Prefetch Depth is < 1. ReadAhead disabled for this vmdk";
        return;
     }
     // Initialize max_offset_ to check for disk boundary
     // Unread Area = Predictability size
     int64_t adjust_safety = (prefetch_depth_ * block_size) + block_size;
     if(disk_size > adjust_safety) {
		max_offset_ = disk_size - adjust_safety;
        if(not IsBlockSizeAlgined(max_offset_, block_size)) {
        	max_offset_ = AlignDownToBlockSize(max_offset_, block_size);
        }
 	
		return;
	}
    force_disable_read_ahead_ = true;
    LOG(WARNING) << "For VmdkID = " << vmdkp_->GetID() << ", Disk Size = " << disk_size <<
    		" is too small to participate in ReadAhead. ReadAhead disabled for this vmdk";
}

void ReadAhead::UpdateTotalReadMissBlocks(int64_t size) {
	if(st_read_ahead_stats_.stats_rh_read_misses_ + size >= ULONG_MAX - 10) {
		LOG(INFO) << "Resetting stats_rh_read_misses_ counter, current value = [" 
				<< st_read_ahead_stats_.stats_rh_read_misses_ << "].";
		st_read_ahead_stats_.stats_rh_read_misses_ = 0;
	}
	st_read_ahead_stats_.stats_rh_read_misses_ += size;
}

void ReadAhead::UpdateTotalReadAheadBlocks(int64_t size) {
	if(st_read_ahead_stats_.stats_rh_blocks_size_ + size >= ULONG_MAX - 10) {
		LOG(INFO) << "Resetting stats_rh_blocks_size_ counter, current value = [" 
					<< st_read_ahead_stats_.stats_rh_blocks_size_ << "].";
		st_read_ahead_stats_.stats_rh_blocks_size_ = 0;
	}
	st_read_ahead_stats_.stats_rh_blocks_size_ += size;
}
