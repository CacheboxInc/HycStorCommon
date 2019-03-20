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

ReadAhead::ReadAhead(ActiveVmdk* vmdkp) 
		: vmdkp_(vmdkp) {
	if(vmdkp_ == NULL) {
		LOG(ERROR) <<  __func__  << "vmdkp is passed as nullptr, cannot construct ReadAhead object";
		throw std::runtime_error("At __func__ vmdkp is passed as nullptr, cannot construct ReadAhead object");
	}
	InitializeEssentials();
}

ReadAhead::~ReadAhead() {
	if(initialized_) {
		ghb_finalize(&ghb_);	
	}
}

void ReadAhead::InitializeGHB() {
	ghb_params_.n_index = start_index_;
	ghb_params_.n_history = n_history_;
	ghb_params_.n_lookback = loopback_;
	ghb_init(&ghb_, &ghb_params_);
	initialized_ = true;
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Run(ReqBlockVec& req_blocks, Request* request) {
	auto results = std::make_unique<ReadResultVec>();
	uint32_t num_blocks = request->NumberOfRequestBlocks();
	assert(!req_blocks.empty());
	
	// Filter out read ahead missed blocks if any
	if(!request->IsReadAheadRequired()) {
		return folly::makeFuture(std::move(results));
	}
	
	// Update stats
	UpdateTotalReadMissBlocks(req_blocks.size());
	
	// Large IO, no ReadAhead
	if((num_blocks * vmdkp_->BlockSize()) >= MAX_IO_SIZE) {
		return folly::makeFuture(std::move(results));
	}
	return RunPredictions(req_blocks, num_blocks);
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Run(ReqBlockVec& req_blocks, const std::vector<std::unique_ptr<Request>>& requests) {
	auto results = std::make_unique<ReadResultVec>();
	uint32_t num_blocks = 0;
	assert(!req_blocks.empty());
	
	// Filter out read ahead missed blocks if any
	bool contains_rh_blocks = false, contains_app_blocks = false;
	for(const auto& req : requests) {
		if(!req->IsReadAheadRequired()) {
			contains_rh_blocks = true;
			continue;
		}
		contains_app_blocks = true;
		num_blocks += req->NumberOfRequestBlocks();
	}
	assert(contains_rh_blocks != contains_app_blocks);
	(void)contains_app_blocks;
	
	if(contains_rh_blocks) {
		return folly::makeFuture(std::move(results));	
	}
	
	// Update stats
	UpdateTotalReadMissBlocks(req_blocks.size());
	
	// Large IO, no ReadAhead
	if((num_blocks * vmdkp_->BlockSize()) >= MAX_IO_SIZE) {
		return folly::makeFuture(std::move(results));
	}
	return RunPredictions(req_blocks, num_blocks);
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::RunPredictions(ReqBlockVec& req_blocks, uint32_t io_block_count) {
	assert((io_block_count > 0) && !req_blocks.empty());
	auto block_size = vmdkp_->BlockSize();
	uint64_t* prefetch_lbas = NULL;
	int n_prefetch = 0, idx = 0;
	std::map<int64_t, bool> predictions;
	auto results = std::make_unique<ReadResultVec>();

	// Update history
	std::unique_lock<std::mutex> prediction_lock(prediction_mutex_);
	for(const auto& block : req_blocks) {
	 	auto an_offset = block->GetOffset(); 
		idx = get_index(&ghb_, 1, an_offset);
		update_index_and_history(&ghb_, idx, 1, an_offset);
	}
	// Should prefetch ?	
	if(ShouldPrefetch(req_blocks.size(), io_block_count)) {
		prefetch_lbas = new uint64_t[max_prefetch_depth_];
		bool is_strided = false;
		// Prefetch 
		for(const auto& block : req_blocks) {
	 		auto an_offset = block->GetOffset();
			idx = get_index(&ghb_, 1, an_offset);
			n_prefetch = query_history(&ghb_, idx, prefetch_lbas, max_prefetch_depth_, &is_strided);
		}
		// Determine actual prefetch quantum based on pattern stability
		n_prefetch = UpdatePrefetchDepth(n_prefetch, is_strided);
	}
	prediction_lock.unlock();
	
	// Sanitize predicted offsets and build final list of LBAs to submit
	for(int i = 0; i < n_prefetch; ++i) {
		auto offset = prefetch_lbas[i];
		if(!IsBlockSizeAlgined(offset, block_size)) {
			offset = AlignDownToBlockSize(offset, block_size);
		}
		if(((int64_t)offset <= max_offset_) 
			&& (offset >= block_size)) {
			predictions.insert(std::pair<int64_t, bool>(offset, true));
		}
	}
	delete[] prefetch_lbas;
	prefetch_lbas = NULL;
	
	if(predictions.size() > 0) {
		for(const auto& block : req_blocks) {
			auto it_predictions = predictions.find(block->GetOffset());
			if(it_predictions != predictions.end()) {
				// Should rarely occur, probably this will be removed after thorough testing
				predictions.erase(it_predictions);
			}
		}
		auto pred_size = predictions.size();
		auto pred_size_bytes = pred_size * block_size;
		assert(pred_size_bytes <= MAX_PREDICTION_SIZE); 
		if(pred_size_bytes < MIN_PREDICTION_SIZE) {
			// No read submission if we have found many missed offsets in predicted
			// list of offsets. This should be rare though
			return folly::makeFuture(std::move(results));
		}
		
		// Update stats
		UpdateTotalReadAheadBlocks(pred_size);	
		
		return Read(predictions);
	}
	
	return folly::makeFuture(std::move(results));
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Read(std::map<int64_t, bool>& predictions) {
	assert(!predictions.empty());
	ReadRequestVec requests;
	requests.reserve(predictions.size());
	
	// Coalesce sequential LBAs to yield large single requests
	CoalesceRequests(predictions, requests);

	auto vmp = vmdkp_->GetVM();
	assert(vmp != NULL);
	
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
        if((size == block_size) && (total_size < MAX_PACKET_SIZE)) {
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

bool ReadAhead::ShouldPrefetch(uint32_t miss_count, uint32_t io_block_count) {
	//ToDo--Phase-2: Check if we are throttling or backing off
	
	// Check if Read miss threshold is hit
	if(io_miss_window_.size() >= IO_MISS_WINDOW_SIZE) {
		IOMissWindow a_window = io_miss_window_.front();
		total_io_count_ -= a_window.io_count_;
		total_miss_count_ -= a_window.miss_count_;
		io_miss_window_.pop();
	}
	total_io_count_ += io_block_count;
	total_miss_count_ += miss_count;
	
	io_miss_window_.emplace(io_block_count, miss_count);
	// Check if miss threshold is hit ?
	assert(total_io_count_ > 0);
	return ((int)((100 * total_miss_count_) / total_io_count_) >= IO_MISS_THRESHOLD_PERCENT);
}

int ReadAhead::UpdatePrefetchDepth(int n_prefetch, bool is_strided) {
	if(n_prefetch < 1) {
		// Aggregate a few RANDOM pattern occurrences to melt into one to allow
		// GHB warming up before valid patterns get detected
		if(random_pattern_occurrences_++ >= AGGREGATE_RANDOM_PATTERN_OCCURRENCES) {
            ++pattern_frequency[PatternType::INVALID];
            random_pattern_occurrences_ = 0;
        }
		return 0;
	}
	// Reset periodically to see through MAX_PATTERN_STABILITY_COUNT worth 
	// recent activity only
	if(total_pattern_count_ > MAX_PATTERN_STABILITY_COUNT) {
		memset(pattern_frequency, 0, sizeof(pattern_frequency));
		total_pattern_count_ = 0;
	}
	PatternType pattern_type = is_strided ? PatternType::STRIDED 
								: PatternType::CORRELATED;
	++pattern_frequency[pattern_type];
	++total_pattern_count_;
	auto pattern_count = pattern_frequency[pattern_type];
	if((int)((100 * pattern_count) / total_pattern_count_) > PATTERN_STABILITY_PERCENT) {
		if(last_seen_pattern_ == pattern_type) {
			prefetch_depth_ *= ((prefetch_depth_ * 2) <= max_prefetch_depth_) ? 2 : 1;
		}
	}
	else {
		prefetch_depth_ /= ((prefetch_depth_ / 2) >= min_prefetch_depth_) ? 2 : 1;
	}
	last_seen_pattern_ = pattern_type;
	assert((prefetch_depth_ >= min_prefetch_depth_) 
	&& (prefetch_depth_ <= max_prefetch_depth_));

	return prefetch_depth_;
}

void ReadAhead::InitializeEssentials() {
    auto block_size = vmdkp_->BlockSize();
	auto disk_size = vmdkp_->GetDiskSize();
	// Disk Size check
	if(disk_size < MIN_DISK_SIZE_SUPPORTED) {
    	// We should have not come this far
    	LOG(WARNING) << "For VmdkID = " << vmdkp_->GetID() << ", Disk Size = " << disk_size <<
    			" is too small to participate in ReadAhead. ReadAhead disabled for this vmdk";
		force_disable_read_ahead_ = true;
		return;
	}
	// Too big block size, no ReadAhead
	if(block_size >= MAX_IO_SIZE) {
        LOG(WARNING) << "For VmdkID = " << vmdkp_->GetID()  << ", block size : " 
				<< block_size << " is too big." << " Max block size supported is " 
				<< MAX_IO_SIZE << ". ReadAhead disabled for this vmdk.";
		force_disable_read_ahead_ = true;
        return;
	}
	// Initialize max & min prefetch depth for prediction
    assert(MAX_PREDICTION_SIZE >= MIN_PREDICTION_SIZE);
	max_prefetch_depth_ = MAX_PREDICTION_SIZE / block_size;
    min_prefetch_depth_ = MIN_PREDICTION_SIZE / block_size;
    if((max_prefetch_depth_ < 1 && min_prefetch_depth_ < 1)
	||  (max_prefetch_depth_ >= 1 && min_prefetch_depth_ < 1)) {
        LOG(ERROR) << "For VmdkID = " << vmdkp_->GetID()  <<
                 ", Prefetch Depth is < 1. ReadAhead disabled for this vmdk.";
		force_disable_read_ahead_ = true;
        return;
	}
	if((max_prefetch_depth_ < 1) && (min_prefetch_depth_ >= 1)) {
		max_prefetch_depth_ = min_prefetch_depth_;
	}
	// Initialize prefetch depth default value
	prefetch_depth_ = min_prefetch_depth_;
	
	// Initialize max offset that can qualify as prefetch candidate
	max_offset_ = AlignDownToBlockSize(disk_size - (block_size * 4), block_size);
	
	// Initialize CZONE setting for GHB, Each CZONE is 1GB
	start_index_ = (disk_size % (1<<30)) ? (disk_size >> 30) + 1 : disk_size >> 30;
	if(start_index_ < 1) {
   		// We shouldn't be seeing this after the disk size check has passed
        LOG(ERROR) << "For VmdkID = " << vmdkp_->GetID()  <<
                 ", number of CZONES is < 1. ReadAhead disabled for this vmdk";
		force_disable_read_ahead_ = true;
        return;
	}
	// Finally initialize and instantiate the GHB
	InitializeGHB();

#ifndef NDEBUG
	LogEssentials();
#endif
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

void ReadAhead::LogEssentials() {
	LOG(INFO) << "======================Absolute Config========================";
	LOG(INFO) << "MAX_PREDICTION_SIZE : " 			<< (MAX_PREDICTION_SIZE >> 20) << "MB";
	LOG(INFO) << "MIN_PREDICTION_SIZE : " 			<< (MIN_PREDICTION_SIZE >> 10) << "KB";
	LOG(INFO) << "MAX_PACKET_SIZE : " 				<< (MAX_PACKET_SIZE >> 10) << "KB";
	LOG(INFO) << "MIN_DISK_SIZE_SUPPORTED : " 		<< (MIN_DISK_SIZE_SUPPORTED >> 30) << "GB";
	LOG(INFO) << "MAX_IO_SIZE : " 					<< (MAX_IO_SIZE >> 10) << "KB";
	LOG(INFO) << "IO_MISS_WINDOW_SIZE : " 			<< IO_MISS_WINDOW_SIZE;
	LOG(INFO) << "IO_MISS_THRESHOLD_PERCENT : "		<< IO_MISS_THRESHOLD_PERCENT << "%";
	LOG(INFO) << "PATTERN_STABILITY_PERCENT : "		<< PATTERN_STABILITY_PERCENT << "%";
	LOG(INFO) << "MAX_PATTERN_STABILITY_COUNT : "	<< MAX_PATTERN_STABILITY_COUNT;
	LOG(INFO) << "AGGREGATE_RANDOM_PATTERN_OCCURRENCES : "	<< AGGREGATE_RANDOM_PATTERN_OCCURRENCES;
	LOG(INFO) << "======================Derived Config========================";
	LOG(INFO) << "Max Prefetch Depth : " 		<< max_prefetch_depth_;	
	LOG(INFO) << "Min Prefetch Depth : " 		<< min_prefetch_depth_;	
	LOG(INFO) << "Disk Size : " 				<< (vmdkp_->GetDiskSize() >> 30) << "GB";	
	LOG(INFO) << "Max Prefetch Offset : " 		<< max_offset_;	
	LOG(INFO) << "Number of CZONEs : " 			<< start_index_;	
	LOG(INFO) << "Default Prefetch Depth : " 	<< prefetch_depth_;	
	LOG(INFO) << "============================================================";	
}
