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
ReadAhead::ReadAheadConfig ReadAhead::g_st_config_;
std::mutex ReadAhead::g_st_config_mutex_;
ReadAhead::ReadAheadConfig ReadAhead::g_st_config_default_;
std::atomic<bool> ReadAhead::global_read_ahead_enabled_ = false;

ReadAhead::ReadAhead(ActiveVmdk* vmdkp) 
		: vmdkp_(vmdkp) {
	// Will never throw
	if(vmdkp_ == nullptr) {
		LOG(ERROR) <<  "vmdkp is passed as nullptr, cannot construct ReadAhead object";
		throw std::runtime_error("vmdkp is passed as nullptr, cannot construct ReadAhead object");
	}
	auto vmdk_config = vmdkp->GetJsonConfig();
	// Will never throw
	if(vmdk_config == nullptr) {
		LOG(ERROR) <<  "No VmdkConfig, cannot construct ReadAhead object";
		throw std::runtime_error("No VmdkConfig, cannot construct ReadAhead object");
	}	
	// Should never throw in real deployment
	if(!IsValidConfig(*vmdk_config)) {
		LOG(ERROR) <<  "Invalid config, cannot construct ReadAhead object";
		throw std::runtime_error("Invalid config, cannot construct ReadAhead object");
	}
	// No protection required
	st_config_ = g_st_config_;
	st_config_ = *vmdk_config;
	
	// Should not throw in general
	if(!InitializeEssentials()) {
		throw std::runtime_error(string("Failed to derive runtime essential params required by ReadAhead.") 
		 + string("This should have not happened generally. Please check ERROR log for more details."));
	}
	// Finally initialize and instantiate the GHB
	InitializeGHB();

#ifndef NDEBUG
	LogEssentials();
#endif
}

ReadAhead::~ReadAhead() {
	ghb_finalize(&ghb_);
}

void ReadAhead::InitializeGHB() {
	ghb_params_.n_index = start_index_;
	ghb_params_.n_history = st_config_.ghb_history_length_;
	ghb_params_.n_lookback = loopback_;
	ghb_init(&ghb_, &ghb_params_);
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Run(ReqBlockVec& req_blocks, Request* request) {
	std::unique_ptr<ReadResultVec> results;
	auto num_blocks = request->NumberOfRequestBlocks();
	assert(!req_blocks.empty());
	assert(request != nullptr);
	
	// Update stats
	UpdateReadMissStats(req_blocks.size());
	
	// Large IO, no ReadAhead
	if((num_blocks * vmdkp_->BlockSize()) >= st_config_.max_io_size_) {
		return folly::makeFuture(std::move(results));
	}
	
	return RunPredictions(req_blocks, num_blocks);
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Run(ReqBlockVec& req_blocks, const std::vector<std::unique_ptr<Request>>& requests) {
	std::unique_ptr<ReadResultVec> results;
	auto num_blocks = 0;
	assert(!req_blocks.empty());
	assert(!requests.empty());
	
	for(const auto& req : requests) {
		num_blocks += req->NumberOfRequestBlocks();
	}
	
	// Update stats
	UpdateReadMissStats(req_blocks.size());
	
	// Large IO, no ReadAhead
	if((num_blocks * vmdkp_->BlockSize()) >= st_config_.max_io_size_) {
		return folly::makeFuture(std::move(results));
	}
	
	return RunPredictions(req_blocks, num_blocks);
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::RunPredictions(ReqBlockVec& req_blocks, uint32_t io_block_count) {
	assert((io_block_count > 0) && !req_blocks.empty());
	auto block_size = vmdkp_->BlockSize();
	auto block_shift = vmdkp_->BlockShift();
	uint64_t prefetch_lbas[max_prefetch_depth_] = {0};
	int n_prefetch = 0;
	std::set<uint64_t> predictions;
	std::unique_ptr<ReadResultVec> results;
	
	// Update history & prefetch if determined so
	std::unique_lock<std::mutex> prediction_lock(prediction_mutex_);
	bool is_strided = false;
	for(const auto& block : req_blocks) {
		auto lba = block->GetOffset();
		int idx = get_index(&ghb_, 1, lba);
		bool go_prefetch = update_index_and_history(&ghb_, idx, 1, lba);
		if(go_prefetch && ShouldPrefetch(req_blocks.size(), io_block_count)) { 
			n_prefetch = query_history(&ghb_, idx, prefetch_lbas, max_prefetch_depth_, &is_strided);
		}
	}
	// Determine actual prefetch quantum based on pattern stability
	n_prefetch = UpdatePrefetchDepth(n_prefetch, is_strided);
	prediction_lock.unlock();	
	
	// Sanitize predicted offsets and build final list of LBAs to submit
	for(int i = 0; i < n_prefetch; ++i) {
		auto offset = prefetch_lbas[i];
		if(!IsBlockSizeAlgined(offset, block_size)) {
			offset = AlignDownToBlockSize(offset, block_size);
		}
		if(offset <= max_offset_ && offset >= block_size) {
			predictions.insert(offset);
		}
	}
	if(predictions.size() > 0) {
		auto pred_size = predictions.size();
		auto pred_size_bytes = pred_size << block_shift;
		assert(pred_size_bytes <= st_config_.max_prediction_size_); 
		(void) pred_size_bytes;
		
		// Update stats
		UpdateReadAheadStats(pred_size);	
		
		return Read(predictions);
	}
	
	return folly::makeFuture(std::move(results));
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Read(std::set<uint64_t>& predictions) {
	assert(!predictions.empty());
	ReadRequestVec requests;
	requests.reserve(predictions.size());
	
	// Coalesce sequential LBAs to yield large single requests
	// Currently the mergeability is just 1 block size to benefit
	// selctive locking at Vmdk level
	CoalesceRequests(predictions, requests, vmdkp_->BlockSize());

	auto vmp = vmdkp_->GetVM();
	assert(vmp != nullptr);
	
	return vmp->BulkRead(vmdkp_, requests.begin(), requests.end(), true);
}

void ReadAhead::CoalesceRequests(/*[In]*/std::set<uint64_t>& predictions, 
	/*[Out]*/ReadRequestVec& requests, size_t mergeability = 0) {
    auto req_id = 0;
    auto block_size = vmdkp_->BlockSize();
    auto block_shift = vmdkp_->BlockShift();
    auto num_blocks = 1;
	auto predictions_len = predictions.size();
	ReadRequest a_request = {};
	
	if(predictions_len < 1) {
		return;
	}
	if(predictions_len == 1) {
		a_request.reqid = ++req_id;
		a_request.size = block_size;
		a_request.offset = *predictions.begin();
		requests.emplace_back(a_request);
		return;
	}
	predictions.insert(LONG_MAX);
    auto start_offset = *predictions.begin();
    size_t total_size = 0;
    if(mergeability < 1) {
		mergeability = st_config_.max_packet_size_;
	}
	for(auto it = ++predictions.begin(); it != predictions.end(); ++it) {
		auto size = (*it - start_offset) / num_blocks;
        total_size += (*it == LONG_MAX) ? block_size : size;
        if((size == block_size) && (total_size < mergeability)) {
            ++num_blocks;
        }
        else {
            if(num_blocks > 1) {
                a_request.size = num_blocks << block_shift;
            }
            else {
                a_request.size = block_size;
            }
            a_request.reqid = ++req_id;
            a_request.offset = start_offset;
			requests.emplace_back(a_request);
            start_offset = *it;
            num_blocks = 1;
            total_size = 0;
            a_request = {};
        }
    }
    predictions.erase(LONG_MAX);
}

bool ReadAhead::ShouldPrefetch(uint32_t miss_count, uint32_t io_block_count) {
	//ToDo: Check if we are throttling or backing off
	
	// Check if Read miss threshold is hit
	if(st_config_.io_miss_window_size_ < 1) {
		return true;
	}
	if(io_miss_window_.size() >= st_config_.io_miss_window_size_) {
		IOMissWindow& a_window = io_miss_window_.front();
		total_io_count_ -= a_window.io_count_;
		total_miss_count_ -= a_window.miss_count_;
		io_miss_window_.pop();
	}
	total_io_count_ += io_block_count;
	total_miss_count_ += miss_count;
	
	io_miss_window_.emplace(io_block_count, miss_count);
	// Check if miss threshold is hit ?
	assert(total_io_count_ > 0);
	return ((uint32_t)((100 * total_miss_count_) / total_io_count_) 
			>= st_config_.io_miss_threshold_percent_);
}

uint32_t ReadAhead::UpdatePrefetchDepth(uint32_t n_prefetch, bool is_strided) {
	if(n_prefetch < 1) {
		// Aggregate a few RANDOM pattern occurrences to melt into one to allow
		// GHB warming up before valid patterns get detected
		if(random_pattern_occurrences_++ >= 
			st_config_.aggregate_random_pattern_occurrences_) {
            ++pattern_frequency[PatternType::INVALID];
            random_pattern_occurrences_ = 0;
			UpdatePatternStats(PatternType::INVALID, 1);
        }
		return 0;
	}
	// Reset periodically to see through max_pattern_stability_count_ worth 
	// recent activity only
	if(total_pattern_count_ > st_config_.max_pattern_stability_count_) {
		memset(pattern_frequency, 0, sizeof(pattern_frequency));
		total_pattern_count_ = 0;
	}
	PatternType pattern_type = is_strided ? PatternType::STRIDED 
								: PatternType::CORRELATED;
	++pattern_frequency[pattern_type];
	++total_pattern_count_;
	auto pattern_count = pattern_frequency[pattern_type];
	if((uint32_t)((100 * pattern_count) / total_pattern_count_) 
		> st_config_.pattern_stability_percent_) {
		if(last_seen_pattern_ == pattern_type) {
			prefetch_depth_ <<= (prefetch_depth_ << 1) <= max_prefetch_depth_ ? 1 : 0;
		}
	}
	else {
		prefetch_depth_ >>= (prefetch_depth_ >> 1) >= min_prefetch_depth_ ? 1 : 0;
	}
	last_seen_pattern_ = pattern_type;
	assert((prefetch_depth_ >= min_prefetch_depth_) 
	&& (prefetch_depth_ <= max_prefetch_depth_));
	UpdatePatternStats(pattern_type, 1);
	
	return prefetch_depth_;
}

bool ReadAhead::InitializeEssentials() {
    auto block_size = vmdkp_->BlockSize();
	auto block_shift = vmdkp_->BlockShift();
	auto disk_size = vmdkp_->GetDiskSize();
	// Disk Size check
	if(disk_size < st_config_.min_disk_size_supported_) {
    	LOG(WARNING) << "For VmdkID = " << vmdkp_->GetID() << ", Disk Size = " << disk_size << 
				" is too small to participate in ReadAhead. ReadAhead disabled for this vmdk";
		return false;
	}
	// Too big block size, no ReadAhead
	if(block_size >= st_config_.max_io_size_) {
        LOG(ERROR) << "For VmdkID = " << vmdkp_->GetID()  << ", block size : " 
				<< block_size << " is too big." << " Max block size supported is " 
				<< st_config_.max_io_size_ << ". ReadAhead disabled for this vmdk.";
        return false;
	}
	// Initialize max & min prefetch depth for prediction
    assert(st_config_.max_prediction_size_ >= st_config_.min_prediction_size_);
	max_prefetch_depth_ = st_config_.max_prediction_size_ >> block_shift;
    min_prefetch_depth_ = st_config_.min_prediction_size_ >> block_shift;
    if((max_prefetch_depth_ < 1 && min_prefetch_depth_ < 1)
	||  (max_prefetch_depth_ >= 1 && min_prefetch_depth_ < 1)) {
        LOG(ERROR) << "For VmdkID = " << vmdkp_->GetID()  <<
                 ", Prefetch Depth is < 1. ReadAhead disabled for this vmdk.";
        return false;
	}
	if((max_prefetch_depth_ < 1) && (min_prefetch_depth_ >= 1)) {
		max_prefetch_depth_ = min_prefetch_depth_;
	}
	// Sanitize max_packet_size_ since it is derived from external source
	if(st_config_.max_packet_size_ < (min_prefetch_depth_ * block_size)) {
        LOG(ERROR) << "For VmdkID = " << vmdkp_->GetID()  <<
                 ", max_packet_size_= " << st_config_.max_packet_size_ << 
				 " is too small. ReadAhead disabled for this vmdk.";
        return false;
	}
	// Initialize prefetch depth default value
	prefetch_depth_ = min_prefetch_depth_;
	
	// Initialize max offset that can qualify as prefetch candidate
	max_offset_ = AlignDownToBlockSize(disk_size - (4 << block_shift), block_size);
	
	// Initialize CZONE setting for GHB, Each CZONE is 1GB
	start_index_ = (disk_size % (1<<30)) ? (disk_size >> 30) + 1 : disk_size >> 30;
	if(start_index_ < 1) {
   		// We shouldn't be seeing this after the disk size check has passed
        LOG(ERROR) << "For VmdkID = " << vmdkp_->GetID()  <<
                 ", number of CZONES is < 1. ReadAhead disabled for this vmdk";
        return false;
	}
	
	return true;
}

// Stats getter methods
uint64_t ReadAhead::StatsTotalReadAheadBlocks() const {
	return (st_read_ahead_stats_.stats_rh_blocks_size_ 
		- st_read_ahead_stats_.stats_rh_dropped_reads_);
}

uint64_t ReadAhead::StatsTotalReadMissBlocks() const {
	return st_read_ahead_stats_.stats_rh_read_misses_;
}

uint64_t ReadAhead::StatsTotalRandomPatterns() const {
	return st_read_ahead_stats_.stats_rh_random_pattern_;
}

uint64_t ReadAhead::StatsTotalStridedPatterns() const {
	return st_read_ahead_stats_.stats_rh_strided_pattern_;
}
	
uint64_t ReadAhead::StatsTotalCorrelatedPatterns() const {
	return st_read_ahead_stats_.stats_rh_correlated_pattern_;
}
	
uint64_t ReadAhead::StatsTotalDroppedReads() const {
	return st_read_ahead_stats_.stats_rh_dropped_reads_;
}
	
void ReadAhead::GetReadAheadStats(ReadAheadStats& st_rh_stats) const {
	st_rh_stats.stats_rh_blocks_size_ 		= st_read_ahead_stats_.stats_rh_blocks_size_
												.load(std::memory_order_relaxed);
	st_rh_stats.stats_rh_read_misses_ 		= st_read_ahead_stats_.stats_rh_read_misses_
												.load(std::memory_order_relaxed);
	st_rh_stats.stats_rh_random_pattern_ 	= st_read_ahead_stats_.stats_rh_random_pattern_
												.load(std::memory_order_relaxed);
	st_rh_stats.stats_rh_strided_pattern_ 	= st_read_ahead_stats_.stats_rh_strided_pattern_
												.load(std::memory_order_relaxed);
	st_rh_stats.stats_rh_correlated_pattern_= st_read_ahead_stats_.stats_rh_correlated_pattern_
												.load(std::memory_order_relaxed);
	st_rh_stats.stats_rh_dropped_reads_		= st_read_ahead_stats_.stats_rh_dropped_reads_
												.load(std::memory_order_relaxed);
}

// Stats setter methods
void ReadAhead::UpdateReadMissStats(uint32_t size) {
	if(st_read_ahead_stats_.stats_rh_read_misses_ + size >= ULONG_MAX - 10) {
		LOG(INFO) << "Resetting stats_rh_read_misses_ counter, current value = [" 
				<< st_read_ahead_stats_.stats_rh_read_misses_ << "].";
		st_read_ahead_stats_.stats_rh_read_misses_ = 0;
	}
	st_read_ahead_stats_.stats_rh_read_misses_ += size;
}

void ReadAhead::UpdateReadAheadStats(uint32_t size) {
	if(st_read_ahead_stats_.stats_rh_blocks_size_ + size >= ULONG_MAX - 10) {
		LOG(INFO) << "Resetting stats_rh_blocks_size_ counter, current value = [" 
					<< st_read_ahead_stats_.stats_rh_blocks_size_ << "].";
		st_read_ahead_stats_.stats_rh_blocks_size_ = 0;
	}
	st_read_ahead_stats_.stats_rh_blocks_size_ += size;
}

void ReadAhead::UpdateTotalDroppedReads(uint32_t reads) {
	if(st_read_ahead_stats_.stats_rh_dropped_reads_ + reads >= ULONG_MAX - 10) {
		LOG(INFO) << "Resetting stats_rh_dropped_reads_ counter, current value = [" 
					<< st_read_ahead_stats_.stats_rh_dropped_reads_ << "].";
		st_read_ahead_stats_.stats_rh_dropped_reads_ = 0;
	}
	st_read_ahead_stats_.stats_rh_dropped_reads_ += reads;
}

void ReadAhead::UpdatePatternStats(PatternType pattern, uint32_t count) {
	if(pattern == PatternType::INVALID) {
		if(st_read_ahead_stats_.stats_rh_random_pattern_ + count >= ULONG_MAX - 10) {
			LOG(INFO) << "Resetting  stats_rh_random_pattern_ counter, current value = [" 
						<< st_read_ahead_stats_.stats_rh_random_pattern_ << "].";
			st_read_ahead_stats_.stats_rh_random_pattern_ = 0;
		}
		st_read_ahead_stats_.stats_rh_random_pattern_ += count;
		return;
	}
	if(pattern == PatternType::STRIDED) {
		if(st_read_ahead_stats_.stats_rh_strided_pattern_ + count >= ULONG_MAX - 10) {
			LOG(INFO) << "Resetting  stats_rh_strided_pattern_ counter, current value = [" 
						<< st_read_ahead_stats_.stats_rh_strided_pattern_ << "].";
			st_read_ahead_stats_.stats_rh_strided_pattern_ = 0;
		}
		st_read_ahead_stats_.stats_rh_strided_pattern_ += count;
		return;
	}
	if(pattern == PatternType::CORRELATED) {
		if(st_read_ahead_stats_.stats_rh_correlated_pattern_ + count >= ULONG_MAX - 10) {
			LOG(INFO) << "Resetting stats_rh_correlated_pattern_ counter, current value = [" 
						<< st_read_ahead_stats_.stats_rh_correlated_pattern_ << "].";
			st_read_ahead_stats_.stats_rh_correlated_pattern_ = 0;
		}
		st_read_ahead_stats_.stats_rh_correlated_pattern_ += count;
		return;
	}
	assert(0);
}

// Config getter methods
bool ReadAhead::IsValidConfig(const config::VmdkConfig& config) { 
	return IsValidConfig(ReadAheadConfig(config));	
}

// Checks lower & upper boundary values for config params
bool ReadAhead::IsValidConfig(const ReadAheadConfig& config) {
	bool ret_value = true;
	if(config.ghb_history_length_ < MIN_GHB_HISTORY_LENGTH 
	|| config.ghb_history_length_ > MAX_GHB_HISTORY_LENGTH) {
		LOG(ERROR) << "config.ghb_history_length_ parameter should be >=  " << MIN_GHB_HISTORY_LENGTH 
					<< " and <= " << MAX_GHB_HISTORY_LENGTH;	
		ret_value = false;
	}
	if(!(config.max_prediction_size_ && !(config.max_prediction_size_ & (config.max_prediction_size_-1)))
	|| config.max_prediction_size_ < MAX_PREDICTION_SIZE_MIN
	|| config.max_prediction_size_ > MAX_PREDICTION_SIZE_MAX) {
		LOG(ERROR) << "config.max_prediction_size_ parameter should be power of 2 and >=  " << MAX_PREDICTION_SIZE_MIN
					<< " and <=  " << MAX_PREDICTION_SIZE_MAX;	
		ret_value = false;
	}
	if(!(config.min_prediction_size_ && !(config.min_prediction_size_ & (config.min_prediction_size_-1)))
	|| config.min_prediction_size_ < MAX_PREDICTION_SIZE_MIN
	|| config.min_prediction_size_ > MAX_PREDICTION_SIZE_MAX) {
		LOG(ERROR) << "config.min_prediction_size_ parameter should be power of 2 and >=  " << MAX_PREDICTION_SIZE_MIN
					<< " and <=  " << MAX_PREDICTION_SIZE_MAX;	
		ret_value = false;
	}
	if(!(config.max_packet_size_ && !(config.max_packet_size_ & (config.max_packet_size_-1)))
	|| config.max_packet_size_ < MAX_PACKET_SIZE_MIN
	|| config.max_packet_size_ > MAX_PACKET_SIZE_MAX) {
		LOG(ERROR) << "config.max_packet_size_ parameter should be power of 2 and >=  " << MAX_PACKET_SIZE_MIN
					<< " and <=  " << MAX_PACKET_SIZE_MAX;	
		ret_value = false;
	}
	if(config.min_disk_size_supported_ < MIN_DISK_SIZE_SUPPORTED) {
		LOG(ERROR) << "config.min_disk_size_supported_ parameter should be >= " << MIN_DISK_SIZE_SUPPORTED;	
		ret_value = false;
	}
	if(!(config.max_io_size_ && !(config.max_io_size_ & (config.max_io_size_-1)))
	|| config.max_io_size_ < MAX_IO_SIZE_MIN 
	|| config.max_io_size_ > MAX_IO_SIZE_MAX) {
		LOG(ERROR) << "config.max_io_size_ parameter should be power of 2 and >=  " << MAX_IO_SIZE_MIN
					<< " and <=  " << MAX_IO_SIZE_MAX;	
		ret_value = false;
	}
	if(config.min_disk_size_supported_ < MIN_DISK_SIZE_SUPPORTED) {
		LOG(ERROR) << "config.min_disk_size_supported_ parameter should be >=  " << MIN_DISK_SIZE_SUPPORTED;
		ret_value = false;
	}
	
	return ret_value;
}

uint64_t ReadAhead::GetMinDiskSizeSupported() {
	std::unique_lock<std::mutex> config_lock(g_st_config_mutex_);
	return g_st_config_.min_disk_size_supported_;
}

void ReadAhead::GetGlobalConfig(config::VmdkConfig& config) {
	std::unique_lock<std::mutex> config_lock(g_st_config_mutex_);
	GetVmdkConfig(g_st_config_, config);
}

bool ReadAhead::IsReadAheadGloballyEnabled() {
	return global_read_ahead_enabled_; 
}

void ReadAhead::SetReadAheadGlobally(bool enabled) {
	global_read_ahead_enabled_ = enabled;
}

void ReadAhead::GetLocalConfig(config::VmdkConfig& config) {
	std::unique_lock<std::mutex> config_lock(st_config_mutex_);
	GetVmdkConfig(st_config_, config);
}

void ReadAhead::GetDefaultConfig(config::VmdkConfig& config) {
	GetVmdkConfig(g_st_config_default_, config);
}

// Config setter methods
bool ReadAhead::SetGlobalConfig(const config::VmdkConfig& config) {
	std::unique_lock<std::mutex> config_lock(g_st_config_mutex_);
	if(IsValidConfig(config)) {
		g_st_config_ = config;
		return true;
	}
	return false;
}

bool ReadAhead::SetLocalConfig(const config::VmdkConfig& config) {
	std::unique_lock<std::mutex> config_lock(st_config_mutex_);
	if(IsValidConfig(config)) {
		config::VmdkConfig global_config;
		GetGlobalConfig(global_config);
		st_config_ = global_config;
		st_config_ = config;
		return true;
	}
	return false;
}

void ReadAhead::GetVmdkConfig(/*[IN]*/const ReadAheadConfig& rh_config, 
	/*[OUT]*/config::VmdkConfig& vmdk_config) {
	vmdk_config.SetAggregateRandomOccurrences(rh_config.aggregate_random_pattern_occurrences_);
	vmdk_config.SetReadAheadMaxPatternStability(rh_config.max_pattern_stability_count_);
	vmdk_config.SetReadAheadIoMissWindow(rh_config.io_miss_window_size_);
	vmdk_config.SetReadAheadIoMissThreshold(rh_config.io_miss_threshold_percent_);
	vmdk_config.SetReadAheadPatternStability(rh_config.pattern_stability_percent_);
	vmdk_config.SetReadAheadGhbHistoryLength(rh_config.ghb_history_length_);
	vmdk_config.SetReadAheadMaxPredictionSize(rh_config.max_prediction_size_);
	vmdk_config.SetReadAheadMinPredictionSize(rh_config.min_prediction_size_);
	vmdk_config.SetReadAheadMaxPacketSize(rh_config.max_packet_size_);
	vmdk_config.SetReadAheadMaxIoSize(rh_config.max_io_size_);
	vmdk_config.SetReadAheadMinDiskSize(rh_config.min_disk_size_supported_);
}

// Overloaded Config operators and copy ctor
ReadAhead::ReadAheadConfig::ReadAheadConfig
	(const config::VmdkConfig& config) {
	uint32_t value;
	uint64_t disk_size;
	if(config.GetAggregateRandomOccurrences(value)) {
		aggregate_random_pattern_occurrences_ = value;
	}
	if(config.GetReadAheadMaxPatternStability(value)) {
		max_pattern_stability_count_ = value;	
	}
	if(config.GetReadAheadIoMissWindow(value)) {
		io_miss_window_size_ = value;			
	}
	if(config.GetReadAheadIoMissThreshold(value)) {
		io_miss_threshold_percent_ = value;
	}
	if(config.GetReadAheadPatternStability(value)) {
		pattern_stability_percent_ = value;
	}
	if(config.GetReadAheadGhbHistoryLength(value)) {
		ghb_history_length_	= value;
	}
	if(config.GetReadAheadMaxPredictionSize(value)) {
		max_prediction_size_ = value;
	}
	if(config.GetReadAheadMinPredictionSize(value)) {
		min_prediction_size_ = value;
	}
	if(config.GetReadAheadMaxPacketSize(value)) {
		max_packet_size_ = value;
	}
	if(config.GetReadAheadMaxIoSize(value)) {
		max_io_size_ = value;
	}
	if(config.GetReadAheadMinDiskSize(disk_size)) {
		min_disk_size_supported_ = disk_size;
	}
}

ReadAhead::ReadAheadConfig& ReadAhead::ReadAheadConfig::operator= 
	(const config::VmdkConfig& config) {
	uint32_t value;
	uint64_t disk_size;
	if(config.GetAggregateRandomOccurrences(value)) {
		aggregate_random_pattern_occurrences_ = value;
	}
	if(config.GetReadAheadMaxPatternStability(value)) {
		max_pattern_stability_count_ = value;	
	}
	if(config.GetReadAheadIoMissWindow(value)) {
		io_miss_window_size_ = value;			
	}
	if(config.GetReadAheadIoMissThreshold(value)) {
		io_miss_threshold_percent_ = value;
	}
	if(config.GetReadAheadPatternStability(value)) {
		pattern_stability_percent_ = value;
	}
	if(config.GetReadAheadGhbHistoryLength(value)) {
		ghb_history_length_	= value;
	}
	if(config.GetReadAheadMaxPredictionSize(value)) {
		max_prediction_size_ = value;
	}
	if(config.GetReadAheadMinPredictionSize(value)) {
		min_prediction_size_ = value;
	}
	if(config.GetReadAheadMaxPacketSize(value)) {
		max_packet_size_ = value;
	}
	if(config.GetReadAheadMaxIoSize(value)) {
		max_io_size_ = value;
	}
	if(config.GetReadAheadMinDiskSize(disk_size)) {
		min_disk_size_supported_ = disk_size;
	}
	return *this;
}

ReadAhead::ReadAheadConfig& ReadAhead::ReadAheadConfig::operator=
	(const ReadAhead::ReadAheadConfig& config ) {
	if(this != &config) { 
		aggregate_random_pattern_occurrences_	= config.aggregate_random_pattern_occurrences_;
		max_pattern_stability_count_			= config.max_pattern_stability_count_;
		io_miss_window_size_					= config.io_miss_window_size_;
		io_miss_threshold_percent_				= config.io_miss_threshold_percent_;
		pattern_stability_percent_				= config.pattern_stability_percent_;
		ghb_history_length_						= config.ghb_history_length_;
		max_prediction_size_					= config.max_prediction_size_;
		min_prediction_size_					= config.min_prediction_size_;
		max_packet_size_						= config.max_packet_size_;
		min_disk_size_supported_				= config.min_disk_size_supported_;
		max_io_size_							= config.max_io_size_;
	}
	return *this;
}

void ReadAhead::LogEssentials() {
	LOG(INFO) << "======================Default Config========================";
	LOG(INFO) << "max_prediction_size_ : " 			<< (g_st_config_default_.max_prediction_size_ >> 20) << "MB";
	LOG(INFO) << "min_prediction_size_ : " 			<< (g_st_config_default_.min_prediction_size_ >> 10) << "KB";
	LOG(INFO) << "max_packet_size_ : " 				<< (g_st_config_default_.max_packet_size_ >> 10) << "KB";
	LOG(INFO) << "min_disk_size_supported_ : " 		<< (g_st_config_default_.min_disk_size_supported_ >> 30) << "GB";
	LOG(INFO) << "max_io_size_ : " 					<< (g_st_config_default_.max_io_size_ >> 10) << "KB";
	LOG(INFO) << "io_miss_window_size_ : " 			<< g_st_config_default_.io_miss_window_size_;
	LOG(INFO) << "io_miss_threshold_percent_ : "	<< g_st_config_default_.io_miss_threshold_percent_ << "%";
	LOG(INFO) << "pattern_stability_percent_ : "	<< g_st_config_default_.pattern_stability_percent_ << "%";
	LOG(INFO) << "max_pattern_stability_count_ : "	<< g_st_config_default_.max_pattern_stability_count_;
	LOG(INFO) << "ghb_history_length_: "			<< g_st_config_default_.ghb_history_length_;
	LOG(INFO) << "aggregate_random_pattern_occurrences_ : "	<< g_st_config_default_.aggregate_random_pattern_occurrences_;
	LOG(INFO) << "======================Global Config========================";
	LOG(INFO) << "max_prediction_size_ : " 			<< (g_st_config_.max_prediction_size_ >> 20) << "MB";
	LOG(INFO) << "min_prediction_size_ : " 			<< (g_st_config_.min_prediction_size_ >> 10) << "KB";
	LOG(INFO) << "max_packet_size_ : " 				<< (g_st_config_.max_packet_size_ >> 10) << "KB";
	LOG(INFO) << "min_disk_size_supported_ : " 		<< (g_st_config_.min_disk_size_supported_ >> 30) << "GB";
	LOG(INFO) << "max_io_size_ : " 					<< (g_st_config_.max_io_size_ >> 10) << "KB";
	LOG(INFO) << "io_miss_window_size_ : " 			<< g_st_config_.io_miss_window_size_;
	LOG(INFO) << "io_miss_threshold_percent_ : "	<< g_st_config_.io_miss_threshold_percent_ << "%";
	LOG(INFO) << "pattern_stability_percent_ : "	<< g_st_config_.pattern_stability_percent_ << "%";
	LOG(INFO) << "max_pattern_stability_count_ : "	<< g_st_config_.max_pattern_stability_count_;
	LOG(INFO) << "ghb_history_length_: "			<< g_st_config_.ghb_history_length_;
	LOG(INFO) << "aggregate_random_pattern_occurrences_ : "	<< g_st_config_.aggregate_random_pattern_occurrences_;
	LOG(INFO) << "======================Running Config========================";
	LOG(INFO) << "max_prediction_size_ : " 			<< (st_config_.max_prediction_size_ >> 20) << "MB";
	LOG(INFO) << "min_prediction_size_ : " 			<< (st_config_.min_prediction_size_ >> 10) << "KB";
	LOG(INFO) << "max_packet_size_ : " 				<< (st_config_.max_packet_size_ >> 10) << "KB";
	LOG(INFO) << "min_disk_size_supported_ : " 		<< (st_config_.min_disk_size_supported_ >> 30) << "GB";
	LOG(INFO) << "max_io_size_ : " 					<< (st_config_.max_io_size_ >> 10) << "KB";
	LOG(INFO) << "io_miss_window_size_ : " 			<< st_config_.io_miss_window_size_;
	LOG(INFO) << "io_miss_threshold_percent_ : "	<< st_config_.io_miss_threshold_percent_ << "%";
	LOG(INFO) << "pattern_stability_percent_ : "	<< st_config_.pattern_stability_percent_ << "%";
	LOG(INFO) << "max_pattern_stability_count_ : "	<< st_config_.max_pattern_stability_count_;
	LOG(INFO) << "ghb_history_length_: "			<< st_config_.ghb_history_length_;
	LOG(INFO) << "aggregate_random_pattern_occurrences_ : "	<< st_config_.aggregate_random_pattern_occurrences_;
	LOG(INFO) << "======================Derived Config========================";
	LOG(INFO) << "Max Prefetch Depth : " 	<< max_prefetch_depth_;	
	LOG(INFO) << "Min Prefetch Depth : " 	<< min_prefetch_depth_;	
	LOG(INFO) << "Disk Size : " 			<< (vmdkp_->GetDiskSize() >> 30) << "GB";	
	LOG(INFO) << "Max Prefetch Offset : " 	<< max_offset_;	
	LOG(INFO) << "Number of CZONEs : " 		<< start_index_;	
	LOG(INFO) << "Default Prefetch Depth : " << prefetch_depth_;	
	LOG(INFO) << "============================================================";	
}
