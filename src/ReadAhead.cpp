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

ReadAhead::ReadAhead(ActiveVmdk* vmdkp, int prefetch_depth, int start_index, int loopback) 
		: vmdkp_(vmdkp),prefetch_depth_(prefetch_depth), start_index_(start_index), loopback_(loopback),
		n_history_(MAX_PENDING_IOS_) {
	if(not vmdkp_) {
		LOG(ERROR) <<  __func__  << "vmdkp is passed as nullptr, cannot construct ReadAhead object";
		throw std::runtime_error("At __func__ vmdkp is passed as nullptr, cannot construct ReadAhead object");
	}
	n_history_ = MAX_PENDING_IOS_;
	InitializeMaxOffset();
}

ReadAhead::ReadAhead(ActiveVmdk* vmdkp)
		: vmdkp_(vmdkp),prefetch_depth_(256),start_index_(32), loopback_(8), n_history_(MAX_PENDING_IOS_) {
	if(not vmdkp_) {
		LOG(ERROR) <<  __func__  << "vmdkp is passed as nullptr, cannot construct ReadAhead object";
		throw std::runtime_error("At __func__ vmdkp is passed as nullptr, cannot construct ReadAhead object");
	}
	InitializeMaxOffset();
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
	for(const auto an_offset : offsets) {
		bool offset_matched = false;
		if(not request->IsReadAheadRequired()) {
			request->ForEachRequestBlock([&offset_matched, &an_offset] (RequestBlock* blockp) { 
				if(blockp->GetOffset() == an_offset->GetOffset()) {
					offset_matched = true;
				}
				return true;
			});
		}
		if(not offset_matched) {
			int64_t offset = an_offset->GetOffset();
			if(offset < max_offset_) {
				rh_offsets.push_back(offset);
			}
		}
	}
	if(rh_offsets.empty()) {
		return folly::makeFuture(std::move(results));
	}
	
	return RunPredictions(rh_offsets);
}

folly::Future<std::unique_ptr<ReadResultVec>>
ReadAhead::Run(ReqBlockVec& offsets, const std::vector<std::unique_ptr<Request>>& requests) {
	std::vector<int64_t> rh_offsets;
	auto results = std::make_unique<ReadResultVec>();
	
	// Filter out read ahead missed blocks if any
	for(const auto an_offset : offsets) {
		bool offset_matched = false;
		for(const auto& req : requests) {
			if(not req->IsReadAheadRequired()) {
				req->ForEachRequestBlock([&offset_matched, &an_offset] (RequestBlock* blockp) { 
					if(blockp->GetOffset() == an_offset->GetOffset()) {
						offset_matched = true;
					}
					return true;
				});
			}
		}
		if(not offset_matched) {
			int64_t offset = an_offset->GetOffset();
			if(offset < max_offset_) {
				rh_offsets.push_back(offset);
			}
		}
	}
	if(rh_offsets.empty()) {
		return folly::makeFuture(std::move(results));
	}
	
	return RunPredictions(rh_offsets);
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
	
	auto local_offsets_size = local_offsets.size();	
	if(st_read_ahead_stats_.stats_rh_read_misses_ + local_offsets_size >= ULONG_MAX - 10) {
		LOG(INFO) << "Resetting stats_rh_read_misses_ counter, current value = [" 
				<< st_read_ahead_stats_.stats_rh_read_misses_ << "].";
		st_read_ahead_stats_.stats_rh_read_misses_ = 0;
	}
	st_read_ahead_stats_.stats_rh_read_misses_ += local_offsets_size;

	uint64_t* prefetch_lbas = new uint64_t[prefetch_depth_];
	for(auto it = local_offsets.begin(); it != local_offsets.end(); ++it) {
		if(st_read_ahead_stats_.stats_rh_ghb_lib_calls_ + 1 >= ULONG_MAX - 10) {
			LOG(INFO) << "Resetting stats_rh_ghb_lib_calls_ counter, current value = [" 
					<< st_read_ahead_stats_.stats_rh_ghb_lib_calls_ << "].";
			st_read_ahead_stats_.stats_rh_ghb_lib_calls_ = 0;
		}
		++st_read_ahead_stats_.stats_rh_ghb_lib_calls_;
		
		int n_prefetch = ghb_update_and_query(&ghb_, 1, it->first, prefetch_lbas);
		if(n_prefetch) {
			for(int i = 0; i < n_prefetch; ++i) {
				auto offset = prefetch_lbas[i];
				if(not IsBlockSizeAlgined(offset, block_size)) {
					offset = AlignDownToBlockSize(offset, block_size);
				}
				predictions.insert(std::pair<int64_t, bool>(offset, true));
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
		auto stats_blocks = st_read_ahead_stats_.stats_rh_blocks_size_.load(std::memory_order_relaxed);
		if(stats_blocks + pred_size >= ULONG_MAX - 10) {
			LOG(INFO) << "Resetting stats_rh_blocks_size_ counter, current value = [" 
					<< st_read_ahead_stats_.stats_rh_blocks_size_ << "].";
			st_read_ahead_stats_.stats_rh_blocks_size_ = 0;
			stats_blocks = 0;
		}
		st_read_ahead_stats_.stats_rh_blocks_size_ = stats_blocks + pred_size;
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
	for(auto it = ++predictions.begin(); it != predictions.end(); ++it) {
		float size = (float)(it->first - start_offset->first) / (float)num_blocks;
		if(size == block_size) {
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
			a_request = {};
		}
	}
	predictions.erase(LONG_MAX);
}

uint64_t ReadAhead::AdjustReadMisses(const std::vector<RequestBlock*>& missed, 
		const std::vector<std::unique_ptr<Request>>& requests) {
	uint64_t read_misses = 0;
	for(const auto an_offset : missed) {
		bool offset_matched = false;
		for(const auto& req : requests) {
			if(not req->IsReadAheadRequired()) {
				req->ForEachRequestBlock([&offset_matched, &an_offset] (RequestBlock* blockp) { 
					if(blockp->GetOffset() == an_offset->GetOffset()) {
						offset_matched = true;
						return true;
					}
					return true;
				});
			}
		}
		if(not offset_matched) {
			++read_misses;
		}
	}
	
	return read_misses;
}

uint64_t ReadAhead::AdjustReadMisses(const std::vector<RequestBlock*>& missed, 
		Request* request) {
	uint64_t read_misses = 0;
	for(const auto an_offset : missed) {
		bool offset_matched = false;
		if(not request->IsReadAheadRequired()) {
			request->ForEachRequestBlock([&offset_matched, &an_offset] (RequestBlock* blockp) { 
				if(blockp->GetOffset() == an_offset->GetOffset()) {
					offset_matched = true;
				}
				return true;
			});
		}
		if(not offset_matched) {
			++read_misses;
		}
	}
	
	return read_misses;
}

void ReadAhead::InitializeMaxOffset() {
	max_offset_ = 0;
	force_disable_read_ahead_ = true;
	auto disk_size = vmdkp_->GetDiskSize();
	auto block_size = vmdkp_->BlockSize();
	// Unread Area = 1 * Predictability + 2MB
	int64_t adjust_safety = 2 * ((prefetch_depth_ + PENDING_IOS_SERVE_SIZE ) * block_size) + (2 * 1024 * 1024);
	if(disk_size > adjust_safety) {
		max_offset_ = disk_size - adjust_safety;
		if(not IsBlockSizeAlgined(max_offset_, block_size)) {
			max_offset_ = AlignDownToBlockSize(max_offset_, block_size);
		}
		force_disable_read_ahead_ = false;
	}
	if(force_disable_read_ahead_) {
		LOG(INFO) << "For VmdkID , Disk size " << vmdkp_->GetID() << disk_size <<
				" is too small to enable ReadAhead. Read Ahead is disabled for this disk.";
	}
}
