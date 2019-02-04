#pragma once 

#include <vector>
#include "MetaDataKV.h"
#include "gen-cpp2/StorRpc.h"

#ifdef __cplusplus
extern "C" {
#endif
	#include "prefetch/ghb.h"
#ifdef __cplusplus
}
#endif

using pio::ReadResultVec;
using pio::ReqBlockVec;

namespace pio {
using ReadRequestVec = std::vector<::hyc_thrift::ReadRequest>;

class ReadAhead {

public:
	typedef struct __ReadAheadStats__ {
		std::atomic<uint64_t>	stats_rh_blocks_size_{0};
		std::atomic<uint64_t>	stats_rh_read_misses_{0};
	}ReadAheadStats;
	
	// Methods
	ReadAhead(ActiveVmdk* vmdkp);
	virtual ~ReadAhead();
	folly::Future<std::unique_ptr<ReadResultVec>>
	Run(ReqBlockVec& offsets, const std::vector<std::unique_ptr<Request>>& requests);
	folly::Future<std::unique_ptr<ReadResultVec>>
	Run(ReqBlockVec& offsets, Request* request);
	bool IsReadAheadEnabled() const {
		return !force_disable_read_ahead_;
	}
	void ForceDisableReadAhead() {
		force_disable_read_ahead_ = true;
	}

	// Stats getter methods
	uint64_t StatsTotalReadAheadBlocks() const {
		return st_read_ahead_stats_.stats_rh_blocks_size_;
	}
	
	uint64_t StatsTotalReadMissBlocks() const {
		return st_read_ahead_stats_.stats_rh_read_misses_;
	}
	
	void GetReadAheadStats(ReadAheadStats& st_rh_stats) const {
		st_rh_stats.stats_rh_blocks_size_ = st_read_ahead_stats_.stats_rh_blocks_size_.load(std::memory_order_relaxed);
		st_rh_stats.stats_rh_read_misses_ = st_read_ahead_stats_.stats_rh_read_misses_.load(std::memory_order_relaxed);
	}

private:
	ActiveVmdk*		vmdkp_;
	int				prefetch_depth_;
	int				start_index_;
	int				loopback_;
	int				n_history_;
	int64_t			max_offset_;
	bool			force_disable_read_ahead_;

	static const int64_t 	MAX_PENDING_IOS_ = 1024;
	static const int64_t 	PENDING_IOS_SERVE_SIZE = 8;
	static const int64_t	MAX_PREDICTION_SIZE = 1 << 20; 	// 1M
	static const int64_t	MAX_PACKET_SIZE = 1 << 18; 		// 256K
	std::map<int64_t, bool> pending_ios_;
	std::mutex 				pending_ios_mutex_; 
	static std::mutex		prediction_mutex_;
	static bool				initialized_;
	static ghb_params_t		ghb_params_;
	static ghb_t     		ghb_;
	ReadAheadStats 			st_read_ahead_stats_{0};

	// Methods
	ReadAhead() {}
	void InitializeGHB();
	void RefreshGHB();
	folly::Future<std::unique_ptr<ReadResultVec>>
	Read(std::map<int64_t, bool>& predictions);
	void CoalesceRequests(/*[In]*/std::map<int64_t, bool>& predictions, 
			/*[Out]*/ReadRequestVec& requests); 
	folly::Future<std::unique_ptr<ReadResultVec>>
	RunPredictions(std::vector<int64_t>& offsets);
	void InitializeEssentials();
};
}
