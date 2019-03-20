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
	struct ReadAheadStats {
		std::atomic<uint64_t>	stats_rh_blocks_size_{0};
		std::atomic<uint64_t>	stats_rh_read_misses_{0};
	};
	
	// Methods
	ReadAhead(ActiveVmdk* vmdkp);
	virtual ~ReadAhead();
	folly::Future<std::unique_ptr<ReadResultVec>>
	Run(ReqBlockVec& req_blocks, const std::vector<std::unique_ptr<Request>>& requests);
	folly::Future<std::unique_ptr<ReadResultVec>>
	Run(ReqBlockVec& req_blocks, Request* request);
	
	bool IsReadAheadEnabled() const {
		return !force_disable_read_ahead_;
	}
	void ForceDisableReadAhead() {
		force_disable_read_ahead_ = true;
	}
	static int64_t MinDiskSizeSupported() {
		return MIN_DISK_SIZE_SUPPORTED;
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
	static const int		AGGREGATE_RANDOM_PATTERN_OCCURRENCES = 8;
	static const int		MAX_PATTERN_STABILITY_COUNT = 8;
	static const int		IO_MISS_WINDOW_SIZE = 8;
	static const int		IO_MISS_THRESHOLD_PERCENT = 75;
	static const int		PATTERN_STABILITY_PERCENT = 50;
	static const uint32_t	MAX_PREDICTION_SIZE = 1 << 20; 		// 1M
	static const uint32_t	MIN_PREDICTION_SIZE = 1 << 17; 		// 128K
	static const uint32_t	MAX_PACKET_SIZE = 1 << 18; 			// 256K
	static const uint32_t	MIN_DISK_SIZE_SUPPORTED = 1 << 30; 	// 1G
	static const uint32_t	MAX_IO_SIZE = 1 << 20;				// 1M
	
	bool		force_disable_read_ahead_ = false;
	int			max_prefetch_depth_ = 0;
	int			min_prefetch_depth_ = 0;
	int			prefetch_depth_ = 0;
	int			start_index_ = 0;
	int			loopback_ = 8;
	int			n_history_ = 1024;
	int 		random_pattern_occurrences_ = 0;
	int			total_io_count_ = 0;
	int			total_miss_count_= 0;
	int			total_pattern_count_ = 0;
	int64_t		max_offset_ = 0;
	ActiveVmdk*	vmdkp_;
	
	bool			initialized_ = false;
	std::mutex		prediction_mutex_;
	ghb_params_t	ghb_params_{0};
	ghb_t     		ghb_{};
	ReadAheadStats 	st_read_ahead_stats_{0};

	struct IOMissWindow {
		int io_count_{0};
		int miss_count_{0};
		IOMissWindow(int ios, int misses)
	    : io_count_(ios), miss_count_(misses) {}
	};
	std::queue<IOMissWindow> io_miss_window_;
	
	typedef enum {
  		INVALID =		0,
		STRIDED =		1,
  		CORRELATED =	2,
		NUM_PATTERNS =	3
	}PatternType;
	PatternType last_seen_pattern_{PatternType::INVALID};
	int pattern_frequency[PatternType::NUM_PATTERNS] = {0};

	// Methods
	ReadAhead() {}
	void InitializeGHB();
	folly::Future<std::unique_ptr<ReadResultVec>>
	Read(std::map<int64_t, bool>& predictions);
	void CoalesceRequests(/*[In]*/std::map<int64_t, bool>& predictions, 
			/*[Out]*/ReadRequestVec& requests); 
	folly::Future<std::unique_ptr<ReadResultVec>>
	RunPredictions(ReqBlockVec& req_blocks, uint32_t io_block_count);
	bool ShouldPrefetch(uint32_t miss_count, uint32_t io_block_count);
	int UpdatePrefetchDepth(int n_prefetch, bool is_strided);
	void InitializeEssentials();
	void UpdateTotalReadMissBlocks(int64_t size);
	void UpdateTotalReadAheadBlocks(int64_t size);
	void LogEssentials();
};
}
