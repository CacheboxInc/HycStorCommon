#pragma once 

#include <vector>
#include "MetaDataKV.h"
#include "gen-cpp2/StorRpc.h"
#include "DaemonCommon.h"

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
		std::atomic<uint64_t>	stats_rh_random_pattern_{0};
		std::atomic<uint64_t>	stats_rh_strided_pattern_{0};
		std::atomic<uint64_t>	stats_rh_correlated_pattern_{0};
		std::atomic<uint64_t>	stats_rh_unlocked_reads_{0};
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
	uint64_t StatsTotalRandomPatterns() const {
		return st_read_ahead_stats_.stats_rh_random_pattern_;
	}
	uint64_t StatsTotalStridedPatterns() const {
		return st_read_ahead_stats_.stats_rh_strided_pattern_;
	}
	uint64_t StatsTotalCorrelatedPatterns() const {
		return st_read_ahead_stats_.stats_rh_correlated_pattern_;
	}
	uint64_t StatsTotalUnlockedReads() const {
		return st_read_ahead_stats_.stats_rh_unlocked_reads_;
	}
	void GetReadAheadStats(ReadAheadStats& st_rh_stats) const {
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
		st_rh_stats.stats_rh_unlocked_reads_	= st_read_ahead_stats_.stats_rh_unlocked_reads_
													.load(std::memory_order_relaxed);
	}
	// The only publicly visible stat updater
	void UpdateTotalUnlockedReads(int reads);

private:
	// Absolute Config, controls ReadAhead behaviour, pace & quantum
	static const int		AGGREGATE_RANDOM_PATTERN_OCCURRENCES = 8;
	static const int		MAX_PATTERN_STABILITY_COUNT = 8;
	static const int		IO_MISS_WINDOW_SIZE = 8;
	static const int		IO_MISS_THRESHOLD_PERCENT = 75;
	static const int		PATTERN_STABILITY_PERCENT = 50;
	static const uint32_t	MAX_PREDICTION_SIZE = 1 << 20; 		// 1M
	static const uint32_t	MIN_PREDICTION_SIZE = 1 << 17; 		// 128K
	static const uint32_t	MAX_PACKET_SIZE = kMaxIoSize >> 1;
	static const uint32_t	MIN_DISK_SIZE_SUPPORTED = 1 << 30; 	// 1G
	static const uint32_t	MAX_IO_SIZE = 1 << 20;				// 1M
	// Internal flag to disable ReadAhead
	bool		force_disable_read_ahead_ = false;
	// Derived Config, calculated from absolute config except loopback_ & n_history_
	int			max_prefetch_depth_ = 0;
	int			min_prefetch_depth_ = 0;
	int			prefetch_depth_ = 0;
	int			start_index_ = 0;
	int			loopback_ = 8;
	int			n_history_ = 1024;
	int64_t		max_offset_ = 0;
	// Pattern stability variables
	int 		random_pattern_occurrences_ = 0;
	int			total_io_count_ = 0;
	int			total_miss_count_= 0;
	int			total_pattern_count_ = 0;
	ActiveVmdk*	vmdkp_;
	// GHB global params
	ghb_params_t	ghb_params_{0};
	ghb_t     		ghb_{};
	// The only mutex to protect per vmdk GHB structure
	std::mutex		prediction_mutex_;
	// Stats structure to encapsulate all stats counters
	ReadAheadStats 	st_read_ahead_stats_{0};
	// Structure to hold fixed size window of miss ratio
	struct IOMissWindow {
		int io_count_{0};
		int miss_count_{0};
		IOMissWindow(int ios, int misses)
	    : io_count_(ios), miss_count_(misses) {}
	};
	std::queue<IOMissWindow> io_miss_window_;
	// Supported patterns
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
	Read(std::set<int64_t>& predictions);
	folly::Future<std::unique_ptr<ReadResultVec>>
	RunPredictions(ReqBlockVec& req_blocks, uint32_t io_block_count);
	void CoalesceRequests(/*[In]*/std::set<int64_t>& predictions, 
			/*[Out]*/ReadRequestVec& requests, size_t mergeability); 
	bool ShouldPrefetch(uint32_t miss_count, uint32_t io_block_count);
	int UpdatePrefetchDepth(int n_prefetch, bool is_strided);
	void InitializeEssentials();
	void UpdateReadMissStats(int64_t size);
	void UpdateReadAheadStats(int64_t size);
	void UpdatePatternStats(PatternType pattern, int count);
	void LogEssentials();
};
}
