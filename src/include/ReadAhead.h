#pragma once 

#include <vector>
#include "MetaDataKV.h"
#include "gen-cpp2/StorRpc.h"
#include "DaemonCommon.h"
#include "VmdkConfig.h"

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
	// Stats structure
	struct ReadAheadStats {
		std::atomic<uint64_t> stats_rh_blocks_size_{0};
		std::atomic<uint64_t> stats_rh_read_misses_{0};
		std::atomic<uint64_t> stats_rh_random_pattern_{0};
		std::atomic<uint64_t> stats_rh_strided_pattern_{0};
		std::atomic<uint64_t> stats_rh_correlated_pattern_{0};
		std::atomic<uint64_t> stats_rh_dropped_reads_{0};
	};
	
	// Algorithm & Lifecycle Methods
	ReadAhead(ActiveVmdk* vmdkp);
	virtual ~ReadAhead();
	folly::Future<std::unique_ptr<ReadResultVec>>
	Run(ReqBlockVec& req_blocks, const std::vector<std::unique_ptr<Request>>& requests);
	folly::Future<std::unique_ptr<ReadResultVec>>
	Run(ReqBlockVec& req_blocks, Request* request);
	
	// ReadAheadConfig getter methods
	void GetLocalConfig(config::VmdkConfig& config);
	static bool IsValidConfig(const config::VmdkConfig& config);
	static void GetGlobalConfig(config::VmdkConfig& config);
	static bool IsReadAheadGloballyEnabled();
	static void SetReadAheadGlobally(bool enabled);
	// Not exposed via REST, it's only for unit testing
	static void GetDefaultConfig(config::VmdkConfig& config);
	
	// ReadAheadConfig setter methods
	// Note: Altering the global config and reading the config is protected.
	// 		 These methods will only be used for testing and debugging purposes 
	//		 and won't be exposed to HA. The internal consumers aka testers have 
	//		 to acertain that ReadAhead system is quiesced before using these in
	//		 in the context of REST APIs consuming these methods. 
	bool SetLocalConfig(const config::VmdkConfig& config);
	static bool SetGlobalConfig(const config::VmdkConfig& config);
	
	// Stats getter methods
	void 	 GetReadAheadStats(ReadAheadStats& st_rh_stats) const;
	uint64_t StatsTotalReadAheadBlocks() const;
	uint64_t StatsTotalReadMissBlocks() const;
	uint64_t StatsTotalRandomPatterns() const;
	uint64_t StatsTotalStridedPatterns() const;
	uint64_t StatsTotalCorrelatedPatterns() const;
	uint64_t StatsTotalDroppedReads() const;
	
	// Gets currently set minimum disk size from global/default config
	static uint64_t GetMinDiskSizeSupported();
	
	// The only publicly visible stat updater
	void UpdateTotalDroppedReads(uint32_t reads);

private:
	// Default config
	static const uint32_t AGGREGATE_RANDOM_PATTERN_OCCURRENCES = 8;
	static const uint32_t MAX_PATTERN_STABILITY_COUNT = 8;
	static const uint32_t IO_MISS_WINDOW_SIZE = 8;
	static const uint32_t IO_MISS_THRESHOLD_PERCENT = 75;
	static const uint32_t PATTERN_STABILITY_PERCENT = 50;
	static const uint32_t MAX_PREDICTION_SIZE = 1 << 20; 		// 1M
	static const uint32_t MIN_PREDICTION_SIZE = 1 << 17; 		// 128K
	static const uint32_t MAX_PACKET_SIZE = kBulkReadMaxSize;
	static const uint32_t MAX_IO_SIZE = 1 << 20; 				// 1M
	static const uint32_t GHB_HISTORY_LENGTH = 1024; 
	static const uint64_t MIN_DISK_SIZE_SUPPORTED = 1ULL << 30; // 1G
	
	// Upper and lower bounds for ceratin configs
	static const uint32_t MIN_GHB_HISTORY_LENGTH = 8;
	static const uint32_t MAX_GHB_HISTORY_LENGTH = 1 << 14;		// 16K
	static const uint32_t MAX_IO_SIZE_MIN = 1 << 16; 			// 64K
	static const uint32_t MAX_IO_SIZE_MAX = 1 << 21; 			// 2M
	static const uint32_t MAX_PREDICTION_SIZE_MIN = 1 << 16; 	// 64K
	static const uint32_t MAX_PREDICTION_SIZE_MAX = 1 << 22;	// 4M	
	static const uint32_t MAX_PACKET_SIZE_MIN = kBulkReadMaxSize;
	static const uint32_t MAX_PACKET_SIZE_MAX = kMaxIoSize >> 1;
	
	// Absolute Config, controls ReadAhead behaviour, pace & quantum
	struct ReadAheadConfig {
		uint32_t aggregate_random_pattern_occurrences_
				{AGGREGATE_RANDOM_PATTERN_OCCURRENCES};
		uint32_t max_pattern_stability_count_
				{MAX_PATTERN_STABILITY_COUNT};
		uint32_t io_miss_window_size_
				{IO_MISS_WINDOW_SIZE};
		uint32_t io_miss_threshold_percent_
				{IO_MISS_THRESHOLD_PERCENT};
		uint32_t pattern_stability_percent_
				{PATTERN_STABILITY_PERCENT};
		uint32_t ghb_history_length_
				{GHB_HISTORY_LENGTH};
		uint32_t max_prediction_size_
				{MAX_PREDICTION_SIZE};
		uint32_t min_prediction_size_
				{MIN_PREDICTION_SIZE};
		uint32_t max_packet_size_
				{MAX_PACKET_SIZE};
		uint32_t max_io_size_
				{MAX_IO_SIZE};
		uint64_t min_disk_size_supported_
				{MIN_DISK_SIZE_SUPPORTED};
		// ReadAheadConfig overloaded operators only visible to ReadAhead class
		ReadAheadConfig() {}
		ReadAheadConfig(const config::VmdkConfig& config);
		ReadAheadConfig& operator = (const config::VmdkConfig& config);
		ReadAheadConfig& operator = (const ReadAheadConfig& config);
	};
	// Derived Config, calculated from absolute config except loopback_ & ghb_history_length_
	uint32_t max_prefetch_depth_ = 0;
	uint32_t min_prefetch_depth_ = 0;
	uint32_t prefetch_depth_ = 0;
	uint32_t start_index_ = 0;
	uint32_t loopback_ = 8;
	uint64_t max_offset_ = 0;
	
	// Pattern stability variables
	uint32_t random_pattern_occurrences_ = 0;
	uint32_t total_io_count_ = 0;
	uint32_t total_miss_count_= 0;
	uint32_t total_pattern_count_ = 0;
	
	// GHB handles
	ghb_t     	 ghb_{};
	ghb_params_t ghb_params_{0};

	// The only mutex to protect per vmdk GHB structure
	std::mutex prediction_mutex_;
	
	// Stats structure to encapsulate all stats counters
	ReadAheadStats st_read_ahead_stats_;
	
	// Global flag to test if ReadAhead is globally enabled/disabled ? For experimental use only
	static std::atomic<bool> global_read_ahead_enabled_;

	// Global ReadAheadConfig to drive ReadAhead behavior for all vmdks across all vms
	// Should be set only once, takes affect on subsequent new vmdk objects
	static std::mutex g_st_config_mutex_;
	static ReadAheadConfig g_st_config_;
	
	// Local ReadAheadConfig to drive ReadAhead behavior per vmdk
	std::mutex st_config_mutex_;
	ReadAheadConfig st_config_;
	
	// Default config, not exposed, only for uint testing
	static ReadAheadConfig g_st_config_default_;
	
	// Structure to hold fixed size window of miss ratio
	struct IOMissWindow {
		uint32_t io_count_{0};
		uint32_t miss_count_{0};
		IOMissWindow(uint32_t ios, uint32_t misses)
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
	uint32_t pattern_frequency[PatternType::NUM_PATTERNS] = {0};
	
	ActiveVmdk*	vmdkp_;
	
	// Core internal methods
	ReadAhead() {}
	void InitializeGHB();
	folly::Future<std::unique_ptr<ReadResultVec>>
	Read(std::set<uint64_t>& predictions);
	folly::Future<std::unique_ptr<ReadResultVec>>
	RunPredictions(ReqBlockVec& req_blocks, uint32_t io_block_count);
	void CoalesceRequests(/*[In]*/std::set<uint64_t>& predictions, 
			/*[Out]*/ReadRequestVec& requests, size_t mergeability); 
	bool ShouldPrefetch(uint32_t miss_count, uint32_t io_block_count);
	bool InitializeEssentials();
	void LogEssentials();
	uint32_t UpdatePrefetchDepth(uint32_t n_prefetch, bool is_strided);
	
	// Config method
	static bool IsValidConfig(const ReadAheadConfig& config);
	static void GetVmdkConfig(/*[IN]*/const ReadAheadConfig& rh_config, 
					/*[OUT]*/config::VmdkConfig& vmdk_config);

	// Stats setter methods
	void UpdateReadMissStats(uint32_t size);
	void UpdateReadAheadStats(uint32_t size);
	void UpdatePatternStats(PatternType pattern, uint32_t count);
};
}
