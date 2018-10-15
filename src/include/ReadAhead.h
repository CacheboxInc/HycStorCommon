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
	// Methods
	ReadAhead(ActiveVmdk* vmdkp, int prefetch_depth, int start_index, 
		int loopback, int n_history);
	ReadAhead(ActiveVmdk* vmdkp);
	virtual ~ReadAhead();
	folly::Future<std::unique_ptr<ReadResultVec>>
	Run(ReqBlockVec& offsets);
	
	uint64_t StatsTotalReadMisses() const {
		return stats_input_blocks_size;
	}

	uint64_t StatsTotalReadAheadBlocks() const {
		return stats_rh_blocks_size;
	}

private:
	ActiveVmdk*		vmdkp_;
	int				prefetch_depth_;
	int				start_index_;
	int				loopback_;
	int				n_history_;

	static const int64_t 	MAX_PENDING_IOS_ = 1024;
	static const int64_t 	PENDING_IOS_SERVE_SIZE = 32;
	std::map<int64_t, bool> pending_ios_;
	std::mutex 				pending_ios_mutex_; 
	static std::mutex		prediction_mutex_;
	static bool				initialized_;
	static uint64_t			stats_input_blocks_size;
	static uint64_t			stats_rh_blocks_size;
	static ghb_params_t		ghb_params_;
	static ghb_t     		ghb_;
	
	// Methods
	void InitializeGHB();
	void RefreshGHB();
	folly::Future<std::unique_ptr<ReadResultVec>>
	Read(std::map<int64_t, bool>& predictions);
	void CoalesceRequests(/*[In]*/std::map<int64_t, bool>& predictions, 
			/*[Out]*/ReadRequestVec& requests); 
	void PrintStats();
};
}
