#pragma once

#include <limits>
#include <atomic>
#include <iostream>
#include <mutex>

#include "gen-cpp2/MetaData_types.h"

using namespace ::ondisk;
using namespace std;

namespace pio {

class VmdkCacheStats {
public:
	uint64_t FlushesInProgress() const noexcept;
	uint64_t MovesInProgress() const noexcept;
	void IncrReadBytes(size_t read_bytes);
	void IncrWriteBytes(size_t write_bytes);
	void IncrNwReadBytes(size_t read_bytes);
	void IncrNwWriteBytes(size_t write_bytes);
	void IncrNwTotalReads(size_t total_reads);
	void IncrNwTotalWrites(size_t total_writes);
	void IncrAeroReadBytes(size_t read_bytes);
	void IncrAeroWriteBytes(size_t write_bytes);

public:
	VmdkCacheStats& operator=(VmdkCacheStats& other) {
		atomic_store(&total_reads_, atomic_load(&other.total_reads_));
		atomic_store(&total_writes_, atomic_load(&other.total_writes_));
		atomic_store(&total_blk_reads_, atomic_load(&other.total_blk_reads_));
		atomic_store(&total_bytes_reads_, 
			atomic_load(&other.total_bytes_reads_));
		atomic_store(&total_bytes_writes_, 
			atomic_load(&other.total_bytes_writes_));
		atomic_store(&parent_blks_, atomic_load(&other.parent_blks_));
		atomic_store(&read_populates_, atomic_load(&other.read_populates_));
		atomic_store(&cache_writes_, atomic_load(&other.cache_writes_));
		atomic_store(&read_hits_, atomic_load(&other.read_hits_));
		atomic_store(&read_miss_, atomic_load(&other.read_miss_));
		atomic_store(&read_failed_, atomic_load(&other.read_failed_));
		atomic_store(&write_failed_, atomic_load(&other.write_failed_));
		atomic_store(&reads_in_progress_, 
			atomic_load(&other.reads_in_progress_));
		atomic_store(&writes_in_progress_, 
			atomic_load(&other.writes_in_progress_));
		atomic_store(&flushes_in_progress_, 
			atomic_load(&other.flushes_in_progress_));
		atomic_store(&moves_in_progress_, 
			atomic_load(&other.moves_in_progress_));
		atomic_store(&block_size_, atomic_load(&other.block_size_));
		atomic_store(&flushed_chkpnts_, 
			atomic_load(&other.flushed_chkpnts_));
		atomic_store(&unflushed_chkpnts_, 
			atomic_load(&other.unflushed_chkpnts_));
		atomic_store(&flushed_blocks_, atomic_load(&other.flushed_blocks_));
		atomic_store(&moved_blocks_, atomic_load(&other.moved_blocks_));
		atomic_store(&dirty_blocks_, atomic_load(&other.dirty_blocks_));
		atomic_store(&clean_blocks_, atomic_load(&other.clean_blocks_));
		atomic_store(&pending_blocks_, atomic_load(&other.pending_blocks_));
		atomic_store(&read_ahead_blks_, atomic_load(&other.read_ahead_blks_));
		atomic_store(&rh_random_patterns_, atomic_load(&other.rh_random_patterns_));
		atomic_store(&rh_strided_patterns_, atomic_load(&other.rh_strided_patterns_));
		atomic_store(&rh_correlated_patterns_, atomic_load(&other.rh_correlated_patterns_));
		atomic_store(&rh_unlocked_reads_, atomic_load(&other.rh_unlocked_reads_));
		atomic_store(&nw_bytes_write_, atomic_load(&other.nw_bytes_write_));
		atomic_store(&nw_bytes_read_, atomic_load(&other.nw_bytes_read_));
		atomic_store(&aero_bytes_write_, 
			atomic_load(&other.aero_bytes_write_));
		atomic_store(&aero_bytes_read_, 
			atomic_load(&other.aero_bytes_read_));
		atomic_store(&bufsz_before_compress, 
			atomic_load(&other.bufsz_before_compress));
		atomic_store(&bufsz_after_compress, 
			atomic_load(&other.bufsz_after_compress));
		atomic_store(&bufsz_before_uncompress, 
			atomic_load(&other.bufsz_before_uncompress));
		atomic_store(&bufsz_after_uncompress, 
			atomic_load(&other.bufsz_after_uncompress));
		atomic_store(&nw_total_reads_, atomic_load(&other.nw_total_reads_));
		atomic_store(&nw_total_writes_, 
			atomic_load(&other.nw_total_writes_));
		return *this;
	}
	void GetDeltaCacheStats(VmdkCacheStats &old_stats,
		::ondisk::IOAVmdkStats& stats) noexcept;

public:
	std::atomic<uint64_t> total_reads_{0};		//Total reads issued by app
	std::atomic<uint64_t> total_writes_{0};		//Total writes issued by app
	std::atomic<uint64_t> total_blk_reads_{0};		//Total reads in terms of blk size
	std::atomic<size_t> total_bytes_reads_{0};
	std::atomic<size_t> total_bytes_writes_{0};

	std::atomic<uint64_t> parent_blks_{0};
	std::atomic<uint64_t> read_populates_{0};
	std::atomic<uint64_t> cache_writes_{0};
	std::atomic<uint64_t> read_hits_{0};
	std::atomic<uint64_t> read_miss_{0};
	std::atomic<uint64_t> read_failed_{0};
	std::atomic<uint64_t> write_failed_{0};

	std::atomic<uint64_t> reads_in_progress_{0};
	std::atomic<uint64_t> writes_in_progress_{0};
	std::atomic<uint64_t> flushes_in_progress_{0};
	std::atomic<uint64_t> moves_in_progress_{0};

	std::atomic<uint64_t> block_size_{0};

	std::atomic<uint64_t> flushed_chkpnts_{0};
	std::atomic<uint64_t> unflushed_chkpnts_{0};
	std::atomic<uint64_t> flushed_blocks_{0};
	std::atomic<uint64_t> moved_blocks_{0};

	std::atomic<uint64_t> dirty_blocks_{0};
	std::atomic<uint64_t> clean_blocks_{0};
	std::atomic<uint64_t> pending_blocks_{0};

	std::atomic<uint64_t> read_ahead_blks_{0};
	std::atomic<uint64_t> rh_random_patterns_{0}; 
	std::atomic<uint64_t> rh_strided_patterns_{0}; 
	std::atomic<uint64_t> rh_correlated_patterns_{0}; 
	std::atomic<uint64_t> rh_unlocked_reads_{0}; 

	std::atomic<size_t> nw_bytes_write_{0};
	std::atomic<size_t> nw_bytes_read_{0};
	std::atomic<size_t> aero_bytes_write_{0};
	std::atomic<size_t> aero_bytes_read_{0};

	std::atomic<uint64_t> bufsz_before_compress{0};
	std::atomic<uint64_t> bufsz_after_compress{0};
	std::atomic<uint64_t> bufsz_before_uncompress{0};
	std::atomic<uint64_t> bufsz_after_uncompress{0};

	std::atomic<size_t> nw_total_reads_{0};
	std::atomic<size_t> nw_total_writes_{0};

	//old stuff
	std::atomic<unsigned long long> w_total_latency{0};
	std::atomic<uint64_t> w_io_count{0};
	std::atomic<uint64_t> w_io_blks_count{0};

	std::atomic<unsigned long long> r_total_latency{0};
	std::atomic<uint64_t> r_io_count{0};
	std::atomic<uint64_t> r_io_blks_count{0};
	std::atomic<uint64_t> r_pending_count{0}, w_pending_count{0};
	std::mutex r_stat_lock_, w_stat_lock_;

	std::atomic<unsigned long long> w_aero_total_latency_{0};
	std::atomic<unsigned long long> r_aero_total_latency_{0};
	std::atomic<uint64_t> w_aero_io_blks_count_{0};
	std::atomic<uint64_t> r_aero_io_blks_count_{0};
	std::mutex r_aero_stat_lock_, w_aero_stat_lock_;
};

}
