#include "VmdkCacheStats.h"

using namespace ::ondisk;
using namespace std;

namespace pio {

void VmdkCacheStats::GetDeltaCacheStats(VmdkCacheStats& old_stats,
		IOAVmdkStats& stats) noexcept {
	//App stats are irrespective of block size
	stats.app_reads = total_reads_ - old_stats.total_reads_;
	stats.app_writes = total_writes_ - old_stats.total_writes_;

	//Analyser needs stats in terms of block size sending blk_size stats to them.
	stats.total_reads = total_blk_reads_ - old_stats.total_blk_reads_;
	stats.total_writes = cache_writes_ - old_stats.cache_writes_;
	stats.read_populates = read_populates_ - old_stats.read_populates_;
	stats.cache_writes = stats.total_writes;
	stats.read_hits = read_hits_ - old_stats.read_hits_;
	stats.read_miss = read_miss_ - old_stats.read_miss_;
	stats.read_failed = read_failed_ - old_stats.read_failed_;
	stats.write_failed = write_failed_ - old_stats.write_failed_;

	stats.read_ahead_blks = read_ahead_blks_;
	stats.rh_random_patterns = rh_random_patterns_;
	stats.rh_strided_patterns = rh_strided_patterns_;
	stats.rh_correlated_patterns = rh_correlated_patterns_;
	stats.rh_unlocked_reads = rh_unlocked_reads_;

	stats.reads_in_progress = reads_in_progress_;
	stats.writes_in_progress = writes_in_progress_;
	stats.flushes_in_progress = flushes_in_progress_;
	stats.moves_in_progress  = moves_in_progress_;

	stats.block_size = block_size_;
	stats.flushed_chkpnts = flushed_chkpnts_;
	stats.unflushed_chkpnts = unflushed_chkpnts_;

	stats.flushed_blocks = flushed_blocks_ - old_stats.flushed_blocks_;
	stats.moved_blocks = moved_blocks_ - old_stats.moved_blocks_;
	stats.pending_blocks = pending_blocks_;
	stats.dirty_blocks = dirty_blocks_;
	stats.clean_blocks = clean_blocks_;

	stats.nw_bytes_write = nw_bytes_write_ - old_stats.nw_bytes_write_;
	stats.nw_bytes_read = nw_bytes_read_ - old_stats.nw_bytes_read_;
	stats.aero_bytes_write =
		aero_bytes_write_ - old_stats.aero_bytes_write_;
	stats.aero_bytes_read =
		aero_bytes_read_ - old_stats.aero_bytes_read_;

	stats.total_bytes_reads =
		total_bytes_reads_ - old_stats.total_bytes_reads_;
	stats.total_bytes_writes =
		total_bytes_writes_ - old_stats.total_bytes_writes_;

	stats.bufsz_before_compress =
		bufsz_before_compress - old_stats.bufsz_before_compress;
	stats.bufsz_after_compress =
		bufsz_after_compress - old_stats.bufsz_after_compress;
	stats.bufsz_before_uncompress =
		bufsz_before_uncompress - old_stats.bufsz_before_uncompress;
	stats.bufsz_after_uncompress =
		bufsz_after_uncompress - old_stats.bufsz_after_uncompress;
	

	old_stats = *this;
}


uint64_t VmdkCacheStats::FlushesInProgress() const noexcept {
        return flushes_in_progress_;
}

uint64_t VmdkCacheStats::MovesInProgress() const noexcept {
        return moves_in_progress_;
}

void VmdkCacheStats::IncrReadBytes(size_t read_bytes) {
	total_bytes_reads_ += read_bytes;
	return;
}

void VmdkCacheStats::IncrWriteBytes(size_t write_bytes) {
	total_bytes_writes_ += write_bytes;
	return;
}

void VmdkCacheStats::IncrNwTotalReads(size_t total_reads) {
	nw_total_reads_ += total_reads;
}

void VmdkCacheStats::IncrNwTotalWrites(size_t total_writes) {
	nw_total_writes_ += total_writes;
}

void VmdkCacheStats::IncrNwReadBytes(size_t read_bytes) {
	nw_bytes_read_ += read_bytes;
	return;
}

void VmdkCacheStats::IncrNwWriteBytes(size_t write_bytes) {
	nw_bytes_write_ += write_bytes;
	return;
}

void VmdkCacheStats::IncrAeroReadBytes(size_t read_bytes) {
	aero_bytes_read_ += read_bytes;
	return;
}

void VmdkCacheStats::IncrAeroWriteBytes(size_t write_bytes) {
	aero_bytes_write_ += write_bytes;
	return;
}

}

