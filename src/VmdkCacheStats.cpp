#include "VmdkCacheStats.h"

using namespace ::ondisk;

namespace pio {

void VmdkCacheStats::GetCummulativeCacheStats(VmdkCacheStats& old_stats,
		IOAVmdkStats& stats) const noexcept {
	stats.total_reads = total_reads_ - old_stats.total_reads_;
	stats.total_writes = total_writes_ - old_stats.total_writes_;
	stats.read_populates = read_populates_ - old_stats.read_populates_;
	stats.cache_writes = cache_writes_ - old_stats.cache_writes_;
	stats.read_hits = read_hits_ - old_stats.read_hits_;
	stats.read_miss = read_miss_ - old_stats.read_miss_;
	stats.read_failed = read_failed_ - old_stats.read_failed_;
	stats.write_failed = write_failed_ - old_stats.write_failed_;

	stats.read_ahead_blks = read_ahead_blks_;
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

}

