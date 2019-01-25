#pragma once

#include <limits>

#include "gen-cpp2/MetaData_types.h"

using namespace ::ondisk;

namespace pio {

class VmdkCacheStats {
public:
	uint64_t total_reads_{0};
	uint64_t total_writes_{0};
	uint64_t total_blk_reads_{0};
	size_t total_bytes_reads_{0};
	size_t total_bytes_writes_{0};

	uint64_t parent_blks_{0};
	uint64_t read_populates_{0};
	uint64_t cache_writes_{0};
	uint64_t read_hits_{0};
	uint64_t read_miss_{0};
	uint64_t read_failed_{0};
	uint64_t write_failed_{0};

	uint64_t reads_in_progress_{0};
	uint64_t writes_in_progress_{0};
	uint64_t flushes_in_progress_{0};
	uint64_t moves_in_progress_{0};

	uint64_t block_size_{0};

	uint64_t flushed_chkpnts_{0};
	uint64_t unflushed_chkpnts_{0};
	uint64_t flushed_blocks_{0};
	uint64_t moved_blocks_{0};

	uint64_t dirty_blocks_{0};
	uint64_t clean_blocks_{0};
	uint64_t pending_blocks_{0};

	uint64_t read_ahead_blks_{0};

	size_t nw_bytes_write_{0};
	size_t nw_bytes_read_{0};
	size_t aero_bytes_write_{0};
	size_t aero_bytes_read_{0};

	uint64_t bufsz_before_compress{0};
	uint64_t bufsz_after_compress{0};
	uint64_t bufsz_before_uncompress{0};
	uint64_t bufsz_after_uncompress{0};

public:
	void GetCummulativeCacheStats(VmdkCacheStats &old_stats,
		::ondisk::IOAVmdkStats& stats) const noexcept;
};

}
