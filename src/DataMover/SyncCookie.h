#pragma once

#include <string>
#include <tuple>

#include "DataMoverCommonTypes.h"

namespace pio {

/*
 * SyncCookie is made up of following fields
 * 1. VmUUID
 * 2. Number of VMDKs
 * 3. CheckPoint Traversal being used B or U
 * 4. Initial Base CheckPointID
 * 5. Number of CheckPointID to sync at a time
 * 6. batch CheckPointID start
 * 7. batch CheckPointID in progress
 * 8. batch CheckPointID last
 * 9. Cookie hash
 */

class SyncCookie {
public:
	using Cookie = std::string;

	enum Traversal {
		kBottomUp,
		kTopDown
	};

	SyncCookie() noexcept;
	SyncCookie(::ondisk::VmUUID,
			uint16_t vmdks,
			SyncCookie::Traversal traversal,
			::ondisk::CheckPointID base,
			uint16_t batch_size,
			CkptBatch batch
		) noexcept;

	const ::ondisk::VmUUID& GetUUID() const noexcept;
	uint16_t GetVmdkCount() const noexcept;
	::ondisk::CheckPointID GetCheckPointBase() const noexcept;
	uint16_t GetCheckPointBatchSize() const noexcept;
	void SetCheckPointBatch(CkptBatch batch) noexcept;
	const CkptBatch& GetCheckPointBatch() const noexcept;

	const SyncCookie::Cookie GetCookie() const;
	void SetCookie(const SyncCookie::Cookie& cookie);

private:
	::ondisk::VmUUID uuid_{};
	uint16_t vmdks_{};
	SyncCookie::Traversal traversal_{Traversal::kBottomUp};

	::ondisk::CheckPointID base_{
		::ondisk::MetaData_constants::kInvalidCheckPointID()
	};

	uint16_t batch_size_{0};
	CkptBatch batch_{kCkptBatchInitial};

	std::string hash_{};

public:
	const static char kDelim = ':';
	const static size_t kFields = 9;
};
}
