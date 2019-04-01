#pragma once

#include <vector>
#include <memory>

#include "DataMoverCommonTypes.h"
#include "VmSync.h"

namespace pio {

class RequestHandler;
using RequestHandlerNames = std::vector<std::string>;

class SyncAny : public virtual VmSync {
public:
	SyncAny(const SyncAny&) = delete;
	SyncAny(SyncAny&&) = delete;
	SyncAny& operator = (const SyncAny&) = delete;
	SyncAny& operator = (SyncAny&&) = delete;

	SyncAny(VirtualMachine*, const ::ondisk::CheckPointID base,
		uint16_t batch_size) noexcept;
	// SyncAny(VirtualMachine*, SynceCookie::Cookie cookie) noexcept;
	virtual ~SyncAny() noexcept;

	int SyncStart(const RequestHandlerNames& srcs,
		const RequestHandlerNames& dsts) noexcept;
	void SetCheckPoints(::ondisk::CheckPointID latest,
		::ondisk::CheckPointID flushed) noexcept override;
private:
	RequestHandlerPtrVec NameToRequestHandlers(ActiveVmdk* vmdkp,
		const RequestHandlerNames& names);
private:
	VirtualMachine* vmp_{};
	std::vector<std::unique_ptr<VmdkSync>> syncs_;
};

}
