#include <algorithm>
#include <memory>

#include "CommonMacros.h"
#include "Vmdk.h"
#include "VirtualMachine.h"
#include "SyncAny.h"
#include "DataSync.h"
#include "DataCopier.h"

namespace pio {
SyncAny::SyncAny(VirtualMachine* vmp,
			const ::ondisk::CheckPointID base,
			uint16_t batch_size
		) noexcept :
			VmSync(vmp, base, batch_size),
			vmp_(vmp) {
}

SyncAny::~SyncAny() noexcept {
}

RequestHandlerPtrVec SyncAny::NameToRequestHandlers(ActiveVmdk* vmdkp,
		const RequestHandlerNames& names) {
	RequestHandlerPtrVec handlers;
	handlers.reserve(names.size());
	std::transform(names.begin(), names.end(), std::back_inserter(handlers),
		[vmdkp] (const std::string& name) {
			auto handlerp = vmdkp->GetRequestHandler(name.c_str());
			if (pio_unlikely(not handlerp)) {
				std::string msg = "SyncAny: could not find ";
				msg += name;
				throw std::runtime_error(msg);
			}
			return handlerp;
		}
	);
	return handlers;
}

int SyncAny::SyncStart(const RequestHandlerNames& src_names,
		const RequestHandlerNames& dst_names) noexcept {
	if (pio_unlikely(src_names.empty() or dst_names.empty())) {
		LOG(ERROR) << "SyncAny: expected source and destination names";
		return -EINVAL;
	}

	std::vector<std::unique_ptr<VmdkSync>> syncs;
	for (ActiveVmdk* vmdkp : vmp_->GetAllVmdks()) {
		auto sync = std::make_unique<VmdkSync>(vmdkp);
		if (pio_unlikely(not sync)) {
			LOG(ERROR) << "SyncAny: creating VmdkSync failed";
			return -ENOMEM;
		}

		try {
			sync->SetSyncSource(NameToRequestHandlers(vmdkp, src_names));
			sync->SetSyncDest(NameToRequestHandlers(vmdkp, dst_names));
		} catch (const std::exception& e) {
			return -EINVAL;
		}
		syncs.emplace_back(std::move(sync));
	}
	auto rc = this->SetVmdkToSync(std::move(syncs));
	if (pio_unlikely(rc < 0)) {
		return rc;
	}
	return VmSync::SyncStart();
}

void SyncAny::SetCheckPoints(::ondisk::CheckPointID latest,
		[[maybe_unused]] ::ondisk::CheckPointID flushed) noexcept {
	SyncTill(latest);
}

}
