#include "VirtualMachine.h"
#include "Vmdk.h"
#include "ArmSync.h"
#include "DataSync.h"
#include "DataCopier.h"
// #include "NetworkTargetHandler.h"
#include "DirtyHandler.h"
#include "CleanHandler.h"

namespace pio {
ArmSync::ArmSync(VirtualMachine* vmp,
			const ::ondisk::CheckPointID base,
			uint16_t batch_size
		) noexcept :
			VmSync(vmp, base, batch_size),
			vmp_(vmp) {
	SyncTill(vmp_->GetCurCkptID() - 1);
}

#if 0
ArmSync::ArmSync(VirtualMachine* vmp,
			SynceCookie::Cookie&& cookie
		) :
			VmSync(vmp, std::forward<SynceCookie::Cookie>(cookie)),
			vmp_(vmp) {
}
#endif

ArmSync::~ArmSync() noexcept {

}

void ArmSync::SetCheckPoints(CheckPointID latest, CheckPointID flushed) {
	(void) flushed;
	SyncTill(latest);
}

int ArmSync::VCenterConnnect(std::string&& moid, VCenterInfo&& info) {
	if (pio_unlikely(vcenter_)) {
		LOG(ERROR) << "ArmSync: existing VC connection is live";
		return -EINVAL;
	}
	vcenter_ = std::make_unique<VCenter>(std::forward<std::string>(moid),
		std::forward<VCenterInfo>(info));
	if (pio_unlikely(not vcenter_)) {
		LOG(ERROR) << "ArmSync: creating VC connection failed";
		return -ENOMEM;
	}

	auto rc = vcenter_->Connect();
	if (pio_unlikely(rc < 0)) {
		vcenter_.reset();

		LOG(ERROR) << "ArmSync: connecting to VC failed, rc = " << rc;
		return rc;
	}

	return 0;
}

int ArmSync::SyncStart(const VddkPathInfoMap& paths) {
	int errno;

	std::vector<std::unique_ptr<VmdkSync>> syncs;
	try {
		auto targets = CreateVddkTargets(paths);
		if (pio_unlikely(targets.empty())) {
			LOG(ERROR) << "ArmSync: no target specified";
			return -EINVAL;
		}
		auto sources = FindSyncSource();
		if (pio_unlikely(sources.empty())) {
			LOG(ERROR) << "ArmSync: could not figure out data source";
			return -EINVAL;
		}
		if (pio_unlikely(targets.size() != sources.size())) {
			LOG(ERROR) << "ArmSync: source and target do not match";
			return -EINVAL;
		}

		for (auto& target : targets) {
			auto it = sources.find(target.first);
			if (pio_unlikely(it == sources.end())) {
				LOG(ERROR) << "ArmSync: could not find source with VmdkID "
					<< target.first << ". "
					<< "Source and target do not match";
				return -EINVAL;
			}

			auto vmdkp = vmp_->FindVmdk(target.first);
			if (pio_unlikely(not vmdkp)) {
				LOG(ERROR) << "ArmSync: could not find Vmdk for VmdkID "
					<< target.first << ". ";
				return -EINVAL;
			}

			auto sync = std::make_unique<VmdkSync>(vmdkp);
			if (pio_unlikely(not sync)) {
				LOG(ERROR) << "ArmSync: creating sync object for a VMDK failed";
				return -ENOMEM;
			}

			sync->SetSyncSource(std::move(it->second));
			sources.erase(it);

			RequestHandlerPtrVec dst;
			dst.emplace_back(target.second.get());
			sync->SetSyncDest(std::move(dst));
			syncs.emplace_back(std::move(sync));
		}
		targets_.swap(targets);
	} catch (const std::exception& e) {
		LOG(ERROR) << e.what();
		return -EINVAL;
	}

	auto rc = this->SetVmdkToSync(std::move(syncs));
	if (pio_unlikely(rc < 0)) {
		targets_.clear();
		return rc;
	}
	return VmSync::SyncStart();
}

std::unordered_map<::ondisk::VmdkID, VddkTargetHandlerPtr>
ArmSync::CreateVddkTargets(const VddkPathInfoMap& paths) {
	std::unordered_map<::ondisk::VmdkID, VddkTargetHandlerPtr> targets;

	for (ActiveVmdk* vmdkp : vmp_->GetAllVmdks()) {
		auto it = paths.find(vmdkp->GetID());
		if (pio_unlikely(it == paths.end())) {
			std::ostringstream os;
			os << "ArmSync: could not find VDDK path for VmdkID = "
				<< vmdkp->GetID();
			throw std::invalid_argument(os.str());
		}

		auto target = std::make_unique<VddkTargetHandler>(vmdkp, vcenter_.get(),
			it->second);
		if (pio_unlikely(not target)) {
			std::ostringstream os;
			os << "ArmSync: failed to create VDDK target for VmdkID = "
				<< vmdkp->GetID();
			throw std::runtime_error(os.str());
		}
		targets.emplace(vmdkp->GetID(), std::move(target));
	}
	return targets;
}

std::unordered_map<::ondisk::VmdkID, RequestHandlerPtrVec>
ArmSync::FindSyncSource() {
	std::unordered_map<::ondisk::VmdkID, RequestHandlerPtrVec> sources;

	for (ActiveVmdk* vmdkp : vmp_->GetAllVmdks()) {
		const auto& id = vmdkp->GetID();
		RequestHandlerPtrVec src;
		src.reserve(3);

		auto dirtyp = vmdkp->GetRequestHandler(DirtyHandler::kName);
		if (pio_unlikely(not dirtyp)) {
			throw std::runtime_error("ArmSync: could not find DirtyHandler");
		}
		auto cleanp = vmdkp->GetRequestHandler(CleanHandler::kName);
		if (pio_unlikely(not cleanp)) {
			throw std::runtime_error("ArmSync: could not find CleanHandler");
		}

		src.emplace_back(dirtyp);
		src.emplace_back(cleanp);
		// src.emplace_back(vmdkp->GetRequestHandler(NetworkTargetHandler::kName));

		sources.emplace(id, std::move(src));
	}
	return sources;
}
}
