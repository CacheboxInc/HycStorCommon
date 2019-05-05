#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <string>

#include "gen-cpp2/StorRpc_types.h"
#include "gen-cpp2/StorRpc_constants.h"
#include "gen-cpp2/MetaData_types.h"
#include "IDs.h"
#include "DaemonCommon.h"
#include "Vmdk.h"

namespace pio {

class VmdkManager {
public:
	template <typename T, typename... Args>
	::hyc_thrift::VmdkHandle CreateInstance(::ondisk::VmdkID vmdkid, Args&&... args) {
		try {
			std::lock_guard<std::mutex> lock(mutex_);

			/* Verify that VMDK for given ID is not present */
			if (auto it = ids_.find(vmdkid); pio_unlikely(it != ids_.end())) {
				assert(0);
			}
			auto handle = ++handle_;
			auto vmdk = std::make_unique<T>(handle, vmdkid,
				std::forward<Args>(args)...);

			handles_.insert(std::make_pair(handle, vmdk.get()));
			ids_.insert(std::make_pair(std::move(vmdkid), std::move(vmdk)));
			return handle;
		} catch (const std::exception& e) {
			return ::hyc_thrift::StorRpc_constants::kInvalidVmdkHandle();
		}
		return ::hyc_thrift::StorRpc_constants::kInvalidVmdkHandle();
	}

	Vmdk* GetInstance(::hyc_thrift::VmdkHandle handle);
	Vmdk* GetInstance(const ::ondisk::VmdkID& vmdkid);
	void FreeVmdkInstance(::hyc_thrift::VmdkHandle handle);

private:
	std::mutex mutex_;
	::hyc_thrift::VmdkHandle handle_{0};
	std::unordered_map<::ondisk::VmdkID, std::unique_ptr<Vmdk>> ids_;
	std::unordered_map<::hyc_thrift::VmdkHandle, Vmdk*> handles_;
};
}
