#include <new>
#include <cerrno>

#include <glog/logging.h>

#include "SharedMemory.h"

namespace hyc {
SharedMemory::SharedMemory() noexcept {
}

SharedMemory::SharedMemory(
			std::string name
		) noexcept :
			name_(std::move(name)) {
}

SharedMemory::~SharedMemory() {
	Destroy();
}

bool SharedMemory::Destroy() noexcept {
	if (not created_) {
		return false;
	}
	bool removed = bip::shared_memory_object::remove(name_.c_str());
	created_ = not removed;
	return removed;
}

int SharedMemory::Create(std::string name, size_t size) noexcept {
	if (hyc_unlikely(attached_ or created_)) {
		LOG(FATAL) << "SharedMemory: already created or attached";
		return -EINVAL;
	}
	name_ = std::move(name);
	return Create(size);
}

int SharedMemory::Create(size_t size) noexcept {
	if (hyc_unlikely(created_ or attached_)) {
		return 0;
	}
	try {
		segment_ = Segment(bip::create_only, name_.c_str(), size);
	} catch (const bip::interprocess_exception& e) {
		LOG(ERROR) << "SharedMemory: creating shm segment (name : "
			<< name_ << ") failed " << e.what();
		return -EEXIST;
	}
	created_ = true;
	return 0;
}

int SharedMemory::Attach(std::string name) noexcept {
	if (hyc_unlikely(attached_ or created_)) {
		LOG(FATAL) << "SharedMemory: already attached";
		return -EINVAL;
	}
	name_ = std::move(name);
	return Attach();
}

int SharedMemory::Attach() noexcept {
	if (hyc_unlikely(attached_ or created_)) {
		return 0;
	}
	try {
		segment_ = Segment(bip::open_only, name_.c_str());
	} catch (const bip::interprocess_exception& e) {
		LOG(ERROR) << "SharedMemory: attaching to shm segment (name : "
			<< name_ << ") failed";
		return -EINVAL;
	}
	attached_ = true;
	return 0;
}

const std::string& SharedMemory::GetName() const noexcept {
	return name_;
}

size_t SharedMemory::Size() const noexcept {
	return segment_.get_size();
}

size_t SharedMemory::FreeSize() const noexcept {
	return segment_.get_free_memory();
}

void* SharedMemory::Allocate(const size_t nbytes) noexcept {
	return segment_.allocate(nbytes, std::nothrow_t{});
}

void* SharedMemory::AllocateAligned(const size_t nbytes, const size_t alignment) noexcept {
	return segment_.allocate_aligned(nbytes, alignment, std::nothrow_t{});
}

void* SharedMemory::HandleToAddress(const SharedMemory::Handle& handle) const noexcept {
	void* addrp = segment_.get_address_from_handle(handle);
	if (hyc_unlikely(not segment_.belongs_to_segment(addrp))) {
		LOG(ERROR) << "SharedMemory: handle " << handle
			<< " does not belong to segment";
		return nullptr;
	}
	return addrp;
}

SharedMemory::Handle SharedMemory::AddressToHandle(void* addrp) const noexcept {
	if (hyc_unlikely(not segment_.belongs_to_segment(addrp))) {
		LOG(ERROR) << "SharedMemory: address " << addrp
			<< " does not belongs to segment";
		return -EINVAL;
	}
	return segment_.get_handle_from_address(addrp);
}

void* SharedMemory::BaseAddress() const noexcept {
	return segment_.get_address();
}
}
