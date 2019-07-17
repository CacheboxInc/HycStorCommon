#pragma once

#include <string>
#include <boost/interprocess/managed_shared_memory.hpp>

#ifndef hyc_likely
#define hyc_likely(x) (__builtin_expect(!!(x), 1))
#endif

#ifndef hyc_unlikely
#define hyc_unlikely(x) (__builtin_expect(!!(x), 0))
#endif

namespace bip = boost::interprocess;

namespace hyc {

class SharedMemory {
private:
	using Segment = bip::managed_shared_memory;

public:
	using Handle = bip::managed_shared_memory::handle_t;

public:
	SharedMemory(const SharedMemory&) = delete;
	SharedMemory(SharedMemory&&) = delete;
	SharedMemory& operator = (const SharedMemory&) = delete;

	SharedMemory() noexcept;
	SharedMemory(std::string name) noexcept;
	SharedMemory& operator = (SharedMemory&&) noexcept;
	~SharedMemory();

	bool Destroy() noexcept;
	int Create(size_t size) noexcept;
	int Create(std::string name, size_t size) noexcept;
	int Attach() noexcept;
	int Attach(std::string name) noexcept;

	const std::string& GetName() const noexcept;
	size_t Size() const noexcept;
	size_t FreeSize() const noexcept;
	void* BaseAddress() const noexcept;

	void* Allocate(const size_t nbytes) noexcept;
	void* AllocateAligned(const size_t nbytes, const size_t alignment) noexcept;

	void* HandleToAddress(const Handle& handle) const noexcept;
	Handle AddressToHandle(void* addrp) const noexcept;

private:
	std::string name_{};
	Segment segment_{};
	bool created_{false};
	bool attached_{false};
};
}
