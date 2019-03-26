#include <string>
#include <sstream>
#include <stdexcept>
#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>

#include "SyncCookie.h"

namespace pio {
std::ostream& operator << (std::ostream& os, enum SyncCookie::Traversal rhs) {
	switch (rhs) {
	case SyncCookie::kBottomUp:
		return os << 'B';
	case SyncCookie::kTopDown:
		return os << 'U';
	}
	return os;
}

std::istream& operator >> (std::istream& is, enum SyncCookie::Traversal& rhs) {
	char c;
	is >> c;
	switch (c) {
	default: {
		std::string s{"SyncCookie: unrecognized traversal "};
		s += c;
		throw std::runtime_error(std::move(s));
	}
	case 'B':
		rhs = SyncCookie::kBottomUp;
		break;
	case 'U':
		rhs = SyncCookie::kTopDown;
		break;
	}
	return is;
}

SyncCookie::SyncCookie() noexcept {
}

SyncCookie::SyncCookie(::ondisk::VmUUID uuid,
			uint16_t vmdks,
			SyncCookie::Traversal traversal,
			::ondisk::CheckPointID base,
			uint16_t batch_size,
			CkptBatch batch
		) noexcept :
			uuid_(std::move(uuid)),
			vmdks_(vmdks),
			traversal_(traversal),
			base_(base),
			batch_size_(batch_size),
			batch_(batch) {
}

const SyncCookie::Cookie SyncCookie::GetCookie() const {
	std::hash<std::string> hash_fn;

	std::ostringstream os;
	os << uuid_ << kDelim
		<< vmdks_ << kDelim
		<< traversal_ << kDelim
		<< base_ << kDelim
		<< batch_size_ << kDelim
		<< std::get<0>(batch_) << kDelim
		<< std::get<1>(batch_) << kDelim
		<< std::get<2>(batch_) << kDelim
		<< hash_fn(os.str());
	return os.str();
}

void SyncCookie::SetCookie(const SyncCookie::Cookie& cookie) {
	auto n = cookie.rfind(kDelim);

	auto hash = std::stoull(cookie.substr(n+1));
	auto c = cookie.substr(0, n+1);

	std::hash<std::string> hash_fn;
	if (hash_fn(c) != hash) {
		throw std::runtime_error("SyncCookie: invalid cookie.");
	}

	std::vector<std::string> strs;
	boost::split(strs, cookie, [this] (const auto& c) { return c == this->kDelim; });
	if (strs.size() != kFields) {
		throw std::runtime_error("SyncCookie: invalid cookie.");
	}

	uuid_ = strs[0];
	vmdks_ = std::stoul(strs[1]);
	std::istringstream in(strs[2]);
	in >> traversal_;
	base_ = std::stoul(strs[3]);
	batch_size_ = std::stoul(strs[4]);
	batch_ = {std::stoul(strs[5]), std::stoul(strs[6]), std::stoul(strs[7])};
}

const ::ondisk::VmUUID& SyncCookie::GetUUID() const noexcept {
	return uuid_;
}

uint16_t SyncCookie::GetVmdkCount() const noexcept {
	return vmdks_;
}

::ondisk::CheckPointID SyncCookie::GetCheckPointBase() const noexcept {
	return base_;
}

uint16_t SyncCookie::GetCheckPointBatchSize() const noexcept {
	return batch_size_;
}

void SyncCookie::SetCheckPointBatch(CkptBatch batch) noexcept {
	batch_ = batch;
}

const CkptBatch& SyncCookie::GetCheckPointBatch() const noexcept {
	return batch_;
}

}
