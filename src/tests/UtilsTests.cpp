#include <vector>
#include <set>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "Utils.h"
#include "Request.h"
#include "Vmdk.h"
#include "IDs.h"

using namespace pio;

TEST(UtilsTest, VectorUniquePtrMove) {
	const size_t block_size = 4096u;
	const size_t max_blocks = 5u;
	const size_t requests_per_block = 10u;

	std::vector<std::unique_ptr<Request>> requests;
	ActiveVmdk vmdk(nullptr, 1, "1", block_size);

	auto id = kInvalidRequestID + 1u;
	for (auto nblocks = 1u; nblocks <= max_blocks; ++nblocks) {
		size_t buffer_size = block_size * nblocks;
		auto bufferp = std::make_unique<RequestBuffer>(buffer_size);
		auto payload = bufferp->Payload();
		::memset(payload, 'A', bufferp->Size());

		auto eid = id + requests_per_block;
		for (; id < eid; ++id) {
			Offset offset = id * kSectorSize;
			auto r = std::make_unique<Request>(id, &vmdk, Request::Type::kWrite,
				bufferp->Payload(), buffer_size, buffer_size, offset);

			requests.emplace_back(std::move(r));
		}
	}

	EXPECT_EQ(requests.size(), max_blocks * requests_per_block);

	std::set<RequestID> ids;
	auto to_move = 11u;
	while (not requests.empty()) {
		std::vector<std::unique_ptr<Request>> dst;

		auto sz1 = requests.size();
		pio::MoveLastElements(dst, requests, to_move);
		auto sz2 = requests.size();

		EXPECT_LT(sz2, sz1);

		if (not requests.empty()) {
			EXPECT_EQ(sz2 + to_move, sz1);
		} else {
			EXPECT_GT(sz2 + to_move, sz1);
		}

		for (const auto& reqp : dst) {
			auto it = ids.find(reqp->GetID());
			EXPECT_EQ(it, ids.end());

			ids.insert(reqp->GetID());
		}
	}

	EXPECT_EQ(ids.size(), max_blocks * requests_per_block);
	id = kInvalidRequestID + 1u;
	for (auto it = ids.begin(); it != ids.end(); ++it, ++id) {
		EXPECT_EQ(*it, id);
	}
}

TEST(UtilsTest, MoveErasesElemetsFromSource) {
	const auto kMaxElements = 20;
	for (auto i = 1; i < 30; ++i) {
		std::vector<uint32_t> src(kMaxElements);
		std::iota(src.begin(), src.end(), 0);

		std::vector<uint32_t> dst;
		MoveLastElements(dst, src, i);

		EXPECT_EQ(dst.size() + src.size(), kMaxElements);

		for (auto e : dst) {
			auto it = std::find(src.begin(), src.end(), e);
			EXPECT_TRUE(it == src.end());
		}
	}
}