#include <memory>
#include <chrono>
#include <numeric>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/MetaData_types.h"
#include "gen-cpp2/StorRpc_types.h"
#include "Request.h"
#include "Vmdk.h"
#include "DaemonUtils.h"
#include "VmdkConfig.h"

#include "LockHandler.h"
#include "MultiTargetHandler.h"

using namespace pio;
using namespace pio::config;
using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;
using namespace ::ondisk;

static const size_t kVmdkBlockSize = 8192;
static const VmdkID kVmdkid = "kVmdkid";
static const VmID kVmid = "kVmid";
static const VmdkUUID kVmdkUUID = "kVmdkUUID";
static const VmUUID kVmUUID = "kVmUUID";

class SuccessHandlerTests : public ::testing::Test {
protected:
	const CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdkp;
	std::unique_ptr<ActiveVmdk> vmp;
	std::atomic<RequestID> req_id_;

	RequestID NextRequestID() {
		return ++req_id_;
	}

	virtual void SetUp() {
		config::VmdkConfig config;
		DefaultVmdkConfig(config);

		vmdkp = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize());
		EXPECT_NE(vmdkp, nullptr);

		auto configp = vmdkp->GetJsonConfig();
		auto lock = std::make_unique<LockHandler>();
		auto multi_target = std::make_unique<MultiTargetHandler>(
			vmdkp.get(), configp);
		EXPECT_NE(lock, nullptr);
		EXPECT_NE(multi_target, nullptr);

		vmdkp->RegisterRequestHandler(std::move(lock));
		vmdkp->RegisterRequestHandler(std::move(multi_target));
	}

	void DefaultVmdkConfig(VmdkConfig& config) {
		config.SetVmdkId(kVmdkid);
		config.SetVmId(kVmid);
		config.SetVmdkUUID(kVmdkUUID);
		config.SetVmUUID(kVmUUID);
		config.SetBlockSize(kVmdkBlockSize);
		config.ConfigureCompression("snappy", 1);
		config.ConfigureEncryption("aes128-gcm", "abcd");
		config.DisableCompression();
		config.DisableEncryption();
		config.DisableFileCache();
		config.DisableRamCache();
		config.DisableErrorHandler();
		config.EnableSuccessHandler();
		config.DisableFileTarget();
		config.DisableNetworkTarget();
	}

	folly::Future<int> VmdkWrite(BlockID block, size_t skip, size_t size,
			char fillchar) {
		EXPECT_LE(size + skip, vmdkp->BlockSize());

		Offset offset = (block << vmdkp->BlockShift()) + skip;
		auto req_id = NextRequestID();
		auto bufferp = NewRequestBuffer(size);
		auto payload = bufferp->Payload();
		::memset(payload, fillchar, bufferp->Size());

		auto reqp = std::make_unique<Request>(req_id, vmdkp.get(),
			Request::Type::kWrite, payload, bufferp->Size(), bufferp->Size(),
			offset);

		return vmdkp->Write(reqp.get(), ckpt_id)
		.then([reqp = std::move(reqp)] (int rc) {
			return rc;
		});
	}

	folly::Future<int> VmdkBulkWrite(std::vector<BlockID> blocks, char fillchar) {
		auto requests = std::make_unique<std::vector<std::unique_ptr<Request>>>();
		auto process = std::make_unique<std::vector<RequestBlock*>>();

		for (auto block : blocks) {
			auto offset = block << vmdkp->BlockShift();
			auto req_id = NextRequestID();
			auto bufferp = NewRequestBuffer(vmdkp->BlockSize());
			auto p = bufferp->Payload();
			::memset(p, fillchar, bufferp->Size());

			auto req = std::make_unique<Request>(req_id, vmdkp.get(),
				Request::Type::kWrite, p, bufferp->Size(), bufferp->Size(),
				offset);

			req->ForEachRequestBlock([&] (RequestBlock *blockp) mutable {
				process->emplace_back(blockp);
				return true;
			});

			requests->emplace_back(std::move(req));
		}

		return vmdkp->BulkWrite(ckpt_id, *requests, *process)
		.then([requests = std::move(requests), process = std::move(process)]
				(int rc) {
			return rc;
		});
	}

	folly::Future<int> VmdkBulkRead(std::vector<BlockID> blocks) {
		auto requests = std::make_unique<std::vector<std::unique_ptr<Request>>>();
		auto process = std::make_unique<std::vector<RequestBlock*>>();
		std::vector<std::unique_ptr<RequestBuffer>> buffers;

		for (auto block : blocks) {
			auto offset = block << vmdkp->BlockShift();
			auto req_id = NextRequestID();
			auto bufferp = NewRequestBuffer(vmdkp->BlockSize());
			auto p = bufferp->Payload();

			auto req = std::make_unique<Request>(req_id, vmdkp.get(),
				Request::Type::kRead, p, bufferp->Size(), bufferp->Size(),
				offset);

			req->ForEachRequestBlock([&] (RequestBlock *blockp) mutable {
				process->emplace_back(blockp);
				return true;
			});

			requests->emplace_back(std::move(req));
			buffers.emplace_back(std::move(bufferp));
		}

		auto ckpts = std::make_pair(1, 1);
		return vmdkp->BulkRead(ckpts, *requests, *process)
		.then([requests = std::move(requests), process = std::move(process),
				buffers = std::move(buffers)] (int rc) {
			return rc;
		});
	}

	folly::Future<int> VmdkRead(BlockID block, size_t skip, size_t size) {
		EXPECT_LE(size + skip, vmdkp->BlockSize());

		Offset offset = (block << vmdkp->BlockShift()) + skip;
		auto req_id = NextRequestID();
		auto bufferp = NewRequestBuffer(size);
		auto payload = bufferp->Payload();

		auto reqp = std::make_unique<Request>(req_id, vmdkp.get(),
			Request::Type::kRead, payload, bufferp->Size(), bufferp->Size(),
			offset);

		return vmdkp->Read(reqp.get(), std::make_pair(1, 1))
		.then([bufferp = std::move(bufferp), reqp = std::move(reqp)] (int rc) mutable {
			return rc;
		});
	}
};

TEST_F(SuccessHandlerTests, RequestSuccess) {
	const int kMaxWrites = 1024;
	std::vector<folly::Future<int>> futures;
	futures.reserve(kMaxWrites);

	for (auto block = 0; block < kMaxWrites; ++block) {
		auto f = VmdkWrite(block, 0, vmdkp->BlockSize(), 'A');
		futures.emplace_back(std::move(f));
	}

	folly::collectAll(std::move(futures))
	.then([] (const std::vector<folly::Try<int>>& tries) {
		for (const auto& t : tries) {
			bool failed{false};
			if (t.hasException<std::exception>()) {
				failed = true;
			} else if (t.value() < 0) {
				failed = true;
			}
			EXPECT_FALSE(failed);
		}
	})
	.wait();

	futures.clear();

	for (auto block = 0; block < kMaxWrites; ++block) {
		auto f = VmdkRead(block, 0, vmdkp->BlockSize());
		futures.emplace_back(std::move(f));
	}

	folly::collectAll(std::move(futures))
	.then([] (const std::vector<folly::Try<int>>& tries) {
		for (const auto& t : tries) {
			bool failed{false};
			if (t.hasException<std::exception>()) {
				failed = true;
			} else if (t.value() < 0) {
				failed = true;
			}
			EXPECT_FALSE(failed);
		}
	})
	.wait();
}

TEST_F(SuccessHandlerTests, BulkRequestSuccess) {
	using namespace std::chrono_literals;

	const int kMaxWrites = 1024;
	std::vector<BlockID> blocks(kMaxWrites);
	std::iota(blocks.begin(), blocks.end(), 0);

	auto f = VmdkBulkWrite(blocks, 'A')
	.then([] (int rc) {
		EXPECT_EQ(rc, 0);
		return rc;
	});
	f.wait(1s);
	EXPECT_TRUE(f.isReady());

	auto rf = VmdkBulkRead(blocks);
	rf.wait(1s);
	EXPECT_TRUE(rf.isReady());
}
