#include <memory>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gen-cpp2/StorRpc_types.h"
#include "DaemonTgtTypes.h"
#include "Request.h"
#include "Vmdk.h"
#include "DaemonUtils.h"
#include "VmdkConfig.h"
#include "CacheHandler.h"
#include "DaemonTgtTypes.h"

using namespace pio;
using namespace pio::config;
using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;

static const size_t kVmdkBlockSize = 8192;
static const VmdkID kVmdkid = "kVmdkid";
static const VmID kVmid = "kVmid";

class SuccessHandlerTests : public ::testing::Test {
protected:
	const CheckPointID ckpt_id{1};
	std::unique_ptr<ActiveVmdk> vmdkp;
	std::atomic<RequestID> req_id_;

	RequestID NextRequestID() {
		return ++req_id_;
	}

	virtual void SetUp() {
		config::VmdkConfig config;
		DefaultVmdkConfig(config);

		vmdkp = std::make_unique<ActiveVmdk>(1, "1", nullptr, config.Serialize(), nullptr);
		EXPECT_NE(vmdkp, nullptr);

		auto h = std::make_unique<CacheHandler>(vmdkp->GetJsonConfig());
		EXPECT_NE(vmdkp, nullptr);
		vmdkp->RegisterRequestHandler(std::move(h));
	}

	void DefaultVmdkConfig(VmdkConfig& config) {
		config.SetVmdkId(kVmdkid);
		config.SetVmId(kVmid);
		config.SetBlockSize(kVmdkBlockSize);
		config.ConfigureCompression("snappy", 1);
		config.ConfigureEncrytption("abcd");
		config.DisableEncryption();
		config.DisableFileCache();
		config.DisableRamCache();
		config.DisableErrorHandler();
		config.EnableSuccessHandler();
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

	folly::Future<int> VmdkRead(BlockID block, size_t skip, size_t size) {
		EXPECT_LE(size + skip, vmdkp->BlockSize());

		Offset offset = (block << vmdkp->BlockShift()) + skip;
		auto req_id = NextRequestID();
		auto bufferp = NewRequestBuffer(size);
		auto payload = bufferp->Payload();

		auto reqp = std::make_unique<Request>(req_id, vmdkp.get(),
			Request::Type::kRead, payload, bufferp->Size(), bufferp->Size(),
			offset);

		return vmdkp->Read(reqp.get())
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
