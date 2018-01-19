#include <vector>
#include <algorithm>
#include <chrono>
#include <iterator>
#include <random>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <roaring/roaring64map.hh>

const static uint32_t kNumberOfBits = 1ul << 10;
const static uint32_t kNumberOfTests = 1ul << 12;

TEST(RoaringBitmapSpaceBenchmark, Benchmark) {
	auto InitializeBitmap = [] (Roaring& bitmap, const uint32_t set, const uint32_t unset) {
		for (uint32_t i = 0; i < kNumberOfBits; ) {
			for (auto j = i; j < i + set && j < kNumberOfBits; ++j) {
				bitmap.add(j);
			}

			i += set + unset;
		}
	};

	for (auto set = 1; set <= 32; ++set) {
		for (auto unset = 0; unset < 8; ++unset) {
			Roaring bitmap;
			InitializeBitmap(bitmap, set, unset);

			auto stats1 = bitmap.getStats();
			auto ds_sz1 = stats1.n_bytes_array_containers +
				stats1.n_bytes_run_containers + stats1.n_bytes_bitset_containers;
			auto seri_sz1 = bitmap.getSizeInBytes(false);

			bitmap.runOptimize();
			auto saved = bitmap.shrinkToFit();
			(void) saved;

			auto stats2 = bitmap.getStats();
			auto ds_sz2 = stats2.n_bytes_array_containers +
				stats2.n_bytes_run_containers + stats2.n_bytes_bitset_containers;
			auto seri_sz2 = bitmap.getSizeInBytes(false);

			VLOG(1) << set << ","
				<< unset << ","
				<< bitmap.cardinality() << ","
				<< seri_sz1 << ","
				<< ds_sz1 << ","
				<< seri_sz2 << ","
				<< ds_sz2;
		}
	}
}

class RoaringBitmapTimeBenchmarkTest : public ::testing::Test {
protected:
	std::unique_ptr<Roaring> bitmap;
	std::vector<uint32_t> set_bits;
	std::vector<uint32_t> unset_bits;

	virtual void SetUp() {
		bitmap = std::make_unique<Roaring>();
		InitializeBitmap(1024, 1024);
	}

	virtual void TearDown() {
		bitmap = nullptr;
	}

	void InitializeBitmap(const uint32_t set, const uint32_t unset) {
		auto bitmapp = bitmap.get();
		for (uint32_t i = 0; i < kNumberOfBits; ) {
			std::vector<uint32_t> sn(set);
			std::iota(sn.begin(), sn.end(), i);
			std::copy(sn.begin(), sn.end(), std::back_inserter(set_bits));
			i += set;

			std::vector<uint32_t> un(unset);
			std::iota(un.begin(), un.end(), i);
			std::copy(un.begin(), un.end(), std::back_inserter(unset_bits));
			i += unset;
		}

		bitmapp->addMany(set_bits.size(), set_bits.data());

		std::random_device rd;
		std::mt19937 g(rd());
		std::shuffle(set_bits.begin(), set_bits.end(), g);
		std::shuffle(unset_bits.begin(), unset_bits.end(), g);
	}

	void OptimizeBitmap() {
		bitmap->runOptimize();
		bitmap->shrinkToFit();
	}

	Roaring* GetBitmap() {
		return bitmap.get();
	}

	template <typename Lambda>
	void ForEachSetBit(Lambda&& func) {
		std::for_each(set_bits.begin(), set_bits.end(), func);
	}

	template <typename Lambda>
	void ForEachUnsetBit(Lambda&& func) {
		std::for_each(unset_bits.begin(), unset_bits.end(), func);
	}
};

TEST_F(RoaringBitmapTimeBenchmarkTest, NoOptimize) {
	auto bitmapp = GetBitmap();
	EXPECT_GE(bitmapp->cardinality(), kNumberOfBits/2);

	auto s = std::chrono::high_resolution_clock::now();
	uint32_t hits = 0;
	while (hits <= kNumberOfTests) {
		ForEachSetBit([&hits, bitmapp] (uint32_t bit) mutable {
			auto rc = bitmapp->contains(bit);
			EXPECT_TRUE(rc);
			++hits;
			return true;
		});
	}
	auto e = std::chrono::high_resolution_clock::now();
	auto total_run_time = std::chrono::duration_cast<std::chrono::nanoseconds>(e-s).count();
	VLOG(1) << "NoOptimize total_run_time " << total_run_time
		<< " Hits " << hits
		<< " Time Per Hit (nanoseconds) " << total_run_time / hits;

	s = std::chrono::high_resolution_clock::now();
	uint32_t miss = 0;
	while (miss <= kNumberOfTests) {
		ForEachUnsetBit([&miss, bitmapp] (uint32_t bit) mutable {
			auto rc = bitmapp->contains(bit);
			EXPECT_FALSE(rc);
			++miss;
			return true;
		});
	}
	e = std::chrono::high_resolution_clock::now();
	total_run_time = std::chrono::duration_cast<std::chrono::nanoseconds>(e-s).count();
	VLOG(1) << "NoOptimize total_run_time " << total_run_time
		<< " Miss " << miss
		<< " Time Per Miss (nanoseconds) " << total_run_time / miss;
}

TEST_F(RoaringBitmapTimeBenchmarkTest, Optimize) {
	auto bitmapp = GetBitmap();
	bitmapp->runOptimize();
	bitmapp->shrinkToFit();
	EXPECT_GE(bitmapp->cardinality(), kNumberOfBits/2);

	auto s = std::chrono::high_resolution_clock::now();
	uint32_t hits = 0;
	while (hits <= kNumberOfTests) {
		ForEachSetBit([&hits, bitmapp] (uint32_t bit) mutable {
			auto rc = bitmapp->contains(bit);
			EXPECT_TRUE(rc);
			++hits;
			return true;
		});
	}
	auto e = std::chrono::high_resolution_clock::now();
	auto total_run_time = std::chrono::duration_cast<std::chrono::nanoseconds>(e-s).count();
	VLOG(1) << "Optimize total_run_time " << total_run_time
		<< " Hits " << hits
		<< " Time Per Hit (nanoseconds) " << total_run_time / hits;

	s = std::chrono::high_resolution_clock::now();
	uint32_t miss = 0;
	while (miss <= kNumberOfTests) {
		ForEachUnsetBit([&miss, bitmapp] (uint32_t bit) mutable {
			auto rc = bitmapp->contains(bit);
			EXPECT_FALSE(rc);
			++miss;
			return true;
		});
	}
	e = std::chrono::high_resolution_clock::now();
	total_run_time = std::chrono::duration_cast<std::chrono::nanoseconds>(e-s).count();
	VLOG(1) << "Optimize total_run_time " << total_run_time
		<< " Miss " << miss
		<< " Time Per Miss (nanoseconds) " << total_run_time / miss;
}