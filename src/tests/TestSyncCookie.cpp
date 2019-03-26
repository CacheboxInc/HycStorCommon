#include <gtest/gtest.h>
#include <glog/logging.h>

#include <boost/algorithm/string.hpp>

#include "SyncCookie.h"

using namespace pio;

TEST(TestSyncCookie, MatchCookie) {
	const ::ondisk::VmUUID kUUID = "1";
	const int vmdks = 4;
	const SyncCookie::Traversal t = SyncCookie::kBottomUp;
	const ::ondisk::CheckPointID base = 1;
	const int batch_size = 10;
	const ::ondisk::CheckPointID batch_begin = 11;
	const ::ondisk::CheckPointID batch_in_progress = 14;
	const ::ondisk::CheckPointID batch_end = 15;

	SyncCookie cookie1(kUUID, vmdks, t, base, batch_size, {batch_begin, batch_in_progress, batch_end});
	auto batch1 = cookie1.GetCheckPointBatch();
	auto cookie1_str = cookie1.GetCookie();

	{
		/* calling GetCookie always returns same cookie */
		EXPECT_EQ(cookie1_str, cookie1.GetCookie());

		SyncCookie cookie2;
		EXPECT_NE(cookie2.GetCookie(), cookie1.GetCookie());

		cookie2.SetCookie(cookie1.GetCookie());

		auto b2 = cookie2.GetCheckPointBatch();
		EXPECT_EQ(std::get<0>(batch1), std::get<0>(b2));
		EXPECT_EQ(std::get<1>(batch1), std::get<1>(b2));
		EXPECT_EQ(std::get<2>(batch1), std::get<2>(b2));

		EXPECT_EQ(cookie1.GetUUID(), cookie2.GetUUID());
		EXPECT_EQ(cookie1.GetVmdkCount(), cookie2.GetVmdkCount());
		EXPECT_EQ(cookie1.GetCheckPointBase(), cookie2.GetCheckPointBase());
		EXPECT_EQ(cookie1.GetCheckPointBatchSize(), cookie2.GetCheckPointBatchSize());
		EXPECT_EQ(cookie1.GetCookie(), cookie2.GetCookie());
	}

	CkptBatch b3{std::get<0>(batch1), std::get<1>(batch1)+1, std::get<2>(batch1)};
	cookie1.SetCheckPointBatch(b3);
	EXPECT_NE(cookie1.GetCookie(), cookie1_str);

	CkptBatch b4{std::get<0>(batch1), std::get<1>(batch1), std::get<2>(batch1)};
	cookie1.SetCheckPointBatch(b4);
	EXPECT_EQ(cookie1.GetCookie(), cookie1_str);
}

TEST(TestSyncCookie, SetCookieThrows) {
	const ::ondisk::VmUUID kUUID = "1";
	const int vmdks = 4;
	const SyncCookie::Traversal t = SyncCookie::kBottomUp;
	const ::ondisk::CheckPointID base = 1;
	const int batch_size = 10;
	const ::ondisk::CheckPointID batch_begin = 11;
	const ::ondisk::CheckPointID batch_in_progress = 14;
	const ::ondisk::CheckPointID batch_end = 15;

	SyncCookie cookie1(kUUID, vmdks, t, base, batch_size, {batch_begin, batch_in_progress, batch_end});
	auto cookie1_str = cookie1.GetCookie();
	std::vector<std::string> cookie1_tokens;
	boost::split(cookie1_tokens, cookie1_str,
		[] (const auto& c) {
			return c == SyncCookie::kDelim;
		}
	);

	{
		/* cookie changed throws */
		SyncCookie cookie2(kUUID, vmdks, t, base, batch_size, {batch_begin, batch_in_progress+1, batch_end});
		auto cookie2_str = cookie2.GetCookie();
		std::vector<std::string> cookie2_tokens;
		boost::split(cookie2_tokens, cookie2_str,
			[] (const auto& c) {
				return c == SyncCookie::kDelim;
			}
		);

		ASSERT_NE(cookie1_tokens.back(), cookie2_tokens.back());

		std::string& cookie2_hash = cookie2_tokens.back();
		cookie2_hash = cookie1_tokens.back();

		bool first = true;
		std::string new_cookie;
		for (const auto& token : cookie2_tokens) {
			if (not first) {
				new_cookie += SyncCookie::kDelim;
			}
			new_cookie += token;
			first = false;
		}

		EXPECT_THROW(cookie2.SetCookie(new_cookie), std::runtime_error);
	}

	{
		/* Adding extra field throws */
		SyncCookie cookie2(kUUID, vmdks, t, base, batch_size, {batch_begin, batch_in_progress+1, batch_end});
		auto cookie2_str = cookie2.GetCookie();
		std::vector<std::string> cookie2_tokens;
		boost::split(cookie2_tokens, cookie2_str,
			[] (const auto& c) {
				return c == SyncCookie::kDelim;
			}
		);

		cookie2_tokens.emplace_back(cookie1_tokens.front());

		bool first = true;
		std::string new_cookie;
		for (const auto& token : cookie2_tokens) {
			if (not first) {
				new_cookie += SyncCookie::kDelim;
			}
			new_cookie += token;
			first = false;
		}

		EXPECT_THROW(cookie2.SetCookie(new_cookie), std::runtime_error);
	}

	{
		SyncCookie cookie2(kUUID, vmdks, t, base, batch_size, {batch_begin, batch_in_progress+1, batch_end});
		auto cookie2_str = cookie2.GetCookie();
		std::vector<std::string> cookie2_tokens;
		boost::split(cookie2_tokens, cookie2_str,
			[] (const auto& c) {
				return c == SyncCookie::kDelim;
			}
		);

		/* replacing traversal from B to U - throws */
		cookie2_tokens[2] = 'U';

		bool first = true;
		std::string new_cookie;
		for (const auto& token : cookie2_tokens) {
			if (not first) {
				new_cookie += SyncCookie::kDelim;
			}
			new_cookie += token;
			first = false;
		}

		EXPECT_THROW(cookie2.SetCookie(new_cookie), std::runtime_error);
	}

	{
		SyncCookie cookie2(kUUID, vmdks, t, base, batch_size, {batch_begin, batch_in_progress+1, batch_end});
		auto cookie2_str = cookie2.GetCookie();
		std::vector<std::string> cookie2_tokens;
		boost::split(cookie2_tokens, cookie2_str,
			[] (const auto& c) {
				return c == SyncCookie::kDelim;
			}
		);

		cookie2_tokens[2] = 'R';

		cookie2_tokens.pop_back();
		bool first = true;
		std::string new_cookie;
		for (const auto& token : cookie2_tokens) {
			if (not first) {
				new_cookie += SyncCookie::kDelim;
			}
			new_cookie += token;
			first = false;
		}
		new_cookie += SyncCookie::kDelim;

		std::hash<std::string> hash_fn;
		new_cookie += std::to_string(hash_fn(new_cookie));
		EXPECT_THROW(cookie2.SetCookie(new_cookie), std::runtime_error);
	}
}
