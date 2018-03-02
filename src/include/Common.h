#pragma once

#ifndef hyc_likely
#define hyc_likely(x) (__builtin_expect(!!(x), 1))
#endif

#ifndef hyc_unlikely
#define hyc_unlikely(x) (__builtin_expect(!!(x), 0))
#endif

#ifndef LOG_DUMP
#define LOG_DUMP(expr) do { \
	VLOG(1) << "Assertion Failed " << expr; \
} while (0)
#endif

#ifndef log_assert
#define log_assert(expr) do { \
	if (hyc_unlikely(!(expr))) { \
		LOG_DUMP(#expr); \
		assert(expr); \
	} \
} while(0)
#endif
