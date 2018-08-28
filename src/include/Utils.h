#pragma once

#include <utility>

namespace hyc {

template <typename T>
void MoveLastElements(std::vector<T>& dst, std::vector<T>& src, size_t tomove) {
	if (src.size() < tomove) {
		tomove = src.size();
	}

	auto eit = src.end();
	auto sit = std::prev(eit, tomove);
	std::move(sit, eit, std::back_inserter(dst));
	src.erase(sit, eit);
}

namespace os {

unsigned int NumberOfCpus(void) {
	return std::thread::hardware_concurrency();
}

int GetCurCpuCore(void) {
	auto core = sched_getcpu();
	if (core < 0) {
		return -1;
	}
	return core;
}
}
}