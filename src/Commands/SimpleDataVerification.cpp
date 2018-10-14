#include <iostream>
#include <string>
#include <algorithm>
#include <random>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <cassert>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <gflags/gflags.h>

const size_t kPageSize = 4096;
uint64_t DiskSize;
std::vector<uint64_t> bsrange;

DEFINE_string(disk, "/dev/null", "Comma seperated list of block devices for IO verification");
DEFINE_int32(iodepth, 32, "Number of concurrent IOs");
DEFINE_int32(disk_size, 100, "Use initial MB for IOs");
DEFINE_string(blocksize, "512-262144", "Block Size Range");
DEFINE_int32(iterations, 100, "number of iterations");

struct WriteDetails {
	uint64_t offset;
	size_t size;
	char fill_ch;

	WriteDetails(uint64_t off, size_t sz, char ch) {
		offset = off;
		size = sz;
		fill_ch = ch;
	}
};

ssize_t SafeWrite(int fd, const char* datap, const size_t size, uint64_t offset) {
	size_t copied = 0;
	size_t to_write = size;
	const char* bufp = datap;

	while (copied < size) {
		auto nwrote = ::pwrite(fd, bufp, to_write, offset);
		if (nwrote < 0) {
			size_t err = errno;
			if (errno == EINTR) {
				continue;
			}
			std::cerr << "Wrote only " << copied
				<< " expected " << size
				<< " error " << err << std::endl;
			return -err;
		}

		copied += nwrote;
		bufp += nwrote;
		to_write -= nwrote;
	}
	return size;
}

ssize_t SafeRead(int fd, char* datap, const size_t size, uint64_t offset) {
	size_t copied = 0;
	char* bufp = datap;
	size_t to_read = size;

	while (copied < size) {
		auto nread = ::pread(fd, reinterpret_cast<void*>(bufp), to_read, offset);
		if (nread < 0) {
			size_t err = errno;
			if (errno == EINTR) {
				continue;
			}
			std::cerr << "Read only " << copied
				<< " expected " << size
				<< " error " << err << std::endl;
			return -err;
		}

		copied += nread;
		bufp += nread;
		to_read -= nread;
	}
	return size;
}

template<typename Iter, typename RandomGenerator>
Iter SelectRandomly(Iter start, Iter end, RandomGenerator& gen) {
	std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
	return std::next(start, dis(gen));
}


template<typename Iter>
Iter SelectRandomly(Iter start, Iter end) {
	static std::random_device rd;
	static std::mt19937 gen(rd());
	return SelectRandomly(start, end, gen);
}

int WriteData(int fd, const char begin_char, std::vector<WriteDetails>& wrote) {
	const size_t kMaxBlockSize = *bsrange.rbegin();

	char* datap;
	auto rc = ::posix_memalign(reinterpret_cast<void**>(&datap), kPageSize, kMaxBlockSize);
	if (rc < 0) {
		return -ENOMEM;
	}
	assert(datap);

	auto bsrange_start = bsrange.begin();
	auto bsrange_end = bsrange.end();
	uint64_t count = 0;
	for (size_t offset = 0; offset < DiskSize; ) {
		auto it = SelectRandomly(bsrange_start, bsrange_end);
		assert(it != bsrange_end);
		auto block_size = *it;
		const char ch = begin_char + (count % 26);
		::memset(datap, ch, block_size);

		ssize_t nwrote = SafeWrite(fd, datap, block_size, offset);
		if (nwrote != static_cast<ssize_t>(block_size)) {
			::free(datap);
			return nwrote;
		}

		wrote.emplace_back(offset, block_size, ch);
		offset += block_size;
		++count;

		if (offset >= DiskSize) {
			break;
		}
	}
	::free(datap);
	return 0;
}

int VerifyData(int fd, const std::vector<WriteDetails> wrote) {
	const size_t kMaxBlockSize = *bsrange.rbegin();
	char expected[kMaxBlockSize];

	char* readp;
	auto rc = ::posix_memalign(reinterpret_cast<void**>(&readp), kPageSize, kMaxBlockSize);
	if (rc < 0) {
		return -ENOMEM;
	}

	for (const auto& w : wrote) {
		const uint64_t offset = w.offset;
		const size_t size = w.size;
		const char ch = w.fill_ch;

		::memset(expected, ch, size);

		auto nread = SafeRead(fd, readp, size, offset);
		if (nread != static_cast<ssize_t>(size)) {
			return nread;
		}
		auto rc = ::memcmp(expected, readp, size);
		if (rc != 0) {
			std::cerr << "Data Corruption at offset " << offset
				<< " Expected " << ch
				<< std::endl;
				return -1;
		}
	}
	free(readp);
	return 0;

}

std::vector<std::string> Split(const std::string &str, char delim) {
	std::vector<std::string> tokens;
	size_t s = 0;
	size_t e = 0;

	while ((e = str.find(delim, s)) != std::string::npos) {
		if (e != s) {
			tokens.emplace_back(str.substr(s, e - s));
		}
		s = e + 1;
	}

	if (s != e) {
		tokens.emplace_back(str.substr(s));
	}
	return tokens;
}

bool IsPowerOfTwo(uint32_t number) {
	return not (number & (number - 1));
}

std::vector<uint64_t> StringVectorToInt(const std::vector<std::string>& in) {
	std::vector<uint64_t> result;
	result.reserve(in.size());
	for (const auto& s : in) {
		try {
			uint64_t rc = std::stoul(s);
			result.emplace_back(rc);
		} catch (...) {
			continue;
		}
	}
	return result;
}

static void GenerateBlockSize(std::vector<uint64_t>& bsrange, uint64_t start,
		uint64_t end) {
	assert(IsPowerOfTwo(start));
	assert(IsPowerOfTwo(end));
	auto nitems = end / start;

	bsrange.clear();
	bsrange.resize(nitems);
	std::generate_n(bsrange.begin(), nitems, [n = start, inc = start] () mutable {
		auto x = n;
		n += inc;
		return x;
	});
}

int main(int argc, char* argv[]) {
	google::ParseCommandLineFlags(&argc, &argv, true);

	auto str_bsrange = Split(FLAGS_blocksize, '-');
	assert(str_bsrange.size() == 2);

	bsrange = StringVectorToInt(str_bsrange);
	assert(IsPowerOfTwo(bsrange[0]));
	assert(IsPowerOfTwo(bsrange[1]));

	GenerateBlockSize(bsrange, bsrange[0], bsrange[1]);
	assert(bsrange.size() >= 2);

	DiskSize = FLAGS_disk_size * 1024 * 1024;

	assert(not FLAGS_disk.empty() and FLAGS_iterations);

	int fd = ::open(FLAGS_disk.c_str(), O_DIRECT | O_SYNC | O_RDWR);
	if (fd < 0) {
		auto rc = errno;
		std::cerr << "Could not open " << FLAGS_disk << std::endl;
		return rc;
	}

	auto iter = FLAGS_iterations;
	for (auto i = iter; i >= 0; --i) {
		const char ch = 'A' + (i % 26);
		std::cout << "Iteration " << i << std::endl;

		std::vector<WriteDetails> details;
		auto rc = WriteData(fd, ch, details);
		if (rc < 0) {
			break;
		}

		rc = VerifyData(fd, details);
		if (rc < 0) {
			break;
		}
	}

	std::cout << "DONE" << std::endl;
	close(fd);
	return 0;
}
