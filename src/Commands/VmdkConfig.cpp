#include <iostream>

#include "VmdkConfig.h"
#include "Utils.h"

using namespace pio;
using namespace pio::config;

static void ConfigureCompression(VmdkConfig& config) {
	std::string enable;
	std::cout << "Is compression enabled [y/n]? ";
	std::cin >> enable;
	if (enable == "n") {
		config.DisableCompression();
		return;
	}


	std::cout << "Select compression algorithm";
	auto i = 0;
	for (const auto& a : VmdkConfig::kCompressAlgos) {
		std::cout << std::endl << i+1 << " " << a;
		++i;
	}

	auto compression = 0u;
	std::cout << std::endl << "Select: ";
	std::cin >> compression;
	--compression;

	auto level = 0u;
	std::cout << "Compression Level: ";
	std::cin >> level;

	config.ConfigureCompression(VmdkConfig::kCompressAlgos[compression], level);
}

static void ConfigureEncrytption(VmdkConfig& config) {
	std::string enable;
	std::cout << "Is encryption enabled [y/n]? ";
	std::cin >> enable;
	if (enable == "n") {
		config.DisableEncryption();
		return;
	}

	std::string key;
	std::cout << "Enter encryption key: ";
	std::cin >> key;
	config.ConfigureEncrytption(key);
}

static void ConfigureRamCache(VmdkConfig& config) {
	std::string enable;
	std::cout << "Is RamCache enabled [y/n]? ";
	std::cin >> enable;
	if (enable == "n") {
		config.DisableRamCache();
		return;
	}


	uint16_t mb;
	std::cout << "Memory Limit in MB: ";
	std::cin >> mb;
	config.ConfigureRamCache(mb);
}

static void ConfigureFileCache(VmdkConfig& config) {
	std::string enable;
	std::cout << "Is FileCache enabled [y/n]? ";
	std::cin >> enable;
	if (enable == "n") {
		config.DisableFileCache();
		return;
	}

	std::string fp;
	std::cout << "Enter the file cache file path: ";
	std::cin >> fp;
	config.ConfigureFileCache(fp);
}

int main(int argc, char* argv[]) {
	std::string vmid;
	std::string vmdkid;
	uint64_t block_size;

	std::cout << "VMDK Configuration JSON Dump Utility" << std::endl;
	std::cout << "Enter VmID: ";
	std::cin >> vmid;
	std::cout << "Enter VmdkID: ";
	std::cin >> vmdkid;

	std::cout << "Enter BlockSize: ";
	std::cin >> block_size;

	VmdkConfig config;

	config.SetVmId(vmid);
	config.SetVmdkId(vmdkid);
	config.SetBlockSize(block_size);

	ConfigureCompression(config);
	ConfigureEncrytption(config);
	ConfigureRamCache(config);
	ConfigureFileCache(config);

	std::cout << "VMDK Configuration\n\n"
		<< config.Serialize(true) << std::endl;

	return 0;
}