#include <iostream>

#include "VmdkConfig.h"
#include "DaemonUtils.h"
#include <unistd.h>
#include <sys/types.h>

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


static void ConfigureFileTarget(VmdkConfig& config) {
	std::string enable;
	std::cout << "Is FileTarget enabled [y/n]? ";
	std::cin >> enable;
	if (enable == "n") {
		config.DisableFileTarget();
		return;
	}

	bool cf;
	std::cout << "Should file target be created: ";
	std::cin >> cf;
	config.ConfigureFileTargetCreate(cf);

	std::string fp;
	std::cout << "Enter the File Target file path: ";
	std::cin >> fp;
	config.ConfigureFileTarget(fp);

	off_t size;
	std::cout << "Enter the File Target file size (in bytes): ";
	std::cin >> size;
	config.ConfigureFileTargetSize(size);
}

static void ConfigureSuccessHandler(VmdkConfig& config) {
	std::string enable;
	std::cout << "Is SuccessHandler enabled [y/n]> ";
	std::cin >> enable;
	if (enable == "n") {
		config.DisableSuccessHandler();
		return;
	}

	config.EnableSuccessHandler();
}

int main(int argc, char* argv[]) {
	std::string vmid;
	std::string vmdkid;
	uint64_t block_size;
	uint32_t tid;
	uint32_t lid;
	std::string path;

	std::cout << "VMDK Configuration JSON Dump Utility" << std::endl;
	std::cout << "Enter VmID: ";
	std::cin >> vmid;
	std::cout << "Enter VmdkID: ";
	std::cin >> vmdkid;

	std::cout << "Enter BlockSize: ";
	std::cin >> block_size;

	std::cout << "Enter TargetID: ";
	std::cin >> tid;
	std::cout << "Enter LunID: ";
	std::cin >> lid;
	std::cout << "Enter DevPath: ";
	std::cin >> path;

	VmdkConfig config;

	config.SetVmId(vmid);
	config.SetVmdkId(vmdkid);
	config.SetBlockSize(block_size);
	config.SetTargetId(tid);
	config.SetLunId(lid);
	config.SetDevPath(path);

	ConfigureCompression(config);
	ConfigureEncrytption(config);
	ConfigureRamCache(config);
	ConfigureFileCache(config);
	ConfigureSuccessHandler(config);
	ConfigureFileTarget(config);

	std::cout << "VMDK Configuration\n\n"
		<< config.Serialize() << std::endl;

	return 0;
}
