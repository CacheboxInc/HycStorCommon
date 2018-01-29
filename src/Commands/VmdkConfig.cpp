#include <iostream>

#include "JsonConfig.h"
#include "ConfigConsts.h"

using namespace pio;

int main(int argc, char* argv[]) {
	std::string vmid;
	std::string vmdkid;
	uint64_t block_size;
	std::string enable_compression;
	uint64_t compression;
	std::string enable_encryption;
	std::string encryption_key;

	std::cout << "VMDK Configuration JSON Dump Utility" << std::endl;
	std::cout << "Enter VmID: ";
	std::cin >> vmid;
	std::cout << "Enter VmdkID: ";
	std::cin >> vmdkid;

	std::cout << "Enter BlockSize: ";
	std::cin >> block_size;

	std::cout << "Is compression enabled [y/n]? ";
	std::cin >> enable_compression;

	if (enable_compression == "y") {
		std::cout << "Select compression algorithm";
		auto i = 0;
		for (const auto& a : VmdkConfig::kCompressAlgos) {
			std::cout << std::endl << i+1 << " " << a;
			++i;
		}

		std::cout << std::endl << "Select: ";
		std::cin >> compression;
		--compression;
	}

	std::cout << "Is encryption enabled [y/n]? ";
	std::cin >> enable_encryption;

	if (enable_encryption == "y") {
		std::cout << "Enter encryption key: ";
		std::cin >> encryption_key;
	}

	config::JsonConfig config;
	config.SetKey(VmConfig::kVmID, vmid);
	config.SetKey(VmdkConfig::kVmdkID, vmdkid);
	config.SetKey(VmdkConfig::kBlockSize, block_size);
	if (enable_compression == "y") {
		std::cout << compression << std::endl;
		std::cout << "Selected "
			<< VmdkConfig::kCompressAlgos[compression] << std::endl;
		config.SetKey(VmdkConfig::kCompression,
			VmdkConfig::kCompressAlgos[compression]);
	}
	if (enable_encryption == "y") {
		config.SetKey(VmdkConfig::kEncryptionKey, encryption_key);
	}

	std::cout << "VMDK Configuration\n\n"
		<< config.Serialize() << std::endl;

	return 0;
}