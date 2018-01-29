#include <iostream>

#include "JsonConfig.h"
#include "ConfigConsts.h"
#include "Utils.h"

using namespace pio;

static void ConigureCompression(config::JsonConfig& config) {
	std::string result;

	{
		StringDelimAppend(result, '.',
			{VmdkConfig::kCompression, VmdkConfig::kEnabled});

		std::string enable_compression;
		std::cout << "Is compression enabled [y/n]? ";
		std::cin >> enable_compression;
		if (enable_compression == "n") {
			config.SetKey(result, false);
			return;
		}

		config.SetKey(result, true);
	}

	{
		StringDelimAppend(result, '.', {VmdkConfig::kCompression,
			VmdkConfig::kCompressionType});

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

		config.SetKey(result, VmdkConfig::kCompressAlgos[compression]);
	}

	{
		StringDelimAppend(result, '.', {VmdkConfig::kCompression,
			VmdkConfig::kCompressionLevel});

		auto level = 0u;
		std::cout << "Compression Level: ";
		std::cin >> level;
		config.SetKey(result, level);
	}
}

static void ConigureEncryption(config::JsonConfig& config) {
	std::string result;
	{
		StringDelimAppend(result, '.',
			{VmdkConfig::kEncryption, VmdkConfig::kEnabled});

		std::string enable_encryption;
		std::cout << "Is encryption enabled [y/n]? ";
		std::cin >> enable_encryption;
		if (enable_encryption == "n") {
			config.SetKey(result, false);
			return;
		}

		config.SetKey(result, true);
	}

	{
		StringDelimAppend(result, '.',
			{VmdkConfig::kEncryption, VmdkConfig::kEncryptionKey});

		std::string key;
		std::cout << "Enter encryption key: ";
		std::cin >> key;

		config.SetKey(result, key);
	}

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

	config::JsonConfig config;

	config.SetKey(VmConfig::kVmID, vmid);
	config.SetKey(VmdkConfig::kVmdkID, vmdkid);
	config.SetKey(VmdkConfig::kBlockSize, block_size);

	ConigureCompression(config);
	ConigureEncryption(config);

	std::cout << "VMDK Configuration\n\n"
		<< config.Serialize() << std::endl;

	return 0;
}