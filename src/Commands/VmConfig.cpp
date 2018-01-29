#include <iostream>

#include "JsonConfig.h"
#include "ConfigConsts.h"

using namespace pio;

int main(int argc, char* argv[]) {
	config::JsonConfig config;
	std::string vmid;

	std::cout << "VM Configuration JSON Dump Utility" << std::endl;
	std::cout << "Enter VmID: ";
	std::cin >> vmid;

	config.SetKey(VmConfig::kVmID, vmid);

	std::cout << "VM Configuration\n\n"
		<< config.Serialize() << std::endl;
	return 0;
}