#include <iostream>

#include "VmConfig.h"

using namespace pio;
using namespace pio::config;

int main(int argc, char* argv[]) {
	VmConfig config;
	std::string vmid;

	std::cout << "VM Configuration JSON Dump Utility" << std::endl;
	std::cout << "Enter VmID: ";
	std::cin >> vmid;

	config.SetVmId(vmid);

	std::cout << "VM Configuration\n\n"
		<< config.Serialize() << std::endl;
	return 0;
}