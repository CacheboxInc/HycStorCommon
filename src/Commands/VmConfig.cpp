#include <iostream>

#include "JsonConfig.h"

using namespace pio;

int main(int argc, char* argv[]) {
	config::JsonConfig config;
	std::string vmid;

	std::cout << "VM Configuration JSON Dump Utility" << std::endl;
	std::cout << "Enter VmID: ";
	std::cin >> vmid;

	config.SetKey("VmID", vmid);

	std::cout << "VM Configuration\n\n"
		<< config.Serialize() << std::endl;
	return 0;
}