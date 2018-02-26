#include <iostream>

#include "VmConfig.h"

using namespace pio;
using namespace pio::config;

int main(int argc, char* argv[]) {
	VmConfig config;
	std::string vmid;
	uint32_t tid;
	std::string name;

	std::cout << "VM Configuration JSON Dump Utility" << std::endl;
	std::cout << "Enter VmID: ";
	std::cin >> vmid;
	std::cout << "Enter TargetID: ";
	std::cin >> tid;
	std::cout << "Enter TargetName: ";
	std::cin >> name;

	config.SetVmId(vmid);
	config.SetTargetId(tid);
	config.SetTargetName(name);

	std::cout << "VM Configuration\n\n"
		<< config.Serialize() << std::endl;
	return 0;
}