#include <iostream>

#include "VmConfig.h"

using namespace pio;
using namespace pio::config;

int main(int, char*[]) {
	VmConfig config;
	std::string vmid, cluster_id;
	uint32_t tid;
	std::string name;

	std::cout << "VM Configuration JSON Dump Utility" << std::endl;
	std::cout << "Enter VmID: ";
	std::cin >> vmid;
	std::cout << "Enter TargetID: ";
	std::cin >> tid;
	std::cout << "Enter TargetName: ";
	std::cin >> name;
	std::cout << "Enter Aero Cluster ID: ";
	std::cin >> cluster_id;

	config.SetVmId(vmid);
	config.SetTargetId(tid);
	config.SetTargetName(name);
	config.SetAeroClusterID(cluster_id);

	std::cout << "VM Configuration\n\n"
		<< config.Serialize() << std::endl;
	return 0;
}
