#include <iostream>

#include "AeroConfig.h"
#include "IDs.h"

using namespace pio;
using namespace pio::config;

int main(int, char*[]) {
	AeroConfig config;
	std::string ips;
	uint32_t port;
	AeroClusterID id;

	std::cout << "Aero Configuration JSON Dump Utility" << std::endl;
	std::cout << "Enter IPs: ";
	std::cin >> ips;
	std::cout << "Enter Port number: ";
	std::cin >> port;
	std::cout << "Enter ID: ";
	std::cin >> id;

	config.SetAeroIPs(ips);
	config.SetAeroPort(port);
	config.SetAeroID(id);

	std::cout << "Aero Configuration\n\n"
		<< config.Serialize() << std::endl;

	ips = config.GetAeroIPs();
	std::cout << "ips::" << ips << std::endl;
	port = 0;

	bool ret = config.GetAeroPort(port);
	if (ret) { 
		std::cout << "port::" << port << std::endl;
	}

	ret = config.GetAeroID(id);
	if (ret) { 
		std::cout << "id::" << id << std::endl;
	}

	return 0;
}
