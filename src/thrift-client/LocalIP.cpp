#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <vector>
#include <string>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <linux/if_link.h>

namespace hyc {
std::vector<std::string> GetLocalIPs() {
	std::vector<std::string> ips;

	struct ifaddrs* ifaddr;
	if (getifaddrs(&ifaddr) < 0) {
		return ips;
	}

	for (struct ifaddrs* ifa = ifaddr ; ifa != nullptr; ifa = ifa->ifa_next) {
		if (ifa->ifa_addr == nullptr) {
			continue;
		}
		int family = ifa->ifa_addr->sa_family;
		if (family != AF_INET and family != AF_INET6) {
			continue;
		}

		char host[NI_MAXHOST];
		int rc = getnameinfo(ifa->ifa_addr,
				family == AF_INET ?
					sizeof(struct sockaddr_in) :
					sizeof(struct sockaddr_in6),
				host, sizeof(host), nullptr, 0, NI_NUMERICHOST);
		if (rc < 0) {
			continue;
		}
		ips.emplace_back(host);
	}
	freeifaddrs(ifaddr);
	return ips;
}
}
