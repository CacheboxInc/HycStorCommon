#pragma once

#include "RequestHandler.h"
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_event.h>
#include <AeroConfig.h>

namespace pio {
class AeroSpikeConn{
public:	
	AeroSpikeConn(AeroClusterID cluster_id, const std::string& config);
	~AeroSpikeConn();
	int Connect();
	int Disconnect();
	config::AeroConfig* GetJsonConfig() const noexcept;
private:
	AeroClusterID cluster_id_;
	std::unique_ptr<config::AeroConfig> config_;
public:
	aerospike  as_;
	as_monitor as_mon_;
	bool       as_started_{false};
};
}
