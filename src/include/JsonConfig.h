#pragma once

#include <string>
#include "PropertyTree.h"

namespace pio { namespace config {
class JsonConfig {
public:
	JsonConfig();
	JsonConfig(const std::string& config);

	JsonConfig(const JsonConfig& rhs) = delete;
	JsonConfig(JsonConfig&& rhs) = delete;
	JsonConfig& operator = (const JsonConfig& rhs) = delete;
	JsonConfig& operator = (JsonConfig&& rhs) = delete;

	void Serialize(std::ostringstream& to, bool pretty = false) const;
	std::string Serialize(bool pretty = false) const;
	void Deserialize(std::istringstream& from);
	void Deserialize(const std::string& from);

	template <typename V>
	bool GetKey(const std::string& key, V& value) const {
		try {
			value = tree_.get<V>(key);
		} catch (const boost::property_tree::ptree_error& e) {
			return false;
		}
		return true;
	}

	template <typename V>
	void SetKey(const std::string& key, const V& value) {
		tree_.put(key, value);
	}
private:
	boost::property_tree::ptree tree_;
};
}
}