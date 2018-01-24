#pragma once

#include <string>
#include "PropertyTree.h"

namespace pio { namespace config {
class JsonConfig {
public:
	JsonConfig();

	void Serialize(std::ostringstream& to) const;
	std::string Serialize() const;
	void Deserialize(std::istringstream& from);
	void Deserialize(const std::string& from);

	template <typename V>
	V GetKey(const std::string& key) const {
		return tree_.get<V>(key);
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