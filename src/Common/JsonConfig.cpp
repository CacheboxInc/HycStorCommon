#include "JsonConfig.h"

namespace pio { namespace config {

namespace pt = boost::property_tree;

JsonConfig::JsonConfig() {
	tree_.clear();
}

void JsonConfig::Serialize(std::ostringstream& to) const {
	pt::write_json(to, tree_);
}

std::string JsonConfig::Serialize() const {
	std::ostringstream os;
	Serialize(os);
	return os.str();
}

void JsonConfig::Deserialize(std::istringstream& from) {
	pt::read_json(from, tree_);
}

void JsonConfig::Deserialize(const std::string& from) {
	std::istringstream is(from);
	pt::read_json(is, tree_);
}

}}