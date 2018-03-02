#include "JsonConfig.h"

namespace hyc { namespace config {

namespace pt = boost::property_tree;

JsonConfig::JsonConfig() {
	tree_.clear();
}

JsonConfig::JsonConfig(const std::string& from) {
	this->Deserialize(from);
}

void JsonConfig::Serialize(std::ostringstream& to, bool pretty) const {
	pt::write_json(to, tree_, pretty);
}

std::string JsonConfig::Serialize(bool pretty) const {
	std::ostringstream os;
	Serialize(os, pretty);
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
