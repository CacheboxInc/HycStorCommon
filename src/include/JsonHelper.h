#include "JsonConfig.h"
#include "jansson.h"

namespace pio {
class JsonHelper : pio::config::JsonConfig {
public:
	JsonHelper(std::string json_data)
		: pio::config::JsonConfig(json_data) {}
	template <typename V>
	bool GetScalar(const std::string& key, V& value) const {
		return this->GetKey(key, value);
	}

	/*****************************************************************
	 * Given a key extract the array of type V(int, boolean and real)
	 * and return a vector of V
	 *****************************************************************
	*/
	template <typename V>
	bool GetVector(const std::string& key, std::vector<V>& vec) {
		std::string value;
		if(not this->GetKey(key, value)) {
			LOG(ERROR) << __func__ << "key not found:" << key;
			return false;
		}

		json_t *root;
		json_error_t error;
		root = json_loads(value.c_str(), 0, &error);
		if(!json_is_array(root)) {
			json_decref(root);
			return false;
		}
		for(size_t i = 0; i < json_array_size(root); ++i)
		{
			json_t* data = json_array_get(root, i);
			if(json_is_integer(data)) {
				vec.push_back(json_integer_value(data));
			} else if(json_is_boolean(data)) {
				vec.push_back(json_boolean_value(data));
			} else if(json_is_real(data)) {
				vec.push_back(json_real_value(data));
			} else {
				LOG(ERROR) << __func__ << "Invalid data type";
				json_decref(root);
				return false;
			}
		}
		json_decref(root);
		return true;
	}

	/**********************************************************************
	 * Given a key extract the array of type std::string and return vector
	 * of std::string
	 **********************************************************************
	*/
	bool GetVmdkVector(const std::string& key, std::vector<std::string>& vec) {

		std::string value;
		if(not this->GetKey(key, value)) {
			return false;
		}

		json_t *root;
		json_error_t error;
		root = json_loads(value.c_str(), 0, &error);
		if(!json_is_array(root)) {
			json_decref(root);
			return false;
		}

		for(size_t i = 0; i < json_array_size(root); ++i)
		{
			json_t* data = json_array_get(root, i);
			if(!json_is_string(data)) {
				LOG(ERROR) << __func__ << "Not a string type.";
				json_decref(root);
				return false;
			}
			vec.emplace_back(std::string(json_string_value((data))));
		}
		json_decref(root);
		return true;
	}
};
}
