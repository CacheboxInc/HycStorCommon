#pragma once

namespace hyc {
namespace BigEndian {
template <typename T>
constexpr void SerializeUInt8(T no, uint8_t* destp) {
	*destp = no;
}

template <typename T>
constexpr void SerializeUInt16(T no, uint8_t* destp) {
	SerializeUInt8(no >> 8, destp);
	SerializeUInt8(no, destp + 1);
}

template <typename T>
constexpr void SerializeUInt32(T no, uint8_t* destp) {
	SerializeUInt16(no >> 16, destp);
	SerializeUInt16(no, destp + 2);
}

template <typename T>
constexpr void SerializeUInt64(T no, uint8_t* destp) {
	SerializeUInt32(no >> 32, destp);
	SerializeUInt32(no, destp + 4);
}

template <typename T>
constexpr void SerializeUInt(T no, uint8_t* destp) {
	if constexpr (sizeof(T) >= 8) {
		SerializeUInt64(no, destp);
	} else if constexpr (sizeof(T) >= 4) {
		SerializeUInt32(no, destp);
	} else if constexpr (sizeof(T) >= 2) {
		SerializeUInt16(no, destp);
	} else {
		SerializeUInt8(no, destp);
	}
}

template <typename T>
constexpr T DeserializeUInt8(const uint8_t* srcp) {
	return static_cast<T>(*srcp);
}

template <typename T>
constexpr T DeserializeUInt16(const uint8_t* srcp) {
	return DeserializeUInt8<T>(srcp) << 8 | DeserializeUInt8<T>(srcp + 1);
}

template <typename T>
constexpr T DeserializeUInt32(const uint8_t* srcp) {
	return DeserializeUInt16<T>(srcp) << 16 | DeserializeUInt16<T>(srcp + 2);
}

template <typename T>
constexpr T DeserializeUInt64(const uint8_t* srcp) {
	return DeserializeUInt32<T>(srcp) << 32 | DeserializeUInt32<T>(srcp + 4);
}

template <typename T>
constexpr T DeserializeUInt(const uint8_t* srcp) {
	if constexpr (sizeof(T) >= 8) {
		return DeserializeUInt64<T>(srcp);
	} else if constexpr (sizeof(T) >= 4) {
		return DeserializeUInt32<T>(srcp);
	} else if constexpr (sizeof(T) >= 2) {
		return DeserializeUInt16<T>(srcp);
	} else {
		return DeserializeUInt8<T>(srcp);
	}
}
}
}
