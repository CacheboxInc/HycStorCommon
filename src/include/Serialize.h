#pragma once

namespace hyc {
namespace BigEndian {
template <typename T>
constexpr void SerializeInt8(T no, uint8_t* destp) {
	*destp = no;
}

template <typename T>
constexpr void SerializeInt16(T no, uint8_t* destp) {
	SerializeInt8(no >> 8, destp);
	SerializeInt8(no, destp + 1);
}

template <typename T>
constexpr void SerializeInt32(T no, uint8_t* destp) {
	SerializeInt16(no >> 16, destp);
	SerializeInt16(no, destp + 2);
}

template <typename T>
constexpr void SerializeInt64(T no, uint8_t* destp) {
	SerializeInt32(no >> 32, destp);
	SerializeInt32(no, destp + 4);
}

template <typename T>
constexpr void SerializeInt(T no, uint8_t* destp) {
	if constexpr (sizeof(T) >= 8) {
		SerializeInt64(no, destp);
	} else if constexpr (sizeof(T) >= 4) {
		SerializeInt32(no, destp);
	} else if constexpr (sizeof(T) >= 2) {
		SerializeInt16(no, destp);
	} else {
		SerializeInt8(no, destp);
	}
}

template <typename T>
constexpr T DeserializeInt8(const uint8_t* srcp) {
	return static_cast<T>(*srcp);
}

template <typename T>
constexpr T DeserializeInt16(const uint8_t* srcp) {
	return DeserializeInt8<T>(srcp) << 8 | DeserializeInt8<T>(srcp + 1);
}

template <typename T>
constexpr T DeserializeInt32(const uint8_t* srcp) {
	return DeserializeInt16<T>(srcp) << 16 | DeserializeInt16<T>(srcp + 2);
}

template <typename T>
constexpr T DeserializeInt64(const uint8_t* srcp) {
	return DeserializeInt32<T>(srcp) << 32 | DeserializeInt32<T>(srcp + 4);
}

template <typename T>
constexpr T DeserializeInt(const uint8_t* srcp) {
	if constexpr (sizeof(T) >= 8) {
		return DeserializeInt64<T>(srcp);
	} else if constexpr (sizeof(T) >= 4) {
		return DeserializeInt32<T>(srcp);
	} else if constexpr (sizeof(T) >= 2) {
		return DeserializeInt16<T>(srcp);
	} else {
		return DeserializeInt8<T>(srcp);
	}
}
}
}
