#include <stdexcept>
#include <cassert>
#include <string.h>
#include <iostream>

#define CREATE_STRUCT_MEMBER(NAME) \
	template<typename T> \
	struct NAME { \
		T NAME; \
		constexpr static char const *name() { return #NAME; } \
		using type = T; \
		T &value() & { return this->NAME; } \
		T const &value() const & { return this->NAME; } \
		T &&value() && { return this->NAME; } \
	};


namespace custom_struct {

template<typename...Fields>
struct Struct : Fields... {
	using struct_type = Struct;

	Struct() = default;
	Struct(Struct const &) = default;

	template<typename...T>
	constexpr Struct(T &&...x) : Fields{static_cast<T&&>(x)}... {}
};

template<typename R, typename Field, typename...Fields, typename Visitor, typename...AllFields>
R _select_field(Visitor &&v, char const *name, Struct<AllFields...> const &a_struct)
{
	if (strcmp(name, Field::name()) == 0)
		return v(a_struct.Field::value());
	else
		return _select_field<R, Fields...>(static_cast<Visitor&&>(v), name, a_struct);
}

template<typename R, typename Visitor, typename...AllFields>
R _select_field(Visitor &&v, char const *name, Struct<AllFields...> const &a_struct)
{
	// TODO: Need static_assert.
	throw std::runtime_error(std::string{} + "Bad field name in (" +
			__func__ + ") " + __FILE__ + ":" + std::to_string(__LINE__));
}

template<typename Visitor, typename Field, typename...AllFields>
auto select_field(Visitor &&v, char const *name, Struct<Field, AllFields...> const &a_struct)
-> decltype(v(a_struct.Field::value()))
{
	return _select_field<decltype(v(a_struct.Field::value())), Field, AllFields...>
			(static_cast<Visitor&&>(v), name, a_struct);
}

template<typename T>
struct converter {
	template<typename U> T operator()(U &&x) { return T(x); }
};

} // namespace custom_struct.
