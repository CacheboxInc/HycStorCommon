#pragma once

#include <memory>
#include <atomic>

namespace pio {
template <typename T>
class SingletonHolder {
public:
	static std::shared_ptr<T> GetInstance() {
		return m_instancep_;
	}

	template <typename... Args>
	static void CreateInstance(Args&&... args) {
		m_instancep_ = std::make_shared<T>(std::forward<Args>(args)...);
	}

	static void DestroyInstance() {
		m_instancep_= nullptr;
	}

private:
	SingletonHolder();
	SingletonHolder(const SingletonHolder&) {}
	SingletonHolder(const SingletonHolder&&) {}
	SingletonHolder& operator == (const SingletonHolder&) {}
	SingletonHolder& operator == (const SingletonHolder&&) {}

	static std::shared_ptr<T> m_instancep_;
};

template<typename T>
std::shared_ptr<T> SingletonHolder<T>::m_instancep_ = nullptr;

}