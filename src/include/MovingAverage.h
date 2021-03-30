
namespace hyc {

template <typename T, uint64_t N>
class MovingAverage {
public:
	MovingAverage() = default;
	~MovingAverage() = default;

	MovingAverage(const MovingAverage& rhs) = delete;
	MovingAverage(MovingAverage&& rhs) = delete;
	MovingAverage& operator ==(const MovingAverage& rhs) = delete;
	MovingAverage& operator ==(MovingAverage&& rhs) = delete;

	T Add(T sample) {
		std::lock_guard<std::mutex> lock(mutex_);
		if (nsamples_ < N) {
			samples_[nsamples_++] = sample;
			total_ += sample;
		} else {
			T& oldest = samples_[nsamples_++ % N];
			total_ -= oldest;
			total_ += sample;
			oldest = sample;
		}
		return Average();
	}

	T Average() const noexcept {
		auto div = std::min(nsamples_, N);
		if (not div) {
			return 0;
		}
		return total_ / div;
	}

	void Reset() {
		std::lock_guard<std::mutex> lock(mutex_);
		total_ = 0;
		nsamples_ = 0;
	}

	uint64_t GetSamples() const { return nsamples_; }
	uint64_t GetMaxSamples() const { return N; }

private:
	std::mutex mutex_;
	T samples_[N];
	T total_{0};
	uint64_t nsamples_{0};
};

} // namespace hyc

