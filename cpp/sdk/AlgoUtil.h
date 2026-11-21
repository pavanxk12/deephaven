// AlgoUtil.h — header-only helpers for strategies: rolling windows, SMA / EMA / Wilder RSI, z-score, tick rounding.
// No allocation: every window has a compile-time capacity. Prices are int64 minor units; indicators return doubles.
#pragma once
#include <cmath>
#include <cstdint>

#include "AlgoSDK.h"

namespace algo {

// Fixed-capacity ring of the last N values (N a power of two keeps the index a mask).
template <typename T, uint32_t N>
class Window {
    static_assert(N > 0 && (N & (N - 1)) == 0, "Window capacity must be a power of two");
public:
    void     push(T v) noexcept { buf_[head_++ & (N - 1)] = v; if (size_ < N) ++size_; }
    uint32_t size() const noexcept { return size_; }
    bool     full(uint32_t n) const noexcept { return size_ >= n; }
    // k = 0 is the newest value
    T        back(uint32_t k = 0) const noexcept { return buf_[(head_ - 1 - k) & (N - 1)]; }
    void     clear() noexcept { head_ = 0; size_ = 0; }
private:
    T        buf_[N] = {};
    uint32_t head_ = 0, size_ = 0;
};

// Mean of the newest n values; NaN until there are n.
template <typename T, uint32_t N>
inline double sma(const Window<T, N>& w, uint32_t n) noexcept {
    if (n == 0 || n > N || !w.full(n)) return NAN;
    double s = 0;
    for (uint32_t k = 0; k < n; ++k) s += static_cast<double>(w.back(k));
    return s / n;
}

// Population z-score of the newest value against the newest n values; NaN until there are n or when flat.
template <typename T, uint32_t N>
inline double zscore(const Window<T, N>& w, uint32_t n) noexcept {
    const double m = sma(w, n);
    if (std::isnan(m)) return NAN;
    double v = 0;
    for (uint32_t k = 0; k < n; ++k) { const double d = static_cast<double>(w.back(k)) - m; v += d * d; }
    v /= n;
    return v > 0 ? (static_cast<double>(w.back(0)) - m) / std::sqrt(v) : NAN;
}

// Exponential moving average, seeded with the SMA of the first `n` values (the usual definition).
class Ema {
public:
    explicit Ema(uint32_t n = 10) noexcept : n_(n), a_(2.0 / (n + 1.0)) {}
    double update(double x) noexcept {
        if (seen_ < n_) { sum_ += x; if (++seen_ == n_) v_ = sum_ / n_; return seen_ == n_ ? v_ : NAN; }
        v_ += a_ * (x - v_);
        return v_;
    }
    double value() const noexcept { return seen_ >= n_ ? v_ : NAN; }
private:
    uint32_t n_, seen_ = 0;
    double   a_, sum_ = 0, v_ = NAN;
};

// Wilder RSI (the one the Python examples use, checked there against the textbook 14-period values).
class WilderRsi {
public:
    explicit WilderRsi(uint32_t n = 14) noexcept : n_(n) {}
    double update(double close) noexcept {
        if (!has_prev_) { prev_ = close; has_prev_ = true; return NAN; }
        const double d = close - prev_; prev_ = close;
        const double g = d > 0 ? d : 0, l = d < 0 ? -d : 0;
        if (count_ < n_) {
            gain_ += g; loss_ += l;
            if (++count_ < n_) return NAN;
            gain_ /= n_; loss_ /= n_;
        } else {
            gain_ = (gain_ * (n_ - 1) + g) / n_;
            loss_ = (loss_ * (n_ - 1) + l) / n_;
        }
        if (loss_ == 0) return gain_ == 0 ? 50.0 : 100.0;
        return 100.0 - 100.0 / (1.0 + gain_ / loss_);
    }
    bool ready() const noexcept { return count_ >= n_; }
private:
    uint32_t n_, count_ = 0;
    bool     has_prev_ = false;
    double   prev_ = 0, gain_ = 0, loss_ = 0;
};

// Round a price in minor units (may be fractional, e.g. premium * 0.01) down / up to a multiple of tick.
inline int64_t floorTick(double minor, int64_t tick) noexcept {
    if (tick <= 0) return 0;
    return static_cast<int64_t>(std::floor(minor / static_cast<double>(tick) + 1e-9)) * tick;
}
inline int64_t ceilTick(double minor, int64_t tick) noexcept {
    if (tick <= 0) return 0;
    return static_cast<int64_t>(std::ceil(minor / static_cast<double>(tick) - 1e-9)) * tick;
}
inline int64_t atLeast(int64_t v, int64_t floor_v) noexcept { return v > floor_v ? v : floor_v; }

// true while minute_of_day is inside [from, to] (exchange local minutes)
inline bool inWindow(int32_t minute_of_day, int32_t from, int32_t to) noexcept { return minute_of_day >= from && minute_of_day <= to; }

}  // namespace algo
