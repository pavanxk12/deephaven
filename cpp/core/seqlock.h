// Seqlock for one small trivially-copyable value with one writer and any number of readers (quote[token], analytics
// slots, the open bar the API reads). The writer never waits. A reader retries only if a write overlapped its copy.
// The payload is stored as relaxed atomic 64-bit words, so concurrent reads and writes are not a data race (no UB).
#pragma once
#include <atomic>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace algo {

template <typename T>
class Seqlock {
    static_assert(std::is_trivially_copyable_v<T>, "seqlock payload must be trivially copyable");
    static_assert(sizeof(T) % 8 == 0, "seqlock payload size must be a multiple of 8 bytes");
    static constexpr size_t kWords = sizeof(T) / 8;

public:
    void write(const T& v) noexcept {                      // only one thread may write
        const uint64_t s = seq_.load(std::memory_order_relaxed);
        seq_.store(s + 1, std::memory_order_relaxed);      // odd: writing
        std::atomic_thread_fence(std::memory_order_release);
        uint64_t w[kWords];
        std::memcpy(w, &v, sizeof(T));
        for (size_t i = 0; i < kWords; ++i) words_[i].store(w[i], std::memory_order_relaxed);
        seq_.store(s + 2, std::memory_order_release);      // even: done
    }

    // One attempt: false if a write was in progress or overlapped.
    bool tryRead(T& out) const noexcept {
        const uint64_t s1 = seq_.load(std::memory_order_acquire);
        if (s1 & 1) return false;
        uint64_t w[kWords];
        for (size_t i = 0; i < kWords; ++i) w[i] = words_[i].load(std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_acquire);
        if (seq_.load(std::memory_order_relaxed) != s1) return false;
        std::memcpy(&out, w, sizeof(T));
        return true;
    }
    void read(T& out) const noexcept { while (!tryRead(out)) {} }
    uint64_t version() const noexcept { return seq_.load(std::memory_order_acquire); }

private:
    std::atomic<uint64_t> seq_{0};
    std::atomic<uint64_t> words_[kWords]{};
};

}  // namespace algo
