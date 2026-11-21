// Single-producer / single-consumer ring, in-process. Wait-free on both sides: one acquire load, one release store,
// no CAS. Capacity is a power of two (index = seq & mask). Head and tail live on their own cache lines, each next to
// the side's cached copy of the other index, so the two threads never write the same line.
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>

namespace algo {

inline constexpr size_t kCacheLine = 64;

template <typename T, size_t Capacity>
class SpscRing {
    static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0, "capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>, "ring slots hold trivially copyable messages");
    static constexpr uint64_t kMask = Capacity - 1;

public:
    // producer ---------------------------------------------------------------------------------
    // A slot to construct the next message in place, or nullptr when full. Call publish() after filling it.
    T* claim() noexcept {
        const uint64_t h = prod_.head;
        if (h - prod_.tail_cache >= Capacity) {
            prod_.tail_cache = tail_.value.load(std::memory_order_acquire);
            if (h - prod_.tail_cache >= Capacity) return nullptr;
        }
        return &slots_[h & kMask];
    }
    void publish() noexcept { head_.value.store(++prod_.head, std::memory_order_release); }
    bool tryPush(const T& v) noexcept { T* s = claim(); if (!s) return false; *s = v; publish(); return true; }

    // consumer ---------------------------------------------------------------------------------
    // Pointer to the oldest message, or nullptr when empty. Call pop() when done with it.
    const T* front() noexcept {
        const uint64_t t = cons_.tail;
        if (t == cons_.head_cache) {
            cons_.head_cache = head_.value.load(std::memory_order_acquire);
            if (t == cons_.head_cache) return nullptr;
        }
        return &slots_[t & kMask];
    }
    void pop() noexcept { tail_.value.store(++cons_.tail, std::memory_order_release); }
    bool tryPop(T& out) noexcept { const T* s = front(); if (!s) return false; out = *s; pop(); return true; }

    // Drain everything available: fn(const T&) per message, one release store at the end. Returns how many.
    template <typename Fn>
    size_t drain(Fn&& fn) noexcept {
        const uint64_t h = head_.value.load(std::memory_order_acquire);
        uint64_t t = cons_.tail;
        const size_t n = static_cast<size_t>(h - t);
        for (; t != h; ++t) fn(slots_[t & kMask]);
        cons_.tail = h;
        cons_.head_cache = h;
        if (n) tail_.value.store(h, std::memory_order_release);
        return n;
    }

    size_t sizeApprox() const noexcept {
        return static_cast<size_t>(head_.value.load(std::memory_order_acquire) - tail_.value.load(std::memory_order_acquire));
    }
    static constexpr size_t capacity() noexcept { return Capacity; }

private:
    struct alignas(kCacheLine) Index { std::atomic<uint64_t> value{0}; };
    struct alignas(kCacheLine) ProducerSide { uint64_t head = 0, tail_cache = 0; };
    struct alignas(kCacheLine) ConsumerSide { uint64_t tail = 0, head_cache = 0; };

    Index        head_;
    ProducerSide prod_;
    Index        tail_;
    ConsumerSide cons_;
    alignas(kCacheLine) T slots_[Capacity];
};

}  // namespace algo
