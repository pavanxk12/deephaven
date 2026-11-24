#include "data/token_ids.h"

#include <algorithm>

namespace algo {

TokenIds::TokenIds(int32_t day, std::span<const int64_t> initial, uint32_t capacity) : day_(day) {
    base_.build(initial, 0);                                     // sorts + de-duplicates; ids 0..n-1
    initial_ = base_.size();
    capacity_ = std::max(capacity, initial_);
    const uint32_t extra = capacity_ - initial_;
    bits_ = 4;
    while ((1ull << bits_) < 2ull * std::max<uint32_t>(extra, 1)) ++bits_;
    const size_t slots = size_t(1) << bits_;
    keys_.reset(new std::atomic<int64_t>[slots]);
    vals_.reset(new std::atomic<int32_t>[slots]);
    for (size_t i = 0; i < slots; ++i) { keys_[i].store(kNoToken, std::memory_order_relaxed); vals_[i].store(kNone, std::memory_order_relaxed); }
    added_token_.reset(new std::atomic<int64_t>[std::max<uint32_t>(extra, 1)]);
    for (uint32_t i = 0; i < std::max<uint32_t>(extra, 1); ++i) added_token_[i].store(kNoToken, std::memory_order_relaxed);
    size_.store(initial_, std::memory_order_release);
}

int32_t TokenIds::probe(int64_t token) const noexcept {
    const uint32_t mask = (1u << bits_) - 1;
    for (uint32_t i = slotOf(token, bits_), n = 0; n <= mask; i = (i + 1) & mask, ++n) {
        const int64_t k = keys_[i].load(std::memory_order_acquire);
        if (k == token) return vals_[i].load(std::memory_order_relaxed);
        if (k == kNoToken) return kNone;
    }
    return kNone;
}

int32_t TokenIds::find(int64_t token) const noexcept {
    if (token == kNoToken) return kNone;
    const int32_t id = base_.find(token);
    if (id != kNone) return id;
    return size() > initial_ ? probe(token) : kNone;
}

int32_t TokenIds::findOrAdd(int64_t token) {
    const int32_t known = find(token);
    if (known != kNone || token == kNoToken) return known;
    std::lock_guard<std::mutex> g(add_);
    const int32_t again = probe(token);                          // another thread may have added it meanwhile
    if (again != kNone) return again;
    const uint32_t id = size_.load(std::memory_order_relaxed);
    if (id >= capacity_) return kNone;
    const uint32_t mask = (1u << bits_) - 1;
    for (uint32_t i = slotOf(token, bits_);; i = (i + 1) & mask) {
        if (keys_[i].load(std::memory_order_relaxed) != kNoToken) continue;
        vals_[i].store(static_cast<int32_t>(id), std::memory_order_relaxed);
        added_token_[id - initial_].store(token, std::memory_order_relaxed);
        keys_[i].store(token, std::memory_order_release);        // publishes vals_[i] and added_token_[...]
        size_.store(id + 1, std::memory_order_release);
        return static_cast<int32_t>(id);
    }
}

int64_t TokenIds::tokenOf(int32_t id) const noexcept {
    if (id < 0) return kNoToken;
    const auto u = static_cast<uint32_t>(id);
    if (u < initial_) return base_.tokenOf(u);
    if (u >= size()) return kNoToken;
    return added_token_[u - initial_].load(std::memory_order_acquire);
}

}  // namespace algo
