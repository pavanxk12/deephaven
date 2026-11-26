// TokenIds: the day's ONE token -> slot (dense id) map. Every per-token array in algod (quote table, candle state,
// strategy state, the API's views) is indexed by these ids, so a slot means the same token everywhere, all day.
//
//   * created once per trading day by the Master service, from the day's contracts (sorted tokens, ids 0..n-1);
//   * APPEND-ONLY: a token the master does not know (the feed meets it) or a contract a mid-day master reload adds gets
//     the next free id; nothing is ever renumbered or removed, so ids held by any service stay valid all day;
//   * fixed capacity (contracts + an overflow headroom from config), allocated once: the per-token arrays sized from
//     it never grow;
//   * find() is lock-free from any thread: the day's contracts through a direct array (dense tokens) or an
//     open-addressed table, additions through an atomic open-addressed table. Adding (rare: a never-seen token, or a
//     reload) takes a short lock, because both the feed thread and the Master service may add.
#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

#include "data/token_index.h"

namespace algo {

class TokenIds {
public:
    static constexpr int32_t kNone = -1;
    static constexpr int64_t kNoToken = INT64_MIN;

    // initial: the day's contract tokens (any order; sorted and de-duplicated here, ids follow the sorted order).
    // capacity: the most ids the day may ever hold (>= the number of initial tokens).
    TokenIds(int32_t day_yyyymmdd, std::span<const int64_t> initial, uint32_t capacity);

    int32_t  find(int64_t token) const noexcept;                 // any thread, lock-free
    int32_t  findOrAdd(int64_t token);                           // lock-free when known; kNone when full
    int64_t  tokenOf(int32_t id) const noexcept;                 // kNoToken for an id not (yet) in use

    uint32_t size() const noexcept { return size_.load(std::memory_order_acquire); }   // ids in use
    uint32_t capacity() const noexcept { return capacity_; }
    uint32_t initialCount() const noexcept { return initial_; }
    int32_t  day() const noexcept { return day_; }
    bool     direct() const noexcept { return base_.direct(); }

private:
    static uint32_t slotOf(int64_t token, uint32_t bits) noexcept {
        return static_cast<uint32_t>((static_cast<uint64_t>(token) * 0x9E3779B97F4A7C15ull) >> (64 - bits));
    }
    int32_t probe(int64_t token) const noexcept;

    int32_t     day_;
    uint32_t    capacity_, initial_;
    TokenIndex  base_;                                           // the day's contracts: read-only after construction
    // additions: keys go from empty to a token exactly once (release), so a reader never sees a half-written entry
    uint32_t                                bits_ = 0;
    std::unique_ptr<std::atomic<int64_t>[]> keys_;
    std::unique_ptr<std::atomic<int32_t>[]> vals_;
    std::unique_ptr<std::atomic<int64_t>[]> added_token_;        // id - initial_ -> token
    std::atomic<uint32_t>                   size_{0};
    std::mutex                              add_;
};

}  // namespace algo
