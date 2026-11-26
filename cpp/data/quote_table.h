// QuoteTable: quote[dense_id], the latest state of every instrument, written by the feed thread on every usable tick
// and read by anyone (strategies through the SDK's quote(), the straddle's mid/spread checks, P&L, the API) without a
// lock. One writer, any number of readers.
//
// Each slot is exactly one cache line: an 8-byte seqlock version + 56 bytes of quote, stored as relaxed atomic
// words, so a concurrent read and write is not a data race. The writer never waits; a reader retries only when a
// write overlapped its ~20 ns copy, and can never see a mixed (half old, half new) quote.
//
// A field is only replaced by a usable value: a tick with no trade leaves ltp/volume/time as they were, a tick with
// no bid leaves the bid as it was. So the slot is always "the last known value of each part", like the one-row-per-
// token feed table the Python stack reads.
//
// The array is allocated once (capacity = the TokenIndex's, master + overflow). Living in a shared-memory region for
// out-of-process readers is a matter of where the array is placed; the slot layout is already position-independent.
#pragma once
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>

#include "data/tick.h"

namespace algo {

struct Quote {                    // what a reader gets: 56 bytes of payload (+ the version it was read at)
    int64_t ltp = 0, bid = 0, ask = 0;
    int64_t cum_volume = 0, time_ns = 0;
    int32_t ltq = 0, bid_qty = 0, ask_qty = 0;
    int32_t _pad = 0;
};
static_assert(sizeof(Quote) == 56);

class QuoteTable {
public:
    explicit QuoteTable(uint32_t capacity);

    // feed thread only
    void update(const Tick& t) noexcept;

    // any thread. false: no tick yet for this id (or id out of range). One attempt never fails on a quiet slot;
    // under a concurrent write it retries until it reads a consistent copy.
    bool read(int32_t idx, Quote& out) const noexcept;

    uint32_t capacity() const noexcept { return capacity_; }
    uint64_t version(int32_t idx) const noexcept;      // even and > 0 once written; bumps by 2 per update

private:
    struct alignas(64) Slot {
        std::atomic<uint64_t> seq{0};
        std::atomic<uint64_t> w[7]{};
    };
    static_assert(sizeof(Slot) == 64, "one quote slot per cache line");

    uint32_t                capacity_;
    std::unique_ptr<Slot[]> slots_;
};

}  // namespace algo
