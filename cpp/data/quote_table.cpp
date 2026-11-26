#include "data/quote_table.h"

namespace algo {

QuoteTable::QuoteTable(uint32_t capacity) : capacity_(capacity), slots_(new Slot[capacity]) {}

void QuoteTable::update(const Tick& t) noexcept {
    if (t.idx < 0 || static_cast<uint32_t>(t.idx) >= capacity_ || !(t.hasTrade() || t.hasQuote())) return;
    Slot& s = slots_[static_cast<uint32_t>(t.idx)];

    // the current value (only this thread writes, so a plain relaxed read of our own words is consistent)
    uint64_t w[7];
    for (int i = 0; i < 7; ++i) w[i] = s.w[i].load(std::memory_order_relaxed);
    Quote q;
    std::memcpy(&q, w, sizeof q);
    if (t.hasTrade()) { q.ltp = t.ltp; q.cum_volume = t.cum_volume; q.time_ns = t.time_ns; q.ltq = t.ltq; }
    if (t.hasBid())   { q.bid = t.bid; q.bid_qty = t.bid_qty; }
    if (t.hasAsk())   { q.ask = t.ask; q.ask_qty = t.ask_qty; }
    std::memcpy(w, &q, sizeof q);

    const uint64_t v = s.seq.load(std::memory_order_relaxed);
    s.seq.store(v + 1, std::memory_order_relaxed);          // odd: writing
    std::atomic_thread_fence(std::memory_order_release);
    for (int i = 0; i < 7; ++i) s.w[i].store(w[i], std::memory_order_relaxed);
    s.seq.store(v + 2, std::memory_order_release);          // even: done
}

bool QuoteTable::read(int32_t idx, Quote& out) const noexcept {
    if (idx < 0 || static_cast<uint32_t>(idx) >= capacity_) return false;
    const Slot& s = slots_[static_cast<uint32_t>(idx)];
    for (;;) {
        const uint64_t v1 = s.seq.load(std::memory_order_acquire);
        if (v1 == 0) return false;                          // never written
        if (v1 & 1) continue;                               // a write is in progress
        uint64_t w[7];
        for (int i = 0; i < 7; ++i) w[i] = s.w[i].load(std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_acquire);
        if (s.seq.load(std::memory_order_relaxed) != v1) continue;
        std::memcpy(&out, w, sizeof out);
        return true;
    }
}

uint64_t QuoteTable::version(int32_t idx) const noexcept {
    if (idx < 0 || static_cast<uint32_t>(idx) >= capacity_) return 0;
    return slots_[static_cast<uint32_t>(idx)].seq.load(std::memory_order_acquire);
}

}  // namespace algo
