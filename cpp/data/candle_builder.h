// One batch of Ticks -> the aggregator, with the rules of the Python Candles plugin (_feed, _on_update):
//   * only ticks that carry a trade count (a quote-only tick is for the quote table, not for candles);
//   * with an instrument filter, a tick whose dense id is not wanted is skipped;
//   * a tick whose (dedup_key, cum_volume) equals the token's last accepted trade is a repeat and is skipped
//     (the Python compares the raw ltt text and vol; dedup_key is that text's hash);
//   * after the batch, bars starting before floor((latest trade second in the batch - grace) / bucket) * bucket close.
// Source-independent: the text parsing and the _clean rules live in the feed adapter (feed_source.h), so a direct
// binary feed uses this unchanged. Prices are int64 minor units.
#pragma once
#include <cstdint>
#include <span>
#include <vector>

#include "data/candle_aggregator.h"
#include "data/tick.h"

namespace algo {

struct CandleStats {
    uint64_t trades = 0, filtered = 0, repeats = 0, ticks = 0, bars = 0, batches = 0;
};

class CandleBuilder {
public:
    // capacity: the TokenIndex capacity (dense ids). allowed: optional per-dense-id filter (nullptr = everything, as
    // Python with no symbols= / kind=).
    CandleBuilder(uint32_t capacity, int64_t start, int32_t bucket, int32_t grace_s = 10,
                  const std::vector<uint8_t>* allowed = nullptr)
        : agg_(capacity, start, bucket), grace_(grace_s), allowed_(allowed), capacity_(capacity) {}

    template <typename Sink>
    void batch(std::span<const Tick> ticks, Sink&& emit) noexcept {
        int64_t latest = 0;
        auto counting = [&](const ClosedBar& b) { ++stats_.bars; emit(b); };
        for (const Tick& t : ticks) {
            if (!t.hasTrade()) continue;
            ++stats_.trades;
            if (t.idx < 0 || static_cast<uint32_t>(t.idx) >= capacity_) { ++stats_.filtered; continue; }
            const auto idx = static_cast<uint32_t>(t.idx);
            if (allowed_ && !(*allowed_)[idx]) { ++stats_.filtered; continue; }
            if (!agg_.isNew(idx, t.dedup_key, t.cum_volume)) { ++stats_.repeats; continue; }
            agg_.mark(idx, t.dedup_key, t.cum_volume);
            const int64_t sec = CandleAggregator::floorDiv(t.time_ns, 1000000000LL);
            agg_.tick(idx, t.token, t.ltp, t.cum_volume, sec, t.ltq, counting);
            ++stats_.ticks;
            if (sec > latest) latest = sec;
        }
        if (latest) agg_.flush(CandleAggregator::floorDiv(latest - grace_, agg_.bucket()) * agg_.bucket(), counting);
        ++stats_.batches;
    }

    const CandleStats& stats() const noexcept { return stats_; }
    CandleAggregator& aggregator() noexcept { return agg_; }

private:
    CandleAggregator             agg_;
    int32_t                      grace_;
    const std::vector<uint8_t>*  allowed_;
    uint32_t                     capacity_;
    CandleStats                  stats_;
};

}  // namespace algo
