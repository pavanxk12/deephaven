// Tick: the ONE normalised market-data record every feed source produces and everything downstream consumes.
//
// A FeedSource adapter (Kafka / Deephaven rows today, a direct exchange snapshot or tick-by-tick feed later) turns its
// own wire format into Ticks; candles, the quote table, analytics and strategies only ever see Ticks. Swapping the
// source is a new adapter, never a change downstream.
//
// One cache line, trivially copyable, no pointers, no text. Prices are int64 minor units (paise / the contract's own
// divisor units), times are epoch nanoseconds. A tick may carry a trade, a quote, or both. A usable price is always
// above 0 (the Python rule), so 0 means "not usable" and no flag word is needed: ltp > 0 = a trade, bid > 0 = a bid,
// ask > 0 = an ask. A quote-only update (bid/ask moved, no new trade) still reaches the quote table while the candle
// builder ignores it.
#pragma once
#include <cstdint>
#include <type_traits>

namespace algo {

struct alignas(64) Tick {
    int32_t  token     = 0;    // exchange token
    int32_t  idx       = -1;   // dense id, filled by the pipeline (TokenIndex), -1 until then
    int64_t  ltp       = 0;    // last traded price, minor units; > 0 only when the whole trade part is usable
    int64_t  bid       = 0;    // best bid, minor units
    int64_t  ask       = 0;    // best ask, minor units
    int64_t  cum_volume= 0;    // the day's cumulative traded volume
    int64_t  time_ns   = 0;    // last trade time (exchange clock), epoch ns
    uint32_t dedup_key = 0;    // identity of the source's trade-time field: the candle builder's repeat check
                               // compares (dedup_key, cum_volume) with the token's last trade, as the Python
                               // compares (raw ltt text, vol). Text sources: a hash of the raw text; binary
                               // sources: the low bits of the exchange sequence number or time.
    int32_t  ltq       = 0;    // last traded quantity (0 = none)
    int32_t  bid_qty   = 0;
    int32_t  ask_qty   = 0;

    bool hasTrade() const noexcept { return ltp > 0; }       // ltp, cum_volume, time_ns, dedup_key all usable
    bool hasBid() const noexcept { return bid > 0; }
    bool hasAsk() const noexcept { return ask > 0; }
    bool hasQuote() const noexcept { return bid > 0 || ask > 0; }
};
static_assert(sizeof(Tick) == 64, "Tick is one cache line");
static_assert(std::is_trivially_copyable_v<Tick>);

}  // namespace algo
