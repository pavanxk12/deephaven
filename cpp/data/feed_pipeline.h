// FeedPipeline: the TickSink every FeedSource delivers to. Per batch, on the feed thread:
//   1. token -> slot, ONCE per tick, through the day's TokenIds (published by the Master service, so the slot means the
//      same token in every service). A token the master does not know is appended (the Python builds candles for it
//      too). From here on everything indexes flat arrays with the slot.
//   2. quote[idx] updated for every tick that carries a trade or a bid / ask (quote-only updates included).
//   3. the trades go to the candle builder, which flushes once per batch.
// Single-threaded today; the sharded version (feed -> SPSC rings -> candle-k threads) keeps exactly these steps, with
// steps 1-2 on the feed thread and step 3 on the shard that owns the token.
#pragma once
#include <cstdint>
#include <functional>
#include <span>

#include "data/candle_builder.h"
#include "data/feed_source.h"
#include "data/quote_table.h"
#include "data/token_ids.h"

namespace algo {

class FeedPipeline final : public TickSink {
public:
    using BarSink = std::function<void(const ClosedBar&)>;    // off the per-tick path: called once per closed bar

    FeedPipeline(TokenIds& ids, QuoteTable& quotes, CandleBuilder& candles, BarSink on_bar)
        : index_(ids), quotes_(quotes), candles_(candles), on_bar_(std::move(on_bar)) {}

    void onTicks(std::span<Tick> batch) override {
        for (Tick& t : batch) {
            const int32_t id = index_.findOrAdd(t.token);
            if (id < 0) { ++overflow_; t.idx = -1; continue; }
            t.idx = id;
            quotes_.update(t);
        }
        candles_.batch(std::span<const Tick>(batch.data(), batch.size()), on_bar_);
        ++batches_;
    }

    uint64_t overflow() const noexcept { return overflow_; }
    uint64_t batches() const noexcept { return batches_; }

private:
    TokenIds&       index_;
    QuoteTable&     quotes_;
    CandleBuilder&  candles_;
    BarSink         on_bar_;
    uint64_t        overflow_ = 0, batches_ = 0;
};

}  // namespace algo
