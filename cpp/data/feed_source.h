// FeedSource: the plug every market-data source implements. Downstream (the pipeline: quote table, candles, later
// analytics and strategies) sees only batches of Ticks, so replacing Kafka with a direct exchange snapshot or a
// tick-by-tick multicast feed is one new FeedSource, with no change anywhere else.
//
//   KafkaFeed       hft.marketdata.fo rows (text ltt)   -> TextRowAdapter -> Ticks      (needs librdkafka; spike 0)
//   DhFeed          fo_market_data via the DH C++ client -> TextRowAdapter -> Ticks      (needs the DH client; spike 1)
//   SnapshotFeed    a direct exchange snapshot / TBT feed: already binary -> Ticks directly (later)
//   ReplayFeed      a recorded CSV (tools/candles_replay)  -> TextRowAdapter -> Ticks
//
// A batch is one delivery of the source: one Deephaven update cycle, one Kafka poll, one multicast packet. Candles flush
// once per batch (the Python rule), so a source must not split one delivery into several batches.
// Everything a source owns (sockets, threads, reconnect, gap recovery) stays inside it.
#pragma once
#include <cmath>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "data/ltt_parser.h"
#include "data/tick.h"

namespace algo {

class TickSink {
public:
    virtual ~TickSink() = default;
    // One batch, in arrival order. The span is the source's buffer: the sink may fill in `idx` but must not keep it.
    virtual void onTicks(std::span<Tick> batch) = 0;
};

class FeedSource {
public:
    virtual ~FeedSource() = default;
    virtual bool start(TickSink& sink, std::string& error) = 0;   // begin delivering batches (on the source's own thread)
    virtual void stop() = 0;                                      // stop and join; no batch is delivered after it returns
    virtual std::string describe() const = 0;                     // one line for status
};

// ----------------------------------------------------------------------------------------------------------------
// The Kafka / Deephaven row shape: one row per token update, ltt as text, numbers that may be NaN / inf / missing.
// ----------------------------------------------------------------------------------------------------------------
struct TextRow {
    int64_t     token;
    double      ltp;         // raw feed units (minor units); NaN / inf / <= 0 = no usable trade
    double      vol;         // cumulative volume
    double      ltq;
    double      bid, ask;    // NaN / <= 0 = none
    double      bid_qty, ask_qty;
    const char* ltt;         // nullptr = null
    uint32_t    ltt_len;
};

struct AdapterStats {
    uint64_t rows = 0, trades = 0, quote_only = 0, no_trade = 0, bad_time = 0, bad_token = 0;
};

// TextRow -> Tick, with the exact rules of the Python Candles plugin for the trade part (_clean + LttParser):
//   * a trade needs ltp finite and > 0, vol finite and >= 0, ltt not null, and an ltt that parses;
//     otherwise the row carries no trade (ltp = 0) but its bid / ask still go through;
//   * ltq that is missing / not finite / <= 0 is 0; vol is truncated to an integer (numpy astype(int64));
//   * dedup_key is a hash of the RAW ltt text, so the repeat check compares the text itself, as the Python does.
// A row with neither a usable trade nor a usable bid / ask is dropped.
class TextRowAdapter {
public:
    explicit TextRowAdapter(LttParser& parser, size_t reserve = 1 << 16) : parser_(parser) { buf_.reserve(reserve); }

    // Convert one delivery and hand it to the sink as one batch.
    void deliver(std::span<const TextRow> rows, TickSink& sink) {
        buf_.clear();
        for (const TextRow& r : rows) { Tick t; if (convert(r, t)) buf_.push_back(t); }
        sink.onTicks(std::span<Tick>(buf_.data(), buf_.size()));
    }

    bool convert(const TextRow& r, Tick& t) noexcept {
        ++stats_.rows;
        if (r.token < INT32_MIN || r.token > INT32_MAX) { ++stats_.bad_token; return false; }
        t = Tick{};
        t.token = static_cast<int32_t>(r.token);
        if (std::isfinite(r.bid) && r.bid > 0) { t.bid = std::llround(r.bid); t.bid_qty = qty(r.bid_qty); }
        if (std::isfinite(r.ask) && r.ask > 0) { t.ask = std::llround(r.ask); t.ask_qty = qty(r.ask_qty); }

        bool trade = std::isfinite(r.ltp) && r.ltp > 0 && std::isfinite(r.vol) && r.vol >= 0 && r.ltt != nullptr;
        int64_t price = 0, sec = 0;
        if (trade) {
            price = std::llround(r.ltp);
            if (price <= 0) trade = false;
        }
        if (trade) {
            sec = parser_.parse(std::string_view(r.ltt, r.ltt_len));
            if (sec == LttParser::kBad) { trade = false; ++stats_.bad_time; }
        }
        if (trade) {
            t.ltp = price;
            t.cum_volume = static_cast<int64_t>(r.vol);
            t.time_ns = sec * 1000000000LL;
            t.ltq = (std::isfinite(r.ltq) && r.ltq > 0) ? static_cast<int32_t>(std::llround(r.ltq)) : 0;
            t.dedup_key = fnv1a32(r.ltt, r.ltt_len);
            ++stats_.trades;
        } else {
            ++stats_.no_trade;
        }
        if (!t.hasTrade() && !t.hasQuote()) return false;
        if (!t.hasTrade()) ++stats_.quote_only;
        return true;
    }

    const AdapterStats& stats() const noexcept { return stats_; }

    static uint32_t fnv1a32(const char* p, uint32_t n) noexcept {
        uint32_t h = 2166136261u;
        for (uint32_t i = 0; i < n; ++i) { h ^= static_cast<unsigned char>(p[i]); h *= 16777619u; }
        return h;
    }

private:
    static int32_t qty(double q) noexcept { return (std::isfinite(q) && q > 0) ? static_cast<int32_t>(std::llround(q)) : 0; }

    LttParser&        parser_;
    std::vector<Tick> buf_;
    AdapterStats      stats_;
};

}  // namespace algo
