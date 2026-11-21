// One feed batch -> the aggregator, with the exact rules of the Python Candles plugin (_clean, _feed, _on_update):
//   * a row is dropped when ltp is not a finite number above 0, vol is not a finite number >= 0, or ltt is null;
//     with an instrument filter, also when the token is not one of the wanted ones;
//   * a missing / bad / non-positive ltq is 0 (it never drops the row);
//   * a row whose (raw ltt text, vol) equals the token's last accepted one is a repeat and is skipped (checked BEFORE
//     parsing); a row whose ltt cannot be parsed is skipped and NOT marked;
//   * the batch's `added` rows are processed before its `modified` rows;
//   * after the batch, bars starting before floor((latest ltt in the batch - grace) / bucket) * bucket are closed.
// Prices stay in the feed's raw minor units (paise / the contract's divisor); conversion to currency happens at the edges.
#pragma once
#include <cmath>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "data/candle_aggregator.h"
#include "data/ltt_parser.h"
#include "data/token_index.h"

namespace algo {

struct FeedRow {
    int64_t          token;
    double           ltp;        // raw feed units (NaN / inf / <= 0 = unusable)
    double           vol;        // cumulative volume
    double           ltq;        // last traded quantity (0 = none)
    const char*      ltt;        // nullptr = null
    uint32_t         ltt_len;
};

struct CandleStats {
    uint64_t rows = 0, dropped = 0, repeats = 0, bad_time = 0, overflow = 0, ticks = 0, bars = 0, batches = 0;
};

class CandleBuilder {
public:
    // allowed: optional per-dense-id filter (nullptr = every instrument, as Python with no symbols= / kind=)
    CandleBuilder(TokenIndex& index, LttParser& parser, int64_t start, int32_t bucket, int32_t grace_s = 10,
                  const std::vector<uint8_t>* allowed = nullptr)
        : index_(index), parser_(parser), agg_(index.capacity(), start, bucket), grace_(grace_s), allowed_(allowed) {}

    template <typename Sink>
    void batch(std::span<const FeedRow> added, std::span<const FeedRow> modified, Sink&& emit) noexcept {
        int64_t latest = 0;
        auto counting = [&](const ClosedBar& b) { ++stats_.bars; emit(b); };
        feedAll(added, latest, counting);
        feedAll(modified, latest, counting);
        if (latest) agg_.flush(CandleAggregator::floorDiv(latest - grace_, agg_.bucket()) * agg_.bucket(), counting);
        ++stats_.batches;
    }

    const CandleStats& stats() const noexcept { return stats_; }
    CandleAggregator& aggregator() noexcept { return agg_; }

    // FNV-1a of the raw ltt bytes: the repeat check compares the text itself, as the Python does
    static uint64_t fingerprint(const char* p, uint32_t n) noexcept {
        uint64_t h = 1469598103934665603ull;
        for (uint32_t i = 0; i < n; ++i) { h ^= static_cast<unsigned char>(p[i]); h *= 1099511628211ull; }
        return h;
    }

private:
    template <typename Sink>
    void feedAll(std::span<const FeedRow> rows, int64_t& latest, Sink& emit) noexcept {
        for (const FeedRow& r : rows) {
            ++stats_.rows;
            if (!(std::isfinite(r.ltp) && r.ltp > 0 && std::isfinite(r.vol) && r.vol >= 0 && r.ltt)) { ++stats_.dropped; continue; }
            int32_t id;
            if (allowed_) {
                id = index_.find(r.token);
                if (id < 0 || !(*allowed_)[static_cast<size_t>(id)]) { ++stats_.dropped; continue; }
            } else {
                id = index_.findOrAdd(r.token);
                if (id < 0) { ++stats_.overflow; continue; }
            }
            const auto idx = static_cast<uint32_t>(id);
            const int64_t price = std::llround(r.ltp);
            if (price <= 0) { ++stats_.dropped; continue; }
            const int64_t vol = static_cast<int64_t>(r.vol);                       // numpy astype(int64): truncation
            const int64_t ltq = (std::isfinite(r.ltq) && r.ltq > 0) ? std::llround(r.ltq) : 0;
            const uint64_t fp = fingerprint(r.ltt, r.ltt_len);
            if (!agg_.isNew(idx, fp, vol)) { ++stats_.repeats; continue; }
            const int64_t sec = parser_.parse(std::string_view(r.ltt, r.ltt_len));
            if (sec == LttParser::kBad) { ++stats_.bad_time; continue; }
            agg_.mark(idx, fp, vol);
            agg_.tick(idx, r.token, price, vol, sec, ltq, emit);
            ++stats_.ticks;
            if (sec > latest) latest = sec;
        }
    }

    TokenIndex&                  index_;
    LttParser&                   parser_;
    CandleAggregator             agg_;
    int32_t                      grace_;
    const std::vector<uint8_t>*  allowed_;
    CandleStats                  stats_;
};

}  // namespace algo
