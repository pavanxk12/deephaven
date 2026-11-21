// Ticks -> closed candles of `bucket` seconds, aligned to the epoch. A state-for-state port of the Python
// CandleAggregator (algo/data/candles.py), so the bars are identical:
//   * a candle closes when a later bar arrives for its token, or in flush(cutoff) once the clock has moved past it;
//   * Volume = cumulative volume at the candle's last tick minus the cumulative volume at the previous candle's close
//     (the first candle of a token counts from its own first tick), never negative;
//   * ticks before `start` build no candle (they only record the cumulative volume);
//   * a tick for an earlier bar than the open one is ignored for OHLC;
//   * CVD: tick-rule running total of ltq (up-tick adds, down-tick subtracts, unchanged keeps the last direction),
//     updated on EVERY tick (also before `start` and for late ticks), anchored at 0 when the aggregator is created.
// Prices are int64 minor units. All state is in flat arrays indexed by the dense token id (no maps, no allocation after
// construction). One aggregator is owned by one thread (a candle shard).
#pragma once
#include <cstdint>
#include <vector>

namespace algo {

struct ClosedBar {
    int64_t token;
    int32_t idx;
    int32_t _pad;
    int64_t start;      // epoch seconds of the bar start
    int64_t open, high, low, close;
    int64_t volume;
    int64_t cvd;
};

class CandleAggregator {
public:
    CandleAggregator(uint32_t capacity, int64_t start, int32_t bucket);

    // The (ltt text fingerprint, cumulative volume) repeat check of the Python is_new / mark.
    bool isNew(uint32_t idx, uint64_t ltt_fp, int64_t vol) const noexcept {
        return !(seen_[idx] && seen_fp_[idx] == ltt_fp && seen_vol_[idx] == vol);
    }
    void mark(uint32_t idx, uint64_t ltt_fp, int64_t vol) noexcept { seen_[idx] = 1; seen_fp_[idx] = ltt_fp; seen_vol_[idx] = vol; }

    template <typename Sink>
    void tick(uint32_t idx, int64_t token, int64_t price, int64_t vol, int64_t sec, int64_t ltq, Sink&& emit) noexcept {
        // tick rule (before anything else, like the Python)
        int8_t dir = last_dir_[idx];
        if (has_last_[idx] && price != last_price_[idx]) dir = price > last_price_[idx] ? 1 : -1;
        last_dir_[idx] = dir;
        last_price_[idx] = price;
        has_last_[idx] = 1;
        if (ltq) cvd_[idx] += dir * ltq;

        const int64_t minute = floorDiv(sec, bucket_) * bucket_;
        if (minute < start_) { prev_cum_[idx] = vol; has_prev_[idx] = 1; return; }
        Open& c = cur_[idx];
        if (c.active && minute > c.minute) { close(idx, emit); }
        if (!c.active) {
            c = Open{minute, price, price, price, price, vol, vol, token, 1};
            addOpen(idx);
        } else if (minute == c.minute) {
            if (price > c.high) c.high = price;
            if (price < c.low) c.low = price;
            c.close = price;
            c.cum = vol;
        }
        // minute < c.minute: a late tick, ignored
    }

    // Close every candle whose bar starts before cutoff (epoch seconds).
    template <typename Sink>
    void flush(int64_t cutoff, Sink&& emit) noexcept {
        uint32_t keep = 0;
        for (uint32_t k = 0; k < open_n_; ++k) {
            const uint32_t idx = open_[k];
            if (cur_[idx].minute < cutoff) {
                emitBar(idx, emit);
            } else {
                open_[keep] = idx; pos_[idx] = keep; ++keep;
            }
        }
        open_n_ = keep;
    }

    static int64_t floorDiv(int64_t a, int64_t b) noexcept { const int64_t q = a / b; return (a % b != 0 && ((a < 0) != (b < 0))) ? q - 1 : q; }

    uint32_t openCount() const noexcept { return open_n_; }
    int64_t  start() const noexcept { return start_; }
    int32_t  bucket() const noexcept { return bucket_; }

private:
    struct Open {
        int64_t minute, open, high, low, close, cum, first;
        int64_t token;
        uint8_t active;
    };

    template <typename Sink>
    void close(uint32_t idx, Sink&& emit) noexcept {          // a later bar arrived: close and remove from the open list
        emitBar(idx, emit);
        const uint32_t p = pos_[idx], last = open_[--open_n_];
        open_[p] = last; pos_[last] = p;
    }
    template <typename Sink>
    void emitBar(uint32_t idx, Sink&& emit) noexcept {        // emit and deactivate (the open list is fixed by the caller)
        Open& c = cur_[idx];
        const int64_t base = has_prev_[idx] ? prev_cum_[idx] : c.first;
        const int64_t v = c.cum - base;
        emit(ClosedBar{c.token, static_cast<int32_t>(idx), 0, c.minute, c.open, c.high, c.low, c.close, v > 0 ? v : 0, cvd_[idx]});
        prev_cum_[idx] = c.cum;
        has_prev_[idx] = 1;
        c.active = 0;
    }
    void addOpen(uint32_t idx) noexcept { pos_[idx] = open_n_; open_[open_n_++] = idx; }

    int64_t start_;
    int32_t bucket_;
    std::vector<Open>     cur_;
    std::vector<int64_t>  prev_cum_;  std::vector<uint8_t> has_prev_;
    std::vector<uint64_t> seen_fp_;   std::vector<int64_t> seen_vol_; std::vector<uint8_t> seen_;
    std::vector<int64_t>  last_price_; std::vector<uint8_t> has_last_; std::vector<int8_t> last_dir_;
    std::vector<int64_t>  cvd_;
    std::vector<uint32_t> open_, pos_;
    uint32_t              open_n_ = 0;
};

}  // namespace algo
