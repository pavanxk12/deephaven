// BasketRsiSingle: the idea of examples/07_basket_rsi_crossorder.py as an AlgoSDK plugin (the single-order style).
//
//   Universe   the symbols of one baskets.txt basket
//   Filter     the symbol's % from the open AND % from the VWAP must sit in the middle of its basket (rel percentile
//              inside [mid_lo, mid_hi]): computed ONCE per minute by algod for every strategy that wants it
//   Signal     1-minute Wilder RSI: below rsi_low -> BUY, above rsi_high -> SELL. One position per symbol at a time.
//   Order      a NEW portfolio per trade, started by ONE subscribe that carries the engine's single-order fields
//              (strategyname=single = the engine's CrossOrder plugin: TOKEN, SIDE, QTY, SERVERCODE, ACCOUNT, ALGOID)
//   Exit       its own target / stop on the 1-minute close -> squareup of that portfolio; unsubscribe `unsub_after_s` later
#include <cmath>
#include <cstdio>
#include <cstring>

#include "AlgoSDK.h"
#include "AlgoUtil.h"

class BasketRsiSingle final : public AlgoStrategy {
public:
    void onStart(const AlgoSettings& s) override {
        copyText(basket_, sizeof basket_, s.get("basket"), "banks");
        rsiN_       = static_cast<uint32_t>(s.getInt("rsi.n", 9));
        rsiHigh_    = s.getDouble("rsi.high", 70);
        rsiLow_     = s.getDouble("rsi.low", 30);
        targetPct_  = s.getDouble("exit.target_pct", 1.5);
        stopPct_    = s.getDouble("exit.stop_pct", 0.5);
        unsubAfter_ = static_cast<int32_t>(s.getInt("exit.unsub_after_s", 60));
        lots_       = s.getInt("order.qty_lots", 1);
        midLo_      = static_cast<float>(s.getDouble("filter.mid_lo", 25));
        midHi_      = static_cast<float>(s.getDouble("filter.mid_hi", 75));
        maxOpen_    = static_cast<uint32_t>(s.getInt("max_portfolios", 28));
        s.toParams("engine", engine_);                                     // strategyname=single, SERVERCODE, ACCOUNT, ALGOID

        n_ = basket(basket_, tokens_, kMax);
        if (n_ < 3) { log("BasketRsiSingle: the basket needs at least 3 symbols; not started", AlgoLogLevel::ERROR); return; }
        for (uint32_t i = 0; i < n_; ++i) { rsi_[i] = algo::WilderRsi(rsiN_); value_[i] = NAN; }   // NaN = no RSI yet (0 would read as oversold)

        // the same two series for the whole basket, computed once a minute by algod (relative() of the Python stack)
        hOpen_ = want(AlgoSeries::PCT, AlgoLevel::INTRADAY, AlgoRef::open(), basket_, 0);
        hVwap_ = want(AlgoSeries::PCT, AlgoLevel::INTRADAY, AlgoRef::vwap(), basket_, 0);

        for (uint32_t i = 0; i < n_; ++i) {                                 // seed the RSI from today's bars so far
            const AlgoBar* b = nullptr;
            for (uint32_t k = 0, m = bars(tokens_[i], 200, b); k < m; ++k) rsi_[i].update(static_cast<double>(b[k].close));
        }
        ready_ = true;
    }

    void onBars(const AlgoBarBatch& batch) override {
        if (!ready_) return;
        for (uint32_t k = 0; k < batch.count; ++k) {
            const AlgoBar& b = batch.bars[k];
            const int i = slotOf(b.token);
            if (i < 0 || b.close <= 0) continue;
            last_[i] = b.close;
            value_[i] = rsi_[i].update(static_cast<double>(b.close));
            checkExit(static_cast<uint32_t>(i));
        }
    }

    // the basket's analytics for this minute are published: entries are decided here, with the filter values fresh
    void onAnalytics(int32_t /*minute*/) override {
        if (!ready_ || !sending()) return;
        for (uint32_t i = 0; i < n_; ++i) {
            if (pos_[i].pf || std::isnan(value_[i]) || open_ >= maxOpen_) continue;
            const int side = value_[i] < rsiLow_ ? 1 : value_[i] > rsiHigh_ ? 2 : 0;    // 1 = BUY, 2 = SELL (the engine's SIDE)
            if (!side || !inMiddle(tokens_[i])) continue;
            enter(i, side);
        }
    }

    void onPfState(const AlgoPfState& st) override {
        for (uint32_t i = 0; i < n_; ++i)
            if (pos_[i].pf == st.pf && st.squared && pos_[i].state == OPEN) { pos_[i].state = CLOSING; pos_[i].closed_at = clockNs(); }
    }

    void onTimer() override {                                               // release the closed portfolios (the engine has limited plugin slots)
        const int64_t now = clockNs();
        bool pending = false;
        for (uint32_t i = 0; i < n_; ++i) {
            Pos& p = pos_[i];
            if (p.state != CLOSING) continue;
            if (now - p.closed_at >= static_cast<int64_t>(unsubAfter_) * 1000000000LL) {
                unsubscribe(p.pf, tokens_[i]);
                p = Pos{};
                --open_;
            } else {
                pending = true;
            }
        }
        if (pending) setTimerMs(1000);
    }

private:
    static constexpr uint32_t kMax = 512;
    enum State : uint8_t { FLAT = 0, OPEN = 1, CLOSING = 2 };
    struct Pos { int32_t pf = 0; int side = 0; int64_t entry = 0; int64_t closed_at = 0; State state = FLAT; };

    int slotOf(int32_t token) const noexcept {
        for (uint32_t i = 0; i < n_; ++i) if (tokens_[i] == token) return static_cast<int>(i);
        return -1;
    }

    bool inMiddle(int32_t token) const noexcept {
        AlgoAnalytics o, v;
        if (!analytics(hOpen_, token, o) || !analytics(hVwap_, token, v)) return false;
        if (o.rel_idx < 0 || v.rel_idx < 0) return false;
        return o.rel_pctile >= midLo_ && o.rel_pctile <= midHi_ && v.rel_pctile >= midLo_ && v.rel_pctile <= midHi_;
    }

    void enter(uint32_t i, int side) {
        AlgoContract c;
        if (!info(tokens_[i], c)) return;
        const int32_t pf = newPf(c.symbol);
        if (!pf) return;
        AlgoParams p = engine_;
        p.setInt("TOKEN", tokens_[i]);
        p.setInt("SIDE", side);
        p.setInt("QTY", lots_);
        const int64_t r = subscribe(pf, tokens_[i], p);
        if (r <= 0) return;                                                  // OFF / FULL / BLOCKED ...: try again on a later signal
        pos_[i] = Pos{pf, side, last_[i], 0, OPEN};
        ++open_;
        char msg[128];
        std::snprintf(msg, sizeof msg, "%s: %s %lld lot(s) on PF %d (RSI %.1f)", c.symbol, side == 1 ? "BUY" : "SELL", (long long)lots_, pf, value_[i]);
        event(msg);
    }

    void checkExit(uint32_t i) {
        Pos& p = pos_[i];
        if (p.state != OPEN || p.entry <= 0) return;
        const double move = (static_cast<double>(last_[i] - p.entry) / static_cast<double>(p.entry)) * 100.0 * (p.side == 1 ? 1 : -1);
        if (move >= targetPct_ || move <= -stopPct_) {
            if (squareup(p.pf) > 0) { p.state = CLOSING; p.closed_at = clockNs(); setTimerMs(1000); }
        }
    }

    static void copyText(char* dst, size_t n, const char* src, const char* dflt) noexcept {
        std::strncpy(dst, (src && *src) ? src : dflt, n - 1);
        dst[n - 1] = '\0';
    }

    char      basket_[32] = {};
    int32_t   tokens_[kMax] = {};
    uint32_t  n_ = 0, rsiN_ = 9, maxOpen_ = 28, open_ = 0;
    double    rsiHigh_ = 70, rsiLow_ = 30, targetPct_ = 1.5, stopPct_ = 0.5;
    float     midLo_ = 25, midHi_ = 75;
    int32_t   unsubAfter_ = 60, hOpen_ = -1, hVwap_ = -1;
    int64_t   lots_ = 1;
    AlgoParams engine_;
    algo::WilderRsi rsi_[kMax];
    double    value_[kMax] = {};
    int64_t   last_[kMax] = {};
    Pos       pos_[kMax];
    bool      ready_ = false;
};

static const char* const kClasses[] = {"BasketRsiSingle"};
static const AlgoPluginInfo kInfo = {ALGO_SDK_API_VERSION, 1, "basket_rsi_single", "1.0.0", kClasses};

extern "C" ALGO_EXPORT const AlgoPluginInfo* algoGetInfo() { return &kInfo; }
extern "C" ALGO_EXPORT AlgoStrategy* algoCreate(const char* cls, const char*) {
    return (cls && std::strcmp(cls, "BasketRsiSingle") == 0) ? new BasketRsiSingle() : nullptr;
}
extern "C" ALGO_EXPORT void algoDestroy(AlgoStrategy* s) { delete s; }
