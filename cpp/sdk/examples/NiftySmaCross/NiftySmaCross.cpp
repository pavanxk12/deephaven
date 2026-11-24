// NiftySmaCross — examples/05_nifty_sma_cross.py as an AlgoSDK plugin.
//
// NIFTY near-expiry options: churn the N strikes nearest the money when the fast SMA of the NIFTY future crosses the slow SMA.
//   fast crosses ABOVE slow -> CALLs on the N nearest strikes        fast crosses BELOW slow -> PUTs (the CALL portfolios retire)
// The book picks every portfolio number; activate() sends [retire old] subscribe, pause, apply, unpause. The engine's
// "Churning" plugin (named in the .ini [subscribe] section) works the orders.
#include <cmath>
#include <cstdio>
#include <cstring>

#include "AlgoSDK.h"
#include "AlgoUtil.h"

class NiftySmaCross final : public AlgoStrategy {
public:
    void onStart(const AlgoSettings& s) override {
        copy(underlying_, s.get("underlying"), "NIFTY");
        expiryIndex_  = static_cast<int32_t>(s.getInt("expiry_index", 0));
        nStrikes_     = static_cast<uint32_t>(s.getInt("n_strikes", 5));
        fast_         = static_cast<uint32_t>(s.getInt("signal.fast", 5));
        slow_         = static_cast<uint32_t>(s.getInt("signal.slow", 20));
        failCooldown_ = static_cast<int32_t>(s.getInt("fail_cooldown_s", 30));
        from_         = s.getMinute("trade_from", 9 * 60 + 20);
        to_           = s.getMinute("trade_to", 15 * 60 + 20);
        ivalPct_      = s.getDouble("sizing.ival_pct", 0.01);
        rangePct_     = s.getDouble("sizing.range_pct", 0.05);
        trpPct_       = s.getDouble("sizing.trp_pct", 0.01);
        jvalFrac_     = s.getDouble("sizing.jval_frac", 0.75);
        svalMult_     = s.getDouble("sizing.sval_mult", 3.0);
        s.toParams("apply", applyFields_);                     // target, stoploss, maxloss, jqty1 ... passed through as they are

        if (nStrikes_ < 1 || nStrikes_ > kMaxStrikes || fast_ < 1 || fast_ >= slow_ || slow_ > kWindow) {
            log("NiftySmaCross: n_strikes must be 1..16 and fast < slow <= 256; not started", AlgoLogLevel::ERROR);
            return;
        }
        future_ = findFut(underlying_);                        // the nearest unexpired future of the underlying
        if (!future_) { log("NiftySmaCross: no future for the underlying; not started", AlgoLogLevel::ERROR); return; }

        track(&future_, 1, nStrikes_);                         // re-adopt the portfolios held before a restart (one per strike slot)

        const AlgoBar* b = nullptr;                            // seed from today's bars so far
        for (uint32_t i = 0, n = bars(future_, kWindow, b); i < n; ++i) pushClose(b[i].close);
        side_ = side();                                        // where the averages stand now, so a start never counts as a crossing
        ready_ = true;

        char msg[160];
        std::snprintf(msg, sizeof msg, "NiftySmaCross: %s future=%d strikes=%u fast=%u slow=%u seeded=%u side=%d",
                      underlying_, future_, nStrikes_, fast_, slow_, closes_.size(), side_);
        log(msg);
    }

    void onBars(const AlgoBarBatch& batch) override {
        if (!ready_) return;
        bool newBar = false;
        for (uint32_t i = 0; i < batch.count; ++i) {
            const AlgoBar& b = batch.bars[i];
            if (b.token == future_ && b.close > 0) { pushClose(b.close); newBar = true; }
        }
        if (!newBar) return;
        const int now = side();
        if (now != 0) {                                        // 0 = not enough bars yet, or equal: changes nothing
            if (side_ != 0 && now != side_) churn(now > 0 ? AlgoLeg::CALL : AlgoLeg::PUT);
            side_ = now;
        }
    }

    void onCommandResult(const AlgoCmdResult& r) override {
        if (r.status != AlgoStatus::OK) {
            char msg[192];
            std::snprintf(msg, sizeof msg, "PF %d: command %d failed, status %d %s", r.pf, static_cast<int>(r.cmd),
                          static_cast<int>(r.status), r.detail);
            log(msg, AlgoLogLevel::WARN);
        }
    }

private:
    static constexpr uint32_t kWindow = 256, kMaxStrikes = 16;

    void pushClose(int64_t c) noexcept { closes_.push(c); lastFut_ = c; }

    // +1 fast above slow, -1 below, 0 equal or not enough bars
    int side() const noexcept {
        const double f = algo::sma(closes_, fast_), s = algo::sma(closes_, slow_);
        if (std::isnan(f) || std::isnan(s)) return 0;
        return (f > s) - (f < s);
    }

    // Move every strike slot to `leg`: slot 0 is the strike nearest the future.
    void churn(AlgoLeg target) {
        if (!algo::inWindow(minuteOfDay(), from_, to_) || !sending() || lastFut_ <= 0) return;
        int32_t near[kMaxStrikes];
        const uint32_t n = nearest(underlying_, lastFut_, target == AlgoLeg::CALL ? AlgoOptType::CE : AlgoOptType::PE,
                                   expiryIndex_, nStrikes_, near);
        for (uint32_t k = 0; k < n; ++k) {
            const int32_t option = near[k], slot = static_cast<int32_t>(k);
            if (!allowed(future_, slot)) continue;                                   // unticked by the operator
            if (leg(future_, slot) == target || cooling(future_, failCooldown_, slot)) continue;
            AlgoContract c;
            const int64_t premium = lastPrice(option);
            AlgoParams p;
            if (!info(option, c) || !orderParams(premium, c.tick, p)) {
                char msg[128];
                std::snprintf(msg, sizeof msg, "churn: slot %u (token %d) skipped: no usable price or tick", k, option);
                log(msg, AlgoLogLevel::WARN);
                continue;
            }
            AlgoAssignment a;
            if (assign(future_, option, underlying_, slot, a) == AlgoStatus::OK) {
                const int64_t r = activate(a, target, p);
                if (r <= 0) {
                    char msg[96];
                    std::snprintf(msg, sizeof msg, "churn: slot %u not sent, status %d", k, static_cast<int>(statusOf(r)));
                    log(msg, AlgoLogLevel::WARN);
                }
            }
        }
    }

    // The `apply` fields for BUYing an option, every price on the tick (minor units). False if price or tick is unusable.
    bool orderParams(int64_t premium, int64_t tick, AlgoParams& p) const noexcept {
        if (premium <= 0 || tick < 1) return false;
        using algo::atLeast; using algo::floorTick; using algo::ceilTick;
        const double  px   = static_cast<double>(premium);
        const int64_t ival = atLeast(floorTick(px * ivalPct_, tick), tick);
        p = applyFields_;
        p.setPrice("ival", ival);
        p.setPrice("jval", atLeast(floorTick(ival * jvalFrac_, tick), tick));
        p.setPrice("sval", atLeast(floorTick(ival * svalMult_, tick), tick));
        p.setPrice("trp", ceilTick(px * (1 + trpPct_ / 100), tick));
        p.setPrice("lowerrange", atLeast(floorTick(px * (1 - rangePct_), tick), tick));
        p.setPrice("upperrange", floorTick(px * (1 + rangePct_), tick));
        p.setStr("side", "BUY");
        return true;
    }

    static void copy(char (&dst)[24], const char* src, const char* dflt) noexcept {
        std::strncpy(dst, (src && *src) ? src : dflt, sizeof dst - 1);
        dst[sizeof dst - 1] = '\0';
    }

    char     underlying_[24] = {};
    int32_t  future_ = 0, expiryIndex_ = 0, failCooldown_ = 30, from_ = 0, to_ = 0;
    uint32_t nStrikes_ = 5, fast_ = 5, slow_ = 20;
    double   ivalPct_ = 0.01, rangePct_ = 0.05, trpPct_ = 0.01, jvalFrac_ = 0.75, svalMult_ = 3.0;
    AlgoParams applyFields_;
    algo::Window<int64_t, kWindow> closes_;
    int64_t  lastFut_ = 0;
    int      side_ = 0;
    bool     ready_ = false;
};

// ============================================================
// Required C-linkage factory symbols. DO NOT rename.
// ============================================================
static const char* const kClasses[] = {"NiftySmaCross"};
static const AlgoPluginInfo kInfo = {ALGO_SDK_API_VERSION, 1, "nifty_sma_cross", "1.0.0", kClasses};

extern "C" ALGO_EXPORT const AlgoPluginInfo* algoGetInfo() { return &kInfo; }
extern "C" ALGO_EXPORT AlgoStrategy* algoCreate(const char* cls, const char* /*instance*/) {
    return (cls && std::strcmp(cls, "NiftySmaCross") == 0) ? new NiftySmaCross() : nullptr;
}
extern "C" ALGO_EXPORT void algoDestroy(AlgoStrategy* s) { delete s; }
