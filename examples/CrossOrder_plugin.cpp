// CrossOrder plugin (the engine's strategy plugin libCrossOrder.so). Changes against the version it was copied from:
//   1. SquareUpEvent now re-arms the timer. The timer is a one-shot that onTimer re-arms on every tick; once the plugin went IDLE (entry complete) the timer stopped for good, so a
//      later squareup set state_ = SQUARE but no tick ever ran to place the closing order. timerArmed_ remembers whether a tick is still scheduled, and SquareUpEvent starts it again.
//   2. The one-second "calling onTimer" log line and the per-quote "onMarketData: bid ask" line are gone (they filled the PF log: about 3 lines a second per portfolio).
//   Everything else is unchanged. Not compiled or run here: build it and test on a small position first.
#include "StrategySDK.h"
#include <cstdio>
#include <cstdlib>

class CrossOrder : public StrategyPlugin {
public:
    explicit CrossOrder(int pf) : pf_(pf) {}

    void onParamsUpdate(const SDKParamMap& p) override {
        if (init_) return;
        const int token = p.get_int("TOKEN", 0);
        if (!token) { logf("onParamsUpdate: TOKEN missing"); return; }


        const int lot = contract().getLotSize(token);

        const int sc = p.get_int("SERVERCODE", 1);
        const char* acct = p.get("ACCOUNT"); if (!acct) acct = "";
        const char* algo = p.get("ALGOID");  if (!algo) algo = "";

        token_ = static_cast<uint32_t>(token);
        side_  = p.get_int("SIDE", 1);             // 1 = BUY, 2 = SELL
        qty_   = p.get_int("QTY", 1) * lot;        // QTY in lots

        buySlot_  = allocOrderSlot(token, SDKSide::BUY,  sc, acct, algo);
        sellSlot_ = allocOrderSlot(token, SDKSide::SELL, sc, acct, algo);

        logf("onParamsUpdate: identifiers | token=%d side=%d qty=%d lot=%d serverCode=%d account=%s algoId=%s buySlot=%d sellSlot=%d",
             token, side_, qty_, lot, sc, acct, algo, buySlot_, sellSlot_);

        if (buySlot_ < 0 || sellSlot_ < 0 ) { logf("onParamsUpdate: init failed"); return; }

        init_  = true;
        state_ = ENTRY;
        logf("onParamsUpdate complete — waiting for first quote");
    }

    // Quotes are only cached here (no log line: it printed on every price tick); all order actions run from onTimer (1 per tick).
    void onMarketData(const SDKMarketData& d) override {
        if (d.Token != token_) return;
        bid_ = d.Buy[0].Price;
        ask_ = d.Sell[0].Price;
    }

    void onTimer() override {
        if (state_ == IDLE) { timerArmed_ = false; return; }   // done: the timer stops here (SquareUpEvent starts it again if a squareup comes later)
        setTimerMs(TICK_MS);
        timerArmed_ = true;
        if (!init_ || cancelling_) return;
        const int bid = bid_, ask = ask_;
        if (bid <= 0 || ask <= 0 || qty_ <= 0) return;

        int slot, px, qty;
        if (state_ == ENTRY) {
            const bool buy = (side_ == 1);
            slot = buy ? buySlot_ : sellSlot_;
            px   = buy ? ask : bid;                     // cross the spread
            qty  = qty_ - std::abs(pos_);
        } else {
            if (pos_ > 0)      { slot = sellSlot_; px = bid; qty = pos_;  }
            else if (pos_ < 0) { slot = buySlot_;  px = ask; qty = -pos_; }
            else               { state_ = IDLE; logf("onTimer: square-up complete - flat"); return; }
        }
        if (qty <= 0) { state_ = IDLE; logf("onTimer: entry complete - pos=%d, going IDLE", pos_); return; }

        if (!sent_) {
            if (placeOrder(slot, px, qty, state_ == SQUARE ? SDKPriority::SqrUp
                                                          : SDKPriority::Normal) == SDKOrderResult::SENT) {
                sent_ = true; lastPx_ = px;
                clogf(slot, "onTimer: PLACE %s slot=%d px=%d qty=%d bid=%d ask=%d state=%d",
                      sideName(slot), slot, px, qty, bid, ask, state_);
            }
        } else if (px != lastPx_) {
            const SDKOrderStatus st = getOrderStatus(slot);
            if (st != SDKOrderStatus::NEW && st != SDKOrderStatus::REPLACED &&
                st != SDKOrderStatus::PARTIALFILLED) return;          // never touch PENDING
            if (mods_ >= MAX_TRIES) {                                // max modifies reached -> cancel
                cancelling_ = true;
                cancelOrder(slot);
                clogf(slot, "onTimer: CANCEL %s slot=%d - modify limit %d reached",
                      sideName(slot), slot, MAX_TRIES);
                return;
            }
            if (modifyOrder(slot, px, qty) == SDKOrderResult::SENT) {
                ++mods_; lastPx_ = px;
                clogf(slot, "onTimer: MODIFY %s slot=%d px=%d qty=%d mod=%d/%d",
                      sideName(slot), slot, px, qty, mods_, MAX_TRIES);
            } else {
                clogf(slot, "onTimer: MODIFY not sent %s slot=%d px=%d qty=%d", sideName(slot), slot, px, qty);
            }
        }
    }

    void onOrderUpdate(const SDKOrderInfo& o) override {
        if (o.SlotIndex != buySlot_ && o.SlotIndex != sellSlot_) return;
        switch (static_cast<SDKOrderStatus>(o.Status)) {
        case SDKOrderStatus::PARTIALFILLED:
        case SDKOrderStatus::FILLED: {
            pos_ += (o.SlotIndex == buySlot_) ? o.LastTradedQty : -o.LastTradedQty;
            clogf(o.SlotIndex, "onOrderUpdate: FILL %s slot=%d qty=%d px=%d pos=%d/%d state=%d",
                  sideName(o.SlotIndex), o.SlotIndex, o.LastTradedQty, o.LastTradedPrice,
                  pos_, qty_, state_);
            char v[64];
            snprintf(v, sizeof(v), "positions=[%d]", pos_);
            update_ui_vars(v);
            if (static_cast<SDKOrderStatus>(o.Status) == SDKOrderStatus::FILLED) {
                sent_ = false; mods_ = 0; retries_ = 0;
                uilogf(o.SlotIndex, "onOrderUpdate: %s ORDER COMPLETE — slot=%d pos=%d",
                       sideName(o.SlotIndex), o.SlotIndex, pos_);
            }
            break;
        }
        case SDKOrderStatus::REPLACED:
            clogf(o.SlotIndex, "onOrderUpdate: REPLACED %s slot=%d px=%d mod=%d/%d",
                  sideName(o.SlotIndex), o.SlotIndex, o.LastTradedPrice, mods_, MAX_TRIES);
            break;
        case SDKOrderStatus::CANCELLED:
            sent_ = false;
            clogf(o.SlotIndex, "onOrderUpdate: CANCELLED %s slot=%d", sideName(o.SlotIndex), o.SlotIndex);
            if (cancelling_) { cancelling_ = false; state_ = IDLE; }
            break;
        case SDKOrderStatus::REJECTED:
        case SDKOrderStatus::NEWREJECTED:
        case SDKOrderStatus::INFRAREJECTED:
            sent_ = false;
            ++retries_;
            clogf(o.SlotIndex, "onOrderUpdate: REJECTED %s slot=%d status=%d err=%d retryCount=%d/%d",
                  sideName(o.SlotIndex), o.SlotIndex, o.Status, o.ErrorCode, retries_, MAX_TRIES);
            if (retries_ >= MAX_TRIES) {
                state_ = IDLE;
                uilogf(o.SlotIndex, "onOrderUpdate: %d rejects (err=%d) — stopped", MAX_TRIES, o.ErrorCode);
            }
            break;
        case SDKOrderStatus::MODREJECTED:
            clogf(o.SlotIndex, "onOrderUpdate: MODREJECTED %s slot=%d err=%d outStat=%d",
                  sideName(o.SlotIndex), o.SlotIndex, o.ErrorCode, getOrderLastOutStatus(o.SlotIndex));
            lastPx_ = 0;
            break;
        case SDKOrderStatus::CANREJECTED:
            clogf(o.SlotIndex, "onOrderUpdate: CANREJECTED %s slot=%d err=%d",
                  sideName(o.SlotIndex), o.SlotIndex, o.ErrorCode);
            cancelling_ = false;
            break;
        default: break;
        }
    }

    void SquareUpEvent() override {
        logf("SquareUpEvent: forcing close | state=%d pos=%d sent=%d", state_, pos_, sent_ ? 1 : 0);
        if (!init_) return;
        const bool wasEntry = (state_ == ENTRY);
        state_ = SQUARE;
        mods_ = 0; retries_ = 0; cancelling_ = false;
        if (!timerArmed_) {                                      // the timer stopped when the plugin went IDLE: start it, so the next tick places the closing order
            timerArmed_ = true;
            setTimerMs(TICK_MS);
        }
        if (wasEntry && sent_) {
            const int slot = (side_ == 1) ? buySlot_ : sellSlot_;
            if (getOrderStatus(slot) != SDKOrderStatus::PENDING) {
                cancelOrder(slot);
                clogf(slot, "SquareUpEvent: CANCEL %s slot=%d (working entry order)", sideName(slot), slot);
            }
        }
    }

    void PauseUnPauseEvent(bool) override {}

private:
    enum : int { IDLE = 0, ENTRY = 1, SQUARE = 2 };
    static constexpr int  TICK_MS   = 1000;
    static constexpr int  MAX_TRIES = 3;

    const char* sideName(int slot) const { return slot == buySlot_ ? "BUY" : "SELL"; }
    const char* sideColor(int slot) const { return slot == buySlot_ ? "BLUE" : "RED"; }

    // plain line: "CrossOrder PF=<pf> | <msg>"
    template <typename... A>
    void logf(const char* fmt, A... a) {
        char b[256];
        int n = snprintf(b, sizeof(b), "CrossOrder PF=%d | ", pf_);
        if (n < 0 || n >= static_cast<int>(sizeof(b))) return;
        snprintf(b + n, sizeof(b) - n, fmt, a...);
        log(b);
    }

    // coloured line (BLUE = buy slot, RED = sell slot): "... ,COLOR=BLUE"
    template <typename... A>
    void clogf(int slot, const char* fmt, A... a) {
        char b[256];
        int n = snprintf(b, sizeof(b), "CrossOrder PF=%d | ", pf_);
        if (n < 0 || n >= static_cast<int>(sizeof(b))) return;
        int m = snprintf(b + n, sizeof(b) - n, fmt, a...);
        if (m < 0) return;
        n += m;
        if (n >= static_cast<int>(sizeof(b))) n = static_cast<int>(sizeof(b)) - 1;
        snprintf(b + n, sizeof(b) - n, " ,COLOR=%s", sideColor(slot));
        log(b);
    }

    // coloured line to both the log file and the UI log
    template <typename... A>
    void uilogf(int slot, const char* fmt, A... a) {
        char b[256];
        int n = snprintf(b, sizeof(b), "CrossOrder PF=%d | ", pf_);
        if (n < 0 || n >= static_cast<int>(sizeof(b))) return;
        int m = snprintf(b + n, sizeof(b) - n, fmt, a...);
        if (m < 0) return;
        n += m;
        if (n >= static_cast<int>(sizeof(b))) n = static_cast<int>(sizeof(b)) - 1;
        snprintf(b + n, sizeof(b) - n, " ,COLOR=%s", sideColor(slot));
        log(b);
        update_ui_log(b);
    }

    int  pf_;
    uint32_t token_ = 0;
    int  buySlot_ = -1, sellSlot_ = -1;
    int  side_ = 1, qty_ = 0, pos_ = 0, lastPx_ = 0;
    int  mods_ = 0, retries_ = 0, state_ = IDLE;
    int  bid_ = 0, ask_ = 0;
    bool sent_ = false, init_ = false, cancelling_ = false;
    bool timerArmed_ = true;                                     // a tick is scheduled (the engine starts the first one when the plugin is created)
};

static const StrategyPluginInfo kInfo = { SDK_API_VERSION, 0, "CrossOrder", "1.0.0" };
extern "C" SDK_EXPORT StrategyPluginInfo* getStrategyInfo() { return const_cast<StrategyPluginInfo*>(&kInfo); }
extern "C" SDK_EXPORT StrategyPlugin* getDriver(int pf, void*) { return new CrossOrder(pf); }
extern "C" SDK_EXPORT void destroyStrategy(StrategyPlugin* p) { delete p; }
