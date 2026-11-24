// AlgoSDK.h — signal-strategy plugin API for algod (the C++ port of the Deephaven `algo` stack)
//
// This is the ONLY header a strategy needs. It has zero dependency on algod's own headers.
// A strategy decides WHAT to trade and WHEN (candles, quotes, analytics, its own indicators). It does not place
// exchange orders itself: it takes a portfolio from its book and sends portfolio commands (subscribe, pause, apply,
// unpause, squareup, unsubscribe) that algod forwards over NATS to the trading engine, whose own plugin
// (StrategySDK.h: CrossOrder, Churning, ...) works the orders. Same split as the Python scripts in examples/.
//
// To write a strategy:
//   1. Inherit from AlgoStrategy and implement onStart() and onBars() (the other handlers are optional)
//   2. Export the three C-linkage factory symbols (bottom of this file)
//   3. Build as a shared library: -std=c++17 -fPIC -O3 -fvisibility=hidden -shared, no -flto, link nothing but libc/libstdc++/libm
//
// ABI contract:
//   * ALGO_SDK_API_VERSION must equal the host's at load time; any change to a struct or to AlgoHostAPI bumps it.
//   * Only PODs, fixed char arrays, C strings and function pointers cross the boundary. No std::string, std::vector,
//     std::function or exceptions cross it, so a strategy may be built by a different compiler than algod.
//   * Prices are int64 MINOR UNITS everywhere (paise for NSE, the contract's own divisor for others). Never doubles.
//   * Every pointer the host hands to a callback is valid until that callback returns, unless stated otherwise.
//     Copy what you need to keep.

#pragma once
#include <cstdint>
#include <cstdlib>
#include <cstring>

static constexpr uint32_t ALGO_SDK_API_VERSION = 2;   // 2: find (exact) / find_fut / find_spot / find_opt / moneyness / step_strike, INDEX

#define ALGO_EXPORT __attribute__((visibility("default")))

// ---------------------------------------------------------------------------
// Enums (one byte each, packed into structs)
// ---------------------------------------------------------------------------
enum class AlgoKind    : uint8_t { UNKNOWN = 0, FUT = 1, OPT = 2, EQ = 3, INDEX = 4 };   // from the master, by token
enum class AlgoOptType : uint8_t { NONE = 0, CE = 1, PE = 2 };
enum class AlgoLeg     : uint8_t { NONE = 0, CALL = 1, PUT = 2, BUY = 3, SELL = 4 };
enum class AlgoLogLevel: uint8_t { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3 };

// History / analytics (absolute() and relative() of the Python stack)
enum class AlgoSeries  : uint8_t { PCT = 1, IV = 2, VOLUME = 3, TURNOVER = 4 };
enum class AlgoLevel   : uint8_t { INTRADAY = 1, INTRAWEEK = 2, INTRAMONTH = 3 };
enum class AlgoRefKind : uint8_t { NONE = 0, OPEN = 1, VWAP = 2, PREV_CLOSE = 3, MINUTES_AGO = 4, AT_MINUTE = 5 };

// The reference point of a PCT (or IV) series. value = minutes for MINUTES_AGO, exchange minute-of-day for AT_MINUTE.
struct AlgoRef {
    AlgoRefKind kind  = AlgoRefKind::OPEN;
    uint8_t     _r    = 0;
    uint16_t    value = 0;
    static AlgoRef open()                 { return {AlgoRefKind::OPEN, 0, 0}; }
    static AlgoRef vwap()                 { return {AlgoRefKind::VWAP, 0, 0}; }
    static AlgoRef prevClose()            { return {AlgoRefKind::PREV_CLOSE, 0, 0}; }
    static AlgoRef minutesAgo(uint16_t m) { return {AlgoRefKind::MINUTES_AGO, 0, m}; }
    static AlgoRef atMinute(int hh, int mm) { return {AlgoRefKind::AT_MINUTE, 0, static_cast<uint16_t>(hh * 60 + mm)}; }
    static AlgoRef none()                 { return {AlgoRefKind::NONE, 0, 0}; }   // VOLUME / TURNOVER take no reference
};

// Portfolio commands, and the statuses they come back with. A call returns at once: QUEUED (with a job id), or why it
// was refused before anything was sent. The engine's answer arrives later in onCommandResult with one of the
// remaining statuses (the Python Reply statuses).
enum class AlgoCmd : uint8_t { SUBSCRIBE = 1, PAUSE = 2, UNPAUSE = 3, APPLY = 4, SQUAREUP = 5, UNSUBSCRIBE = 6, ACTIVATE = 7 };

enum class AlgoStatus : int32_t {
    OK               = 0,    // the engine answered "ok" (or a dry run)
    QUEUED           = 1,    // accepted by algod; the outcome comes in onCommandResult
    // refused at once, nothing sent
    OFF              = 2,    // the master switch or this strategy's switch is off
    BLOCKED          = 3,    // user or portfolio blocked by the operator
    FULL             = 4,    // that portfolio's lane, or this strategy's outbound ring, is full
    NOT_ALLOWED      = 5,    // slot unticked by the operator (pf_allow) or a limit is breached
    COOLING          = 6,    // this slot failed a moment ago (fail_cooldown_s)
    INVALID          = 7,    // bad argument (unknown token, slot out of range, bad params)
    NOT_FOUND        = 8,    // no such portfolio / contract / basket
    NO_SERVICE       = 9,    // the NATS service is not running
    // the engine's side (arrive in onCommandResult only)
    TIMEOUT          = 20,   // no answer in time: UNKNOWN whether the engine acted; never re-sent by algod
    NO_RESPONDERS    = 21,
    DISCONNECTED     = 22,
    BAD_REPLY        = 23,
    ENGINE_ERROR     = 24,   // the engine refused; detail holds its text
};

// ---------------------------------------------------------------------------
// Data structures (fixed size, trivially copyable)
// ---------------------------------------------------------------------------

// The last state of one instrument: shared-memory seqlock read, never torn. ~20 ns.
struct alignas(64) AlgoQuote {
    int32_t  token      = 0;
    int32_t  idx        = 0;     // algod's dense index of the token (stable for the day)
    int64_t  ltp        = 0;     // minor units
    int64_t  bid        = 0;
    int64_t  ask        = 0;
    int32_t  bid_qty    = 0;
    int32_t  ask_qty    = 0;
    int32_t  ltq        = 0;     // last traded quantity
    int32_t  _pad       = 0;
    int64_t  cum_volume = 0;
    int64_t  ltt_ns     = 0;     // last trade time, epoch nanoseconds
};
static_assert(sizeof(AlgoQuote) == 64, "AlgoQuote is one cache line");

// One CLOSED bar. Bars are only ever delivered closed (10 s grace after the bar ends, as in the Python candles).
struct alignas(64) AlgoBar {
    int32_t  token        = 0;
    int32_t  idx          = 0;
    int32_t  start_minute = 0;   // bar start, minutes since the Unix epoch (UTC)
    uint16_t minute_of_day= 0;   // bar start, exchange local minute of the day (09:15 NSE = 555)
    uint16_t flags        = 0;   // reserved
    int64_t  open         = 0;   // minor units
    int64_t  high         = 0;
    int64_t  low          = 0;
    int64_t  close        = 0;
    int64_t  volume       = 0;   // traded inside the bar
    int64_t  cvd          = 0;   // day-anchored cumulative volume delta (tick rule), as the Python CVD column
};
static_assert(sizeof(AlgoBar) == 64, "AlgoBar is one cache line");

// The bars that closed in one flush, all tokens of the strategy's universe. `bars` is contiguous (zero copy).
struct AlgoBarBatch {
    const AlgoBar* bars        = nullptr;
    uint32_t       count       = 0;
    uint32_t       bar_seconds = 60;
    int32_t        start_minute= 0;   // the bar start these bars belong to (epoch minutes)
    int32_t        _pad        = 0;
};

// A contract from the master. Filled by copy (cold data: look it up in onStart and keep what you need).
struct AlgoContract {
    int32_t     token         = 0;
    int32_t     future_token  = 0;    // the future that goes with it: an option's = nearest expiring on/after it; a future's = itself; 0 if none
    int32_t     spot_token    = 0;    // the cash / index instrument of the underlying (0 if none)
    int32_t     alt_token     = 0;    // the paired CE <-> PE at the same strike and expiry (0 if none)
    int32_t     lot_size      = 0;
    int32_t     expiry        = 0;    // yyyymmdd, 0 for equities
    int64_t     strike        = 0;    // minor units, 0 if not an option
    int64_t     tick          = 0;    // minor units
    int32_t     divisor       = 0;    // raw units per currency unit (NSE 100, US options 10000)
    AlgoKind    kind          = AlgoKind::UNKNOWN;
    AlgoOptType opt_type      = AlgoOptType::NONE;
    uint8_t     _r[2]         = {};
    char        symbol[48]    = {};   // "NIFTY 27OCT26 25000 CE"
    char        underlying[24]= {};   // "NIFTY"
};

// The result of one analytics series for one symbol, now. Same meaning as the Python absolute() / relative() result.
//   abs_*: now against the same session minute of the last n periods      rel_*: now against the other symbols of the basket
//   *_idx = how many valid values are strictly below now (-1 = cannot tell: no value now, or too few valid ones)
struct AlgoAnalytics {
    double  now          = 0.0;    // NaN when there is no value now
    float   abs_pctile   = 0.0f;   // 100 * abs_idx / abs_n_valid, NaN when abs_idx is -1
    float   rel_pctile   = 0.0f;
    int32_t abs_idx      = -1;
    int32_t abs_n_valid  = 0;
    int32_t rel_idx      = -1;
    int32_t rel_n_valid  = 0;
    int32_t minute_of_day= 0;      // the session minute these values are for
    int32_t _pad         = 0;
};

// What the book answered for (token, pf_token, slot): the portfolio to use, and the one to retire if it moved.
struct AlgoAssignment {
    int32_t user       = 0;
    int32_t pf         = 0;        // the portfolio number (without the user prefix; the engine sees user*10000+pf)
    int32_t old_pf     = 0;        // 0, or the portfolio this replaces (pfActivate retires it first)
    int32_t token      = 0;        // the tracked token (the signal), any kind: future, equity, option, index
    int32_t pf_token   = 0;        // the token this portfolio trades
    int16_t slot       = 0;
    uint8_t is_new     = 0;        // 1 = a new portfolio number was taken
    uint8_t _r         = 0;
};

// A portfolio's state as the engine last CONFIRMED it, whoever sent the command (this strategy, the operator, another tool).
struct AlgoPfState {
    int32_t user       = 0;
    int32_t pf         = 0;
    int32_t pf_token   = 0;        // the token it is subscribed to
    uint8_t subscribed = 0;
    uint8_t paused     = 0;
    uint8_t squared    = 0;        // a squareup went through and nothing was applied since
    uint8_t by_operator= 0;        // 1 = the change came from the operator (API / console), not from this strategy
    int64_t at_ns      = 0;
    AlgoCmd cmd        = AlgoCmd::SUBSCRIBE;   // the command that made this change
    uint8_t _r[7]      = {};
    char    by[24]     = {};       // strategy name or "operator"
};

// The engine's answer to one queued command.
struct AlgoCmdResult {
    int64_t    job_id  = 0;
    int32_t    user    = 0;
    int32_t    pf      = 0;
    AlgoStatus status  = AlgoStatus::OK;
    AlgoCmd    cmd     = AlgoCmd::SUBSCRIBE;
    uint8_t    _r[3]   = {};
    char       detail[96] = {};
};

// ---------------------------------------------------------------------------
// AlgoParams — the fields of a subscribe / apply, built on the stack (no allocation), serialized by algod.
//
// setPrice() takes MINOR units and algod writes it in the engine's currency units with the small upward nudge the engine
// needs (it truncates x100 to int), so 230 paise arrives as 2.300001 and never as 2.29. Do not add the nudge yourself.
// ---------------------------------------------------------------------------
struct AlgoParams {
    enum Type : uint8_t { T_INT = 1, T_DOUBLE = 2, T_PRICE = 3, T_STR = 4 };
    struct Entry {
        char    key[24];
        uint8_t type;
        uint8_t _r[7];
        int64_t i;
        double  d;
        char    s[32];
    };
    static constexpr int MAX = 40;

    Entry    e[MAX];
    uint32_t n = 0;

    void clear() noexcept { n = 0; }
    bool setInt(const char* k, int64_t v) noexcept            { Entry* x = put(k); if (!x) return false; x->type = T_INT;    x->i = v; return true; }
    bool setDouble(const char* k, double v) noexcept          { Entry* x = put(k); if (!x) return false; x->type = T_DOUBLE; x->d = v; return true; }
    bool setPrice(const char* k, int64_t minor) noexcept      { Entry* x = put(k); if (!x) return false; x->type = T_PRICE;  x->i = minor; return true; }
    bool setStr(const char* k, const char* v) noexcept {
        Entry* x = put(k); if (!x) return false; x->type = T_STR;
        std::strncpy(x->s, v ? v : "", sizeof(x->s) - 1); x->s[sizeof(x->s) - 1] = '\0'; return true;
    }

private:
    Entry* put(const char* k) noexcept {                    // replaces an existing key, else appends; false when full
        if (!k || !*k) return nullptr;
        for (uint32_t j = 0; j < n; ++j) if (std::strncmp(e[j].key, k, sizeof(e[j].key)) == 0) return &e[j];
        if (n >= MAX) return nullptr;
        Entry* x = &e[n++];
        std::memset(x, 0, sizeof(*x));
        std::strncpy(x->key, k, sizeof(x->key) - 1);
        return x;
    }
};

// ---------------------------------------------------------------------------
// AlgoSettings — this strategy's config/strategies/<name>.ini, read with typed getters (like the engine's SDKParamMap).
// Keys are "section.key" ("signal.fast"); keys of the [strategy] section may be written without it ("underlying").
// ---------------------------------------------------------------------------
struct AlgoSettings {
    using LookupFn = const char* (*)(void* ctx, const char* key) noexcept;
    using FillFn   = uint32_t    (*)(void* ctx, const char* section, AlgoParams* out) noexcept;

    void*    _ctx    = nullptr;
    LookupFn _lookup = nullptr;
    FillFn   _fill   = nullptr;

    const char* get(const char* key) const noexcept { return (_lookup && _ctx) ? _lookup(_ctx, key) : nullptr; }
    bool has(const char* key) const noexcept { return get(key) != nullptr; }

    int64_t getInt(const char* key, int64_t dflt = 0) const noexcept {
        const char* v = get(key); if (!v || !*v) return dflt;
        char* end = nullptr; long long r = std::strtoll(v, &end, 10); return end == v ? dflt : static_cast<int64_t>(r);
    }
    double getDouble(const char* key, double dflt = 0.0) const noexcept {
        const char* v = get(key); if (!v || !*v) return dflt;
        char* end = nullptr; double r = std::strtod(v, &end); return end == v ? dflt : r;
    }
    bool getBool(const char* key, bool dflt = false) const noexcept {
        const char* v = get(key); if (!v || !*v) return dflt;
        return v[0] == '1' || v[0] == 't' || v[0] == 'T' || v[0] == 'y' || v[0] == 'Y' || ((v[0] == 'o' || v[0] == 'O') && (v[1] == 'n' || v[1] == 'N'));
    }
    // "HH:MM" -> minute of the day, or dflt
    int getMinute(const char* key, int dflt = -1) const noexcept {
        const char* v = get(key);
        if (!v || std::strlen(v) < 5 || v[2] != ':') return dflt;
        int h = (v[0] - '0') * 10 + (v[1] - '0'), m = (v[3] - '0') * 10 + (v[4] - '0');
        return (h >= 0 && h < 24 && m >= 0 && m < 60) ? h * 60 + m : dflt;
    }
    // Every key of one [section] copied into params (e.g. the pass-through [apply] fields). Returns how many.
    uint32_t toParams(const char* section, AlgoParams& out) const noexcept { return (_fill && _ctx) ? _fill(_ctx, section, &out) : 0; }

    // A comma-separated value, item by item, with no allocation:  for (int i = 0; settings.item("universe", i, buf, sizeof buf); ++i)
    bool item(const char* key, int index, char* buf, int len) const noexcept {
        const char* v = get(key); if (!v || !buf || len <= 0) return false;
        for (int i = 0; ; ++i) {
            while (*v == ' ' || *v == '\t') ++v;
            const char* end = v; while (*end && *end != ',') ++end;
            if (i == index) {
                const char* t = end; while (t > v && (t[-1] == ' ' || t[-1] == '\t')) --t;
                int n = static_cast<int>(t - v); if (n <= 0) return false; if (n >= len) n = len - 1;
                std::memcpy(buf, v, static_cast<size_t>(n)); buf[n] = '\0'; return true;
            }
            if (!*end) return false;
            v = end + 1;
        }
    }
};

// ---------------------------------------------------------------------------
// AlgoHostAPI — every service algod gives a strategy, as function pointers. Owned by the host, valid for the
// strategy's whole life. Call these through the AlgoStrategy wrappers below, never directly.
// ---------------------------------------------------------------------------
struct AlgoHostAPI {
    uint32_t api_version = ALGO_SDK_API_VERSION;
    uint32_t struct_size = sizeof(AlgoHostAPI);

    // contracts (immutable snapshot of today's master: lock-free, but not free -> prefer onStart)
    //   underlying: "NIFTY", "RELIANCE" ...       expiry_index: 0 = the nearest that has not passed, 1 = the next ...
    //   price (nearest / moneyness): minor units of the underlying's REFERENCE instrument -- its nearest future, else its
    //     cash instrument -- i.e. exactly what quote() / bars() give for that instrument (NIFTY fut LTP in paise)
    //   strike (strikes out / option in / AlgoContract.strike): the OPTION's own minor units (NSE 25000 CE = 2500000)
    int32_t  (*find)(void*, const char* name) noexcept;          // EXACTLY the contract named: "41003", "RELIANCE" (cash), "NIFTY 24NOV26 25000 CE"
    int32_t  (*find_fut)(void*, const char* underlying, int32_t expiry_index) noexcept;    // its future: 0 = nearest unexpired, 1 = next ...
    int32_t  (*find_spot)(void*, const char* underlying) noexcept;                         // its cash stock / index
    int32_t  (*find_opt)(void*, const char* underlying, int32_t expiry_index, AlgoOptType, int64_t strike) noexcept;   // its option at an exact strike
    bool     (*info)(void*, int32_t token, AlgoContract* out) noexcept;
    uint32_t (*expiries)(void*, const char* underlying, AlgoKind kind, int32_t* out_yyyymmdd, uint32_t max) noexcept;
    uint32_t (*strikes)(void*, const char* underlying, int32_t expiry_index, AlgoOptType, int32_t* out_tokens, int64_t* out_strikes, uint32_t max) noexcept;
    uint32_t (*nearest)(void*, const char* underlying, int64_t price, AlgoOptType, int32_t expiry_index, uint32_t n, int32_t* out_tokens) noexcept;
    int32_t  (*moneyness)(void*, const char* underlying, int64_t price, AlgoOptType, int32_t expiry_index, int32_t steps) noexcept;  // 0 = ATM, +k OTM, -k ITM
    int32_t  (*step_strike)(void*, int32_t option_token, int32_t steps) noexcept;   // +k higher strike, -k lower; same expiry and type

    // market data (shared memory, zero copy)
    bool     (*quote)(void*, int32_t token, AlgoQuote* out) noexcept;                               // seqlock read, safe on every callback
    uint32_t (*bars)(void*, int32_t token, uint32_t n, const AlgoBar** out) noexcept;              // today's last n closed bars, oldest first
    uint32_t (*day_bars)(void*, int32_t token, int32_t days_back, const AlgoBar** out) noexcept;   // a saved day (1 = previous trading day); for a "X FUT" name use the contract nearest on that day
    bool     (*watch_quotes)(void*, const int32_t* tokens, uint32_t n) noexcept;                   // turn on onQuotes for these tokens

    // analytics (computed once per minute for every strategy that wants them) and one-off history calls
    int32_t  (*want)(void*, AlgoSeries, AlgoLevel, AlgoRef, const char* basket, uint32_t n) noexcept;   // -> handle (< 0: error)
    bool     (*analytics)(void*, int32_t handle, int32_t token, AlgoAnalytics* out) noexcept;
    bool     (*absolute)(void*, AlgoSeries, AlgoLevel, const char* symbol, uint32_t n, AlgoRef, AlgoAnalytics* out) noexcept;
    bool     (*relative)(void*, AlgoSeries, AlgoLevel, const char* symbol, const char* basket, AlgoRef, AlgoAnalytics* out) noexcept;
    uint32_t (*basket)(void*, const char* name, int32_t* out_tokens, uint32_t max) noexcept;      // the members of a baskets.txt basket

    // the book (this strategy's portfolios; algo_start / pf_* of the Python automation)
    void       (*track)(void*, const int32_t* tokens, uint32_t n, uint32_t slots) noexcept;   // re-adopt the portfolios held before a restart
    AlgoStatus (*assign)(void*, int32_t token, int32_t pf_token, const char* name, int32_t slot, AlgoAssignment* out) noexcept;
    int64_t    (*activate)(void*, const AlgoAssignment*, AlgoLeg, const AlgoParams*) noexcept;    // [retire old] subscribe, pause, apply, unpause. >0 job id, <=0 -AlgoStatus
    AlgoLeg    (*leg)(void*, int32_t token, int32_t slot) noexcept;
    void       (*mark)(void*, int32_t token, AlgoLeg, int32_t pf_token, int32_t slot) noexcept;
    bool       (*allowed)(void*, int32_t token, int32_t slot) noexcept;
    bool       (*cooling)(void*, int32_t token, int32_t seconds, int32_t slot) noexcept;
    int32_t    (*new_pf)(void*, const char* name) noexcept;                                       // a fresh portfolio number for one trade (single-order style), 0 on failure

    // direct portfolio commands. >0 job id, <=0 -AlgoStatus; the engine's answer comes in onCommandResult
    int64_t  (*subscribe)(void*, int32_t pf, int32_t pf_token, const AlgoParams*) noexcept;
    int64_t  (*apply)(void*, int32_t pf, const AlgoParams*) noexcept;
    int64_t  (*pause)(void*, int32_t pf) noexcept;
    int64_t  (*unpause)(void*, int32_t pf) noexcept;
    int64_t  (*squareup)(void*, int32_t pf) noexcept;
    int64_t  (*unsubscribe)(void*, int32_t pf, int32_t pf_token) noexcept;
    bool     (*pf_state)(void*, int32_t pf, AlgoPfState* out) noexcept;

    // control and context
    bool     (*sending)(void*) noexcept;                       // this strategy's switch AND the master switch are on
    void     (*stop_self)(void*, bool unsubscribe) noexcept;   // the algo_stop sequence for all its portfolios, then onStop
    void     (*timer)(void*, int32_t ms) noexcept;             // one-shot: call again from onTimer to repeat
    void     (*log)(void*, AlgoLogLevel, const char* msg, int32_t len) noexcept;   // to a ring, formatted off-thread
    void     (*event)(void*, const char* msg, int32_t len) noexcept;               // to the API `events` stream (the operator sees it)
    void     (*set_var)(void*, const char* key, const char* value) noexcept;       // shown in algo_status (like the engine's update_ui_vars)
    int64_t  (*clock_ns)(void*) noexcept;                      // monotonic nanoseconds
    int32_t  (*minute_of_day)(void*) noexcept;                 // exchange local minute of the day, now
    int32_t  (*today)(void*) noexcept;                         // exchange date, yyyymmdd
    int32_t  (*user)(void*) noexcept;                          // the user this instance trades as
    void     (*name)(void*, char* buf, int32_t len) noexcept;  // this instance's strategy name
};

// ---------------------------------------------------------------------------
// AlgoStrategy — base class for signal strategies
//
// Threading guarantee: every handler of one instance runs on that instance's own worker thread, one at a time,
// never concurrently. No locking is needed inside a strategy. Different instances never share a thread or state.
//
// Rules: no blocking, no file or network I/O, no sleeping, and no heap allocation in onBars / onQuotes /
// onAnalytics. Keep all mutable state in members (never in globals or static locals: the build checks).
// ---------------------------------------------------------------------------
class AlgoStrategy {
public:
    virtual ~AlgoStrategy() = default;

    // Called once, after wiring. Read settings, resolve contracts to tokens, want() analytics, track() portfolios,
    // seed indicators from bars()/day_bars(). Sending is OFF here; the operator (or the .ini `autostart`) turns it on.
    virtual void onStart(const AlgoSettings& settings) = 0;

    // Closed bars of the strategy's universe, one call per flush.
    virtual void onBars(const AlgoBarBatch& batch) = 0;

    // Optional handlers
    virtual void onQuotes(const AlgoQuote* quotes, uint32_t count) {}   // only for tokens given to watchQuotes()
    virtual void onAnalytics(int32_t minute_of_day) {}                  // the wanted analytics for this minute are published
    virtual void onPfState(const AlgoPfState& state) {}                 // a confirmed change of one of its portfolios
    virtual void onCommandResult(const AlgoCmdResult& result) {}        // the engine's answer to a queued command
    virtual void onTimer() {}
    virtual void onSendingChanged(bool on) {}                           // its switch or the master switch flipped
    virtual void onSettingsUpdate(const AlgoSettings& settings) {}      // live keys changed through the API (config_set)
    virtual void onStop() {}                                            // last call before algoDestroy

protected:
    // ---- contracts -------------------------------------------------------------------------------------
    int32_t  find(const char* name) const noexcept                     { return _h ? _h->find(_c, name) : 0; }   // exactly the contract named
    int32_t  findFut(const char* und, int32_t expiry_index = 0) const noexcept { return (_h && _h->find_fut) ? _h->find_fut(_c, und, expiry_index) : 0; }
    int32_t  findSpot(const char* und) const noexcept                  { return (_h && _h->find_spot) ? _h->find_spot(_c, und) : 0; }
    int32_t  findOpt(const char* und, int32_t expiry_index, AlgoOptType t, int64_t strike) const noexcept {
        return (_h && _h->find_opt) ? _h->find_opt(_c, und, expiry_index, t, strike) : 0;
    }
    bool     info(int32_t token, AlgoContract& out) const noexcept     { return _h && _h->info(_c, token, &out); }
    uint32_t expiries(const char* und, AlgoKind k, int32_t* out, uint32_t max) const noexcept { return _h ? _h->expiries(_c, und, k, out, max) : 0; }
    uint32_t strikes(const char* und, int32_t expiry_index, AlgoOptType t, int32_t* tokens, int64_t* strikes_out, uint32_t max) const noexcept {
        return _h ? _h->strikes(_c, und, expiry_index, t, tokens, strikes_out, max) : 0;
    }
    uint32_t nearest(const char* und, int64_t price, AlgoOptType t, int32_t expiry_index, uint32_t n, int32_t* out) const noexcept {
        return _h ? _h->nearest(_c, und, price, t, expiry_index, n, out) : 0;
    }
    // 0 = not found / out of range, for all of these
    // ATM = the strike nearest the price (on a tie, the lower one); OTM is above ATM for a CE and below for a PE, ITM the other way
    int32_t  atm(const char* und, int64_t price, AlgoOptType t, int32_t expiry_index = 0) const noexcept { return moneyness(und, price, t, expiry_index, 0); }
    int32_t  otm(const char* und, int64_t price, AlgoOptType t, int32_t expiry_index = 0, int32_t steps = 1) const noexcept { return moneyness(und, price, t, expiry_index, steps); }
    int32_t  itm(const char* und, int64_t price, AlgoOptType t, int32_t expiry_index = 0, int32_t steps = 1) const noexcept { return moneyness(und, price, t, expiry_index, -steps); }
    int32_t  nextStrike(int32_t option_token, int32_t steps = 1) const noexcept { return (_h && _h->step_strike) ? _h->step_strike(_c, option_token, steps) : 0; }
    int32_t  prevStrike(int32_t option_token, int32_t steps = 1) const noexcept { return (_h && _h->step_strike) ? _h->step_strike(_c, option_token, -steps) : 0; }
    int32_t  moneyness(const char* und, int64_t price, AlgoOptType t, int32_t expiry_index, int32_t steps) const noexcept {
        return (_h && _h->moneyness) ? _h->moneyness(_c, und, price, t, expiry_index, steps) : 0;
    }

    // ---- market data ------------------------------------------------------------------------------------
    bool     quote(int32_t token, AlgoQuote& out) const noexcept                      { return _h && _h->quote(_c, token, &out); }
    uint32_t bars(int32_t token, uint32_t n, const AlgoBar*& out) const noexcept      { out = nullptr; return _h ? _h->bars(_c, token, n, &out) : 0; }
    uint32_t dayBars(int32_t token, int32_t days_back, const AlgoBar*& out) const noexcept { out = nullptr; return _h ? _h->day_bars(_c, token, days_back, &out) : 0; }
    bool     watchQuotes(const int32_t* tokens, uint32_t n) noexcept                  { return _h && _h->watch_quotes(_c, tokens, n); }
    int64_t  lastPrice(int32_t token) const noexcept {                                 // the quote's ltp, else the last bar's close, else 0
        AlgoQuote q; if (quote(token, q) && q.ltp > 0) return q.ltp;
        const AlgoBar* b = nullptr; return bars(token, 1, b) == 1 ? b[0].close : 0;
    }

    // ---- analytics / history ------------------------------------------------------------------------------
    int32_t  want(AlgoSeries s, AlgoLevel l, AlgoRef r, const char* basket, uint32_t n) noexcept { return _h ? _h->want(_c, s, l, r, basket, n) : -1; }
    bool     analytics(int32_t handle, int32_t token, AlgoAnalytics& out) const noexcept { return _h && _h->analytics(_c, handle, token, &out); }
    bool     absolute(AlgoSeries s, AlgoLevel l, const char* sym, uint32_t n, AlgoRef r, AlgoAnalytics& out) const noexcept { return _h && _h->absolute(_c, s, l, sym, n, r, &out); }
    bool     relative(AlgoSeries s, AlgoLevel l, const char* sym, const char* basket, AlgoRef r, AlgoAnalytics& out) const noexcept { return _h && _h->relative(_c, s, l, sym, basket, r, &out); }
    uint32_t basket(const char* name, int32_t* out, uint32_t max) const noexcept { return _h ? _h->basket(_c, name, out, max) : 0; }

    // ---- the book ------------------------------------------------------------------------------------------
    void       track(const int32_t* tokens, uint32_t n, uint32_t slots = 1) noexcept { if (_h) _h->track(_c, tokens, n, slots); }
    AlgoStatus assign(int32_t token, int32_t pf_token, const char* name, int32_t slot, AlgoAssignment& out) noexcept {
        return _h ? _h->assign(_c, token, pf_token, name, slot, &out) : AlgoStatus::NO_SERVICE;
    }
    int64_t    activate(const AlgoAssignment& a, AlgoLeg leg, const AlgoParams& p) noexcept { return _h ? _h->activate(_c, &a, leg, &p) : -static_cast<int64_t>(AlgoStatus::NO_SERVICE); }
    AlgoLeg    leg(int32_t token, int32_t slot = 0) const noexcept   { return _h ? _h->leg(_c, token, slot) : AlgoLeg::NONE; }
    void       mark(int32_t token, AlgoLeg l, int32_t pf_token = 0, int32_t slot = 0) noexcept { if (_h) _h->mark(_c, token, l, pf_token, slot); }
    bool       allowed(int32_t token, int32_t slot = 0) const noexcept { return _h && _h->allowed(_c, token, slot); }
    bool       cooling(int32_t token, int32_t seconds, int32_t slot = 0) const noexcept { return _h && _h->cooling(_c, token, seconds, slot); }
    int32_t    newPf(const char* name) noexcept { return _h ? _h->new_pf(_c, name) : 0; }

    // ---- direct portfolio commands -----------------------------------------------------------------------------
    int64_t subscribe(int32_t pf, int32_t pf_token, const AlgoParams& p) noexcept { return _h ? _h->subscribe(_c, pf, pf_token, &p) : -static_cast<int64_t>(AlgoStatus::NO_SERVICE); }
    int64_t apply(int32_t pf, const AlgoParams& p) noexcept                     { return _h ? _h->apply(_c, pf, &p) : -static_cast<int64_t>(AlgoStatus::NO_SERVICE); }
    int64_t pause(int32_t pf) noexcept                                          { return _h ? _h->pause(_c, pf) : -static_cast<int64_t>(AlgoStatus::NO_SERVICE); }
    int64_t unpause(int32_t pf) noexcept                                        { return _h ? _h->unpause(_c, pf) : -static_cast<int64_t>(AlgoStatus::NO_SERVICE); }
    int64_t squareup(int32_t pf) noexcept                                       { return _h ? _h->squareup(_c, pf) : -static_cast<int64_t>(AlgoStatus::NO_SERVICE); }
    int64_t unsubscribe(int32_t pf, int32_t pf_token) noexcept                    { return _h ? _h->unsubscribe(_c, pf, pf_token) : -static_cast<int64_t>(AlgoStatus::NO_SERVICE); }
    bool    pfState(int32_t pf, AlgoPfState& out) const noexcept                { return _h && _h->pf_state(_c, pf, &out); }
    static AlgoStatus statusOf(int64_t r) noexcept { return r > 0 ? AlgoStatus::QUEUED : static_cast<AlgoStatus>(-r); }

    // ---- control and context ----------------------------------------------------------------------------------
    bool    sending() const noexcept               { return _h && _h->sending(_c); }
    void    stopSelf(bool unsub = true) noexcept   { if (_h) _h->stop_self(_c, unsub); }
    void    setTimerMs(int32_t ms) noexcept        { if (_h) _h->timer(_c, ms); }
    void    log(const char* msg, AlgoLogLevel lv = AlgoLogLevel::INFO) noexcept { if (_h && msg) _h->log(_c, lv, msg, static_cast<int32_t>(std::strlen(msg))); }
    void    event(const char* msg) noexcept       { if (_h && msg) _h->event(_c, msg, static_cast<int32_t>(std::strlen(msg))); }
    void    setVar(const char* k, const char* v) noexcept { if (_h && k && v) _h->set_var(_c, k, v); }
    int64_t clockNs() const noexcept               { return _h ? _h->clock_ns(_c) : 0; }
    int32_t minuteOfDay() const noexcept           { return _h ? _h->minute_of_day(_c) : -1; }
    int32_t today() const noexcept                 { return _h ? _h->today(_c) : 0; }
    int32_t user() const noexcept                  { return _h ? _h->user(_c) : 0; }
    void    name(char* buf, int32_t len) const noexcept { if (_h) _h->name(_c, buf, len); else if (buf && len > 0) buf[0] = '\0'; }

    // ---- internal bridge wiring: DO NOT call from strategy code --------------------------------------------------
    const AlgoHostAPI* _h = nullptr;
    void*              _c = nullptr;

public:
    void _algo_init(const AlgoHostAPI* api, void* ctx) noexcept { _h = api; _c = ctx; }
};

// ---------------------------------------------------------------------------
// Version info. One .so may hold several strategy classes; algoCreate picks one by name.
// ---------------------------------------------------------------------------
struct AlgoPluginInfo {
    uint32_t           api_version;      // must equal ALGO_SDK_API_VERSION
    uint32_t           class_count;
    const char*        plugin_name;      // e.g. "nifty_strategies"
    const char*        plugin_version;   // semver, e.g. "1.0.0"
    const char* const* class_names;      // class_count names
};

// ---------------------------------------------------------------------------
// Required C-linkage exports from every strategy .so:
//
//   extern "C" ALGO_EXPORT const AlgoPluginInfo* algoGetInfo();
//       Static info; called once at load for the version check and the class list.
//
//   extern "C" ALGO_EXPORT AlgoStrategy* algoCreate(const char* class_name, const char* instance_name);
//       A new instance of class_name (nullptr if unknown). The host calls _algo_init() and then onStart().
//
//   extern "C" ALGO_EXPORT void algoDestroy(AlgoStrategy* s);
//       Deletes an instance made by algoCreate (it lives in this .so's heap, so delete it here).
// ---------------------------------------------------------------------------
