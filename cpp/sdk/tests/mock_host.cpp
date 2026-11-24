// mock_host: loads a strategy .so the way algod will (dlopen, version check, algoCreate, _algo_init, onStart), wires a
// fake AlgoHostAPI (a small NIFTY chain, a book, a command recorder) and drives NiftySmaCross through an SMA cross up
// and a cross down. Checks the commands it sends and the exact apply prices.
//
//   mock_host build/libNiftySmaCross.so sdk/examples/NiftySmaCross/nifty_sma.ini
#include <dlfcn.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "AlgoSDK.h"
#include "core/ini.h"

static int g_fail = 0, g_pass = 0;
#define CHECK(c) do { if (c) ++g_pass; else { ++g_fail; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)

struct Activation { int32_t pf, old_pf, pf_token, slot; AlgoLeg leg; AlgoParams params; };

struct MockHost {
    algo::Ini ini;
    std::map<std::string, std::string> held;          // keeps the strings handed to the strategy alive
    // market
    int32_t future = 100;
    std::vector<std::pair<int32_t, int64_t>> ce, pe;  // token, strike (minor units)
    int64_t option_ltp = 12000;                       // 120.00 rupees in paise
    std::vector<AlgoBar> today;                       // future bars so far (for bars())
    // book
    struct Slot { int32_t pf = 0, pf_token = 0; AlgoLeg leg = AlgoLeg::NONE; };
    std::map<std::pair<int32_t, int32_t>, Slot> book;
    std::set<int32_t> unticked;                        // slots the operator unticked
    int32_t next_pf = 1001;
    std::vector<Activation> activations;
    int64_t next_job = 1;
    bool    sending = true;
    int32_t minute = 600;                             // 10:00 exchange time
    std::vector<std::string> logs;
};

static MockHost& H(void* c) { return *static_cast<MockHost*>(c); }

// ---- settings ------------------------------------------------------------------------------------------
static const char* settingLookup(void* c, const char* key) noexcept {
    const std::string k = key;
    const size_t dot = k.find('.');
    const std::string sec = dot == std::string::npos ? "strategy" : k.substr(0, dot), name = dot == std::string::npos ? k : k.substr(dot + 1);
    auto v = H(c).ini.get(sec, name);
    if (!v) return nullptr;
    return (H(c).held[k] = *v).c_str();
}
static uint32_t settingFill(void* c, const char* section, AlgoParams* out) noexcept {
    uint32_t n = 0;
    for (const auto& [k, v] : H(c).ini.section(section)) {              // typed by how the value reads, as algod does
        char* end = nullptr;
        const long long i = std::strtoll(v.c_str(), &end, 10);
        if (!v.empty() && !*end) { out->setInt(k.c_str(), i); ++n; continue; }
        const double d = std::strtod(v.c_str(), &end);
        if (!v.empty() && !*end) { out->setDouble(k.c_str(), d); ++n; continue; }
        out->setStr(k.c_str(), v.c_str()); ++n;
    }
    return n;
}

// ---- contracts and market ------------------------------------------------------------------------------
static int32_t hFind(void*, const char*) noexcept { return 0; }
static int32_t hFindFut(void* c, const char* und, int32_t e) noexcept { return (std::strcmp(und, "NIFTY") == 0 && e == 0) ? H(c).future : 0; }
static bool hInfo(void* c, int32_t t, AlgoContract* o) noexcept {
    *o = AlgoContract{};
    o->token = t; o->tick = 5; o->lot_size = 75; o->divisor = 100; o->expiry = 20261027;
    std::strcpy(o->underlying, "NIFTY");
    if (t == H(c).future) { o->kind = AlgoKind::FUT; std::strcpy(o->symbol, "NIFTY 27OCT26 FUT"); return true; }
    for (auto* side : {&H(c).ce, &H(c).pe})
        for (auto& [tok, k] : *side)
            if (tok == t) { o->kind = AlgoKind::OPT; o->strike = k; o->opt_type = side == &H(c).ce ? AlgoOptType::CE : AlgoOptType::PE; o->future_token = H(c).future; return true; }
    return false;
}
static uint32_t hExpiries(void*, const char*, AlgoKind, int32_t* out, uint32_t max) noexcept { if (max) out[0] = 20261027; return max ? 1 : 0; }
static uint32_t hStrikes(void*, const char*, int32_t, AlgoOptType, int32_t*, int64_t*, uint32_t) noexcept { return 0; }
static uint32_t hNearest(void* c, const char*, int64_t price, AlgoOptType t, int32_t, uint32_t n, int32_t* out) noexcept {
    auto v = t == AlgoOptType::CE ? H(c).ce : H(c).pe;
    std::stable_sort(v.begin(), v.end(), [&](auto& a, auto& b) { return std::llabs(a.second - price) < std::llabs(b.second - price); });
    uint32_t k = 0;
    for (; k < n && k < v.size(); ++k) out[k] = v[k].first;
    return k;
}
static bool hQuote(void* c, int32_t t, AlgoQuote* q) noexcept {
    *q = AlgoQuote{};
    if (t == H(c).future) return false;
    q->token = t; q->ltp = H(c).option_ltp; return true;
}
static uint32_t hBars(void* c, int32_t t, uint32_t n, const AlgoBar** out) noexcept {
    auto& v = H(c).today;
    if (t != H(c).future || v.empty()) { *out = nullptr; return 0; }
    const uint32_t k = std::min<uint32_t>(n, static_cast<uint32_t>(v.size()));
    *out = v.data() + v.size() - k;
    return k;
}
static uint32_t hDayBars(void*, int32_t, int32_t, const AlgoBar** out) noexcept { *out = nullptr; return 0; }
static bool hWatch(void*, const int32_t*, uint32_t) noexcept { return true; }

// ---- analytics (unused by this strategy) -------------------------------------------------------------------
static int32_t hWant(void*, AlgoSeries, AlgoLevel, AlgoRef, const char*, uint32_t) noexcept { return -1; }
static bool hAnalytics(void*, int32_t, int32_t, AlgoAnalytics*) noexcept { return false; }
static bool hAbsolute(void*, AlgoSeries, AlgoLevel, const char*, uint32_t, AlgoRef, AlgoAnalytics*) noexcept { return false; }
static bool hRelative(void*, AlgoSeries, AlgoLevel, const char*, const char*, AlgoRef, AlgoAnalytics*) noexcept { return false; }
static uint32_t hBasket(void*, const char*, int32_t*, uint32_t) noexcept { return 0; }

// ---- book --------------------------------------------------------------------------------------------------
static void hTrack(void*, const int32_t*, uint32_t, uint32_t) noexcept {}
static AlgoStatus hAssign(void* c, int32_t token, int32_t pf_token, const char*, int32_t slot, AlgoAssignment* a) noexcept {
    auto& h = H(c);
    if (h.unticked.count(slot)) return AlgoStatus::NOT_ALLOWED;
    auto& s = h.book[{token, slot}];
    *a = AlgoAssignment{};
    a->user = 3; a->token = token; a->pf_token = pf_token; a->slot = static_cast<int16_t>(slot);
    if (s.pf && s.pf_token == pf_token) { a->pf = s.pf; return AlgoStatus::OK; }
    a->old_pf = s.pf;
    a->pf = h.next_pf++;
    a->is_new = 1;
    return AlgoStatus::OK;
}
static int64_t hActivate(void* c, const AlgoAssignment* a, AlgoLeg leg, const AlgoParams* p) noexcept {
    auto& h = H(c);
    if (!h.sending) return -static_cast<int64_t>(AlgoStatus::OFF);
    h.activations.push_back({a->pf, a->old_pf, a->pf_token, a->slot, leg, *p});
    auto& s = h.book[{a->token, a->slot}];
    s.pf = a->pf; s.pf_token = a->pf_token; s.leg = leg;
    return h.next_job++;
}
static AlgoLeg hLeg(void* c, int32_t token, int32_t slot) noexcept { auto it = H(c).book.find({token, slot}); return it == H(c).book.end() ? AlgoLeg::NONE : it->second.leg; }
static void hMark(void* c, int32_t token, AlgoLeg l, int32_t, int32_t slot) noexcept { H(c).book[{token, slot}].leg = l; }
static bool hAllowed(void* c, int32_t, int32_t slot) noexcept { return !H(c).unticked.count(slot); }
static bool hCooling(void*, int32_t, int32_t, int32_t) noexcept { return false; }
static int32_t hNewPf(void* c, const char*) noexcept { return H(c).next_pf++; }

// ---- direct commands, control ---------------------------------------------------------------------------------
static int64_t hSub(void* c, int32_t, int32_t, const AlgoParams*) noexcept { return H(c).next_job++; }
static int64_t hApply(void* c, int32_t, const AlgoParams*) noexcept { return H(c).next_job++; }
static int64_t hPf(void* c, int32_t) noexcept { return H(c).next_job++; }
static int64_t hUnsub(void* c, int32_t, int32_t) noexcept { return H(c).next_job++; }
static bool hPfState(void*, int32_t, AlgoPfState*) noexcept { return false; }
static bool hSending(void* c) noexcept { return H(c).sending; }
static void hStopSelf(void*, bool) noexcept {}
static void hTimer(void*, int32_t) noexcept {}
static void hLog(void* c, AlgoLogLevel lv, const char* m, int32_t n) noexcept { H(c).logs.emplace_back(m, static_cast<size_t>(n)); std::printf("    [log %d] %.*s\n", static_cast<int>(lv), n, m); }
static void hEvent(void*, const char*, int32_t) noexcept {}
static void hVar(void*, const char*, const char*) noexcept {}
static int64_t hClock(void*) noexcept { return 0; }
static int32_t hMinute(void* c) noexcept { return H(c).minute; }
static int32_t hToday(void*) noexcept { return 20261015; }
static int32_t hUser(void*) noexcept { return 3; }
static void hName(void*, char* b, int32_t n) noexcept { std::snprintf(b, static_cast<size_t>(n), "nifty_sma"); }

static AlgoHostAPI makeApi() {
    AlgoHostAPI a{};
    a.api_version = ALGO_SDK_API_VERSION; a.struct_size = sizeof(AlgoHostAPI);
    a.find = hFind; a.find_fut = hFindFut; a.info = hInfo; a.expiries = hExpiries; a.strikes = hStrikes; a.nearest = hNearest;
    a.quote = hQuote; a.bars = hBars; a.day_bars = hDayBars; a.watch_quotes = hWatch;
    a.want = hWant; a.analytics = hAnalytics; a.absolute = hAbsolute; a.relative = hRelative; a.basket = hBasket;
    a.track = hTrack; a.assign = hAssign; a.activate = hActivate; a.leg = hLeg; a.mark = hMark; a.allowed = hAllowed;
    a.cooling = hCooling; a.new_pf = hNewPf;
    a.subscribe = hSub; a.apply = hApply; a.pause = hPf; a.unpause = hPf; a.squareup = hPf; a.unsubscribe = hUnsub; a.pf_state = hPfState;
    a.sending = hSending; a.stop_self = hStopSelf; a.timer = hTimer; a.log = hLog; a.event = hEvent; a.set_var = hVar;
    a.clock_ns = hClock; a.minute_of_day = hMinute; a.today = hToday; a.user = hUser; a.name = hName;
    return a;
}

static const AlgoParams::Entry* param(const AlgoParams& p, const char* k) {
    for (uint32_t i = 0; i < p.n; ++i) if (std::strcmp(p.e[i].key, k) == 0) return &p.e[i];
    return nullptr;
}
static int64_t priceOf(const AlgoParams& p, const char* k) { auto* e = param(p, k); return e && e->type == AlgoParams::T_PRICE ? e->i : -1; }

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "usage: mock_host <strategy.so> <settings.ini>\n"); return 2; }
    MockHost host;
    std::string err;
    if (!host.ini.loadFile(argv[2], err)) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }
    for (int i = 0; i < 8; ++i) {                                         // strikes 25000, 25050 ... in paise
        host.ce.push_back({201 + i, 2500000 + i * 5000});
        host.pe.push_back({301 + i, 2500000 + i * 5000});
    }

    std::printf("load\n");
    void* so = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    if (!so) { std::fprintf(stderr, "dlopen: %s\n", dlerror()); return 1; }
    auto getInfo = reinterpret_cast<const AlgoPluginInfo* (*)()>(dlsym(so, "algoGetInfo"));
    auto create = reinterpret_cast<AlgoStrategy* (*)(const char*, const char*)>(dlsym(so, "algoCreate"));
    auto destroy = reinterpret_cast<void (*)(AlgoStrategy*)>(dlsym(so, "algoDestroy"));
    CHECK(getInfo && create && destroy);
    const AlgoPluginInfo* info = getInfo();
    CHECK(info->api_version == ALGO_SDK_API_VERSION && info->class_count == 1 && std::strcmp(info->class_names[0], "NiftySmaCross") == 0);
    CHECK(create("NoSuchClass", "x") == nullptr);

    static const AlgoHostAPI api = makeApi();
    AlgoStrategy* s = create("NiftySmaCross", "nifty_sma");
    CHECK(s != nullptr);
    s->_algo_init(&api, &host);
    AlgoSettings settings{&host, settingLookup, settingFill};

    // a downtrend first (fast below slow), part of it already "today" when the strategy starts
    int64_t px = 2560000;
    int32_t minute0 = 29350000;
    auto bar = [&](int64_t close) { AlgoBar b{}; b.token = host.future; b.start_minute = minute0++; b.close = b.open = b.high = b.low = close; return b; };
    for (int i = 0; i < 10; ++i) host.today.push_back(bar(px -= 2000));
    std::printf("onStart\n");
    s->onStart(settings);

    auto feed = [&](int n, int64_t step) {
        for (int i = 0; i < n; ++i) {
            AlgoBar bs[2] = {bar(px += step), AlgoBar{}};
            bs[1].token = 205; bs[1].close = 11111;                      // an pf_token bar in the same batch: ignored by the signal
            AlgoBarBatch batch{bs, 2, 60, bs[0].start_minute, 0};
            s->onBars(batch);
        }
    };
    std::printf("down 15 bars: no crossing yet\n");
    feed(15, -2000);
    CHECK(host.activations.empty());

    std::printf("up 12 bars: fast crosses above slow -> CALLs on the 5 nearest strikes\n");
    feed(12, +4000);
    CHECK(host.activations.size() == 5);
    bool calls = true;
    for (auto& a : host.activations) calls &= a.leg == AlgoLeg::CALL && a.old_pf == 0;
    CHECK(calls);
    // the exact apply prices for a 120.00 premium on a 0.05 tick (the Python order_params gives the same)
    const AlgoParams& p = host.activations[0].params;
    CHECK(priceOf(p, "ival") == 120 && priceOf(p, "jval") == 90 && priceOf(p, "sval") == 360);
    CHECK(priceOf(p, "trp") == 12005 && priceOf(p, "lowerrange") == 11400 && priceOf(p, "upperrange") == 12600);
    auto* side = param(p, "side");
    CHECK(side && side->type == AlgoParams::T_STR && std::strcmp(side->s, "BUY") == 0);
    auto* target = param(p, "target");
    CHECK(target && target->type == AlgoParams::T_INT && target->i == 1000);          // [apply] passed through, typed
    auto* sl = param(p, "stoploss");
    CHECK(sl && sl->type == AlgoParams::T_INT && sl->i == -400);
    std::set<int32_t> opts;
    for (auto& a : host.activations) opts.insert(a.pf_token);
    CHECK(opts.size() == 5);

    std::printf("more up bars: same side, nothing new\n");
    feed(5, +3000);
    CHECK(host.activations.size() == 5);

    std::printf("operator unticks slot 1, then a cross down -> PUTs on 4 slots, each retiring its CALL portfolio\n");
    host.unticked.insert(1);
    feed(20, -5000);
    CHECK(host.activations.size() == 9);
    bool puts = true;
    for (size_t i = 5; i < host.activations.size(); ++i) puts &= host.activations[i].leg == AlgoLeg::PUT && host.activations[i].old_pf != 0 && host.activations[i].slot != 1;
    CHECK(puts);
    CHECK(hLeg(&host, host.future, 1) == AlgoLeg::CALL);                // the unticked slot was left alone

    std::printf("sending OFF, cross up again -> nothing sent\n");
    host.sending = false;
    const size_t before = host.activations.size();
    feed(20, +6000);
    CHECK(host.activations.size() == before);

    s->onStop();
    destroy(s);
    dlclose(so);
    std::printf("%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
