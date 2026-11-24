// Unit tests for cpp/core and cpp/data pieces that need no exchange: INI, time zones, LTT parsing, token index,
// SPSC ring, seqlock, double-mapped broadcast ring. No framework: CHECK counts failures, main returns non-zero on any.
#include <any>
#include <atomic>
#include <cstdlib>
#include <map>
#include <set>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "core/app.h"
#include "core/ini.h"
#include "core/limits.h"
#include "core/market.h"
#include "data/master_service.h"
#include "host/contract_api.h"
#include "data/feed_pipeline.h"
#include "core/pfstate.h"
#include "core/sql.h"
#include "core/statedb.h"
#include <fstream>
#include "core/mirror_ring.h"
#include "core/seqlock.h"
#include "core/spsc_ring.h"
#include "comms/nats_service.h"
#include "strategy/book.h"
#include "strategy/bridge.h"
#include "strategy/guard.h"
#include "strategy/registry.h"
#include "core/tz.h"
#include "data/ltt_parser.h"
#include "data/token_index.h"

using namespace algo;

static int g_fail = 0, g_pass = 0;
#define CHECK(c) do { if (c) ++g_pass; else { ++g_fail; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)
#define SECTION(name) std::printf("%s\n", name)

static int64_t utc(int y, int mo, int d, int h, int mi, int s) { return daysFromCivil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s; }

static void testIni() {
    SECTION("ini");
    Ini ini; std::string err;
    CHECK(ini.loadText("# c\n[a]\nx = 1  # inline\ny=hello#notcomment\nt = 09:15\nr = 09:15-15:30\nb = on\n[b]\nk = 2.5\n", "t.ini", err));
    std::vector<std::string> e;
    int64_t x = 0; std::string y; int32_t t = 0, r0 = 0, r1 = 0; bool b = false; double k = 0;
    CHECK(ini.getInt("a", "x", x, true, e) && x == 1);
    CHECK(ini.getString("a", "y", y, true, e) && y == "hello#notcomment");
    CHECK(ini.getClock("a", "t", t, true, e) && t == 9 * 3600 + 15 * 60);
    CHECK(ini.getClockRange("a", "r", r0, r1, true, e) && r0 == 33300 && r1 == 55800);
    CHECK(ini.getBool("a", "b", b, true, e) && b);
    CHECK(ini.unused().size() == 1);                                   // [b] k not read yet
    CHECK(ini.getDouble("b", "k", k, true, e) && k == 2.5);
    CHECK(ini.unused().empty() && e.empty());
    CHECK(!ini.getInt("a", "missing", x, true, e) && e.size() == 1);   // required and missing
    Ini bad;
    CHECK(!bad.loadText("[a]\nx=1\nx=2\n", "d.ini", err) && err.find("d.ini:3") != std::string::npos);
    CHECK(!bad.loadText("x=1\n", "d.ini", err));                        // key before a section
    CHECK(!bad.loadText("[a]\nnonsense\n", "d.ini", err));
    CHECK(!bad.loadText("[a]\n[a]\n", "d.ini", err));
    Ini v; v.loadText("[a]\nt = 9:15\nn = 12x\n", "v.ini", err);
    std::vector<std::string> e2;
    CHECK(!v.getClock("a", "t", t, true, e2) && !v.getInt("a", "n", x, true, e2) && e2.size() == 2);
}

static void testTz() {
    SECTION("tz");
    std::string err;
    TimeZone ist, ny, utcz = TimeZone::utc();
    CHECK(ist.load("Asia/Kolkata", err));
    CHECK(ny.load("America/New_York", err));
    CHECK(ist.offsetAt(utc(2026, 10, 15, 4, 0, 0)) == 19800);
    CHECK(utcz.offsetAt(utc(2026, 10, 15, 4, 0, 0)) == 0);
    CHECK(ny.offsetAt(utc(2026, 3, 8, 6, 59, 59)) == -5 * 3600);       // 01:59:59 EST
    CHECK(ny.offsetAt(utc(2026, 3, 8, 7, 0, 0)) == -4 * 3600);         // 03:00 EDT
    CHECK(ny.offsetAt(utc(2026, 11, 1, 5, 59, 59)) == -4 * 3600);
    CHECK(ny.offsetAt(utc(2026, 11, 1, 6, 0, 0)) == -5 * 3600);
    CHECK(ny.offsetAt(utc(2045, 7, 1, 12, 0, 0)) == -4 * 3600);        // past the last transition: the footer rule
    CHECK(ny.offsetAt(utc(2045, 1, 1, 12, 0, 0)) == -5 * 3600);
    CHECK(ist.localToUtc(utc(2026, 10, 15, 9, 15, 0)) == utc(2026, 10, 15, 3, 45, 0));
    CHECK(ny.localToUtc(utc(2026, 7, 1, 9, 30, 0)) == utc(2026, 7, 1, 13, 30, 0));
    CHECK(ny.localToUtc(utc(2026, 1, 5, 9, 30, 0)) == utc(2026, 1, 5, 14, 30, 0));
    CHECK(ny.localToUtc(utc(2026, 11, 1, 1, 30, 0)) == utc(2026, 11, 1, 5, 30, 0));   // occurs twice: the first (EDT)
    TimeZone none;
    CHECK(!none.load("Mars/Olympus", err) && !err.empty());
    int y; unsigned m, d;
    civilFromDays(daysFromCivil(2024, 2, 29), y, m, d);
    CHECK(y == 2024 && m == 2 && d == 29);
}

static void testLtt() {
    SECTION("ltt parser");
    std::string err;
    TimeZone ist; ist.load("Asia/Kolkata", err);
    TimeZone u = TimeZone::utc();
    LttParser nse, us;
    CHECK(nse.init("%d-%m-%Y %H:%M:%S", ist, err));
    CHECK(us.init("%Y-%m-%d %H:%M:%S", u, err));
    const int64_t want = utc(2026, 10, 15, 4, 0, 5);                    // 09:30:05 IST
    CHECK(nse.parse("15-10-2026 09:30:05") == want);
    CHECK(nse.parse("  15-10-2026 09:30:05.987 ") == want);
    CHECK(nse.parse("15-10-2026 09:30") == LttParser::kBad);
    CHECK(nse.parse("2026-10-15 09:30:05") == LttParser::kBad);
    CHECK(nse.parse("31-02-2026 09:30:05") == LttParser::kBad);
    CHECK(nse.parse("") == LttParser::kBad);
    CHECK(us.parse("2026-10-15 13:30:00") == utc(2026, 10, 15, 13, 30, 0));
    LttParser bad;
    CHECK(!bad.init("%d-%m-%Y %H:%M", ist, err));                       // must end in :%S
    CHECK(!bad.init("%d-%m-%Y %H:%M:%S %Z", ist, err));
}

static void testTokenIndex() {
    SECTION("token index");
    TokenIndex dense; std::vector<int64_t> t{5, 3, 9, 3};
    dense.build(t, 4);
    CHECK(dense.direct() && dense.masterCount() == 3);
    CHECK(dense.find(3) == 0 && dense.find(5) == 1 && dense.find(9) == 2 && dense.find(4) == -1 && dense.find(-1) == -1);
    const int32_t a = dense.findOrAdd(123456789);
    CHECK(a == 3 && dense.findOrAdd(123456789) == 3 && dense.tokenOf(3) == 123456789);
    for (int i = 0; i < 3; ++i) dense.findOrAdd(1000 + i);
    CHECK(dense.findOrAdd(99999) == -1);                               // overflow range full
    TokenIndex sparse; std::vector<int64_t> s;
    for (int i = 0; i < 1000; ++i) s.push_back(1000000007LL * (i + 1));
    sparse.build(s, 16);
    CHECK(!sparse.direct());
    bool all = true;
    for (int i = 0; i < 1000; ++i) all &= sparse.find(s[static_cast<size_t>(i)]) == i;
    CHECK(all && sparse.find(42) == -1);
}

static void testSpsc() {
    SECTION("spsc ring (2 threads, 2M messages)");
    static SpscRing<uint64_t, 1024> ring;
    constexpr uint64_t N = 2'000'000;
    std::atomic<bool> ok{true};
    std::thread cons([&] {
        uint64_t want = 0;
        while (want < N) ring.drain([&](const uint64_t& v) { if (v != want) ok = false; ++want; });
    });
    for (uint64_t i = 0; i < N;) if (ring.tryPush(i)) ++i;
    cons.join();
    CHECK(ok.load());
    CHECK(ring.sizeApprox() == 0);
    SpscRing<int, 4> small;
    CHECK(small.tryPush(1) && small.tryPush(2) && small.tryPush(3) && small.tryPush(4) && !small.tryPush(5));
    int v = 0;
    CHECK(small.tryPop(v) && v == 1 && small.tryPush(5));
}

struct alignas(64) SeqQuote { int64_t a, b, c, d, e, f, g, h; };

static void testSeqlock() {
    SECTION("seqlock (1 writer, 3 readers, torn-read check)");
    static Seqlock<SeqQuote> q;
    std::atomic<bool> stop{false}, torn{false};
    std::atomic<uint64_t> reads{0};
    q.write(SeqQuote{0, 1, 0, 0, 0, 0, 0, 0});                              // a consistent value before any reader starts
    std::vector<std::thread> rs;
    for (int k = 0; k < 3; ++k) rs.emplace_back([&] {
        SeqQuote x;
        while (!stop.load(std::memory_order_relaxed)) {
            q.read(x);
            if (x.b != x.a + 1 || x.h != x.a * 2) torn = true;          // every field derived from a: a mixed copy shows
            reads.fetch_add(1, std::memory_order_relaxed);
        }
    });
    for (int64_t i = 0; i < 3'000'000; ++i) q.write(SeqQuote{i, i + 1, i, i, i, i, i, i * 2});
    stop = true;
    for (auto& t : rs) t.join();
    CHECK(!torn.load());
    CHECK(reads.load() > 0);
}

struct Msg { uint64_t seq; uint64_t pad[7]; };   // 64 B

static void testMirror() {
    SECTION("double-mapped broadcast ring");
    const size_t page = MirrorMap::pageSize();
    const uint64_t cap = page / sizeof(Msg) * 4;                         // 4 pages
    BroadcastRing<Msg> ring;
    std::string err;
    CHECK(ring.create("", cap, err));
    auto r1 = ring.reader(), r2 = ring.reader();
    // write past the end so a run wraps: the reader must still see one contiguous span
    for (uint64_t i = 0; i < cap - 3; ++i) { ring.claim()->seq = i; ring.publish(); }
    std::span<const Msg> sp;
    CHECK(r1.next(sp) == BroadcastRing<Msg>::Read::OK && sp.size() == cap - 3);
    for (uint64_t i = cap - 3; i < cap + 5; ++i) { ring.claim()->seq = i; ring.publish(); }
    CHECK(r1.next(sp) == BroadcastRing<Msg>::Read::OK && sp.size() == 8);
    bool contiguous = true;
    for (size_t k = 0; k < sp.size(); ++k) contiguous &= sp[k].seq == cap - 3 + k;
    CHECK(contiguous && r1.stillValid());
    CHECK(r2.next(sp) == BroadcastRing<Msg>::Read::LAPPED);            // r2 is cap + 5 behind: more than a whole ring
    BroadcastRing<Msg> ring2; CHECK(ring2.create("", cap, err));
    auto slow = ring2.reader();
    for (uint64_t i = 0; i < cap + 1; ++i) { ring2.claim()->seq = i; ring2.publish(); }
    CHECK(slow.next(sp) == BroadcastRing<Msg>::Read::LAPPED);
    CHECK(slow.next(sp) == BroadcastRing<Msg>::Read::EMPTY);
    BroadcastRing<Msg> badcap;
    CHECK(!badcap.create("", 3, err));
}


static void testMarkets() {
    SECTION("markets from .ini == algo/core/config.py");
    std::vector<std::string> e;
    Market nse, us;
    const bool ok_nse = Market::load("config/markets/NSE.ini", nse, e);
    const bool ok_us = Market::load("config/markets/US.ini", us, e);
    for (auto& x : e) std::printf("    %s\n", x.c_str());
    CHECK(ok_nse && ok_us);
    // NSE = Market(code="NSE", tz="Asia/Kolkata", ltt_tz="Asia/Kolkata", ltt_fmt="%d-%m-%Y %H:%M", price_div=100, expiry_epoch="1980-01-01",
    //              strike_div=100, tick_div=100, session=(9:15, 15:30), trade_window=(9:20, 15:00), straddle_at_close=0, expense ...)
    CHECK(nse.code == "NSE" && nse.tz_name == "Asia/Kolkata" && nse.ltt_tz_name == "Asia/Kolkata" && nse.ltt_format == "%d-%m-%Y %H:%M:%S");
    CHECK(nse.price_div == 100 && nse.expiry_epoch == "1980-01-01" && nse.strike_div == 100 && nse.tick_div == 100);
    CHECK(nse.session_start == 9 * 3600 + 15 * 60 && nse.session_end == 15 * 3600 + 30 * 60 && nse.slots_per_day == 375);
    CHECK(nse.trade_start == 9 * 3600 + 20 * 60 && nse.trade_end == 15 * 3600);
    CHECK(nse.master_at == 9 * 3600 && nse.candles_at == 9 * 3600 + 14 * 60 + 30 && nse.close_at == 15 * 3600 + 30 * 60 && nse.straddle_at_close == 0);
    CHECK(nse.expense.size() == 3 && nse.expense["FUT"].buy_per_crore == 600 && nse.expense["FUT"].sell_per_crore == 5400 &&
          nse.expense["OPT"].buy_per_crore == 5625 && nse.expense["OPT"].sell_per_crore == 20328 && nse.expense["EQ"].sell_per_crore == 200);
    CHECK(nse.dh_table == "fo_market_data" && nse.bar_grace_s == 10 && nse.columns.size() == 10);
    // US = Market(code="US", tz="America/New_York", ltt_tz="UTC", ltt_fmt="%Y-%m-%d %H:%M", price_div=10000, expiry_epoch="1970-01-01",
    //             strike_div=10000, tick_div=10000, session=(9:30, 16:00), trade_window=(9:35, 15:45), currency="$", master 9:15, candles 9:29:30, close 16:00)
    CHECK(us.code == "US" && us.currency == "$" && us.tz_name == "America/New_York" && us.ltt_tz_name == "UTC");
    CHECK(us.price_div == 10000 && us.expiry_epoch == "1970-01-01" && us.strike_div == 10000 && us.slots_per_day == 390);
    CHECK(us.trade_start == 9 * 3600 + 35 * 60 && us.trade_end == 15 * 3600 + 45 * 60 && us.straddle_at_close == -1 && us.expense.empty());
    CHECK(us.master_at == 9 * 3600 + 15 * 60 && us.candles_at == 9 * 3600 + 29 * 60 + 30 && us.close_at == 16 * 3600);
    CHECK(nse.isTradingDay(20261015) && !nse.isTradingDay(20261017) && !nse.isTradingDay(20261018));   // Thu yes, Sat/Sun no
    // strict: a misspelt key stops the load and names the line
    { std::ifstream in("config/markets/NSE.ini"); std::string t((std::istreambuf_iterator<char>(in)), {}); std::ofstream o("build/typo.ini"); o << t << "\n[feed2]\nbar_grace = 5\n"; }
    std::vector<std::string> e2;
    Market typo;
    CHECK(!Market::load("build/typo.ini", typo, e2) && !e2.empty() && e2.back().find("bar_grace") != std::string::npos);
}

namespace {
struct CountingPlugin : Plugin {
    int& starts; int& stops; bool fail_start;
    CountingPlugin(std::string n, int& s, int& st, bool fail = false) : Plugin(std::move(n)), starts(s), stops(st), fail_start(fail) {}
    void start() override { ++starts; if (fail_start) throw std::runtime_error("boom"); }
    void onStop() override { ++stops; }
};
struct NeedsP1 : Plugin {
    int& s;
    explicit NeedsP1(int& s_) : Plugin("p2"), s(s_) {}
    const std::vector<std::string>& needs() const override { static const std::vector<std::string> v{"p1"}; return v; }
    void start() override { ++s; }
};
struct FollowP : Plugin {
    int& seen;
    explicit FollowP(int& s_) : Plugin("p3"), seen(s_) {}
    void start() override { follow("tbl", [this](const std::any& v) { seen = std::any_cast<int>(v); }); }
};
struct ThreadPlugin : Plugin {
    std::atomic<int>& ticks;
    explicit ThreadPlugin(std::atomic<int>& t) : Plugin("thr"), ticks(t) {}
    void start() override { spawn([this]() { while (!halted()) { ++ticks; std::this_thread::sleep_for(std::chrono::milliseconds(1)); } }); }
};
}  // namespace

static void testAppPlugin() {
    SECTION("app / plugin / event bus");
    Market mk; std::vector<std::string> errs;
    CHECK(Market::load("config/markets/NSE.ini", mk, errs));
    App app(mk, ProcessConfig{});

    int starts = 0, stops = 0;
    app.run(std::make_unique<CountingPlugin>("p1", starts, stops));
    CHECK(starts == 1 && stops == 0 && app.has("p1"));
    app.run(std::make_unique<CountingPlugin>("p1", starts, stops));                 // re-run: stops the old one first
    CHECK(starts == 2 && stops == 1);

    int s2 = 0;
    app.run(std::make_unique<NeedsP1>(s2));
    CHECK(s2 == 1);
    app.stop("p1");
    CHECK(!app.has("p1"));
    bool threw = false;
    try { app.run(std::make_unique<NeedsP1>(s2)); } catch (const MissingDependency&) { threw = true; }
    CHECK(threw && s2 == 1);                                                        // start() never ran: the need check failed first

    app.publish("tbl", 1);
    int seen = -1;
    app.run(std::make_unique<FollowP>(seen));
    CHECK(seen == -1);           // follow() does not replay the current value, only a later republish (matches Python's on_publish)
    app.publish("tbl", 42);
    CHECK(seen == 42);

    bool threw2 = false;
    try { app.run(std::make_unique<CountingPlugin>("bad", starts, stops, true)); } catch (...) { threw2 = true; }
    CHECK(threw2 && !app.has("bad") && stops == 3);                                 // a throwing start() is still torn down (its own onStop runs)

    std::atomic<int> ticks{0};
    app.run(std::make_unique<ThreadPlugin>(ticks));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK(ticks.load() > 0);
    app.stop("thr");
    const int after_stop = ticks.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK(ticks.load() == after_stop);                                             // the thread actually stopped, not just detached

    app.stopAll();
    CHECK(!app.has("p2") && !app.has("p3"));
}

static void testStateDb() {
    SECTION("statedb");
    // One shared in-memory SQLite connection, handed out once (the happy path never triggers the reconnect retry).
    auto db = std::make_shared<std::unique_ptr<SqlConn>>(makeSqliteConn(":memory:"));
    StateDB store([db]() { return std::move(*db); });

    // ---- registry: upsert replaces, load is ordered by (user, pf) ----
    store.savePortfolio({3, 1002, "nifty_rsi_book", "n2", "nifty_rsi", "2026-11-21", "", std::nullopt});
    store.savePortfolio({3, 1001, "nifty_rsi_book", "n1", "nifty_rsi", "2026-11-21", "first", std::nullopt});
    store.savePortfolio({1, 5000, "straddle_book", "s", "straddle", "2026-11-20", "", std::nullopt});
    store.savePortfolio({3, 1001, "nifty_rsi_book", "n1b", "nifty_rsi", "2026-11-21", "renamed", std::nullopt});  // upsert same key
    auto pfs = store.loadPortfolios();
    CHECK(pfs.size() == 3);                                                        // the upsert did not add a 4th
    CHECK(pfs[0].user == 1 && pfs[0].pf == 5000);                                  // ordered: user 1 first
    CHECK(pfs[1].user == 3 && pfs[1].pf == 1001 && pfs[1].name == "n1b" && pfs[1].note == "renamed");
    CHECK(pfs[2].user == 3 && pfs[2].pf == 1002);

    // ---- bulk import ----
    store.savePortfolios({{7, 2001, "b", "a", "x", "c", "", std::nullopt}, {7, 2002, "b", "a2", "x", "c", "", std::nullopt}});
    CHECK(store.loadPortfolios().size() == 5);

    // ---- a book's int maps: nullopt until it has rows, then replace-all ----
    CHECK(!store.loadInts(StateDB::BookTable::Map, "nifty_rsi_book").has_value());
    store.saveInts(StateDB::BookTable::Map, "nifty_rsi_book", {{111, 1001}, {222, 1002}});
    store.saveInts(StateDB::BookTable::Sub, "nifty_rsi_book", {{1001, 900111}});
    store.saveInts(StateDB::BookTable::Map, "other_book", {{333, 5000}});          // a second book is independent
    auto m = store.loadInts(StateDB::BookTable::Map, "nifty_rsi_book");
    CHECK(m && m->size() == 2 && m->at(111) == 1001 && m->at(222) == 1002);
    auto sub = store.loadInts(StateDB::BookTable::Sub, "nifty_rsi_book");
    CHECK(sub && sub->size() == 1 && sub->at(1001) == 900111);
    store.saveInts(StateDB::BookTable::Map, "nifty_rsi_book", {{111, 1009}});      // replace-all drops 222
    m = store.loadInts(StateDB::BookTable::Map, "nifty_rsi_book");
    CHECK(m && m->size() == 1 && m->at(111) == 1009);
    CHECK(store.loadInts(StateDB::BookTable::Map, "other_book")->at(333) == 5000); // untouched by the other book

    // ---- limits: null vs value, latch flags, upsert ----
    store.saveLimits({{"nifty_rsi", 5'000'000.0, std::nullopt, false, false},
                      {"straddle", std::nullopt, -50'000.0, false, true}});
    store.saveLimits({{"nifty_rsi", 6'000'000.0, std::nullopt, true, false}});     // upsert one
    auto lims = store.loadLimits();
    CHECK(lims.size() == 2);
    const Limit* nr = nullptr;
    const Limit* st = nullptr;
    for (const auto& l : lims) { if (l.strategy == "nifty_rsi") nr = &l; else if (l.strategy == "straddle") st = &l; }
    CHECK(nr && nr->turnover && *nr->turnover == 6'000'000.0 && !nr->m2m.has_value() && nr->turnover_hit && !nr->m2m_hit);
    CHECK(st && !st->turnover.has_value() && st->m2m && *st->m2m == -50'000.0 && st->m2m_hit && !st->turnover_hit);
}

static void testPfStates() {
    SECTION("pf states");
    PfStates ps;

    // a listener records every change
    int hits = 0;
    int64_t last_user = 0, last_pf = 0;
    auto tok = ps.onChange([&](int64_t u, int64_t p, const PfState&) { ++hits; last_user = u; last_pf = p; });

    // subscribe stamps the pf_token (from TOKEN), clears squared, marks subscribed
    PfPayload sub; sub.token = 900111;
    auto st = ps.record(3, 1001, "subscribe", sub, "nifty_rsi");
    CHECK(st && st->subscribed && st->pf_token == 900111 && !st->squared && st->by == "nifty_rsi" && st->cmd == "subscribe");
    CHECK(hits == 1 && last_user == 3 && last_pf == 1001);

    // squareup then apply toggles `squared`; an empty `by` becomes "operator"
    CHECK(ps.record(3, 1001, "squareup", {}, "")->squared == true);
    auto applied = ps.record(3, 1001, "apply", {}, "");
    CHECK(applied && !applied->squared && applied->by == "operator");

    // pause / resume via the Action field
    PfPayload pause; pause.action = "Pause";
    CHECK(ps.record(3, 1001, "pause_unpause", pause, "operator")->paused == true);
    PfPayload resume; resume.action = "Resume";
    CHECK(ps.record(3, 1001, "pause_unpause", resume, "operator")->paused == false);

    // unsubscribe keeps the pf_token but clears the flag; T1 (churn) overrides the pf_token
    PfPayload unsub; unsub.t1 = 900222;
    auto un = ps.record(3, 1001, "unsubscribe", unsub, "nifty_rsi");
    CHECK(un && !un->subscribed && un->pf_token == 900222);

    // an unrecognised command changes nothing and notifies nobody
    const int before = hits;
    CHECK(!ps.record(3, 1001, "reconcile", {}, "x").has_value());
    CHECK(hits == before);

    // adopt only fills an unseen portfolio, never overwrites
    ps.adopt(3, 1001, 123456);                               // 1001 already seen -> ignored
    CHECK(ps.get(3, 1001)->pf_token == 900222);
    ps.adopt(5, 7000, 555, true);                            // new -> created, paused
    auto a = ps.get(5, 7000);
    CHECK(a && a->subscribed && a->pf_token == 555 && a->paused && a->cmd == "adopted");

    // items filters by user; size counts all
    CHECK(ps.items(3).size() == 1 && ps.items().size() == 2);

    // by_token: a subscribed portfolio wins over an unsubscribed one on the same pf_token token
    ps.record(9, 8001, "subscribe", [] { PfPayload p; p.token = 777; return p; }(), "a");
    ps.record(9, 8002, "unsubscribe", [] { PfPayload p; p.token = 777; return p; }(), "a");  // same pf_token, not subscribed
    auto bo = ps.byToken();
    CHECK(bo.count(777) && std::get<1>(bo[777]) == 8001);   // the subscribed one (pf 8001) wins

    // removing the listener stops notifications
    ps.removeListener(tok);
    const int frozen = hits;
    ps.record(3, 1001, "squareup", {}, "op");
    CHECK(hits == frozen);

    ps.forget(3);
    CHECK(!ps.get(3, 1001).has_value() && ps.get(5, 7000).has_value());
}

static void testLimits() {
    SECTION("limits");
    Limits lim;                                               // in-memory (no StateDB)
    int changes = 0;
    auto tok = lim.onChange([&](const std::string&) { ++changes; });

    // bad inputs throw
    bool threw = false;
    try { lim.setLimit("s", "bogus", 1.0); } catch (const std::invalid_argument&) { threw = true; }
    CHECK(threw);
    threw = false;
    try { lim.setLimit("s", "turnover", -5.0); } catch (const std::invalid_argument&) { threw = true; }
    CHECK(threw);

    // turnover: breaches at >= limit, then latches
    lim.setLimit("nifty", "turnover", 1'000'000.0);
    CHECK(lim.get("nifty").turnover == 1'000'000.0 && !lim.get("nifty").turnover_hit);
    CHECK(lim.update("nifty", 999'999.0, std::nullopt).empty());
    auto n2 = lim.update("nifty", 1'000'000.0, std::nullopt);
    CHECK(n2.size() == 1 && n2[0] == "turnover" && lim.get("nifty").turnover_hit);
    lim.update("nifty", 500'000.0, std::nullopt);             // a later lower value stays latched
    CHECK(lim.get("nifty").turnover_hit);
    CHECK(lim.setLimit("nifty", "turnover", 2'000'000.0).empty() && !lim.get("nifty").turnover_hit);  // raising lifts it

    // m2m: breaches at <= limit; the sign counts
    lim.setLimit("straddle", "m2m", -50'000.0);
    CHECK(lim.update("straddle", std::nullopt, -49'999.0).empty());   // above the limit, ok
    auto m1 = lim.update("straddle", std::nullopt, -50'000.0);        // at the limit, breach
    CHECK(m1.size() == 1 && m1[0] == "m2m" && lim.get("straddle").m2m_hit);

    // a missing value (Deephaven double null) never breaches
    lim.setLimit("x", "m2m", -1'000.0);
    CHECK(lim.update("x", std::nullopt, -1.0e308).empty() && !lim.get("x").m2m_hit);

    // blocked() text
    CHECK(lim.blocked("nifty").empty());
    CHECK(lim.blocked("straddle").find("M2M limit -50,000 reached") != std::string::npos);

    // 0 clears a limit; names() is sorted
    lim.setLimit("nifty", "turnover", 0.0);
    CHECK(!lim.get("nifty").turnover.has_value());
    CHECK(changes > 0);

    // persistence through StateDB: the latch survives a reload
    auto holder = std::make_shared<std::unique_ptr<SqlConn>>(makeSqliteConn(":memory:"));
    StateDB db([holder] { return std::move(*holder); });
    Limits lp(&db);
    lp.setLimit("persisted", "turnover", 700'000.0);
    lp.update("persisted", 800'000.0, std::nullopt);          // breach -> saved to the table
    CHECK(lp.get("persisted").turnover_hit);
    Limits reloaded(&db);
    auto r = reloaded.get("persisted");
    CHECK(r.turnover == 700'000.0 && r.turnover_hit);

    lim.removeListener(tok);
}

static void testRegistry() {
    SECTION("registry");
    Registry reg;                                             // in-memory

    int rows = 0;
    auto tok = reg.onChange([&](int64_t, int64_t, const RegRec&) { ++rows; });

    // allocate runs a strategy's numbers on from the start, stepping over numbers held by others
    const int64_t a = reg.create(3, "rsi_stocks", "RELIANCE");    // first for this owner -> 1001
    const int64_t b = reg.create(3, "rsi_stocks", "TCS");         // next -> 1002
    CHECK(a == 1001 && b == 1002 && rows == 2);
    // a different owner starting at the same point skips the taken numbers
    const int64_t c = reg.allocate(3, "straddle", 1001, "", "straddle", "NIFTY");
    CHECK(c == 1003);                                              // 1001/1002 taken by rsi_stocks -> 1003
    // its own next one runs on from its highest
    CHECK(reg.allocate(3, "straddle", 1001) == 1004);

    // another user is independent
    CHECK(reg.create(4, "rsi_stocks", "INFY") == 1001);

    // get / owner_of / count
    auto rec = reg.get(3, 1001);
    CHECK(rec && rec->owner == "rsi_stocks" && rec->name == "RELIANCE" && !rec->created.empty());
    CHECK(reg.ownerOf(3, 1003) == "straddle");
    CHECK(reg.count().first == 2 && reg.count().second == 5);      // 2 users; user 3 has 4 (1001-1004), user 4 has 1

    // registerPf: free -> registered; same owner unchanged -> exists; changed -> updated; other owner -> taken
    Claim r1 = reg.registerPf(9, 2001, "console");
    CHECK(r1 && r1.status == "registered");
    Claim r2 = reg.registerPf(9, 2001, "console");
    CHECK(r2 && r2.status == "exists");
    Claim r3 = reg.registerPf(9, 2001, "console", "", "", "RELIANCE");  // sets the name
    CHECK(r3 && r3.status == "updated" && reg.get(9, 2001)->name == "RELIANCE");
    Claim r4 = reg.registerPf(9, 2001, "someone_else");
    CHECK(!r4 && r4.status == "taken" && r4.held_by == "console");

    // bad inputs throw
    bool threw = false;
    try { reg.allocate(0, "x"); } catch (const std::invalid_argument&) { threw = true; }
    CHECK(threw);
    threw = false;
    try { reg.create(3, "", "X"); } catch (const std::invalid_argument&) { threw = true; }
    CHECK(threw);

    // portfolios(user) is sorted and filtered
    auto p3 = reg.portfolios(3);
    CHECK(p3.size() == 4 && p3.front().pf == 1001 && p3.back().pf == 1004);

    // persistence: numbers survive a reload from StateDB, and allocation continues past them
    auto holder = std::make_shared<std::unique_ptr<SqlConn>>(makeSqliteConn(":memory:"));
    StateDB db([holder] { return std::move(*holder); });
    Registry rp(&db);
    CHECK(rp.create(7, "algo_x", "A") == 1001 && rp.create(7, "algo_x", "B") == 1002);
    Registry reloaded(&db);
    CHECK(reloaded.get(7, 1001) && reloaded.get(7, 1001)->name == "A");
    CHECK(reloaded.create(7, "algo_x", "C") == 1003);             // continues from the persisted highest

    reg.removeListener(tok);
}

static void testBook() {
    SECTION("book");
    Registry reg;                                             // in-memory numbering
    Portfolios b("m", "s", 1001, &reg, 3, "book_rsi");        // user 3, no StateDB

    b.track({500});
    CHECK(b.tracked.count(500) && b.allow.at(500) == true);

    // first signal: a new PF from the registry
    auto a1 = b.assign(500, 900, "RELIANCE", "rsi");
    CHECK(a1.pf == 1001 && !a1.old_pf.has_value() && a1.created);
    b.subscribe(1001, 900);

    // same pf_token kept, no new number
    auto a2 = b.assign(500, 900);
    CHECK(a2.pf == 1001 && !a2.created && !a2.old_pf.has_value());

    // a different pf_token: the stock moves to a new PF, the old one retires
    auto a3 = b.assign(500, 901, "RELIANCE", "rsi");
    CHECK(a3.pf == 1002 && a3.old_pf == 1001 && a3.created);
    b.subscribe(1002, 901);

    CHECK(b.currentPfMap().size() == 1 && b.currentPfMap().at(500) == 1002);
    CHECK(b.tokenOfPf(1002) == 500);
    CHECK((b.allPfs() == std::vector<int64_t>{1001, 1002}));
    CHECK(b.pfOf(500) == 1002 && !b.pfOf(999).has_value());

    // revert: a new PF that never reached the bridge goes back to the old one
    auto a4 = b.assign(500, 902);
    CHECK(a4.pf == 1003 && a4.old_pf == 1002);
    b.revert(500, 1002, 1003);
    CHECK(b.pfOf(500) == 1002 && (b.allPfs() == std::vector<int64_t>{1001, 1002}));

    // pf_for_row / owner_of: exact subscription wins; else the current PF of the same stock
    auto sym = [](int64_t t) -> std::string {
        switch (t) { case 500: return "RELIANCE FUT"; case 900: return "RELIANCE 2500 CE"; case 901: return "RELIANCE 2600 CE"; }
        return "X";
    };
    CHECK(b.pfForRow(901, sym) == 1002);                      // exact subscription to 901
    CHECK(b.pfForRow(900, sym) == 1001);                      // 900 still subscribed by the retired 1001
    CHECK(b.ownerOf(500, sym) == 500);                        // the future itself is current
    CHECK(b.ownerOf(900, sym) == 500);                        // old leg -> same stock's current future

    // claim_existing: a number already held by someone else for this user is refused
    Registry reg2;
    reg2.registerPf(5, 2001, "other_book");
    Portfolios bc("m2", "s2", 1001, &reg2, 5, "my_book");
    bc.pf_map[600] = 2001;                                     // a number this book's file already held
    auto refused = bc.claimExisting();
    CHECK(refused.size() == 1 && refused[0].held_by == "other_book");

    // persistence: pf_map and subscribed survive a reload from StateDB
    auto holder = std::make_shared<std::unique_ptr<SqlConn>>(makeSqliteConn(":memory:"));
    StateDB db([holder] { return std::move(*holder); });
    Registry rp(&db);
    Portfolios bp("pm", "ps", 1001, &rp, 7, "bk", &db);
    bp.track({500});
    auto ap = bp.assign(500, 900, "R", "rsi");
    bp.subscribe(ap.pf, 900);
    CHECK(ap.pf == 1001);
    Portfolios bp2("pm", "ps", 1001, &rp, 7, "bk", &db);
    CHECK(bp2.pf_map.at(500) == 1001 && bp2.subscribed.at(1001) == 900);
}

namespace {
// Fake transport: records every request, can be slow, can time out on a given label. The service's own tests drive
// it the way tests/test_nats_service.py drives its Wire.
struct FakeWire : Transport {
    struct Rec { std::string subject, cmd, label; Payload payload; };
    mutable std::mutex m;
    std::vector<Rec> sent;
    int now = 0, peak = 0;
    double delay_s = 0.0;
    std::string status = "ok";
    std::string timeout_on;                                 // label "cmd" or "cmd:Action" to time out
    std::string fail_on;                                    // label to answer with a non-ok status ("rejected")
    bool conn = true;

    bool isConnected() const override { return conn; }
    Response request(const std::string& subject, const std::string& cmd, const Payload& payload, double) override {
        std::string label = cmd;
        auto it = payload.find("Action");
        if (it != payload.end() && std::holds_alternative<std::string>(it->second)) label += ":" + std::get<std::string>(it->second);
        { std::lock_guard<std::mutex> g(m); ++now; peak = std::max(peak, now); sent.push_back({subject, cmd, label, payload}); }
        if (delay_s > 0) std::this_thread::sleep_for(std::chrono::duration<double>(delay_s));
        { std::lock_guard<std::mutex> g(m); --now; }
        if (!timeout_on.empty() && label == timeout_on) throw NatsTimeout();
        return Response{(!fail_on.empty() && label == fail_on) ? "rejected" : status, {}};
    }
    std::size_t count() const { std::lock_guard<std::mutex> g(m); return sent.size(); }
    std::vector<std::string> seq(int64_t pf) const {        // the command labels seen for one portfolio, in order
        std::lock_guard<std::mutex> g(m);
        std::vector<std::string> out;
        for (const auto& r : sent) if (r.subject == "cmd.hft." + std::to_string(pf)) out.push_back(r.label);
        return out;
    }
};

template <class F> bool waitUntil(F pred, double secs = 3.0) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::duration<double>(secs);
    while (std::chrono::steady_clock::now() < end) { if (pred()) return true; std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
    return pred();
}

// Helper: a service wired to a fresh FakeWire the test keeps a raw pointer to (the service owns it after connect).
struct Svc {
    std::unique_ptr<NatsService> svc;
    FakeWire* wire;
};
Svc makeSvc(NatsConfig cfg, double delay_s = 0.0, std::string status = "ok", std::string timeout_on = "") {
    auto wire = std::make_unique<FakeWire>();
    wire->delay_s = delay_s; wire->status = std::move(status); wire->timeout_on = std::move(timeout_on);
    auto holder = std::make_shared<std::unique_ptr<Transport>>(std::move(wire));
    FakeWire* w = static_cast<FakeWire*>(holder->get());
    auto svc = std::make_unique<NatsService>(std::move(cfg), [holder] { return std::move(*holder); });
    svc->connect();
    return {std::move(svc), w};
}
}  // namespace

static void testNats() {
    SECTION("nats service");

    // ---- ordering per portfolio, overlap across portfolios, counts ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg, 0.001);
        std::vector<std::future<Reply>> futs;
        std::mutex fm;
        std::vector<std::thread> ths;
        for (int pf = 1; pf <= 6; ++pf)
            ths.emplace_back([&, pf] {
                for (int i = 0; i < 20; ++i) {
                    auto f = svc->send(pf, i % 2 ? "apply" : "squareup", {{"n", int64_t{i}}});
                    std::lock_guard<std::mutex> g(fm); futs.push_back(std::move(f));
                }
            });
        for (auto& t : ths) t.join();
        for (auto& f : futs) f.get();
        // each portfolio's commands arrived in order
        for (int pf = 1; pf <= 6; ++pf) {
            std::vector<int64_t> seen;
            for (auto& r : wire->sent)
                if (r.subject == "cmd.hft." + std::to_string(pf)) seen.push_back(std::get<int64_t>(r.payload.at("n")));
            std::vector<int64_t> want(20); for (int i = 0; i < 20; ++i) want[i] = i;
            CHECK(seen == want);
        }
        CHECK(wire->peak > 1);                               // different portfolios overlapped
        CHECK(svc->counts().ok == 120 && svc->queued() == 0 && svc->inFlight() == 0);
        svc->stop();
    }

    // ---- the in-flight cap holds across all portfolios ----
    {
        NatsConfig cfg; cfg.send_on = true; cfg.max_in_flight = 3;
        auto [svc, wire] = makeSvc(cfg, 0.01);
        std::vector<std::future<Reply>> futs;
        for (int pf = 1; pf <= 30; ++pf) futs.push_back(svc->pause(pf));
        for (auto& f : futs) f.get();
        CHECK(wire->peak > 1 && wire->peak <= 3 && wire->sent.size() == 30);
        svc->stop();
    }

    // ---- the switches: off -> refused, then on -> sent ----
    {
        NatsConfig cfg; cfg.send_on = false;
        auto [svc, wire] = makeSvc(cfg);
        auto r0 = svc->send(1, "squareup").get();
        CHECK(r0.status == "off" && r0.data["detail"] == "nats is off");
        CHECK(svc->on() && svc->isOn());
        CHECK(svc->send(1, "squareup").get().ok);
        // a strategy's own switch: OFF until it switches itself on; the operator never needs it
        auto r1 = svc->send(1, "squareup", {}, std::nullopt, "rsi").get();
        CHECK(r1.status == "off" && r1.data["detail"] == "strategy rsi is off");
        CHECK(svc->strategyOn("rsi") && svc->isStrategyOn("rsi"));
        CHECK(svc->send(1, "squareup", {}, std::nullopt, "rsi").get().ok);
        svc->strategyOff("rsi");
        CHECK(svc->send(1, "squareup", {}, std::nullopt, "rsi").get().status == "off" && svc->send(1, "squareup").get().ok);
        svc->stop();
    }

    // ---- dry run: nothing sent, status "dry" ----
    {
        NatsConfig cfg; cfg.dry_run = true; cfg.send_on = true;
        NatsService svc(cfg);                                // no factory needed in a dry run
        svc.connect();
        CHECK(svc.send(1, "squareup").get().status == "dry");
        CHECK(svc.state() == "dry");
        svc.stop();
    }

    // ---- users: no_user / user_not_allowed / ok ----
    {
        NatsConfig cfg; cfg.send_on = true; cfg.users = {3, 4};
        auto [svc, wire] = makeSvc(cfg);
        CHECK(svc->send(1, "squareup").get().status == "no_user");      // two users, none named
        CHECK(svc->send(1, "squareup", {}, 9).get().status == "user_not_allowed");
        CHECK(svc->send(1, "squareup", {}, 3).get().ok);
        svc->stop();
    }

    // ---- the user and pf switches: for everyone (strategies AND the operator), squareup included ----
    {
        NatsConfig cfg; cfg.send_on = true; cfg.users = {3, 4};
        auto [svc, wire] = makeSvc(cfg);
        svc->strategyOn("rsi");
        svc->pfOff(3, 5);
        auto r = svc->send(5, "squareup", {}, 3).get();
        CHECK(r.status == "off" && r.data["detail"] == "user 3 portfolio 5 is off" && !svc->isPfOn(3, 5));
        CHECK(svc->send(5, "squareup", {}, 3, "rsi").get().status == "off");     // a strategy's command too
        CHECK(svc->send(6, "squareup", {}, 3).get().ok && svc->send(5, "squareup", {}, 4).get().ok);   // other pf / user's pf 5
        svc->pfOn(3, 5);
        CHECK(svc->send(5, "squareup", {}, 3).get().ok);
        svc->userOff(4);
        auto u = svc->send(7, "pause_unpause", {}, 4, "rsi").get();
        CHECK(u.status == "off" && u.data["detail"] == "user 4 is off" && !svc->isUserOn(4));
        CHECK(svc->send(7, "squareup", {}, 4).get().status == "off" && svc->send(7, "squareup", {}, 3).get().ok);
        svc->userOn(4);
        CHECK(svc->send(7, "squareup", {}, 4).get().ok);
        // a switch turned off while a sequence is running stops it at its next request
        std::atomic<bool> second{false};
        auto fut = svc->submit(9, [&, s = svc.get()](Call& call) {
            call("pause_unpause", {{"Action", std::string("Pause")}});
            s->pfOff(3, 9);                                                   // switched off between the two steps
            Reply r2 = call("squareup", {});
            second = r2.status == "off";
            return r2;
        }, 3);
        CHECK(fut.get().status == "off" && second.load());
        const auto& ev = svc->events();
        CHECK(!ev.empty() && ev.back().status == "off" && ev.back().detail == "user 3 portfolio 9 is off");
        svc->stop();
    }

    // ---- a confirmed command updates the shared pf-state ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        CHECK(svc->subscribe(1001, 900, {}, 3).get().ok);
        auto st = svc->pfState(1001, 3);
        CHECK(st && st->subscribed && st->pf_token == 900);
        svc->stop();
    }

    // ---- a multi-step job runs its steps in order on one lane ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        auto r = svc->submit(2000, [](Call& call) {
            call("pause_unpause", {{"Action", std::string("Pause")}});
            return call("apply", {{"maxqty", int64_t{50}}});
        }, 3).get();
        CHECK(r.ok && r.cmd == "apply");
        CHECK(wire->sent.size() == 2 && std::get<std::string>(wire->sent[0].payload.at("Action")) == "Pause");
        svc->stop();
    }

    // ---- a cross-portfolio workflow via go() + lane() ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        std::promise<void> done;
        NatsService* s = svc.get();
        bool started = svc->go([s, &done] {
            s->lane(10, [](Call& c) { return c("squareup", {}); }, 3);
            s->lane(11, [](Call& c) { return c("squareup", {}); }, 3);
            done.set_value();
        });
        CHECK(started);
        done.get_future().wait_for(std::chrono::seconds(5));
        CHECK(wire->sent.size() == 2);
        // go() is dropped while sending is off
        svc->off();
        CHECK(!svc->go([] {}));
        svc->stop();
    }
}

static void testBridge() {
    SECTION("bridge");

    // ---- a fresh-PF swap: subscribe -> pause -> apply -> unpause, and the book is brought in line ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        Registry reg;
        Portfolios book("m", "s", 1001, &reg, 3, "bk");
        book.track({500});
        auto a = book.assign(500, 900, "R", "rsi");          // pf 1001, not yet subscribed
        BridgeConfig bc; bc.user = 3;
        Bridge br(bc, "rsi");
        br.bind(svc.get(), &book);
        CHECK(br.sendOn());
        CHECK(br.swap(a.pf, 500, 900, {{"n", int64_t{1}}}, std::nullopt, std::nullopt, std::nullopt));
        CHECK(waitUntil([&] { return wire->seq(a.pf).size() == 4; }));
        svc->stop();                                          // join the workflow before reading the book
        const std::vector<std::string> want{"subscribe", "pause_unpause:Pause", "apply", "pause_unpause:UnPause"};
        CHECK(wire->seq(a.pf) == want);
        CHECK(book.subscribed.at(a.pf) == 900 && book.caps.at(a.pf) == 5);   // add_qty default
    }

    // ---- a swap whose apply is rejected: left paused, no unpause, bookkeeping rolled back ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        wire->fail_on = "apply";
        Registry reg;
        Portfolios book("m", "s", 1001, &reg, 3, "bk");
        book.track({600});
        auto a = book.assign(600, 950, "R", "rsi");
        BridgeConfig bc; bc.user = 3;
        Bridge br(bc, "rsi");
        br.bind(svc.get(), &book);
        br.sendOn();
        book.setLive(600, std::string("CALL"), 950);
        CHECK(br.swap(a.pf, 600, 950, {{"n", int64_t{1}}}, std::string("CALL"), 950, std::nullopt));
        CHECK(waitUntil([&] { return wire->seq(a.pf).size() == 3; }));       // subscribe, pause, apply(fail)
        svc->stop();
        CHECK((wire->seq(a.pf) == std::vector<std::string>{"subscribe", "pause_unpause:Pause", "apply"}));
        CHECK(book.last_fail.count(600) == 1);                               // a later update may retry
        CHECK(book.live.at(600) == std::optional<std::string>("CALL"));      // prev leg restored
        CHECK(book.caps.at(a.pf) == 0);                                      // cap not advanced (apply failed)
    }

    // ---- a swap that moves the token: the old PF is paused + squared at once, but UNSUBSCRIBED only after
    //      squareup_settle_s (its plugin must first fill the closing order); the new PF does not wait for that ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        Registry reg;
        Portfolios book("m", "s", 1001, &reg, 3, "bk");
        book.track({500});
        const int64_t old_pf = book.assign(500, 900, "R", "rsi").pf;
        book.subscribe(old_pf, 900);                                          // the old PF is live on 900
        auto a = book.assign(500, 950, "R", "rsi");                           // the signal moved: a new PF, the old one retires
        CHECK(a.old_pf && *a.old_pf == old_pf && a.pf != old_pf);
        BridgeConfig bc; bc.user = 3; bc.squareup_settle_s = 0.4;
        Bridge br(bc, "rsi");
        br.bind(svc.get(), &book);
        br.sendOn();
        const auto t0 = std::chrono::steady_clock::now();
        CHECK(br.swap(a.pf, 500, 950, {{"n", int64_t{1}}}, std::string("CALL"), 900, a.old_pf));
        CHECK(waitUntil([&] { return wire->seq(a.pf).size() == 4; }));         // the new PF is fully set up ...
        CHECK((wire->seq(old_pf) == std::vector<std::string>{"pause_unpause:Pause", "squareup"}));   // ... the old one not yet unsubscribed
        CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(300));             // the new PF did not wait
        CHECK(waitUntil([&] { return wire->seq(old_pf).size() == 3; }, 3.0));   // the unsubscribe follows the settle time
        CHECK(std::chrono::steady_clock::now() - t0 >= std::chrono::milliseconds(400));
        CHECK((wire->seq(old_pf) == std::vector<std::string>{"pause_unpause:Pause", "squareup", "unsubscribe"}));
        CHECK(std::get<int64_t>(wire->sent.back().payload.at("T1")) == 900);   // it unsubscribes the token it was on
        svc->stop();
    }

    // ---- a shutdown during the settle wait ends it at once: no hang, no late unsubscribe, an event says so ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        Registry reg;
        Portfolios book("m", "s", 1001, &reg, 3, "bk");
        book.track({500});
        const int64_t old_pf = book.assign(500, 900, "R", "rsi").pf;
        book.subscribe(old_pf, 900);
        auto a = book.assign(500, 950, "R", "rsi");
        BridgeConfig bc; bc.user = 3; bc.squareup_settle_s = 30;              // a long wait
        Bridge br(bc, "rsi");
        br.bind(svc.get(), &book);
        br.sendOn();
        CHECK(br.swap(a.pf, 500, 950, {}, std::string("CALL"), 900, a.old_pf));
        CHECK(waitUntil([&] { return wire->seq(a.pf).size() == 4 && wire->seq(old_pf).size() == 2; }));
        const auto t0 = std::chrono::steady_clock::now();
        svc->stop();
        CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(2));                  // did not wait 30 s
        CHECK(wire->seq(old_pf).size() == 2);                                                    // no unsubscribe went out
        bool noted = false;
        for (auto& e : svc->events()) noted |= e.pf == old_pf && e.cmd == "retire" && e.status == "not_sent";
        CHECK(noted);
    }

    // ---- the send switch gates the workflow (off -> dropped) ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        Registry reg;
        Portfolios book("m", "s", 1001, &reg, 3, "bk");
        book.track({700});
        auto a = book.assign(700, 980, "R", "rsi");
        BridgeConfig bc; bc.user = 3;
        Bridge br(bc, "rsi");
        br.bind(svc.get(), &book);
        CHECK(!br.swap(a.pf, 700, 980, {}, std::nullopt, std::nullopt, std::nullopt));   // not enabled -> dropped
        br.sendOn();
        CHECK(br.swap(a.pf, 700, 980, {}, std::nullopt, std::nullopt, std::nullopt));
        CHECK(waitUntil([&] { return wire->seq(a.pf).size() == 4; }));
        svc->stop();
    }

    // ---- the pf-state listener brings the book in line with a console pause ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        Registry reg;
        Portfolios book("m", "s", 1001, &reg, 3, "bk");
        book.track({800});
        auto a = book.assign(800, 990, "R", "rsi");
        book.subscribe(a.pf, 990);                            // so token_of/subscribed knows it is ours
        BridgeConfig bc; bc.user = 3;
        Bridge br(bc, "rsi");
        br.bind(svc.get(), &book);
        br.sendOn();
        // the "console" (a different client, user 3) pauses the portfolio
        CHECK(svc->pause(a.pf, 3).get().ok);
        CHECK(waitUntil([&] { return book.isPaused(a.pf); }));      // listener applied it to the book
        svc->stop();
    }

    // ---- an operator pause returns the command's future and the listener updates the book ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        Registry reg;
        Portfolios book("m", "s", 1001, &reg, 3, "bk");
        book.track({850});
        auto a = book.assign(850, 995, "R", "rsi");
        book.subscribe(a.pf, 995);
        BridgeConfig bc; bc.user = 3;
        Bridge br(bc, "rsi");
        br.bind(svc.get(), &book);
        br.sendOn();
        auto fut = br.pausePf(a.pf);
        CHECK(fut.has_value() && fut->get().ok);
        CHECK(waitUntil([&] { return book.isPaused(a.pf); }));      // brought in line via the listener
        svc->stop();
    }
}

static void testGuard() {
    SECTION("guard");

    // ---- a turnover breach switches the strategy's sending OFF and latches the limit ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        Registry reg;
        Portfolios book("m", "s", 1001, &reg, 3, "rsi");
        BridgeConfig bc; bc.user = 3; bc.squareup_settle_s = 0;
        Bridge br(bc, "rsi");
        br.bind(svc.get(), &book);
        br.sendOn();
        Limits lim;
        Guard guard(lim, svc.get(), &reg, [&](const std::string& s) -> Bridge* { return s == "rsi" ? &br : nullptr; });

        lim.setLimit("rsi", "turnover", 1'000'000.0);
        CHECK(br.sending());
        guard.on_values("rsi", 2'000'000.0, std::nullopt);   // over the turnover limit
        CHECK(lim.get("rsi").turnover_hit && !br.sending()); // latched + sending switched off
        CHECK(!svc->events().empty());                       // a nats_events row was noted
        svc->stop();
    }

    // ---- an M2M breach squares up + unsubscribes the strategy's subscribed portfolios ----
    {
        NatsConfig cfg; cfg.send_on = true;
        auto [svc, wire] = makeSvc(cfg);
        Registry reg;
        Portfolios book("m", "s", 1001, &reg, 3, "rsi");
        BridgeConfig bc; bc.user = 3; bc.squareup_settle_s = 0;
        Bridge br(bc, "rsi");
        br.bind(svc.get(), &book);
        br.sendOn();
        Limits lim;
        Guard guard(lim, svc.get(), &reg, [&](const std::string& s) -> Bridge* { return s == "rsi" ? &br : nullptr; });

        // one subscribed portfolio owned by "rsi"
        reg.registerPf(3, 1001, "rsi", "", "rsi", "RELIANCE");
        book.track({500});
        book.pf_map[500] = 1001;
        book.subscribe(1001, 900);                            // the book knows the pf_token it trades
        CHECK(svc->subscribe(1001, 900, {}, 3).get().ok);     // the service confirms -> pf state subscribed
        const std::size_t before = wire->count();

        lim.setLimit("rsi", "m2m", -50'000.0);
        guard.on_values("rsi", std::nullopt, -60'000.0);      // below the M2M limit -> breach
        CHECK(lim.get("rsi").m2m_hit && !br.sending());
        CHECK(waitUntil([&] { return wire->count() >= before + 3; }));
        auto s = wire->seq(1001);
        svc->stop();
        CHECK(s.size() >= 4);                                 // subscribe + the three breach steps
        CHECK(s[s.size() - 3] == "pause_unpause:Pause" && s[s.size() - 2] == "squareup" && s.back() == "unsubscribe");
    }

    // ---- set_limit reports a bad value and a still-blocked strategy ----
    {
        Limits lim;
        Guard guard(lim, nullptr, nullptr, {});
        CHECK(guard.set_limit("x", "turnover", -5.0).find("0 up") != std::string::npos);   // bad value message
        lim.setLimit("x", "turnover", 1'000.0);
        lim.update("x", 2'000.0, std::nullopt);               // already past -> latched
        const std::string msg = guard.set_limit("x", "turnover", 1'500.0);   // still below 2000 seen -> still blocked
        CHECK(msg.find("still blocked") != std::string::npos);
        CHECK(guard.set_limit("x", "turnover", 3'000.0).empty());            // now above seen -> cleared, no message
    }
}

static void testPortfolioToken() {
    SECTION("algo_portfolios.token: stored, kept, migrated, set on a confirmed subscribe");
    // ---- StateDB: round trip, and a write without the token never erases a known one ----
    {
        auto db = std::make_shared<std::unique_ptr<SqlConn>>(makeSqliteConn(":memory:"));
        StateDB store([db]() { return std::move(*db); });
        store.savePortfolio({3, 1001, "bk", "NIFTY", "rsi", "2026-11-24", "", 48704});
        auto p = store.loadPortfolios();
        CHECK(p.size() == 1 && p[0].token && *p[0].token == 48704);
        store.savePortfolio({3, 1001, "bk", "NIFTY", "rsi", "2026-11-24", "renamed", std::nullopt});          // no token in this write
        p = store.loadPortfolios();
        CHECK(p[0].note == "renamed" && p[0].token && *p[0].token == 48704);
        store.savePortfolio({3, 1001, "bk", "NIFTY", "rsi", "2026-11-24", "renamed", 48705});   // a new token replaces it
        CHECK(*store.loadPortfolios()[0].token == 48705);
        store.savePortfolio({3, 1002, "bk", "BANKNIFTY", "rsi", "2026-11-24", "", std::nullopt});
        p = store.loadPortfolios();
        CHECK(p.size() == 2 && !p[1].token);                                                       // never subscribed: none
    }
    // ---- migration: a table made by the Python stack (no token column) gets it, and its rows survive ----
    {
        const std::string path = "build/old_registry.sqlite";
        std::remove(path.c_str());
        {
            auto c = makeSqliteConn(path);
            c->exec("CREATE TABLE algo_portfolios (user_id integer NOT NULL, pf bigint NOT NULL, owner text NOT NULL, name text, "
                    "algo text, created text, note text, updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (user_id, pf))", {});
            c->exec("INSERT INTO algo_portfolios (user_id, pf, owner, name, algo, created, note) VALUES (4, 2001, 'py_book', 'SBIN', 'basket', 'x', '')", {});
        }
        StateDB store([path]() { return makeSqliteConn(path); });
        auto p = store.loadPortfolios();
        CHECK(p.size() == 1 && p[0].pf == 2001 && p[0].owner == "py_book" && !p[0].token);
        store.savePortfolio({4, 2001, "py_book", "SBIN", "basket", "x", "", 7777});
        CHECK(*store.loadPortfolios()[0].token == 7777);
        StateDB again([path]() { return makeSqliteConn(path); });                                 // a second start: no double ALTER
        CHECK(*again.loadPortfolios()[0].token == 7777);
        std::remove(path.c_str());
    }
    // ---- migration: algo_pf_sub with the old option_token column is renamed to token, rows kept ----
    {
        const std::string path = "build/old_pf_sub.sqlite";
        std::remove(path.c_str());
        {
            auto c = makeSqliteConn(path);
            c->exec("CREATE TABLE algo_pf_sub (book text NOT NULL, pf bigint NOT NULL, option_token bigint NOT NULL, PRIMARY KEY (book, pf))", {});
            c->exec("INSERT INTO algo_pf_sub (book, pf, option_token) VALUES ('pf_map_rsi_book.json', 1001, 48704)", {});
        }
        StateDB store([path]() { return makeSqliteConn(path); });
        auto sub = store.loadInts(StateDB::BookTable::Sub, "pf_map_rsi_book.json");
        CHECK(sub && sub->size() == 1 && sub->at(1001) == 48704);
        store.saveInts(StateDB::BookTable::Sub, "pf_map_rsi_book.json", {{1001, 48704}, {1002, 48790}});
        StateDB again([path]() { return makeSqliteConn(path); });                                 // second start: nothing to rename
        CHECK(again.loadInts(StateDB::BookTable::Sub, "pf_map_rsi_book.json")->size() == 2);
        std::remove(path.c_str());
    }
    // ---- Registry + Book: a confirmed subscribe records the contract in the registry (and in Postgres) ----
    {
        auto holder = std::make_shared<std::unique_ptr<SqlConn>>(makeSqliteConn(":memory:"));
        StateDB db([holder]() { return std::move(*holder); });
        Registry reg(&db);
        Portfolios book("m", "s", 1001, &reg, 3, "nifty_book", &db);
        const int64_t fut = 26000, pf_token = 48704;
        const int64_t pf = book.create(fut, "NIFTY", "nifty_sma");
        CHECK(reg.get(3, pf) && !reg.get(3, pf)->token);                       // a number, but nothing subscribed yet
        int changes = 0;
        auto tok = reg.onChange([&](int64_t, int64_t, const RegRec&) { ++changes; });
        book.subscribe(pf, pf_token);
        CHECK(reg.get(3, pf)->token && *reg.get(3, pf)->token == pf_token && changes == 1);
        book.subscribe(pf, pf_token);                                                // same contract again: no write, no event
        CHECK(changes == 1);
        reg.removeListener(tok);
        bool found = false;
        for (auto& r : db.loadPortfolios()) if (r.user == 3 && r.pf == pf) found = r.token && *r.token == pf_token && r.algo == "nifty_sma";
        CHECK(found);                                                           // user, pf, strategy and token in one row
        CHECK(!reg.setToken(3, 999999, 1));                                     // a portfolio the registry does not hold
    }
}

// a small but real NSE contract file: the same table / columns / encodings as the daily <yyyymmdd>DB.db3
static void writeDb3(const std::string& path, bool with_banknifty) {
    std::remove(path.c_str());
    auto c = makeSqliteConn(path);
    c->exec("CREATE TABLE ResultSet (Token integer, Symbol text, ExpiryDate integer, InstType text, OptionType text, "
            "StrikePrice integer, LotSize integer, TickSize integer, Divisor integer, Exch text)", {});
    const int64_t exp = (daysFromCivil(2026, 10, 27) - daysFromCivil(1980, 1, 1)) * 86400;   // seconds since the NSE epoch
    auto row = [&](int64_t tok, const char* sym, int64_t e, const char* inst, const char* ot, int64_t strike, int64_t tick) {
        c->exec("INSERT INTO ResultSet VALUES (?, ?, ?, ?, ?, ?, 75, ?, 100, 'NSE')",
                {tok, std::string(sym), e, std::string(inst), std::string(ot), strike, tick});
    };
    row(35001, "NIFTY", exp, "FUTIDX", "XX", 0, 10);
    row(48704, "NIFTY", exp, "OPTIDX", "CE", 2500000, 5);
    row(48705, "NIFTY", exp, "OPTIDX", "PE", 2500000, 5);
    row(2885, "RELIANCE", 0, "EQ", "", 0, 5);
    if (with_banknifty) row(35009, "BANKNIFTY", exp, "FUTIDX", "XX", 0, 10);
}

struct FollowMaster final : Plugin {
    int& seen;
    explicit FollowMaster(int& s) : Plugin("follower"), seen(s) {}
    void start() override { follow("master", [this](const std::any&) { ++seen; }); }
};

static void testMasterService() {
    SECTION("master service: builds once, publishes the snapshot + the day's token ids, reload appends only");
    const std::string dir = "build/master_test";
    std::system(("mkdir -p " + dir).c_str());
    writeDb3(dir + "/20261124DB.db3", false);
    Market mk; std::vector<std::string> errs;
    CHECK(Market::load("config/markets/NSE.ini", mk, errs));
    mk.contracts_source = "file";                                   // no Postgres in the test
    ProcessConfig cfg; cfg.market_data_dir = dir;
    App app(mk, cfg);

    auto& ms = static_cast<MasterService&>(app.run(std::make_unique<MasterService>(MasterServiceOptions{"20261124", 8})));
    CHECK(ms.source().find("20261124DB.db3") != std::string::npos);
    auto master = std::any_cast<std::shared_ptr<const MasterData>>(app.table("master"));
    auto ids = std::any_cast<std::shared_ptr<TokenIds>>(app.table("token_ids"));
    CHECK(master && ids && ids->size() == 4 && ids->capacity() == 4 + 8 && ids->day() == 20261124);
    // the kind lives in the master, by token
    CHECK(master->byToken(35001)->kind == Kind::kFut && master->byToken(48704)->kind == Kind::kOpt &&
          master->byToken(48704)->opt_type == 'C' && master->byToken(2885)->kind == Kind::kEq);
    CHECK(master->byToken(48705)->display == "NIFTY 27OCT26 25000 PE");
    // one slot per token, every contract has one
    std::set<int32_t> slots;
    for (int64_t t : {35001, 48704, 48705, 2885}) slots.insert(ids->find(t));
    CHECK(slots.size() == 4 && *slots.begin() == 0 && *slots.rbegin() == 3);

    // the feed uses the SAME ids: a quote lands at the master token's slot; an unknown token is appended after
    QuoteTable quotes(ids->capacity());
    CandleBuilder candles(ids->capacity(), 0, 60);
    FeedPipeline pipe(*ids, quotes, candles, [](const ClosedBar&) {});
    std::vector<Tick> batch(2);
    batch[0].token = 2885; batch[0].bid = 245000; batch[0].ask = 245010;
    batch[1].token = 777777; batch[1].bid = 10; batch[1].ask = 11;          // not in the master
    pipe.onTicks(batch);
    Quote q;
    CHECK(quotes.read(ids->find(2885), q) && q.bid == 245000);
    const int32_t unknown = ids->find(777777);
    CHECK(unknown == 4 && ids->tokenOf(unknown) == 777777 && quotes.read(unknown, q) && q.ask == 11);

    // a reload the same day (a new contract was listed): existing slots unchanged, the new one appended, followers told
    int seen = 0;
    app.run(std::make_unique<FollowMaster>(seen));
    std::map<int64_t, int32_t> before;
    for (int64_t t : {35001, 48704, 48705, 2885, 777777}) before[t] = ids->find(t);
    writeDb3(dir + "/20261124DB.db3", true);
    auto& ms2 = static_cast<MasterService&>(app.run(std::make_unique<MasterService>(MasterServiceOptions{"20261124", 8})));
    auto ids2 = std::any_cast<std::shared_ptr<TokenIds>>(app.table("token_ids"));
    auto master2 = std::any_cast<std::shared_ptr<const MasterData>>(app.table("master"));
    CHECK(ids2.get() == ids.get() && ms2.added() == 1 && ms2.refused() == 0 && seen == 1);   // same map object, appended
    bool same = true;
    for (auto& [t, id] : before) same &= ids2->find(t) == id;
    CHECK(same && ids2->find(35009) == 5 && master2->byToken(35009) && master2->byToken(35009)->kind == Kind::kFut);
    CHECK(master->byToken(35009) == nullptr);                      // the old snapshot is unchanged (immutable)

    // the next trading day starts a fresh map
    writeDb3(dir + "/20261125DB.db3", true);
    app.run(std::make_unique<MasterService>(MasterServiceOptions{"20261125", 8}));
    auto ids3 = std::any_cast<std::shared_ptr<TokenIds>>(app.table("token_ids"));
    CHECK(ids3.get() != ids.get() && ids3->day() == 20261125 && ids3->size() == 5 && ids3->find(777777) == TokenIds::kNone);
    CHECK(app.at("master").describe().find("ids 5 of 13") != std::string::npos);
    app.stopAll();
}

// NIFTY: an expired / today's / next-month future, a 5-strike chain for today's expiry, one far-month strike and one
// expired option; RELIANCE cash + its future; the NIFTY 50 index. today = 2026-11-24.
static void writeChainDb3(const std::string& path) {
    std::remove(path.c_str());
    auto c = makeSqliteConn(path);
    c->exec("CREATE TABLE ResultSet (Token integer, Symbol text, ExpiryDate integer, InstType text, OptionType text, "
            "StrikePrice integer, LotSize integer, TickSize integer, Divisor integer, Exch text)", {});
    auto ex = [](int y, unsigned m, unsigned d) { return (daysFromCivil(y, m, d) - daysFromCivil(1980, 1, 1)) * 86400; };
    auto row = [&](int64_t tok, const char* sym, int64_t e, const char* inst, const char* ot, int64_t strike) {
        c->exec("INSERT INTO ResultSet VALUES (?, ?, ?, ?, ?, ?, 75, 5, 100, 'NSE')", {tok, std::string(sym), e, std::string(inst), std::string(ot), strike});
    };
    const int64_t oct = ex(2026, 10, 27), nov = ex(2026, 11, 24), dec = ex(2026, 12, 29);
    row(35000, "NIFTY", oct, "FUTIDX", "XX", 0);                                 // expired
    row(35001, "NIFTY", nov, "FUTIDX", "XX", 0);                                 // expires today: still unexpired
    row(35002, "NIFTY", dec, "FUTIDX", "XX", 0);
    for (int i = 0; i < 5; ++i) {                                                // 24900 .. 25100, step 50
        row(41001 + i, "NIFTY", nov, "OPTIDX", "CE", 2490000 + 5000 * i);
        row(42001 + i, "NIFTY", nov, "OPTIDX", "PE", 2490000 + 5000 * i);
    }
    row(43001, "NIFTY", dec, "OPTIDX", "CE", 2500000);
    row(43002, "NIFTY", dec, "OPTIDX", "PE", 2500000);
    row(40001, "NIFTY", oct, "OPTIDX", "CE", 2500000);                           // expired option
    row(2885, "RELIANCE", 0, "EQ", "", 0);
    row(36001, "RELIANCE", dec, "FUTSTK", "XX", 0);
    row(26000, "NIFTY", 0, "INDEX", "", 0);                                      // the NIFTY 50 index (spot)
}

static void testContractApi() {
    SECTION("strategy contract lookups through the SDK table (future / option / chain / ATM / OTM / ITM / steps)");
    writeChainDb3("build/master_test/20261124DB.db3");
    Market mk; std::vector<std::string> errs;
    CHECK(Market::load("config/markets/NSE.ini", mk, errs));
    mk.contracts_source = "file";
    ProcessConfig cfg; cfg.market_data_dir = "build/master_test";
    App app(mk, cfg);
    app.run(std::make_unique<MasterService>(MasterServiceOptions{"20261124", 16}));
    ContractApi api(std::any_cast<std::shared_ptr<const MasterData>>(app.table("master")), 20261124, mk.price_div);
    HostCtx ctx{&api};
    AlgoHostAPI t{};
    wireContracts(t);
    void* x = &ctx;

    // find: EXACTLY the contract named (never a guess): a token, or its symbol (any case / spacing)
    CHECK(t.find(x, "RELIANCE") == 2885 && t.find(x, "NIFTY") == 26000);       // the cash stock; the index named NIFTY
    CHECK(t.find(x, "26000") == 26000 && t.find(x, "99999") == 0 && t.find(x, "NOSUCH") == 0);
    CHECK(t.find(x, "  nifty   24nov26 25000  ce ") == 41003 && t.find(x, "NIFTY 29DEC26 FUT") == 35002);
    // findFut: the underlying's future by expiry index (expired ones skipped); findSpot: its cash stock / index
    CHECK(t.find_fut(x, "NIFTY", 0) == 35001 && t.find_fut(x, "nifty", 1) == 35002 && t.find_fut(x, "NIFTY", 2) == 0);
    CHECK(t.find_fut(x, "RELIANCE", 0) == 36001 && t.find_fut(x, "NOSUCH", 0) == 0);
    CHECK(t.find_spot(x, "RELIANCE") == 2885 && t.find_spot(x, "NIFTY") == 26000 && t.find_spot(x, "NOSUCH") == 0);
    int32_t e[8];
    CHECK(t.expiries(x, "NIFTY", AlgoKind::FUT, e, 8) == 2 && e[0] == 20261124 && e[1] == 20261229);
    CHECK(t.expiries(x, "NIFTY", AlgoKind::OPT, e, 8) == 2 && e[0] == 20261124 && e[1] == 20261229);   // Oct has passed
    // the chain, strikes in the option's own minor units
    int32_t tok[8]; int64_t k[8];
    CHECK(t.strikes(x, "NIFTY", 0, AlgoOptType::CE, tok, k, 8) == 5 && tok[0] == 41001 && k[0] == 2490000 && k[4] == 2510000);
    CHECK(t.strikes(x, "NIFTY", 1, AlgoOptType::PE, tok, k, 8) == 1 && tok[0] == 43002);
    CHECK(t.strikes(x, "NIFTY", 2, AlgoOptType::CE, tok, k, 8) == 0);
    // nearest to the future at 25020.00 (price in the future's minor units, paise); a tie takes the lower strike
    CHECK(t.nearest(x, "NIFTY", 2502000, AlgoOptType::CE, 0, 3, tok) == 3 && tok[0] == 41003 && tok[1] == 41004 && tok[2] == 41002);
    CHECK(t.nearest(x, "NIFTY", 2502500, AlgoOptType::CE, 0, 2, tok) == 2 && tok[0] == 41003 && tok[1] == 41004);
    // ATM / OTM / ITM: OTM is up for a CE, down for a PE
    CHECK(t.moneyness(x, "NIFTY", 2502000, AlgoOptType::CE, 0, 0) == 41003);    // ATM CE 25000
    CHECK(t.moneyness(x, "NIFTY", 2502000, AlgoOptType::CE, 0, 1) == 41004);    // 1 OTM CE 25050
    CHECK(t.moneyness(x, "NIFTY", 2502000, AlgoOptType::CE, 0, -1) == 41002);   // 1 ITM CE 24950
    CHECK(t.moneyness(x, "NIFTY", 2502000, AlgoOptType::PE, 0, 1) == 42002);    // 1 OTM PE 24950
    CHECK(t.moneyness(x, "NIFTY", 2502000, AlgoOptType::PE, 0, -2) == 42005);   // 2 ITM PE 25100
    CHECK(t.moneyness(x, "NIFTY", 2502000, AlgoOptType::CE, 0, 3) == 0);        // past the end of the chain
    CHECK(t.moneyness(x, "NIFTY", 2502500, AlgoOptType::PE, 0, 0) == 42003);    // tie at 25025: the lower strike is ATM
    // an option by exact strike, and stepping along the chain
    CHECK(t.find_opt(x, "NIFTY", 0, AlgoOptType::PE, 2505000) == 42004 && t.find_opt(x, "NIFTY", 0, AlgoOptType::PE, 2505001) == 0);
    CHECK(t.find_opt(x, "NIFTY", 1, AlgoOptType::CE, 2500000) == 43001);
    CHECK(t.step_strike(x, 41003, 1) == 41004 && t.step_strike(x, 41003, -1) == 41002 && t.step_strike(x, 41003, 2) == 41005);
    CHECK(t.step_strike(x, 41001, -1) == 0 && t.step_strike(x, 35001, 1) == 0);  // below the chain; not an option
    // everything about a token, with its kind
    AlgoContract c;
    CHECK(t.info(x, 41003, &c) && c.kind == AlgoKind::OPT && c.opt_type == AlgoOptType::CE && c.expiry == 20261124);
    CHECK(c.strike == 2500000 && c.tick == 5 && c.lot_size == 75 && c.divisor == 100 && c.alt_token == 42003);
    CHECK(c.future_token == 35001 && c.spot_token == 26000);
    CHECK(std::string(c.symbol) == "NIFTY 24NOV26 25000 CE" && std::string(c.underlying) == "NIFTY");
    CHECK(t.info(x, 43001, &c) && c.future_token == 35002);                     // a December option -> the December future
    CHECK(t.info(x, 35002, &c) && c.kind == AlgoKind::FUT && c.future_token == 35002 && c.opt_type == AlgoOptType::NONE);
    CHECK(t.info(x, 26000, &c) && c.kind == AlgoKind::INDEX);
    CHECK(t.info(x, 2885, &c) && c.kind == AlgoKind::EQ && c.future_token == 36001 && c.spot_token == 2885);
    CHECK(!t.info(x, 99999, &c));
    // a null context or a missing master never crashes a strategy
    HostCtx empty{};
    CHECK(t.find(&empty, "NIFTY") == 0 && t.find_fut(nullptr, "NIFTY", 0) == 0 && t.find_spot(&empty, "NIFTY") == 0);
    app.stopAll();
}

int main() {
    testIni();
    testMarkets();
    testTz();
    testLtt();
    testTokenIndex();
    testSpsc();
    testSeqlock();
    testMirror();
    testAppPlugin();
    testStateDb();
    testPfStates();
    testLimits();
    testRegistry();
    testBook();
    testNats();
    testBridge();
    testGuard();
    testPortfolioToken();
    testMasterService();
    testContractApi();
    std::printf("%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
