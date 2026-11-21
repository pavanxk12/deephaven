// Unit tests for cpp/core and cpp/data pieces that need no exchange: INI, time zones, LTT parsing, token index,
// SPSC ring, seqlock, double-mapped broadcast ring. No framework: CHECK counts failures, main returns non-zero on any.
#include <any>
#include <atomic>
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
#include "core/market.h"
#include "core/pfstate.h"
#include "core/sql.h"
#include "core/statedb.h"
#include <fstream>
#include "core/mirror_ring.h"
#include "core/seqlock.h"
#include "core/spsc_ring.h"
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

struct alignas(64) Quote { int64_t a, b, c, d, e, f, g, h; };

static void testSeqlock() {
    SECTION("seqlock (1 writer, 3 readers, torn-read check)");
    static Seqlock<Quote> q;
    std::atomic<bool> stop{false}, torn{false};
    std::atomic<uint64_t> reads{0};
    q.write(Quote{0, 1, 0, 0, 0, 0, 0, 0});                              // a consistent value before any reader starts
    std::vector<std::thread> rs;
    for (int k = 0; k < 3; ++k) rs.emplace_back([&] {
        Quote x;
        while (!stop.load(std::memory_order_relaxed)) {
            q.read(x);
            if (x.b != x.a + 1 || x.h != x.a * 2) torn = true;          // every field derived from a: a mixed copy shows
            reads.fetch_add(1, std::memory_order_relaxed);
        }
    });
    for (int64_t i = 0; i < 3'000'000; ++i) q.write(Quote{i, i + 1, i, i, i, i, i, i * 2});
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
    store.savePortfolio({3, 1002, "nifty_rsi_book", "n2", "nifty_rsi", "2026-11-21", ""});
    store.savePortfolio({3, 1001, "nifty_rsi_book", "n1", "nifty_rsi", "2026-11-21", "first"});
    store.savePortfolio({1, 5000, "straddle_book", "s", "straddle", "2026-11-20", ""});
    store.savePortfolio({3, 1001, "nifty_rsi_book", "n1b", "nifty_rsi", "2026-11-21", "renamed"});  // upsert same key
    auto pfs = store.loadPortfolios();
    CHECK(pfs.size() == 3);                                                        // the upsert did not add a 4th
    CHECK(pfs[0].user == 1 && pfs[0].pf == 5000);                                  // ordered: user 1 first
    CHECK(pfs[1].user == 3 && pfs[1].pf == 1001 && pfs[1].name == "n1b" && pfs[1].note == "renamed");
    CHECK(pfs[2].user == 3 && pfs[2].pf == 1002);

    // ---- bulk import ----
    store.savePortfolios({{7, 2001, "b", "a", "x", "c", ""}, {7, 2002, "b", "a2", "x", "c", ""}});
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

    // subscribe stamps the option (from TOKEN), clears squared, marks subscribed
    PfPayload sub; sub.token = 900111;
    auto st = ps.record(3, 1001, "subscribe", sub, "nifty_rsi");
    CHECK(st && st->subscribed && st->option == 900111 && !st->squared && st->by == "nifty_rsi" && st->cmd == "subscribe");
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

    // unsubscribe keeps the option but clears the flag; T1 (churn) overrides the option
    PfPayload unsub; unsub.t1 = 900222;
    auto un = ps.record(3, 1001, "unsubscribe", unsub, "nifty_rsi");
    CHECK(un && !un->subscribed && un->option == 900222);

    // an unrecognised command changes nothing and notifies nobody
    const int before = hits;
    CHECK(!ps.record(3, 1001, "reconcile", {}, "x").has_value());
    CHECK(hits == before);

    // adopt only fills an unseen portfolio, never overwrites
    ps.adopt(3, 1001, 123456);                               // 1001 already seen -> ignored
    CHECK(ps.get(3, 1001)->option == 900222);
    ps.adopt(5, 7000, 555, true);                            // new -> created, paused
    auto a = ps.get(5, 7000);
    CHECK(a && a->subscribed && a->option == 555 && a->paused && a->cmd == "adopted");

    // items filters by user; size counts all
    CHECK(ps.items(3).size() == 1 && ps.items().size() == 2);

    // by_option: a subscribed portfolio wins over an unsubscribed one on the same option token
    ps.record(9, 8001, "subscribe", [] { PfPayload p; p.token = 777; return p; }(), "a");
    ps.record(9, 8002, "unsubscribe", [] { PfPayload p; p.token = 777; return p; }(), "a");  // same option, not subscribed
    auto bo = ps.byOption();
    CHECK(bo.count(777) && std::get<1>(bo[777]) == 8001);   // the subscribed one (pf 8001) wins

    // removing the listener stops notifications
    ps.removeListener(tok);
    const int frozen = hits;
    ps.record(3, 1001, "squareup", {}, "op");
    CHECK(hits == frozen);

    ps.forget(3);
    CHECK(!ps.get(3, 1001).has_value() && ps.get(5, 7000).has_value());
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
    std::printf("%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
