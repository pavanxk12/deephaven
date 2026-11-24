// Tests for the feed layer: Tick, TextRowAdapter, QuoteTable (incl. concurrent readers), FeedPipeline.
// Needs no Postgres / SQLite, so it builds anywhere (make -C cpp test-feed).
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "core/tz.h"
#include "data/feed_pipeline.h"

using namespace algo;

static int g_fail = 0, g_pass = 0;
#define CHECK(c) do { if (c) ++g_pass; else { ++g_fail; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)
#define SECTION(name) std::printf("%s\n", name)

static TextRow row(int64_t tok, double ltp, double vol, const char* ltt, double ltq = 1, double bid = NAN, double ask = NAN,
                   double bq = NAN, double aq = NAN) {
    return TextRow{tok, ltp, vol, ltq, bid, ask, bq, aq, ltt, ltt ? static_cast<uint32_t>(std::strlen(ltt)) : 0u};
}

static void testTickLayout() {
    SECTION("tick layout");
    CHECK(sizeof(Tick) == 64 && alignof(Tick) == 64);
    Tick t;
    CHECK(!t.hasTrade() && !t.hasQuote() && t.idx == -1);
    t.bid = 5;
    CHECK(t.hasQuote() && t.hasBid() && !t.hasAsk() && !t.hasTrade());
}

static void testAdapter() {
    SECTION("text row adapter (the Kafka/DH row rules)");
    std::string err;
    TimeZone ist; ist.load("Asia/Kolkata", err);
    LttParser p; p.init("%d-%m-%Y %H:%M:%S", ist, err);
    TextRowAdapter a(p);
    Tick t;
    CHECK(a.convert(row(7, 24500, 100, "15-10-2026 09:30:05", 3, 24495, 24505, 10, 20), t));
    CHECK(t.token == 7 && t.ltp == 24500 && t.cum_volume == 100 && t.ltq == 3 && t.bid == 24495 && t.ask == 24505 && t.bid_qty == 10 && t.ask_qty == 20);
    CHECK(t.time_ns == (daysFromCivil(2026, 10, 15) * 86400 + 4 * 3600 + 5) * 1000000000LL);   // 09:30:05 IST = 04:00:05 UTC
    // no usable trade, but the quote still goes through
    CHECK(a.convert(row(7, NAN, 100, "15-10-2026 09:30:05", 1, 24496, 24504), t) && !t.hasTrade() && t.bid == 24496 && t.ask == 24504);
    CHECK(a.convert(row(7, 24500, 100, "garbage", 1, 24496), t) && !t.hasTrade() && t.bid == 24496);     // bad time: quote only
    CHECK(a.convert(row(7, 24500, -1, "15-10-2026 09:30:05", 1, 0, 24504), t) && !t.hasTrade() && !t.hasBid() && t.hasAsk());
    CHECK(a.convert(row(7, 24500, 100, nullptr, 1, 24496), t) && !t.hasTrade());                     // null ltt
    // neither a trade nor a quote: dropped
    CHECK(!a.convert(row(7, 0, 100, "15-10-2026 09:30:05"), t));
    CHECK(!a.convert(row(7, INFINITY, 100, "15-10-2026 09:30:05", 1, NAN, -3), t));
    CHECK(!a.convert(row(int64_t(1) << 40, 24500, 100, "15-10-2026 09:30:05"), t));               // token outside int32
    // ltq that is not usable becomes 0; vol truncates; the dedup key is the RAW text
    CHECK(a.convert(row(7, 24500, 100.9, "15-10-2026 09:30:05", NAN), t) && t.ltq == 0 && t.cum_volume == 100);
    Tick u;
    a.convert(row(7, 24500, 100, " 15-10-2026 09:30:05", 1), u);
    CHECK(u.time_ns == t.time_ns && u.dedup_key != t.dedup_key);   // same instant, different text: not a repeat (as Python)
    CHECK(a.stats().quote_only >= 4 && a.stats().bad_time >= 1 && a.stats().bad_token == 1);
}

static void testQuoteTable() {
    SECTION("quote table: last known value of each part");
    QuoteTable q(8);
    Quote v;
    CHECK(!q.read(3, v) && !q.read(-1, v) && !q.read(99, v));
    Tick t; t.idx = 3; t.token = 77;
    t.ltp = 100; t.cum_volume = 10; t.time_ns = 5; t.ltq = 2; t.bid = 99; t.bid_qty = 4; t.ask = 101; t.ask_qty = 6;
    q.update(t);
    CHECK(q.read(3, v) && v.ltp == 100 && v.bid == 99 && v.ask == 101 && v.cum_volume == 10 && v.bid_qty == 4 && v.ask_qty == 6);
    Tick quoteOnly; quoteOnly.idx = 3; quoteOnly.bid = 98; quoteOnly.bid_qty = 9;            // bid moved, no trade, no ask
    q.update(quoteOnly);
    CHECK(q.read(3, v) && v.ltp == 100 && v.cum_volume == 10 && v.bid == 98 && v.bid_qty == 9 && v.ask == 101);
    Tick tradeOnly; tradeOnly.idx = 3; tradeOnly.ltp = 102; tradeOnly.cum_volume = 12; tradeOnly.time_ns = 9;
    q.update(tradeOnly);
    CHECK(q.read(3, v) && v.ltp == 102 && v.cum_volume == 12 && v.bid == 98 && v.ask == 101);
    const uint64_t ver = q.version(3);
    Tick nothing; nothing.idx = 3;
    q.update(nothing);                                                                       // carries nothing: no write
    CHECK(q.version(3) == ver && ver % 2 == 0 && ver == 6);
}

static void testQuoteConcurrency() {
    SECTION("quote table: 1 writer, 3 readers, never a torn quote (3M updates)");
    static QuoteTable q(4);
    Tick seed; seed.idx = 1; seed.ltp = 1000; seed.bid = 999; seed.ask = 1001; seed.cum_volume = 1000; seed.bid_qty = 1000 % 997; seed.ask_qty = 1000 % 991;
    q.update(seed);
    std::atomic<bool> stop{false}, torn{false};
    std::atomic<uint64_t> reads{0};
    std::vector<std::thread> rs;
    for (int k = 0; k < 3; ++k) rs.emplace_back([&] {
        Quote v;
        while (!stop.load(std::memory_order_relaxed)) {
            if (!q.read(1, v)) continue;
            // every field is derived from ltp, so a copy mixing two updates shows
            if (v.bid != v.ltp - 1 || v.ask != v.ltp + 1 || v.cum_volume != v.ltp || v.bid_qty != v.ltp % 997 || v.ask_qty != v.ltp % 991) torn = true;
            reads.fetch_add(1, std::memory_order_relaxed);
        }
    });
    Tick t; t.idx = 1;
    for (int64_t i = 1001; i < 3'001'000; ++i) {
        t.ltp = i; t.bid = i - 1; t.ask = i + 1; t.cum_volume = i; t.bid_qty = static_cast<int32_t>(i % 997); t.ask_qty = static_cast<int32_t>(i % 991);
        q.update(t);
    }
    stop = true;
    for (auto& th : rs) th.join();
    CHECK(!torn.load());
    CHECK(reads.load() > 0);
}

static void testPipeline() {
    SECTION("pipeline: token resolved once, quotes for every usable tick, candles only from trades");
    std::string err;
    TimeZone ist; ist.load("Asia/Kolkata", err);
    LttParser p; p.init("%d-%m-%Y %H:%M:%S", ist, err);
    std::vector<int64_t> master{100, 200};
    TokenIds index(20261015, master, 6);                                       // 2 contracts + 4 spare ids
    QuoteTable quotes(index.capacity());
    const int64_t start = (daysFromCivil(2026, 10, 15) * 86400) + 3 * 3600 + 45 * 60;    // 09:15 IST
    CandleBuilder candles(index.capacity(), start, 60, 10);
    std::vector<ClosedBar> bars;
    FeedPipeline pipe(index, quotes, candles, [&](const ClosedBar& b) { bars.push_back(b); });
    TextRowAdapter a(p);

    std::vector<TextRow> b1{row(100, 2000, 10, "15-10-2026 09:15:01", 1, 1999, 2001, 5, 5),
                            row(100, NAN, NAN, nullptr, NAN, 1998, 2002, 7, 7),             // quote-only
                            row(100, 2005, 15, "15-10-2026 09:15:30", 2),
                            row(555, 300, 1, "15-10-2026 09:15:40", 1, 299, 301)};          // not in the master: overflow id
    a.deliver(b1, pipe);
    std::vector<TextRow> b2{row(100, 2003, 20, "15-10-2026 09:16:20", 1)};                  // moves the clock: 09:15 bars close
    a.deliver(b2, pipe);

    Quote v;
    CHECK(quotes.read(index.find(100), v) && v.ltp == 2003 && v.bid == 1998 && v.ask == 2002 && v.bid_qty == 7 && v.cum_volume == 20);
    CHECK(index.find(555) >= 2 && quotes.read(index.find(555), v) && v.ltp == 300 && v.bid == 299);
    CHECK(bars.size() == 2);
    const ClosedBar* b100 = nullptr;
    for (auto& b : bars) if (b.token == 100) b100 = &b;
    CHECK(b100 && b100->open == 2000 && b100->high == 2005 && b100->low == 2000 && b100->close == 2005 && b100->volume == 5 && b100->start == start);
    CHECK(candles.stats().trades == 4 && pipe.overflow() == 0 && pipe.batches() == 2);
}

static void testTokenIds() {
    SECTION("token ids: the day's token -> slot map (append-only, lock-free lookups)");
    std::vector<int64_t> initial{500, 100, 300, 100};                     // any order, duplicates ignored
    TokenIds ids(20261124, initial, 5);
    CHECK(ids.initialCount() == 3 && ids.size() == 3 && ids.capacity() == 5 && ids.day() == 20261124);
    CHECK(ids.find(100) == 0 && ids.find(300) == 1 && ids.find(500) == 2 && ids.find(42) == TokenIds::kNone);
    CHECK(ids.tokenOf(1) == 300 && ids.tokenOf(3) == TokenIds::kNoToken && ids.tokenOf(-1) == TokenIds::kNoToken);
    CHECK(ids.findOrAdd(300) == 1);                                       // known: same id, nothing added
    CHECK(ids.findOrAdd(9999999) == 3 && ids.findOrAdd(9999999) == 3 && ids.find(9999999) == 3 && ids.tokenOf(3) == 9999999);
    CHECK(ids.findOrAdd(42) == 4 && ids.size() == 5);
    CHECK(ids.findOrAdd(43) == TokenIds::kNone && ids.size() == 5);      // full: refused, nothing renumbered
    CHECK(ids.find(100) == 0 && ids.find(42) == 4);

    SECTION("token ids: readers on 3 threads while another thread keeps adding (no torn or wrong id)");
    std::vector<int64_t> day(1000);
    for (int i = 0; i < 1000; ++i) day[static_cast<size_t>(i)] = 1000 + 7 * i;
    static TokenIds big(20261124, day, 1000 + 50000);
    std::atomic<bool> stop{false}, wrong{false};
    std::atomic<uint64_t> looks{0};
    std::vector<std::thread> rs;
    for (int k = 0; k < 3; ++k) rs.emplace_back([&, k] {
        uint64_t n = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            const int64_t t = 1000 + 7 * static_cast<int64_t>((n * 31 + static_cast<uint64_t>(k)) % 1000);
            const int32_t id = big.find(t);
            if (id < 0 || big.tokenOf(id) != t) wrong = true;             // a day token is always found, at its own id
            const uint32_t sz = big.size();                               // an added id that is visible maps back to its token
            if (sz > 1000) { const int32_t last = static_cast<int32_t>(sz - 1); const int64_t lt = big.tokenOf(last); if (lt == TokenIds::kNoToken || big.find(lt) != last) wrong = true; }
            ++n;
        }
        looks.fetch_add(n);
    });
    for (int64_t i = 0; i < 50000; ++i) big.findOrAdd(50'000'000 + i);
    stop = true;
    for (auto& t : rs) t.join();
    CHECK(!wrong.load() && looks.load() > 0 && big.size() == 51000);
}

int main() {
    testTickLayout();
    testTokenIds();
    testAdapter();
    testQuoteTable();
    testQuoteConcurrency();
    testPipeline();
    std::printf("%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
