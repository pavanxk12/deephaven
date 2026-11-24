// candles_replay: replay recorded feed rows through the real feed path
//     TextRowAdapter (the Kafka/DH row rules) -> FeedPipeline (TokenIndex, QuoteTable, CandleBuilder)
// and write the closed bars (and optionally the final quote table). Used by the parity test against the Python Candles
// plugin, and to replay a recorded day.
//
//   candles_replay --format "%d-%m-%Y %H:%M:%S" --tz Asia/Kolkata --bucket 60 --start <epoch s> [--grace 10]
//                  [--master tokens.txt] [--overflow 4096] [--quotes quotes.csv] input.csv output.csv
//
// input.csv:  batch,part,token,ltp,vol,ltt,ltq[,bid,ask,bid_qty,ask_qty]
//             (part 0 = added, 1 = modified: one batch is all its added rows, then its modified rows; ltt "<null>" = null)
// output.csv: token,start,open,high,low,close,volume,cvd            (prices in raw minor units)
// quotes.csv: token,ltp,bid,ask,bid_qty,ask_qty,ltq,cum_volume,time_ns   (the last known value of each part)
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "core/tz.h"
#include "data/feed_pipeline.h"

using namespace algo;

namespace {
struct Row { long batch; int part; TextRow r; long text; };

void split(const std::string& line, std::vector<std::string>& out) {
    out.clear();
    size_t a = 0;
    for (;;) {
        const size_t c = line.find(',', a);
        out.push_back(line.substr(a, c == std::string::npos ? std::string::npos : c - a));
        if (c == std::string::npos) break;
        a = c + 1;
    }
}
double num(const std::string& s) { return s.empty() ? NAN : std::strtod(s.c_str(), nullptr); }
}  // namespace

int main(int argc, char** argv) {
    std::string format = "%d-%m-%Y %H:%M:%S", tzname = "Asia/Kolkata", master_file, quotes_path, in_path, out_path;
    int64_t start = 0; int bucket = 60, grace = 10; uint32_t overflow = 4096;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto val = [&]() -> std::string { if (i + 1 >= argc) { std::cerr << a << " needs a value\n"; std::exit(2); } return argv[++i]; };
        if (a == "--format") format = val();
        else if (a == "--tz") tzname = val();
        else if (a == "--bucket") bucket = std::atoi(val().c_str());
        else if (a == "--start") start = std::atoll(val().c_str());
        else if (a == "--grace") grace = std::atoi(val().c_str());
        else if (a == "--master") master_file = val();
        else if (a == "--overflow") overflow = static_cast<uint32_t>(std::atoi(val().c_str()));
        else if (a == "--quotes") quotes_path = val();
        else if (in_path.empty()) in_path = a;
        else out_path = a;
    }
    if (in_path.empty() || out_path.empty()) { std::cerr << "usage: candles_replay [options] input.csv output.csv\n"; return 2; }

    std::string err;
    TimeZone zone;
    if (tzname == "UTC") zone = TimeZone::utc();
    else if (!zone.load(tzname, err)) { std::cerr << err << "\n"; return 1; }
    LttParser parser;
    if (!parser.init(format, zone, err)) { std::cerr << err << "\n"; return 1; }
    std::vector<int64_t> master;
    if (!master_file.empty()) { std::ifstream m(master_file); int64_t t; while (m >> t) master.push_back(t); }
    TokenIds index(0, master, static_cast<uint32_t>(master.size()) + overflow);   // the day's token -> slot map

    // read everything first: the ltt texts must outlive the rows that point at them
    std::ifstream in(in_path);
    if (!in) { std::cerr << "cannot open " << in_path << "\n"; return 1; }
    std::vector<std::string> texts;
    std::vector<Row> rows;
    std::string line;
    std::vector<std::string> f;
    std::getline(in, line);                                               // header
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        split(line, f);
        if (f.size() != 7 && f.size() != 11) { std::cerr << "bad line (7 or 11 fields): " << line << "\n"; return 1; }
        Row r{std::atol(f[0].c_str()), std::atoi(f[1].c_str()), {}, -1};
        r.r.token = std::atoll(f[2].c_str());
        r.r.ltp = num(f[3]); r.r.vol = num(f[4]); r.r.ltq = num(f[6]);
        r.r.bid = r.r.ask = r.r.bid_qty = r.r.ask_qty = NAN;
        if (f.size() == 11) { r.r.bid = num(f[7]); r.r.ask = num(f[8]); r.r.bid_qty = num(f[9]); r.r.ask_qty = num(f[10]); }
        if (f[5] != "<null>") { r.text = static_cast<long>(texts.size()); texts.push_back(f[5]); }
        rows.push_back(r);
    }
    for (auto& r : rows) {
        if (r.text < 0) { r.r.ltt = nullptr; r.r.ltt_len = 0; }
        else { r.r.ltt = texts[static_cast<size_t>(r.text)].data(); r.r.ltt_len = static_cast<uint32_t>(texts[static_cast<size_t>(r.text)].size()); }
    }

    QuoteTable quotes(index.capacity());
    CandleBuilder candles(index.capacity(), start, bucket, grace);
    std::vector<ClosedBar> bars;
    bars.reserve(1 << 16);
    FeedPipeline pipeline(index, quotes, candles, [&](const ClosedBar& b) { bars.push_back(b); });
    TextRowAdapter adapter(parser);

    std::vector<TextRow> batch;
    const auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < rows.size();) {
        const long b = rows[i].batch;
        batch.clear();
        size_t j = i;
        for (; j < rows.size() && rows[j].batch == b; ++j) if (rows[j].part == 0) batch.push_back(rows[j].r);   // added first
        for (size_t k = i; k < j; ++k) if (rows[k].part != 0) batch.push_back(rows[k].r);                     // then modified
        adapter.deliver(batch, pipeline);
        i = j;
    }
    const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();

    FILE* out = std::fopen(out_path.c_str(), "w");
    if (!out) { std::cerr << "cannot write " << out_path << "\n"; return 1; }
    std::fprintf(out, "token,start,open,high,low,close,volume,cvd\n");
    for (const auto& x : bars)
        std::fprintf(out, "%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld\n", (long long)x.token, (long long)x.start, (long long)x.open,
                     (long long)x.high, (long long)x.low, (long long)x.close, (long long)x.volume, (long long)x.cvd);
    std::fclose(out);

    if (!quotes_path.empty()) {
        FILE* q = std::fopen(quotes_path.c_str(), "w");
        if (!q) { std::cerr << "cannot write " << quotes_path << "\n"; return 1; }
        std::fprintf(q, "token,ltp,bid,ask,bid_qty,ask_qty,ltq,cum_volume,time_ns\n");
        for (uint32_t id = 0; id < index.size(); ++id) {
            Quote v;
            if (!quotes.read(static_cast<int32_t>(id), v)) continue;
            std::fprintf(q, "%lld,%lld,%lld,%lld,%d,%d,%d,%lld,%lld\n", (long long)index.tokenOf(static_cast<int32_t>(id)), (long long)v.ltp, (long long)v.bid,
                         (long long)v.ask, v.bid_qty, v.ask_qty, v.ltq, (long long)v.cum_volume, (long long)v.time_ns);
        }
        std::fclose(q);
    }

    const auto& a = adapter.stats();
    const auto& c = candles.stats();
    std::fprintf(stderr, "rows=%llu trades=%llu quote_only=%llu no_trade=%llu bad_time=%llu ticks=%llu repeats=%llu overflow=%llu bars=%llu batches=%llu index=%s %.2f ms (%.0f ns/row)\n",
                 (unsigned long long)a.rows, (unsigned long long)a.trades, (unsigned long long)a.quote_only, (unsigned long long)a.no_trade,
                 (unsigned long long)a.bad_time, (unsigned long long)c.ticks, (unsigned long long)c.repeats, (unsigned long long)pipeline.overflow(),
                 (unsigned long long)c.bars, (unsigned long long)c.batches, index.direct() ? "direct" : "hashed", ms, a.rows ? ms * 1e6 / a.rows : 0.0);
    return 0;
}
