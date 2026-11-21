// candles_replay: feed batches from a CSV through TokenIndex + LttParser + CandleBuilder and write the closed bars.
// Used by the parity test against the Python Candles plugin, and to replay a recorded day.
//
//   candles_replay --format "%d-%m-%Y %H:%M:%S" --tz Asia/Kolkata --bucket 60 --start <epoch s> [--grace 10]
//                  [--master tokens.txt] [--overflow 4096] input.csv output.csv
//
// input.csv:  batch,part,token,ltp,vol,ltt,ltq     (part 0 = added, 1 = modified; ltt "<null>" = null; rows in order)
// output.csv: token,start,open,high,low,close,volume,cvd   (prices in raw minor units), one line per closed bar
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "core/tz.h"
#include "data/candle_builder.h"

using namespace algo;

namespace {
struct Row { long batch; int part; FeedRow r; };

bool split(const std::string& line, std::vector<std::string>& out) {
    out.clear();
    size_t a = 0;
    for (;;) {
        const size_t c = line.find(',', a);
        out.push_back(line.substr(a, c == std::string::npos ? std::string::npos : c - a));
        if (c == std::string::npos) break;
        a = c + 1;
    }
    return out.size() == 7;
}
}  // namespace

int main(int argc, char** argv) {
    std::string format = "%d-%m-%Y %H:%M:%S", tzname = "Asia/Kolkata", master_file, in_path, out_path;
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
        else if (in_path.empty()) in_path = a;
        else out_path = a;
    }
    if (in_path.empty() || out_path.empty()) { std::cerr << "usage: candles_replay [options] input.csv output.csv\n"; return 2; }

    std::string err;
    TimeZone zone;
    if (!zone.load(tzname, err)) { std::cerr << err << "\n"; return 1; }
    LttParser parser;
    if (!parser.init(format, zone, err)) { std::cerr << err << "\n"; return 1; }
    std::vector<int64_t> master;
    if (!master_file.empty()) { std::ifstream m(master_file); int64_t t; while (m >> t) master.push_back(t); }
    TokenIndex index;
    index.build(master, overflow);

    // read everything first (the ltt texts must outlive the rows)
    std::ifstream in(in_path);
    if (!in) { std::cerr << "cannot open " << in_path << "\n"; return 1; }
    std::vector<std::string> texts;
    std::vector<Row> rows;
    std::string line;
    std::vector<std::string> f;
    std::getline(in, line);                                   // header
    std::vector<std::pair<long, int>> ltt_ref;                 // row -> index into texts (or -1)
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        if (!split(line, f)) { std::cerr << "bad line: " << line << "\n"; return 1; }
        Row r{std::atol(f[0].c_str()), std::atoi(f[1].c_str()), {}};
        r.r.token = std::atoll(f[2].c_str());
        r.r.ltp = std::strtod(f[3].c_str(), nullptr);
        r.r.vol = std::strtod(f[4].c_str(), nullptr);
        r.r.ltq = std::strtod(f[6].c_str(), nullptr);
        if (f[5] == "<null>") ltt_ref.push_back({static_cast<long>(rows.size()), -1});
        else { ltt_ref.push_back({static_cast<long>(rows.size()), static_cast<int>(texts.size())}); texts.push_back(f[5]); }
        rows.push_back(r);
    }
    for (auto& [ri, ti] : ltt_ref) {
        if (ti < 0) { rows[static_cast<size_t>(ri)].r.ltt = nullptr; rows[static_cast<size_t>(ri)].r.ltt_len = 0; }
        else { rows[static_cast<size_t>(ri)].r.ltt = texts[static_cast<size_t>(ti)].data(); rows[static_cast<size_t>(ri)].r.ltt_len = static_cast<uint32_t>(texts[static_cast<size_t>(ti)].size()); }
    }

    CandleBuilder builder(index, parser, start, bucket, grace);
    std::vector<ClosedBar> bars;
    bars.reserve(1 << 16);
    auto sink = [&](const ClosedBar& b) { bars.push_back(b); };

    std::vector<FeedRow> added, modified;
    const auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < rows.size();) {
        const long b = rows[i].batch;
        added.clear(); modified.clear();
        for (; i < rows.size() && rows[i].batch == b; ++i) (rows[i].part == 0 ? added : modified).push_back(rows[i].r);
        builder.batch(added, modified, sink);
    }
    const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();

    FILE* out = std::fopen(out_path.c_str(), "w");
    if (!out) { std::cerr << "cannot write " << out_path << "\n"; return 1; }
    std::fprintf(out, "token,start,open,high,low,close,volume,cvd\n");
    for (const auto& x : bars)
        std::fprintf(out, "%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld\n", (long long)x.token, (long long)x.start, (long long)x.open,
                     (long long)x.high, (long long)x.low, (long long)x.close, (long long)x.volume, (long long)x.cvd);
    std::fclose(out);
    const auto& s = builder.stats();
    std::fprintf(stderr, "rows=%llu ticks=%llu dropped=%llu repeats=%llu bad_time=%llu overflow=%llu bars=%llu batches=%llu index=%s %.2f ms (%.0f ns/row)\n",
                 (unsigned long long)s.rows, (unsigned long long)s.ticks, (unsigned long long)s.dropped, (unsigned long long)s.repeats,
                 (unsigned long long)s.bad_time, (unsigned long long)s.overflow, (unsigned long long)s.bars, (unsigned long long)s.batches,
                 index.direct() ? "direct" : "hashed", ms, s.rows ? ms * 1e6 / s.rows : 0.0);
    return 0;
}
