// master_dump: load a day's contracts (a .db3 file, or Postgres with --pg-day/--pg-conninfo) through a market .ini,
// classify them and print a canonical dump for the parity check against algo.data.master (tests/parity/master_parity.py).
//
//   master_dump --market cpp/config/markets/NSE.ini --db3 <path>.db3 out.csv
//   master_dump --market cpp/config/markets/NSE.ini --pg-day 20261120 --pg-conninfo "host=... dbname=... user=... password=..." out.csv
//
// out.csv:
//   # chain,opt,futs,eqs,tokens
//   <counts>
//   token,display,kind,tick,divisor        (kind: EQ, FUT or OPT; tick/divisor "nan" when the symbol has none)
//   ... one row per contract, sorted by token
#include <algorithm>
#include <cstdio>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "core/market.h"
#include "data/master.h"

using namespace algo;

namespace {
const char* kindName(Kind k) { return k == Kind::kEq ? "EQ" : k == Kind::kFut ? "FUT" : k == Kind::kOpt ? "OPT" : "?"; }
}  // namespace

int main(int argc, char** argv) {
    std::string market_ini, db3_path, pg_day, pg_conninfo, out_path;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto val = [&]() -> std::string { if (i + 1 >= argc) { std::cerr << a << " needs a value\n"; std::exit(2); } return argv[++i]; };
        if (a == "--market") market_ini = val();
        else if (a == "--db3") db3_path = val();
        else if (a == "--pg-day") pg_day = val();
        else if (a == "--pg-conninfo") pg_conninfo = val();
        else out_path = a;
    }
    if (market_ini.empty() || out_path.empty() || (db3_path.empty() && pg_day.empty())) {
        std::cerr << "usage: master_dump --market <ini> (--db3 <path> | --pg-day <yyyymmdd> --pg-conninfo <conninfo>) out.csv\n";
        return 2;
    }

    Market mk;
    std::vector<std::string> errors;
    if (!Market::load(market_ini, mk, errors)) {
        for (auto& e : errors) std::cerr << e << "\n";
        return 1;
    }

    std::vector<RawRow> raw;
    try {
        raw = db3_path.empty() ? readContractsPg(pg_day, mk, pg_conninfo) : readContractsDb3(db3_path, mk);
    } catch (const std::exception& e) {
        std::cerr << "load failed: " << e.what() << "\n";
        return 1;
    }

    const MasterData data = MasterData::build(std::move(raw));

    std::vector<int64_t> toks;
    toks.reserve(data.contracts.size());
    for (auto& c : data.contracts) toks.push_back(c.token);
    std::sort(toks.begin(), toks.end());

    FILE* out = std::fopen(out_path.c_str(), "w");
    if (!out) { std::cerr << "cannot write " << out_path << "\n"; return 1; }
    std::fprintf(out, "# chain,opt,futs,eqs,tokens\n%zu,%zu,%zu,%zu,%zu\n", data.chain.size(), data.opt.size(), data.futs.size(),
                data.eqs.size(), data.contracts.size());
    std::fprintf(out, "token,display,kind,tick,divisor\n");
    for (int64_t t : toks) {
        const Contract* c = data.byToken(t);
        const auto tick = data.tickOf(t);
        const auto divisor = data.divisorOf(t);
        std::fprintf(out, "%lld,%s,%s,%.10g,%.10g\n", static_cast<long long>(t), c->display.c_str(), kindName(c->kind),
                    tick ? *tick : std::numeric_limits<double>::quiet_NaN(), divisor ? *divisor : std::numeric_limits<double>::quiet_NaN());
    }
    std::fclose(out);
    std::fprintf(stderr, "chain=%zu opt=%zu futs=%zu eqs=%zu tokens=%zu\n", data.chain.size(), data.opt.size(), data.futs.size(),
                data.eqs.size(), data.contracts.size());
    return 0;
}
