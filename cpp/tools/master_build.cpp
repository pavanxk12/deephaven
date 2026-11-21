// master_build: the Postgres-first, .db3-fallback path (algo.data.master's Master plugin, source="auto"), driven
// entirely by algod.ini + markets/<CODE>.ini -- no host, path, table or password in this file.
//
//   master_build --config cpp/config/algod.ini --day 20261120
//
// Prints which source was used (and the fallback warning, if any) and the same counts as master_dump.
#include <cstdio>
#include <iostream>
#include <string>

#include "core/market.h"
#include "core/process_config.h"
#include "data/master.h"

using namespace algo;

int main(int argc, char** argv) {
    std::string config_path, day;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto val = [&]() -> std::string { if (i + 1 >= argc) { std::cerr << a << " needs a value\n"; std::exit(2); } return argv[++i]; };
        if (a == "--config") config_path = val();
        else if (a == "--day") day = val();
    }
    if (config_path.empty() || day.empty()) { std::cerr << "usage: master_build --config <algod.ini> --day <yyyymmdd>\n"; return 2; }

    ProcessConfig cfg;
    std::vector<std::string> errors;
    if (!ProcessConfig::load(config_path, cfg, errors)) {
        for (auto& e : errors) std::cerr << e << "\n";
        return 1;
    }
    const std::string market_ini = config_path.substr(0, config_path.find_last_of('/')) + "/markets/" + cfg.market + ".ini";
    Market mk;
    if (!Market::load(market_ini, mk, errors)) {
        for (auto& e : errors) std::cerr << e << "\n";
        return 1;
    }

    const std::string conninfo = pgConninfo(cfg.pg);
    std::string used_source, warning;
    try {
        const MasterData data = loadMaster(mk, day, cfg.market_data_dir, conninfo, used_source, warning);
        if (!warning.empty()) std::cerr << "warning: " << warning << "\n";
        std::cout << "source: " << used_source << "\n";
        std::cout << "chain=" << data.chain.size() << " opt=" << data.opt.size() << " futs=" << data.futs.size()
                  << " eqs=" << data.eqs.size() << " tokens=" << data.contracts.size() << "\n";
    } catch (const std::exception& e) {
        std::cerr << "failed: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
