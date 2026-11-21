#include "core/process_config.h"

#include <cstdlib>

#include "core/ini.h"

namespace algo {

bool ProcessConfig::load(const std::string& path, ProcessConfig& out, std::vector<std::string>& errors) {
    out = ProcessConfig{};
    Ini ini;
    std::string err;
    if (!ini.loadFile(path, err)) { errors.push_back(err); return false; }
    auto& e = errors;
    const size_t before = e.size();

    ini.getString("process", "market", out.market, true, e);
    ini.getString("paths", "market_data", out.market_data_dir, true, e);
    ini.getString("paths", "cache", out.cache_dir, false, e);

    ini.getString("postgres", "host", out.pg.host, false, e);
    int64_t port = out.pg.port;
    ini.getInt("postgres", "port", port, false, e);
    out.pg.port = static_cast<int32_t>(port);
    ini.getString("postgres", "database", out.pg.database, false, e);
    ini.getString("postgres", "user", out.pg.user, false, e);
    ini.getString("postgres", "password_env", out.pg.password_env, false, e);

    for (auto& u : ini.unused()) e.push_back(u);
    return e.size() == before;
}

std::string pgConninfo(const PgSettings& pg) {
    std::string out = "host=" + pg.host + " port=" + std::to_string(pg.port) + " dbname=" + pg.database + " user=" + pg.user;
    if (!pg.password_env.empty()) {
        const char* v = std::getenv(pg.password_env.c_str());
        if (v && *v) out += " password=" + std::string(v);
    }
    return out;
}

}  // namespace algo
