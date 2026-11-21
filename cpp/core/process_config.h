// algod.ini: the process-level settings that are the SAME for every market -- which market to run, where the
// container's read-only data volume is mounted, and how to reach Postgres. Nothing here is a market difference (that
// is markets/<CODE>.ini); nothing here is a secret (the Postgres password is a named environment variable, read only
// at connect time, never stored in the .ini or in code -- the same rule algo/core/pg.py follows).
#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace algo {

struct PgSettings {
    std::string host = "postgres";
    int32_t     port = 5432;
    std::string database = "algofusion";
    std::string user = "algofusion";
    std::string password_env = "ALGO_PG_PASSWORD";    // the env var holding the password; empty = connect without one
};

struct ProcessConfig {
    std::string market;             // which config/markets/<market>.ini to load
    std::string market_data_dir;    // contract files, history parquet (read-only mount)
    std::string cache_dir;          // rebuildable caches (empty = caching disabled)
    PgSettings  pg;

    static bool load(const std::string& path, ProcessConfig& out, std::vector<std::string>& errors);
};

// A libpq conninfo string from `pg` and the password found in its `password_env` (empty env value / unset -> no
// password field, same as Python's pg_connect when ALGO_PG_PASSWORD is unset).
std::string pgConninfo(const PgSettings& pg);

}  // namespace algo
