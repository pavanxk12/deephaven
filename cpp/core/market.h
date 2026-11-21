// A market (exchange) loaded from config/markets/<CODE>.ini. Everything that differs between exchanges lives here and
// nowhere in code. Loaded once at startup into plain values (seconds, slot counts, zones), so nothing downstream ever
// reads a string. A new exchange is a new .ini file.
#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "core/ini.h"
#include "core/tz.h"

namespace algo {

struct Expense { double buy_per_crore = 0, sell_per_crore = 0; };

struct Market {
    // [market]
    std::string code, currency, tz_name, holidays_file;
    uint8_t     weekend_mask = 0;            // bit d set = weekday d is closed (0 = Monday ... 6 = Sunday)
    std::vector<int32_t> holidays;           // yyyymmdd, sorted
    // [feed]
    std::string ltt_tz_name, ltt_format, dh_table, kafka_topic;
    double      price_div = 100;
    int32_t     bar_grace_s = 10;
    // [session], seconds of the exchange day
    int32_t     session_start = 0, session_end = 0, trade_start = 0, trade_end = 0;
    int32_t     slots_per_day = 0;           // one-minute slots in the regular session (NSE 375, US 390)
    // [schedule]
    int32_t     master_at = 0, candles_at = 0, close_at = 0, retry_s = 60, retries = 10;
    int32_t     straddle_at_close = -1;      // -1 = not started at the close
    // [contracts]
    std::string contracts_source, file_pattern, pg_table, expiry_epoch;
    double      strike_div = 100, tick_div = 100;
    std::vector<std::string> columns;
    // [expense]
    std::map<std::string, Expense> expense;  // "FUT" / "OPT" / "EQ"; empty = no expense
    // [display]
    double      turnover_unit = 1;

    TimeZone    zone, ltt_zone;

    bool isTradingDay(int32_t yyyymmdd) const noexcept;

    // Loads and validates. On failure returns false with every problem in `errors` ("file:line: [section] key: ...").
    static bool load(const std::string& path, Market& out, std::vector<std::string>& errors,
                     const std::string& zoneinfo_root = "/usr/share/zoneinfo");
};

}  // namespace algo
