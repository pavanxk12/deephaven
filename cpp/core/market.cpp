#include "core/market.h"

#include <algorithm>
#include <fstream>
#include <sstream>

namespace algo {

namespace {

std::vector<std::string> splitList(const std::string& v) {
    std::vector<std::string> out;
    std::stringstream ss(v);
    std::string item;
    while (std::getline(ss, item, ',')) { item = Ini::trim(item); if (!item.empty()) out.push_back(item); }
    return out;
}

std::string dirOf(const std::string& path) {
    const size_t s = path.find_last_of('/');
    return s == std::string::npos ? std::string(".") : path.substr(0, s);
}

// 1970-01-01 was a Thursday; 0 = Monday
int weekdayOf(int32_t yyyymmdd) {
    const int64_t days = daysFromCivil(yyyymmdd / 10000, static_cast<unsigned>(yyyymmdd / 100 % 100), static_cast<unsigned>(yyyymmdd % 100));
    return static_cast<int>(((days % 7) + 10) % 7);
}

bool parseDate(const std::string& s, int32_t& out) {      // yyyy-mm-dd
    if (s.size() != 10 || s[4] != '-' || s[7] != '-') return false;
    for (size_t i : {0, 1, 2, 3, 5, 6, 8, 9}) if (s[i] < '0' || s[i] > '9') return false;
    const int y = std::stoi(s.substr(0, 4)), m = std::stoi(s.substr(5, 2)), d = std::stoi(s.substr(8, 2));
    if (m < 1 || m > 12 || d < 1 || d > 31) return false;
    out = y * 10000 + m * 100 + d;
    return true;
}

}  // namespace

bool Market::isTradingDay(int32_t yyyymmdd) const noexcept {
    if (weekend_mask & (1u << weekdayOf(yyyymmdd))) return false;
    return !std::binary_search(holidays.begin(), holidays.end(), yyyymmdd);
}

bool Market::load(const std::string& path, Market& m, std::vector<std::string>& errors, const std::string& zoneinfo_root) {
    m = Market{};
    Ini ini;
    std::string err;
    if (!ini.loadFile(path, err)) { errors.push_back(err); return false; }
    auto& e = errors;
    const size_t before = e.size();

    ini.getString("market", "code", m.code, true, e);
    ini.getString("market", "currency", m.currency, true, e);
    ini.getString("market", "tz", m.tz_name, true, e);
    ini.getString("market", "holidays", m.holidays_file, false, e);
    std::string weekend = "Sat,Sun";
    ini.getString("market", "weekend", weekend, false, e);
    static const char* kDays[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
    for (const auto& d : splitList(weekend)) {
        const auto* it = std::find_if(std::begin(kDays), std::end(kDays), [&](const char* k) { return d == k; });
        if (it == std::end(kDays)) e.push_back(path + ": [market] weekend: \"" + d + "\" is not Mon..Sun");
        else m.weekend_mask |= static_cast<uint8_t>(1u << (it - std::begin(kDays)));
    }

    ini.getString("feed", "ltt_tz", m.ltt_tz_name, true, e);
    ini.getString("feed", "ltt_format", m.ltt_format, true, e);
    ini.getString("feed", "dh_table", m.dh_table, true, e);
    ini.getString("feed", "kafka_topic", m.kafka_topic, false, e);
    ini.getDouble("feed", "price_div", m.price_div, true, e);
    int64_t grace = m.bar_grace_s;
    ini.getInt("feed", "bar_grace_s", grace, false, e);
    m.bar_grace_s = static_cast<int32_t>(grace);

    ini.getClockRange("session", "regular", m.session_start, m.session_end, true, e);
    ini.getClockRange("session", "trade_window", m.trade_start, m.trade_end, true, e);
    m.slots_per_day = (m.session_end - m.session_start) / 60;

    ini.getClock("schedule", "master_at", m.master_at, true, e);
    ini.getClock("schedule", "candles_at", m.candles_at, true, e);
    ini.getClock("schedule", "close_at", m.close_at, true, e);
    std::string sac;
    ini.getString("schedule", "straddle_at_close", sac, false, e);
    if (!sac.empty()) {
        int64_t n = 0;
        if (!ini.getInt("schedule", "straddle_at_close", n, false, e) || n < 0) e.push_back(path + ": [schedule] straddle_at_close must be empty or 0, 1, 2 ...");
        else m.straddle_at_close = static_cast<int32_t>(n);
    }
    int64_t retry = m.retry_s, retries = m.retries;
    ini.getInt("schedule", "retry_s", retry, false, e);
    ini.getInt("schedule", "retries", retries, false, e);
    m.retry_s = static_cast<int32_t>(retry); m.retries = static_cast<int32_t>(retries);

    ini.getString("contracts", "source", m.contracts_source, true, e);
    if (m.contracts_source != "file" && m.contracts_source != "postgres" && m.contracts_source != "auto")
        e.push_back(path + ": [contracts] source must be file, postgres or auto");
    ini.getString("contracts", "file_pattern", m.file_pattern, true, e);
    ini.getString("contracts", "pg_table", m.pg_table, false, e);
    ini.getString("contracts", "expiry_epoch", m.expiry_epoch, true, e);
    ini.getDouble("contracts", "strike_div", m.strike_div, true, e);
    ini.getDouble("contracts", "tick_div", m.tick_div, true, e);
    std::string cols;
    ini.getString("contracts", "columns", cols, true, e);
    m.columns = splitList(cols);

    for (const auto& [kind, v] : ini.section("expense")) {
        const auto parts = splitList(v);
        Expense x;
        char* end = nullptr;
        if (parts.size() != 2 || (x.buy_per_crore = std::strtod(parts[0].c_str(), &end), *end) || (x.sell_per_crore = std::strtod(parts[1].c_str(), &end), *end))
            e.push_back(path + ": [expense] " + kind + ": expected \"buy,sell\" in currency per crore");
        else m.expense[kind] = x;
    }
    ini.getDouble("display", "turnover_unit", m.turnover_unit, false, e);

    if (m.session_start && m.trade_start && (m.trade_start < m.session_start || m.trade_end > m.session_end))
        e.push_back(path + ": [session] trade_window must lie inside the regular session");
    if (m.bar_grace_s < 0 || m.bar_grace_s > 300) e.push_back(path + ": [feed] bar_grace_s must be 0..300");
    if (m.price_div <= 0) e.push_back(path + ": [feed] price_div must be above 0");

    if (!m.tz_name.empty() && !m.zone.load(m.tz_name, err, zoneinfo_root)) e.push_back(path + ": [market] tz: " + err);
    if (!m.ltt_tz_name.empty()) {
        if (m.ltt_tz_name == "UTC") m.ltt_zone = TimeZone::utc();
        else if (!m.ltt_zone.load(m.ltt_tz_name, err, zoneinfo_root)) e.push_back(path + ": [feed] ltt_tz: " + err);
    }

    if (!m.holidays_file.empty()) {
        const std::string hp = m.holidays_file.front() == '/' ? m.holidays_file : dirOf(path) + "/" + m.holidays_file;
        std::ifstream h(hp);
        if (!h) e.push_back(path + ": [market] holidays: cannot open " + hp);
        std::string line;
        int n = 0;
        while (std::getline(h, line)) {
            ++n;
            const size_t hash = line.find('#');
            line = Ini::trim(hash == std::string::npos ? line : line.substr(0, hash));
            if (line.empty()) continue;
            int32_t d = 0;
            if (!parseDate(line, d)) e.push_back(hp + ":" + std::to_string(n) + ": \"" + line + "\" is not yyyy-mm-dd");
            else m.holidays.push_back(d);
        }
        std::sort(m.holidays.begin(), m.holidays.end());
    }

    for (auto& u : ini.unused()) e.push_back(u);
    return e.size() == before;
}

}  // namespace algo
