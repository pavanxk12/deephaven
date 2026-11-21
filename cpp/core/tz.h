// Time zones from the system tz database (TZif files in /usr/share/zoneinfo), so an exchange's zone is just a name in its
// .ini ("Asia/Kolkata", "America/New_York", "UTC"). Loaded once at startup; lookups are a binary search over the
// transitions (or the POSIX rule in the file's footer for dates past the last transition). Off the hot path: callers cache
// the offset per day / hour.
#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace algo {

// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm), and back.
constexpr int64_t daysFromCivil(int64_t y, unsigned m, unsigned d) noexcept {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}
void civilFromDays(int64_t z, int& y, unsigned& m, unsigned& d) noexcept;

class TimeZone {
public:
    // name: a tz database name. root: the zoneinfo folder. False + error when the file is missing or malformed.
    bool load(const std::string& name, std::string& error, const std::string& root = "/usr/share/zoneinfo");
    static TimeZone utc();

    // offset (seconds east of UTC) in force at UTC instant t
    int32_t offsetAt(int64_t utc_seconds) const noexcept;
    // local wall-clock seconds -> UTC seconds (for a local time that occurs twice, the first; for one skipped, shifted forward)
    int64_t localToUtc(int64_t local_seconds) const noexcept;
    const std::string& name() const noexcept { return name_; }

private:
    struct Rule {                      // POSIX TZ footer, e.g. EST5EDT,M3.2.0,M11.1.0
        bool    valid = false, has_dst = false;
        int32_t std_off = 0, dst_off = 0;
        int     start_m = 0, start_w = 0, start_d = 0; int32_t start_t = 7200;
        int     end_m = 0, end_w = 0, end_d = 0;       int32_t end_t = 7200;
    };
    static bool parseRule(const std::string& s, Rule& r);
    int32_t ruleOffset(int64_t utc_seconds) const noexcept;

    std::string          name_;
    std::vector<int64_t> trans_;       // UTC transition instants, ascending
    std::vector<int32_t> trans_off_;   // offset after each transition
    int32_t              first_off_ = 0;
    Rule                 rule_;
};

}  // namespace algo
