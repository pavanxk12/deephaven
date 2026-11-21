#include "core/tz.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iterator>

namespace algo {

void civilFromDays(int64_t z, int& y, unsigned& m, unsigned& d) noexcept {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y = static_cast<int>(static_cast<int64_t>(yoe) + era * 400 + (m <= 2));
}

namespace {

int64_t be(const unsigned char* p, int n) {
    uint64_t v = 0;
    for (int i = 0; i < n; ++i) v = (v << 8) | p[i];
    if (n == 4) return static_cast<int32_t>(static_cast<uint32_t>(v));
    return static_cast<int64_t>(v);
}

// POSIX TZ pieces -----------------------------------------------------------------------------------
bool skipName(const std::string& s, size_t& i) {
    if (i < s.size() && s[i] == '<') { const size_t e = s.find('>', i); if (e == std::string::npos) return false; i = e + 1; return true; }
    const size_t st = i;
    while (i < s.size() && std::isalpha(static_cast<unsigned char>(s[i]))) ++i;
    return i - st >= 3;
}
bool readHms(const std::string& s, size_t& i, int32_t& out) {      // [+-]hh[:mm[:ss]]
    int sign = 1;
    if (i < s.size() && (s[i] == '+' || s[i] == '-')) { if (s[i] == '-') sign = -1; ++i; }
    int32_t parts[3] = {0, 0, 0};
    for (int k = 0; k < 3; ++k) {
        if (i >= s.size() || !std::isdigit(static_cast<unsigned char>(s[i]))) { if (k == 0) return false; break; }
        int32_t v = 0;
        while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) v = v * 10 + (s[i++] - '0');
        parts[k] = v;
        if (k < 2 && i < s.size() && s[i] == ':') ++i; else break;
    }
    out = sign * (parts[0] * 3600 + parts[1] * 60 + parts[2]);
    return true;
}
bool readMRule(const std::string& s, size_t& i, int& m, int& w, int& d, int32_t& t) {   // Mm.w.d[/time]
    if (i >= s.size() || s[i] != 'M') return false;
    ++i;
    auto num = [&](int& out) { if (i >= s.size() || !std::isdigit(static_cast<unsigned char>(s[i]))) return false; out = 0; while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) out = out * 10 + (s[i++] - '0'); return true; };
    if (!num(m) || i >= s.size() || s[i++] != '.' || !num(w) || i >= s.size() || s[i++] != '.' || !num(d)) return false;
    t = 7200;
    if (i < s.size() && s[i] == '/') { ++i; if (!readHms(s, i, t)) return false; }
    return m >= 1 && m <= 12 && w >= 1 && w <= 5 && d >= 0 && d <= 6;
}
// day of the month of the w-th weekday d (0 = Sunday) of month m in year y; w = 5 means the last one
int ruleDay(int y, int m, int w, int d) {
    const int64_t first = daysFromCivil(y, static_cast<unsigned>(m), 1);
    const int wd_first = static_cast<int>(((first % 7) + 11) % 7);          // 1970-01-01 was a Thursday (4)
    int day = 1 + ((d - wd_first + 7) % 7) + (w - 1) * 7;
    static const int mdays[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int dim = mdays[m - 1];
    if (m == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) dim = 29;
    while (day > dim) day -= 7;
    return day;
}

}  // namespace

bool TimeZone::parseRule(const std::string& s, Rule& r) {
    r = Rule{};
    if (s.empty()) return false;
    size_t i = 0;
    if (!skipName(s, i)) return false;
    int32_t off = 0;
    if (!readHms(s, i, off)) return false;
    r.std_off = -off;                                   // POSIX offsets are west of UTC
    r.valid = true;
    if (i >= s.size()) return true;
    if (!skipName(s, i)) return false;
    r.has_dst = true;
    r.dst_off = r.std_off + 3600;
    if (i < s.size() && s[i] != ',') { if (!readHms(s, i, off)) return false; r.dst_off = -off; }
    if (i >= s.size() || s[i++] != ',') return false;
    if (!readMRule(s, i, r.start_m, r.start_w, r.start_d, r.start_t)) return false;
    if (i >= s.size() || s[i++] != ',') return false;
    if (!readMRule(s, i, r.end_m, r.end_w, r.end_d, r.end_t)) return false;
    return true;
}

int32_t TimeZone::ruleOffset(int64_t t) const noexcept {
    if (!rule_.has_dst) return rule_.std_off;
    int y; unsigned m, d;
    civilFromDays((t + rule_.std_off) / 86400 - ((t + rule_.std_off) % 86400 < 0), y, m, d);
    auto at = [&](int mm, int w, int dd, int32_t tt, int32_t off_before) {
        return daysFromCivil(y, static_cast<unsigned>(mm), static_cast<unsigned>(ruleDay(y, mm, w, dd))) * 86400 + tt - off_before;
    };
    const int64_t start = at(rule_.start_m, rule_.start_w, rule_.start_d, rule_.start_t, rule_.std_off);
    const int64_t end = at(rule_.end_m, rule_.end_w, rule_.end_d, rule_.end_t, rule_.dst_off);
    const bool dst = start < end ? (t >= start && t < end) : (t >= start || t < end);   // southern hemisphere wraps
    return dst ? rule_.dst_off : rule_.std_off;
}

bool TimeZone::load(const std::string& name, std::string& error, const std::string& root) {
    *this = TimeZone{};
    name_ = name;
    if (name == "UTC" || name == "Etc/UTC") { rule_.valid = true; return true; }
    if (name.empty() || name.find("..") != std::string::npos) { error = "bad time zone name \"" + name + "\""; return false; }
    std::ifstream f(root + "/" + name, std::ios::binary);
    if (!f) { error = "time zone \"" + name + "\" not found in " + root + " (install tzdata?)"; return false; }
    const std::vector<unsigned char> b((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    auto bad = [&](const char* why) { error = "time zone file " + root + "/" + name + ": " + why; return false; };
    if (b.size() < 44 || b[0] != 'T' || b[1] != 'Z' || b[2] != 'i' || b[3] != 'f') return bad("not a TZif file");

    auto block = [&](size_t at, int tsize, size_t& next, bool keep) -> bool {
        if (at + 44 > b.size()) return false;
        const unsigned char* h = b.data() + at + 20;
        const int64_t isut = be(h, 4), isstd = be(h + 4, 4), leap = be(h + 8, 4), timecnt = be(h + 12, 4), typecnt = be(h + 16, 4), charcnt = be(h + 20, 4);
        if (typecnt <= 0 || timecnt < 0) return false;
        const size_t p = at + 44;
        const size_t need = static_cast<size_t>(timecnt * tsize + timecnt + typecnt * 6 + charcnt + leap * (tsize + 4) + isstd + isut);
        if (p + need > b.size()) return false;
        if (keep) {
            const unsigned char* tp = b.data() + p;
            const unsigned char* ip = tp + timecnt * tsize;
            const unsigned char* ty = ip + timecnt;
            std::vector<int32_t> offs(static_cast<size_t>(typecnt));
            for (int64_t k = 0; k < typecnt; ++k) offs[static_cast<size_t>(k)] = static_cast<int32_t>(be(ty + k * 6, 4));
            trans_.clear(); trans_off_.clear();
            for (int64_t k = 0; k < timecnt; ++k) {
                const unsigned idx = ip[k];
                if (idx >= offs.size()) return false;
                trans_.push_back(be(tp + k * tsize, tsize));
                trans_off_.push_back(offs[idx]);
            }
            first_off_ = offs[0];
        }
        next = p + need;
        return true;
    };
    size_t next = 0;
    const char version = static_cast<char>(b[4]);
    if (version == 0) {
        if (!block(0, 4, next, true)) return bad("truncated");
        return true;
    }
    if (!block(0, 4, next, false)) return bad("truncated");
    if (!block(next, 8, next, true)) return bad("truncated v2 data");
    if (next < b.size() && b[next] == '\n') {                                   // footer: \nPOSIX-TZ\n
        const size_t e = std::find(b.begin() + static_cast<long>(next) + 1, b.end(), '\n') - b.begin();
        const std::string footer(b.begin() + static_cast<long>(next) + 1, b.begin() + static_cast<long>(e));
        parseRule(footer, rule_);
    }
    return true;
}

TimeZone TimeZone::utc() {
    TimeZone z;
    z.name_ = "UTC";
    z.rule_.valid = true;
    return z;
}

int32_t TimeZone::offsetAt(int64_t t) const noexcept {
    if (trans_.empty()) return rule_.valid ? ruleOffset(t) : first_off_;
    if (t < trans_.front()) return first_off_;
    if (t >= trans_.back() && rule_.valid) return ruleOffset(t);
    const auto it = std::upper_bound(trans_.begin(), trans_.end(), t);
    return trans_off_[static_cast<size_t>(it - trans_.begin()) - 1];
}

int64_t TimeZone::localToUtc(int64_t local) const noexcept {
    // two refinements settle on the offset in force at the result (a local time that occurs twice: the earlier one)
    const int32_t o1 = offsetAt(local - offsetAt(local));
    int64_t t = local - o1;
    const int32_t o2 = offsetAt(t);
    if (o2 != o1) {
        const int64_t t2 = local - o2;
        if (offsetAt(t2) == o2) t = std::min(t, t2);
    }
    return t;
}

}  // namespace algo
