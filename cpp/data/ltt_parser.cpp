#include "data/ltt_parser.h"

namespace algo {

bool LttParser::init(const std::string& format, const TimeZone& zone, std::string& error) {
    zone_ = &zone;
    utc_ = zone.name() == "UTC" || zone.name() == "Etc/UTC";
    len_ = 0;
    y_ = mo_ = d_ = h_ = mi_ = s_ = -1;
    cache_local_ = INT64_MIN;
    for (size_t i = 0; i < format.size(); ++i) {
        if (len_ + 4 > sizeof literal_) { error = "ltt_format is too long"; return false; }
        if (format[i] != '%') { literal_[len_] = format[i]; is_field_[len_] = false; ++len_; continue; }
        if (i + 1 >= format.size()) { error = "ltt_format ends with a lone %"; return false; }
        const char f = format[++i];
        int8_t* slot = nullptr;
        size_t width = 2;
        switch (f) {
            case 'Y': slot = &y_; width = 4; break;
            case 'm': slot = &mo_; break;
            case 'd': slot = &d_; break;
            case 'H': slot = &h_; break;
            case 'M': slot = &mi_; break;
            case 'S': slot = &s_; break;
            default: error = std::string("ltt_format: %") + f + " is not supported (use %Y %m %d %H %M %S)"; return false;
        }
        if (*slot >= 0) { error = std::string("ltt_format: %") + f + " appears twice"; return false; }
        *slot = static_cast<int8_t>(len_);
        for (size_t k = 0; k < width; ++k) { is_field_[len_] = true; literal_[len_] = 0; ++len_; }
    }
    if (y_ < 0 || mo_ < 0 || d_ < 0 || h_ < 0 || mi_ < 0 || s_ < 0) { error = "ltt_format must contain %Y %m %d %H %M %S"; return false; }
    if (static_cast<size_t>(s_) + 2 != len_ || len_ < 3 || literal_[len_ - 3] != ':') {
        error = "ltt_format must end in :%S (the seconds are read from the last two characters, as the Python parser does)";
        return false;
    }
    return true;
}

namespace {
inline bool digit(char c) noexcept { return c >= '0' && c <= '9'; }
inline int d2(const char* p) noexcept { return (p[0] - '0') * 10 + (p[1] - '0'); }
inline bool leap(int y) noexcept { return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0; }
constexpr int kMonthDays[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
}  // namespace

int64_t LttParser::parse(std::string_view s) noexcept {
    size_t a = 0, b = s.size();
    while (a < b && (s[a] == ' ' || s[a] == '\t' || s[a] == '\n' || s[a] == '\r')) ++a;
    while (b > a && (s[b - 1] == ' ' || s[b - 1] == '\t' || s[b - 1] == '\n' || s[b - 1] == '\r')) --b;
    s = s.substr(a, b - a);
    const size_t dot = s.find('.');
    if (dot != std::string_view::npos && dot > 0) s = s.substr(0, dot);
    if (s.size() != len_) return kBad;
    const char* p = s.data();
    for (size_t i = 0; i < len_; ++i) {
        if (is_field_[i]) { if (!digit(p[i])) return kBad; }
        else if (p[i] != literal_[i]) return kBad;
    }
    const int y = d2(p + y_) * 100 + d2(p + y_ + 2), mo = d2(p + mo_), d = d2(p + d_), h = d2(p + h_), mi = d2(p + mi_);
    if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || y < 1) return kBad;
    const int dim = (mo == 2 && leap(y)) ? 29 : kMonthDays[mo - 1];
    if (d > dim) return kBad;
    const int sec = d2(p + s_);                                 // like the Python parser: 00..99, not range checked
    const int64_t local = daysFromCivil(y, static_cast<unsigned>(mo), static_cast<unsigned>(d)) * 86400 + h * 3600 + mi * 60;
    if (local != cache_local_) {
        cache_local_ = local;
        cache_utc_ = utc_ ? local : zone_->localToUtc(local);
    }
    return cache_utc_ + sec;
}

}  // namespace algo
