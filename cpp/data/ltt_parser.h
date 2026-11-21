// The feed's `ltt` text -> epoch seconds. Same rules as the Python LttParser: surrounding whitespace is ignored,
// anything from the first '.' is cut (fractional seconds), the text must end in ":SS", and the date/time before it must
// match the format exactly. The format comes from the market's .ini ([feed] ltt_format), in strptime spelling with
// fixed-width fields only: %Y %m %d %H %M %S plus literal characters, e.g. "%d-%m-%Y %H:%M:%S" (NSE) or
// "%Y-%m-%d %H:%M:%S" (US). The text is in the market's ltt_tz ("Asia/Kolkata", "UTC").
//
// Hot path: no strptime, no allocation. Digits are read at precomputed offsets; the UTC instant of a local minute is
// cached, so the zone lookup runs once per minute of feed time.
#pragma once
#include <cstdint>
#include <string>
#include <string_view>

#include "core/tz.h"

namespace algo {

class LttParser {
public:
    static constexpr int64_t kBad = INT64_MIN;

    bool init(const std::string& format, const TimeZone& zone, std::string& error);

    // epoch seconds, or kBad when the text cannot be read
    int64_t parse(std::string_view s) noexcept;

    size_t length() const noexcept { return len_; }

private:
    const TimeZone* zone_ = nullptr;
    bool     utc_ = false;
    size_t   len_ = 0;
    char     literal_[40] = {};      // the format with fields blanked out
    bool     is_field_[40] = {};
    int8_t   y_ = -1, mo_ = -1, d_ = -1, h_ = -1, mi_ = -1, s_ = -1;
    int64_t  cache_local_ = INT64_MIN, cache_utc_ = 0;
};

}  // namespace algo
