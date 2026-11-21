// Strict INI reader: [section] / key = value / # or ; comments. Used at startup and on reload only, never on a hot path.
//
// Strict on purpose: a duplicate section or key, a line that is neither, or a key before any section is an error with
// file:line. Every key a loader reads is marked used; `unused()` lists the rest, so a typo in a key name stops start-up
// instead of being silently ignored.
#pragma once
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace algo {

class Ini {
public:
    // Parse a file / a text. On failure returns false and sets `error` ("file:line: reason").
    bool loadFile(const std::string& path, std::string& error);
    bool loadText(const std::string& text, const std::string& name, std::string& error);

    bool hasSection(const std::string& section) const { return sections_.count(section) != 0; }
    bool has(const std::string& section, const std::string& key) const;

    // Raw value (trimmed), or nullopt. Marks the key used.
    std::optional<std::string> get(const std::string& section, const std::string& key) const;
    // Every key of a section in file order, with its value. Marks them used.
    std::vector<std::pair<std::string, std::string>> section(const std::string& section) const;

    // Typed readers: return false and append "file:line: section.key: reason" to `errors` when the value is bad.
    // A missing optional key leaves `out` unchanged and returns true.
    bool getString(const std::string& s, const std::string& k, std::string& out, bool required, std::vector<std::string>& errors) const;
    bool getInt(const std::string& s, const std::string& k, int64_t& out, bool required, std::vector<std::string>& errors) const;
    bool getDouble(const std::string& s, const std::string& k, double& out, bool required, std::vector<std::string>& errors) const;
    bool getBool(const std::string& s, const std::string& k, bool& out, bool required, std::vector<std::string>& errors) const;
    // "HH:MM" or "HH:MM:SS" -> seconds of the day
    bool getClock(const std::string& s, const std::string& k, int32_t& out_seconds, bool required, std::vector<std::string>& errors) const;
    // "HH:MM-HH:MM" -> [start, end) seconds of the day, start < end
    bool getClockRange(const std::string& s, const std::string& k, int32_t& start, int32_t& end, bool required, std::vector<std::string>& errors) const;

    // Keys that were never read: "file:line: [section] key is not a known setting".
    std::vector<std::string> unused() const;

    // `section.key=value` overrides (command line). Replaces or adds; the line is reported as "<override>".
    void set(const std::string& section, const std::string& key, const std::string& value);

    const std::string& name() const { return name_; }

    static std::string trim(const std::string& s);
    static bool parseClock(const std::string& v, int32_t& seconds);   // "HH:MM[:SS]"

private:
    struct Value { std::string text; int line; };
    std::string where(const std::string& s, const std::string& k) const;

    std::string name_;
    std::map<std::string, std::map<std::string, Value>> data_;
    std::map<std::string, std::vector<std::string>> order_;
    std::set<std::string> sections_;
    mutable std::set<std::pair<std::string, std::string>> used_;
};

}  // namespace algo
