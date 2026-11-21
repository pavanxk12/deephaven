#include "core/ini.h"

#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <sstream>

namespace algo {

std::string Ini::trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

bool Ini::loadFile(const std::string& path, std::string& error) {
    std::ifstream f(path, std::ios::binary);
    if (!f) { error = path + ": cannot open"; return false; }
    std::stringstream ss;
    ss << f.rdbuf();
    return loadText(ss.str(), path, error);
}

bool Ini::loadText(const std::string& text, const std::string& name, std::string& error) {
    name_ = name;
    data_.clear(); order_.clear(); sections_.clear(); used_.clear();
    std::string current;
    bool in_section = false;
    std::istringstream in(text);
    std::string raw;
    int line = 0;
    while (std::getline(in, raw)) {
        ++line;
        if (!raw.empty() && raw.back() == '\r') raw.pop_back();
        // an inline comment needs whitespace before it, so "a#b" stays a value
        for (size_t i = 0; i < raw.size(); ++i) {
            if ((raw[i] == '#' || raw[i] == ';') && (i == 0 || std::isspace(static_cast<unsigned char>(raw[i - 1])))) { raw.resize(i); break; }
        }
        const std::string s = trim(raw);
        if (s.empty()) continue;
        if (s.front() == '[') {
            if (s.back() != ']' || s.size() < 3) { error = name + ":" + std::to_string(line) + ": bad section header \"" + s + "\""; return false; }
            current = trim(s.substr(1, s.size() - 2));
            if (current.empty()) { error = name + ":" + std::to_string(line) + ": empty section name"; return false; }
            if (!sections_.insert(current).second) { error = name + ":" + std::to_string(line) + ": section [" + current + "] appears twice"; return false; }
            in_section = true;
            continue;
        }
        const size_t eq = s.find('=');
        if (eq == std::string::npos) { error = name + ":" + std::to_string(line) + ": expected key = value, got \"" + s + "\""; return false; }
        if (!in_section) { error = name + ":" + std::to_string(line) + ": key before any [section]"; return false; }
        const std::string key = trim(s.substr(0, eq)), value = trim(s.substr(eq + 1));
        if (key.empty()) { error = name + ":" + std::to_string(line) + ": empty key"; return false; }
        auto& sec = data_[current];
        if (sec.count(key)) { error = name + ":" + std::to_string(line) + ": [" + current + "] " + key + " is set twice (first on line " + std::to_string(sec[key].line) + ")"; return false; }
        sec[key] = Value{value, line};
        order_[current].push_back(key);
    }
    return true;
}

bool Ini::has(const std::string& section, const std::string& key) const {
    auto s = data_.find(section);
    return s != data_.end() && s->second.count(key) != 0;
}

std::optional<std::string> Ini::get(const std::string& section, const std::string& key) const {
    auto s = data_.find(section);
    if (s == data_.end()) return std::nullopt;
    auto k = s->second.find(key);
    if (k == s->second.end()) return std::nullopt;
    used_.insert({section, key});
    return k->second.text;
}

std::vector<std::pair<std::string, std::string>> Ini::section(const std::string& section) const {
    std::vector<std::pair<std::string, std::string>> out;
    auto o = order_.find(section);
    if (o == order_.end()) return out;
    for (const auto& k : o->second) {
        used_.insert({section, k});
        out.emplace_back(k, data_.at(section).at(k).text);
    }
    return out;
}

void Ini::set(const std::string& section, const std::string& key, const std::string& value) {
    auto& sec = data_[section];
    if (!sec.count(key)) order_[section].push_back(key);
    sec[key] = Value{value, 0};
    sections_.insert(section);
}

std::string Ini::where(const std::string& s, const std::string& k) const {
    int line = 0;
    auto sec = data_.find(s);
    if (sec != data_.end()) { auto v = sec->second.find(k); if (v != sec->second.end()) line = v->second.line; }
    return name_ + ":" + (line ? std::to_string(line) : std::string("<override>")) + ": [" + s + "] " + k;
}

bool Ini::getString(const std::string& s, const std::string& k, std::string& out, bool required, std::vector<std::string>& errors) const {
    auto v = get(s, k);
    if (!v) { if (required) errors.push_back(name_ + ": [" + s + "] " + k + " is required"); return !required; }
    out = *v;
    return true;
}

bool Ini::getInt(const std::string& s, const std::string& k, int64_t& out, bool required, std::vector<std::string>& errors) const {
    auto v = get(s, k);
    if (!v) { if (required) errors.push_back(name_ + ": [" + s + "] " + k + " is required"); return !required; }
    errno = 0;
    char* end = nullptr;
    const long long r = std::strtoll(v->c_str(), &end, 10);
    if (v->empty() || errno || *end) { errors.push_back(where(s, k) + ": \"" + *v + "\" is not a whole number"); return false; }
    out = r;
    return true;
}

bool Ini::getDouble(const std::string& s, const std::string& k, double& out, bool required, std::vector<std::string>& errors) const {
    auto v = get(s, k);
    if (!v) { if (required) errors.push_back(name_ + ": [" + s + "] " + k + " is required"); return !required; }
    errno = 0;
    char* end = nullptr;
    const double r = std::strtod(v->c_str(), &end);
    if (v->empty() || errno || *end || !std::isfinite(r)) { errors.push_back(where(s, k) + ": \"" + *v + "\" is not a number"); return false; }
    out = r;
    return true;
}

bool Ini::getBool(const std::string& s, const std::string& k, bool& out, bool required, std::vector<std::string>& errors) const {
    auto v = get(s, k);
    if (!v) { if (required) errors.push_back(name_ + ": [" + s + "] " + k + " is required"); return !required; }
    std::string t;
    for (char c : *v) t += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (t == "true" || t == "yes" || t == "on" || t == "1") { out = true; return true; }
    if (t == "false" || t == "no" || t == "off" || t == "0") { out = false; return true; }
    errors.push_back(where(s, k) + ": \"" + *v + "\" is not true/false");
    return false;
}

bool Ini::parseClock(const std::string& v, int32_t& seconds) {
    int h = 0, m = 0, sec = 0;
    const size_t n = v.size();
    auto two = [&](size_t at, int& out) {
        if (at + 2 > n || !std::isdigit(static_cast<unsigned char>(v[at])) || !std::isdigit(static_cast<unsigned char>(v[at + 1]))) return false;
        out = (v[at] - '0') * 10 + (v[at + 1] - '0');
        return true;
    };
    if (!(n == 5 || n == 8) || v[2] != ':' || !two(0, h) || !two(3, m)) return false;
    if (n == 8 && (v[5] != ':' || !two(6, sec))) return false;
    if (h > 23 || m > 59 || sec > 59) return false;
    seconds = h * 3600 + m * 60 + sec;
    return true;
}

bool Ini::getClock(const std::string& s, const std::string& k, int32_t& out, bool required, std::vector<std::string>& errors) const {
    auto v = get(s, k);
    if (!v) { if (required) errors.push_back(name_ + ": [" + s + "] " + k + " is required"); return !required; }
    if (!parseClock(*v, out)) { errors.push_back(where(s, k) + ": \"" + *v + "\" is not a time (HH:MM or HH:MM:SS)"); return false; }
    return true;
}

bool Ini::getClockRange(const std::string& s, const std::string& k, int32_t& a, int32_t& b, bool required, std::vector<std::string>& errors) const {
    auto v = get(s, k);
    if (!v) { if (required) errors.push_back(name_ + ": [" + s + "] " + k + " is required"); return !required; }
    const size_t dash = v->find('-');
    int32_t x = 0, y = 0;
    if (dash == std::string::npos || !parseClock(trim(v->substr(0, dash)), x) || !parseClock(trim(v->substr(dash + 1)), y)) {
        errors.push_back(where(s, k) + ": \"" + *v + "\" is not a time range (HH:MM-HH:MM)");
        return false;
    }
    if (x >= y) { errors.push_back(where(s, k) + ": the range must end after it starts"); return false; }
    a = x; b = y;
    return true;
}

std::vector<std::string> Ini::unused() const {
    std::vector<std::string> out;
    for (const auto& [sec, keys] : order_)
        for (const auto& k : keys)
            if (!used_.count({sec, k})) out.push_back(where(sec, k) + " is not a known setting");
    return out;
}

}  // namespace algo
