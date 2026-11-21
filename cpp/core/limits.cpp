#include "core/limits.h"

#include <algorithm>
#include <cmath>

#include "core/statedb.h"

namespace algo {
namespace {

constexpr std::string_view kTurnover = "turnover";
constexpr std::string_view kM2m = "m2m";

// A reported value is usable only if finite and not a Deephaven double null (~-1.8e308): a missing value must never
// breach a limit. NaN and huge magnitudes become nullopt, exactly as Python's _num.
std::optional<double> num(std::optional<double> x) {
    if (!x) return std::nullopt;
    const double v = *x;
    if (std::isnan(v) || std::fabs(v) >= 1.0e300) return std::nullopt;
    return v;
}

// Group an integer rupee amount with thousands commas and an optional leading '+', matching Python's ',.0f'/'+,.0f'.
std::string money(double v, bool plus) {
    long long n = std::llround(v);
    const bool neg = n < 0;
    unsigned long long a = neg ? static_cast<unsigned long long>(-(n + 1)) + 1ull : static_cast<unsigned long long>(n);
    std::string digits = std::to_string(a), grouped;
    int c = 0;
    for (auto it = digits.rbegin(); it != digits.rend(); ++it) {
        grouped.push_back(*it);
        if (++c % 3 == 0 && std::next(it) != digits.rend()) grouped.push_back(',');
    }
    std::reverse(grouped.begin(), grouped.end());
    return (neg ? "-" : (plus ? "+" : "")) + grouped;
}

}  // namespace

Limits::Limits(StateDB* db) : db_(db) { _load(); }

// ---- reading ----------------------------------------------------------------------------------------------------
LimitRec Limits::get(const std::string& strategy) const {
    std::lock_guard<std::mutex> g(lock_);
    auto it = d_.find(strategy);
    return it == d_.end() ? LimitRec{} : it->second;
}

std::vector<std::string> Limits::names() const {
    std::lock_guard<std::mutex> g(lock_);
    std::vector<std::string> out;
    out.reserve(d_.size());
    for (const auto& [name, _] : d_) out.push_back(name);
    return out;                                       // std::map keeps them sorted
}

std::pair<std::optional<double>, std::optional<double>> Limits::seen(const std::string& strategy) const {
    std::lock_guard<std::mutex> g(lock_);
    auto it = seen_.find(strategy);
    return it == seen_.end() ? std::make_pair(std::optional<double>{}, std::optional<double>{}) : it->second;
}

std::string Limits::blocked(const std::string& strategy) const {
    LimitRec r = get(strategy);
    auto [turnover, m2m] = seen(strategy);
    std::vector<std::string> why;
    if (r.turnover_hit)
        why.push_back("turnover limit " + money(*r.turnover, false) + " reached (" +
                      (turnover ? money(*turnover, false) : std::string("-")) + ")");
    if (r.m2m_hit)
        why.push_back("M2M limit " + money(*r.m2m, true) + " reached (" +
                      (m2m ? money(*m2m, true) : std::string("-")) + ")");
    if (why.empty()) return "";
    std::string out;
    for (size_t i = 0; i < why.size(); ++i) { if (i) out += "; "; out += why[i]; }
    return out + "; change the limit to send again";
}

// ---- changing ---------------------------------------------------------------------------------------------------
std::vector<std::string> Limits::update(const std::string& strategy, std::optional<double> turnover, std::optional<double> m2m) {
    const std::optional<double> t = num(turnover), m = num(m2m);
    std::vector<std::string> newly;
    {
        std::lock_guard<std::mutex> g(lock_);
        auto& s = seen_[strategy];
        s.first = t ? t : s.first;                    // keep the previously seen one where this report is missing
        s.second = m ? m : s.second;
        newly = _check(strategy);
    }
    _after(strategy, /*save=*/!newly.empty(), /*tell=*/false);
    return newly;
}

std::vector<std::string> Limits::setLimit(const std::string& strategy, std::string_view kind, std::optional<double> value) {
    if (kind != kTurnover && kind != kM2m)
        throw std::invalid_argument("limit must be one of (turnover, m2m)");
    const std::optional<double> v = num(value);
    if (value && (!v || (*v < 0 && kind == kTurnover)))
        throw std::invalid_argument(kind == kTurnover
            ? "the turnover limit must be a number from 0 up (0 = no limit)"
            : "the m2m limit must be a number (empty or 0 = no limit)");
    std::vector<std::string> newly;
    {
        std::lock_guard<std::mutex> g(lock_);
        LimitRec& r = _rec(strategy);
        const bool has = v && *v != 0.0;              // 0 (or nullopt) means no limit
        if (kind == kTurnover) r.turnover = has ? v : std::nullopt;
        else                   r.m2m = has ? v : std::nullopt;
        if (!_past(strategy, kind)) { (kind == kTurnover ? r.turnover_hit : r.m2m_hit) = false; }  // lift the latch
        newly = _check(strategy);
    }
    _after(strategy, /*save=*/true, /*tell=*/false);
    return newly;
}

void Limits::forget(const std::string& strategy) {
    {
        std::lock_guard<std::mutex> g(lock_);
        d_.erase(strategy);
        seen_.erase(strategy);
    }
    _after(strategy, /*save=*/true, /*tell=*/false);
}

// ---- listening --------------------------------------------------------------------------------------------------
Limits::Token Limits::onChange(Listener fn) {
    std::lock_guard<std::mutex> g(lock_);
    const Token t = next_token_++;
    cbs_.emplace_back(t, std::move(fn));
    return t;
}

void Limits::removeListener(Token token) {
    std::lock_guard<std::mutex> g(lock_);
    for (auto it = cbs_.begin(); it != cbs_.end(); ++it)
        if (it->first == token) { cbs_.erase(it); return; }
}

void Limits::notify(const std::string& strategy) { _after(strategy, /*save=*/false, /*tell=*/true); }

// ---- inside -----------------------------------------------------------------------------------------------------
LimitRec& Limits::_rec(const std::string& strategy) { return d_[strategy]; }

bool Limits::_past(const std::string& strategy, std::string_view kind) const {
    auto rit = d_.find(strategy);
    if (rit == d_.end()) return false;
    const std::optional<double> limit = (kind == kTurnover) ? rit->second.turnover : rit->second.m2m;
    if (!limit || *limit == 0.0) return false;
    auto sit = seen_.find(strategy);
    const std::pair<std::optional<double>, std::optional<double>> s =
        sit == seen_.end() ? std::make_pair(std::optional<double>{}, std::optional<double>{}) : sit->second;
    if (kind == kTurnover) return s.first && *s.first >= *limit;
    return s.second && *s.second <= *limit;
}

std::vector<std::string> Limits::_check(const std::string& strategy) {
    LimitRec& r = _rec(strategy);
    std::vector<std::string> newly;
    if (!r.turnover_hit && _past(strategy, kTurnover)) { r.turnover_hit = true; newly.emplace_back(kTurnover); }
    if (!r.m2m_hit && _past(strategy, kM2m)) { r.m2m_hit = true; newly.emplace_back(kM2m); }
    return newly;
}

void Limits::_after(const std::string& strategy, bool save, bool tell) {
    if (save) _save();
    if (!save && !tell) return;
    std::vector<std::pair<Token, Listener>> copy;
    { std::lock_guard<std::mutex> g(lock_); copy = cbs_; }
    for (auto& [token, fn] : copy) {
        try { fn(strategy); } catch (...) { /* one bad listener never stops the rest */ }
    }
}

void Limits::_save() {
    if (!db_) return;
    std::vector<Limit> rows;
    {
        std::lock_guard<std::mutex> g(lock_);
        rows.reserve(d_.size());
        for (const auto& [name, r] : d_) rows.push_back({name, r.turnover, r.m2m, r.turnover_hit, r.m2m_hit});
    }
    try { db_->saveLimits(rows); } catch (...) { /* Postgres write failed: keep running on the in-memory copy */ }
}

void Limits::_load() {
    if (!db_) return;
    std::vector<Limit> rows = db_->loadLimits();
    std::lock_guard<std::mutex> g(lock_);
    for (const auto& r : rows)
        d_[r.strategy] = {num(r.turnover), num(r.m2m), r.turnover_hit, r.m2m_hit};
}

}  // namespace algo
