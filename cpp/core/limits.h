// Risk limits per strategy: a TURNOVER limit (in rupees, buys + sells) and an M2M limit (in rupees). A port of
// algo/core/limits.py -- the numbers only, no NATS, no feed: algo.strategy.guard feeds it the live values and acts
// on a breach.
//
//   turnover limit   breached when the strategy's turnover reaches it             -> sending switched OFF
//   M2M limit        breached when live gross M2M is AT OR BELOW the limit (the   -> portfolios paused/squared/
//                    sign counts: -5000 breaches at -5000 and lower)                 unsubscribed, sending OFF
//
// A breach is LATCHED: the strategy stays blocked until that limit is changed to a value it is no longer past. A
// limit of 0 or nullopt means no limit. Persisted through StateDB (algo_limits) when one is given, so a restart
// keeps the limits and the latches. Control-plane only.
#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace algo {

class StateDB;

struct LimitRec {
    std::optional<double> turnover;       // the limit, nullopt = none
    std::optional<double> m2m;
    bool                  turnover_hit = false;
    bool                  m2m_hit = false;
};

class Limits {
public:
    using Listener = std::function<void(const std::string& strategy)>;
    using Token = uint64_t;

    // `db` is optional: with one, limits load from and save to algo_limits; without (nullptr), they live in memory
    // only (tests). Loading from the db happens in the constructor.
    explicit Limits(StateDB* db = nullptr);

    // ---- reading ------------------------------------------------------------------------------------------------
    LimitRec                 get(const std::string& strategy) const;         // a copy
    std::vector<std::string> names() const;                                   // strategies with a record, sorted
    std::pair<std::optional<double>, std::optional<double>> seen(const std::string& strategy) const;  // (turnover, m2m)
    std::string              blocked(const std::string& strategy) const;      // why it may not send ("" = it may)

    // ---- changing -----------------------------------------------------------------------------------------------
    // The live values. nullopt for a field keeps the previously seen one. Returns the limits breached for the first
    // time by this update (to act on): a subset of {"turnover", "m2m"}.
    std::vector<std::string> update(const std::string& strategy, std::optional<double> turnover, std::optional<double> m2m);
    // Change one limit (nullopt or 0 = no limit). Already past the new limit: breached now (returned). No longer
    // past it: the latch is lifted. Throws std::invalid_argument on a bad kind or value.
    std::vector<std::string> setLimit(const std::string& strategy, std::string_view kind, std::optional<double> value);
    void                     forget(const std::string& strategy);

    // ---- listening ----------------------------------------------------------------------------------------------
    Token onChange(Listener fn);              // fn(strategy) after a limit or a breach changes (not on every value)
    void  removeListener(Token token);
    void  notify(const std::string& strategy);  // tell listeners again (the guard calls it after switching OFF)

private:
    LimitRec&                _rec(const std::string& strategy);               // caller holds lock_
    bool                     _past(const std::string& strategy, std::string_view kind) const;  // caller holds lock_
    std::vector<std::string> _check(const std::string& strategy);            // caller holds lock_
    void                     _after(const std::string& strategy, bool save, bool tell);
    void                     _save();
    void                     _load();

    StateDB*                                 db_;
    std::map<std::string, LimitRec>          d_;
    std::map<std::string, std::pair<std::optional<double>, std::optional<double>>> seen_;
    std::vector<std::pair<Token, Listener>>  cbs_;
    mutable std::mutex                       lock_;
    Token                                    next_token_ = 1;
};

}  // namespace algo
