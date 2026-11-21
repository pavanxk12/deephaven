// The portfolio registry: who owns which portfolio number, per user, and what it is for. A port of
// algo/strategy/registry.py (the Registry class; the Deephaven PortfolioRegistry plugin that publishes the tables
// comes later with the API). A portfolio is (user, number): user 3's 1001 and user 4's 1001 are different, but one
// user never has two with the same number, whoever created them, so two algos can never be handed the same one.
//
// Numbers are never reused: a retired portfolio keeps its number (it may still hold a position). Each allocation is
// committed to StateDB (algo_portfolios) BEFORE the number is returned -- a failure is raised and the number is not
// kept -- so a crash or a second strategy still sees every number ever handed out. Control-plane only.
#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/statedb.h"            // Portfolio, StateDB

namespace algo {

inline constexpr int64_t kFirstPf = 1001;   // algo/core/config.py FIRST_PF; the real value flows from config at the call site

// The stored record of one portfolio (everything but its user+pf key).
struct RegRec {
    std::string owner;              // the book that numbers it; others are refused
    std::string name;              // what it trades, e.g. "RELIANCE" (may be empty)
    std::string algo;              // the strategy/screen using it (may be empty; updated on take-over)
    std::string created;           // when first registered (ISO-8601 local)
    std::string note;
};

// The answer to registerPf. status: registered (new), exists (already yours, unchanged), updated (already yours,
// a field changed) or taken (someone else's -- held_by names who).
struct Claim {
    bool        ok = false;
    std::string status;
    int64_t     user = 0;
    int64_t     pf = 0;
    std::string owner;
    std::string held_by;
    explicit operator bool() const { return ok; }
    std::string str() const;
};

class Registry {
public:
    using Listener = std::function<void(int64_t user, int64_t pf, const RegRec&)>;
    using Token = uint64_t;

    // `db` optional: with one, numbers load from and commit to algo_portfolios; without (nullptr) they live in
    // memory only (tests). Loading happens in the constructor.
    explicit Registry(StateDB* db = nullptr);

    // ---- getting numbers ----------------------------------------------------------------------------------------
    // The next free number for `user`, reserved for `owner` before it is returned. An owner with none yet gets
    // `start` (or the first free number above it); an owner with some gets the number after its highest; a number
    // held by anybody else is skipped. Throws std::invalid_argument on a bad user/owner/start.
    int64_t allocate(int64_t user, const std::string& owner, int64_t start = kFirstPf,
                     const std::string& note = "", const std::string& algo = "", const std::string& name = "");
    // The whole interface an algo needs: create(user, strategy, name) -> the number (strategy is the owner + algo).
    int64_t create(int64_t user, const std::string& strategy, const std::string& name,
                   int64_t start = kFirstPf, const std::string& note = "");
    // Claim a specific number, or say what the one you hold is for. Throws on a bad user/pf/owner.
    Claim   registerPf(int64_t user, int64_t pf, const std::string& owner,
                       const std::string& note = "", const std::string& algo = "", const std::string& name = "");

    // ---- looking ------------------------------------------------------------------------------------------------
    std::optional<std::string> ownerOf(int64_t user, int64_t pf) const;
    std::optional<RegRec>      get(int64_t user, int64_t pf) const;
    std::vector<Portfolio>     portfolios(std::optional<int64_t> user = std::nullopt) const;  // sorted
    std::pair<std::size_t, std::size_t> count() const;                                        // (users, portfolios)

    // ---- listening ----------------------------------------------------------------------------------------------
    Token onChange(Listener fn);
    void  removeListener(Token token);

private:
    RegRec      put(int64_t user, int64_t pf, RegRec rec);   // store + persist + notify; rolls back on a write failure
    std::string stamp() const;
    void        load();

    StateDB*                                        db_;
    std::map<int64_t, std::map<int64_t, RegRec>>    data_;    // user -> pf -> record
    std::vector<std::pair<Token, Listener>>         cbs_;
    mutable std::recursive_mutex                    lock_;     // Python uses an RLock (allocate -> put re-enters)
    Token                                           next_token_ = 1;
};

}  // namespace algo
