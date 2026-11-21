// Portfolio bookkeeping shared by every strategy and the console. A port of algo/strategy/book.py's Portfolios
// class (the Deephaven Book plugin comes later with the API). One portfolio (PF) = one option contract for its whole
// life; a stock's PF is created lazily on its first signal, numbered by the registry (per user) so no two strategies
// get the same number, and numbers are never reused.
//
// Two maps must survive a restart -- pf_map (future token -> PF) and subscribed (PF -> option token it trades) --
// held in StateDB (algo_pf_map / algo_pf_sub, keyed by a book name). Everything else (caps, paused, live legs, allow
// switches, cooldowns) is in-memory session state. Control-plane only; a recursive mutex guards compound changes
// (strategy threads, the bridge loop and the API all touch this), matching the Python RLock.
#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "strategy/registry.h"   // Registry, Claim

namespace algo {

class StateDB;

class Portfolios {
public:
    using SymbolOf = std::function<std::string(int64_t token)>;   // token -> its display symbol (first word = the stock)
    using Listener = std::function<void()>;
    using Token = uint64_t;

    // `db` optional (nullptr = in-memory only, tests). With a registry + user, new numbers come from the registry;
    // without, they run on from the highest known. `map_book`/`sub_book` are the StateDB keys for the two maps.
    Portfolios(std::string map_book, std::string sub_book, int64_t pf_start = kFirstPf,
               Registry* registry = nullptr, std::optional<int64_t> user = std::nullopt,
               std::string owner = "", StateDB* db = nullptr);

    // ---- persisted + session state (public, like the Python object; the bridge/API read these) ------------------
    std::map<int64_t, int64_t>                        pf_map;       // future token -> PF (persisted)
    std::map<int64_t, int64_t>                        subscribed;   // PF -> option token (persisted)
    std::map<int64_t, int64_t>                        caps;         // PF -> cumulative maxqty
    std::set<int64_t>                                 paused;       // PFs the bridge confirmed paused
    std::map<int64_t, bool>                           allow;        // future token -> automation may send
    std::map<int64_t, std::optional<std::string>>     live;         // future token -> leg last started (CALL/PUT)
    std::map<int64_t, int64_t>                        opt_of;       // future token -> option token it trades
    std::map<int64_t, double>                         last_fail;    // future token -> time of last failed swap
    std::set<int64_t>                                 tracked;      // future tokens some strategy follows this session
    std::set<int64_t>                                 session_pfs;  // PFs touched this session (current + retired)

    // ---- change notification ------------------------------------------------------------------------------------
    Token onChange(Listener callback);
    void  removeListener(Token token);
    void  changed();

    // ---- registration -------------------------------------------------------------------------------------------
    void track(const std::vector<int64_t>& tokens);                 // a strategy follows these futures
    void reset(const std::optional<std::vector<int64_t>>& tokens = std::nullopt);  // forget live legs/caps/cooldowns

    // ---- lookups ------------------------------------------------------------------------------------------------
    std::optional<int64_t>      pfOf(int64_t token) const;          // the PF of a tracked future, or nullopt
    std::map<int64_t, int64_t>  currentPfMap() const;               // tracked token -> PF (only those with a PF)
    std::vector<int64_t>        currentPfs() const;
    std::vector<int64_t>        allPfs() const;                     // every PF this session touched, retired included
    std::optional<int64_t>      tokenOfPf(int64_t pf) const;
    std::optional<int64_t>      ownerOf(int64_t token, const SymbolOf& symbol_of) const;
    std::optional<int64_t>      pfForRow(int64_t token, const SymbolOf& symbol_of) const;

    // ---- numbering ----------------------------------------------------------------------------------------------
    std::optional<int64_t>      lastPf() const;                     // highest PF in memory
    std::vector<Claim>          claimExisting();                    // register held numbers; returns the refused ones
    std::pair<std::optional<int64_t>, std::optional<int64_t>> plan(int64_t token, int64_t option) const;  // (current, retiring)
    struct Assign { int64_t pf; std::optional<int64_t> old_pf; bool created; };
    Assign  assign(int64_t token, int64_t option, const std::string& name = "", const std::string& algo = "");
    void    label(int64_t pf, const std::string& algo = "", const std::string& name = "");
    int64_t create(int64_t token, const std::string& name = "", const std::string& algo = "");
    void    revert(int64_t token, int64_t old_pf, int64_t new_pf);

    // ---- mutations ----------------------------------------------------------------------------------------------
    std::optional<int64_t> userId() const { return user_; }   // the user this book's numbers belong to (if any)
    bool isPaused(int64_t pf) const { std::lock_guard<std::recursive_mutex> g(lock_); return paused.count(pf) != 0; }

    void subscribe(int64_t pf, int64_t option_token);
    void setPaused(int64_t pf, bool paused_on);
    void setAllow(int64_t token, bool on);
    void setLive(int64_t token, std::optional<std::string> leg, std::optional<int64_t> option = std::nullopt);
    void saveMap();

private:
    enum class Which { Map, Sub };
    std::map<int64_t, int64_t> load(Which which);
    void                       store(Which which, const std::map<int64_t, int64_t>& data);

    std::string               map_book_, sub_book_;
    int64_t                   pf_start_;
    Registry*                 registry_;
    std::optional<int64_t>    user_;
    std::string               owner_;
    StateDB*                  db_;
    mutable std::recursive_mutex              lock_;
    std::vector<std::pair<Token, Listener>>   listeners_;
    Token                                     next_token_ = 1;
};

}  // namespace algo
