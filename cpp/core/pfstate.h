// What the commands have done to each portfolio, in ONE place algos and screens both read and listen to. A port of
// algo/core/pfstate.py. The NATS service writes here after every command the bridge answered "ok", whoever sent it,
// so an algo's pause/squareup/subscribe shows on the console and a console pause/untick tells the algo (on_change).
//
// A portfolio is (user, number); each holds a few small fields. The state is what was CONFIRMED this session; after
// a Deephaven restart it starts empty (the book re-adopts what it subscribed). Control-plane only: reads and writes
// may come from any housekeeping thread, so a mutex guards the map; listeners are called with the lock released.
#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace algo {

// One portfolio as the bridge last confirmed it.
struct PfState {
    std::optional<int64_t> pf_token;        // the token it is subscribed to (nullopt = not known)
    bool        subscribed = false;       // true after subscribe, false after unsubscribe
    bool        paused = false;           // the Pause / Resume button
    bool        squared = false;          // a square up went through and nothing has been applied since
    std::string by;                       // who did the last change: an algo's name, or "operator"
    std::string cmd;                       // the last command that changed it
    double      at = 0.0;                  // when (epoch seconds)
};

// The fields of a confirmed command that a pf-state update reads (the relevant JSON payload keys). `t1` is the churn
// target contract, `token` the single-order strategy's own contract field, `action` is "Pause"/"Resume".
struct PfPayload {
    std::optional<int64_t>     t1;
    std::optional<int64_t>     token;
    std::optional<std::string> action;
};

class PfStates {
public:
    using Key = std::pair<int64_t, int64_t>;                          // (user, pf)
    using Listener = std::function<void(int64_t user, int64_t pf, const PfState&)>;
    using Token = uint64_t;
    using Entry = std::tuple<int64_t, int64_t, PfState>;             // (user, pf, state)

    // ---- reading ------------------------------------------------------------------------------------------------
    std::optional<PfState> get(int64_t user, int64_t pf) const;      // a copy; nullopt if absent
    std::vector<Entry>     items(std::optional<int64_t> user = std::nullopt) const;  // one user or everybody
    std::size_t            size() const;

    // a traded token -> the portfolio that trades it. If several do, a subscribed one wins, then the latest change.
    // `keep(user, pf, state)` may leave some out first (e.g. another strategy's portfolios).
    std::unordered_map<int64_t, Entry> byToken(
        std::optional<int64_t> user = std::nullopt,
        const std::function<bool(int64_t, int64_t, const PfState&)>& keep = {}) const;

    // ---- changing -----------------------------------------------------------------------------------------------
    // Apply one command the bridge confirmed. Returns the new state, or nullopt for a command that changes nothing
    // shown here (an unrecognised cmd). Recognised: subscribe, unsubscribe, pause_unpause, squareup, apply.
    std::optional<PfState> record(int64_t user, int64_t pf, std::string_view cmd, const PfPayload& payload,
                                  std::string_view by);
    // A portfolio the book already had subscribed (from its file) when this session began. Never overwrites a seen one.
    void adopt(int64_t user, int64_t pf, int64_t pf_token, std::optional<bool> paused = std::nullopt);
    void forget(std::optional<int64_t> user = std::nullopt);

    // ---- listening ----------------------------------------------------------------------------------------------
    // fn(user, pf, state) after every change, from whatever thread made it (keep it short). Returns a token; pass it
    // to removeListener to stop.
    Token onChange(Listener fn);
    void  removeListener(Token token);

private:
    void tell(int64_t user, int64_t pf, const PfState& st);

    mutable std::mutex                       lock_;
    std::map<Key, PfState>                   d_;
    std::vector<std::pair<Token, Listener>>  cbs_;
    Token                                    next_token_ = 1;
};

}  // namespace algo
