#include "core/pfstate.h"

#include <chrono>

namespace algo {
namespace {

double nowEpoch() {
    return std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
}

// The contract a command names: T1 (churning), or TOKEN (the single-order strategy's own field), else what was known.
std::optional<int64_t> contractOf(const PfPayload& p, std::optional<int64_t> known) {
    if (p.t1) return p.t1;
    if (p.token) return p.token;
    return known;
}

}  // namespace

std::optional<PfState> PfStates::get(int64_t user, int64_t pf) const {
    std::lock_guard<std::mutex> g(lock_);
    auto it = d_.find({user, pf});
    if (it == d_.end()) return std::nullopt;
    return it->second;
}

std::vector<PfStates::Entry> PfStates::items(std::optional<int64_t> user) const {
    std::lock_guard<std::mutex> g(lock_);
    std::vector<Entry> out;
    for (const auto& [key, st] : d_)
        if (!user || key.first == *user) out.emplace_back(key.first, key.second, st);
    return out;
}

std::size_t PfStates::size() const {
    std::lock_guard<std::mutex> g(lock_);
    return d_.size();
}

std::unordered_map<int64_t, PfStates::Entry> PfStates::byOption(
    std::optional<int64_t> user, const std::function<bool(int64_t, int64_t, const PfState&)>& keep) const {
    std::unordered_map<int64_t, Entry> out;
    for (auto& e : items(user)) {
        const auto& [u, p, s] = e;
        if (!s.option || (keep && !keep(u, p, s))) continue;
        auto it = out.find(*s.option);
        // a subscribed portfolio wins, then the latest change: compare (subscribed, at) as a pair.
        if (it == out.end() ||
            std::make_pair(s.subscribed, s.at) > std::make_pair(std::get<2>(it->second).subscribed, std::get<2>(it->second).at))
            out[*s.option] = e;
    }
    return out;
}

std::optional<PfState> PfStates::record(int64_t user, int64_t pf, std::string_view cmd, const PfPayload& payload,
                                        std::string_view by) {
    PfState snapshot;
    {
        std::lock_guard<std::mutex> g(lock_);
        PfState& st = d_[{user, pf}];
        if (cmd == "subscribe") {
            st.option = contractOf(payload, st.option);
            st.subscribed = true;
            st.squared = false;
        } else if (cmd == "unsubscribe") {
            st.subscribed = false;
            st.option = contractOf(payload, st.option);
        } else if (cmd == "pause_unpause") {
            st.paused = payload.action && *payload.action == "Pause";
        } else if (cmd == "squareup") {
            st.squared = true;
        } else if (cmd == "apply") {
            st.squared = false;
        } else {
            // a command that changes nothing shown here: report nothing without stamping by/cmd/at. The entry the
            // default-construct above created is left in place, exactly as the Python version leaves it.
            return std::nullopt;
        }
        st.by = by.empty() ? std::string("operator") : std::string(by);
        st.cmd = std::string(cmd);
        st.at = nowEpoch();
        snapshot = st;
    }
    tell(user, pf, snapshot);
    return snapshot;
}

void PfStates::adopt(int64_t user, int64_t pf, int64_t option, std::optional<bool> paused) {
    PfState snapshot;
    {
        std::lock_guard<std::mutex> g(lock_);
        const Key key{user, pf};
        if (d_.count(key)) return;                 // never overwrite what was seen
        PfState& st = d_[key];
        st.option = option;
        st.subscribed = true;
        st.cmd = "adopted";
        st.at = nowEpoch();
        if (paused) st.paused = *paused;
        snapshot = st;
    }
    tell(user, pf, snapshot);
}

void PfStates::forget(std::optional<int64_t> user) {
    std::lock_guard<std::mutex> g(lock_);
    for (auto it = d_.begin(); it != d_.end();)
        it = (!user || it->first.first == *user) ? d_.erase(it) : std::next(it);
}

PfStates::Token PfStates::onChange(Listener fn) {
    std::lock_guard<std::mutex> g(lock_);
    const Token t = next_token_++;
    cbs_.emplace_back(t, std::move(fn));
    return t;
}

void PfStates::removeListener(Token token) {
    std::lock_guard<std::mutex> g(lock_);
    for (auto it = cbs_.begin(); it != cbs_.end(); ++it)
        if (it->first == token) { cbs_.erase(it); return; }
}

void PfStates::tell(int64_t user, int64_t pf, const PfState& st) {
    std::vector<std::pair<Token, Listener>> copy;
    { std::lock_guard<std::mutex> g(lock_); copy = cbs_; }      // copy under lock, call without it
    for (auto& [token, fn] : copy) {
        try {
            fn(user, pf, st);
        } catch (...) {
            // a pf-state listener failed: swallow it, as the Python side does (one bad listener never stops the rest)
        }
    }
}

}  // namespace algo
