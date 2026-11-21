#include "strategy/book.h"

#include <algorithm>

#include "core/statedb.h"

namespace algo {
namespace {

std::string firstWord(const std::string& s) {
    size_t a = s.find_first_not_of(" \t");
    if (a == std::string::npos) return "";
    size_t b = s.find_first_of(" \t", a);
    return s.substr(a, b == std::string::npos ? std::string::npos : b - a);
}

StateDB::BookTable tableOf(bool is_map) { return is_map ? StateDB::BookTable::Map : StateDB::BookTable::Sub; }

}  // namespace

Portfolios::Portfolios(std::string map_book, std::string sub_book, int64_t pf_start, Registry* registry,
                       std::optional<int64_t> user, std::string owner, StateDB* db)
    : map_book_(std::move(map_book)), sub_book_(std::move(sub_book)), pf_start_(pf_start),
      registry_(registry), user_(user), owner_(std::move(owner)), db_(db) {
    pf_map = load(Which::Map);
    subscribed = load(Which::Sub);
}

// ---- change notification ----------------------------------------------------------------------------------------
Portfolios::Token Portfolios::onChange(Listener callback) {
    std::lock_guard<std::recursive_mutex> g(lock_);
    const Token t = next_token_++;
    listeners_.emplace_back(t, std::move(callback));
    return t;
}

void Portfolios::removeListener(Token token) {
    std::lock_guard<std::recursive_mutex> g(lock_);
    for (auto it = listeners_.begin(); it != listeners_.end(); ++it)
        if (it->first == token) { listeners_.erase(it); return; }
}

void Portfolios::changed() {
    std::vector<std::pair<Token, Listener>> copy;
    { std::lock_guard<std::recursive_mutex> g(lock_); copy = listeners_; }
    for (auto& [token, cb] : copy) {
        try { cb(); } catch (...) { /* one bad listener never stops the rest */ }
    }
}

// ---- registration -----------------------------------------------------------------------------------------------
void Portfolios::track(const std::vector<int64_t>& tokens) {
    {
        std::lock_guard<std::recursive_mutex> g(lock_);
        for (int64_t t : tokens) {
            tracked.insert(t);
            allow.try_emplace(t, true);
            live.try_emplace(t, std::nullopt);
            auto it = pf_map.find(t);
            if (it != pf_map.end()) { session_pfs.insert(it->second); caps.try_emplace(it->second, 0); }
        }
    }
    changed();
}

void Portfolios::reset(const std::optional<std::vector<int64_t>>& tokens) {
    {
        std::lock_guard<std::recursive_mutex> g(lock_);
        std::vector<int64_t> list = tokens ? *tokens : std::vector<int64_t>(tracked.begin(), tracked.end());
        for (int64_t t : list) {
            live[t] = std::nullopt;
            opt_of.erase(t);
            last_fail.erase(t);
            auto it = pf_map.find(t);
            if (it != pf_map.end()) caps[it->second] = 0;
        }
    }
    changed();
}

// ---- lookups ----------------------------------------------------------------------------------------------------
std::optional<int64_t> Portfolios::pfOf(int64_t token) const {
    std::lock_guard<std::recursive_mutex> g(lock_);
    if (!tracked.count(token)) return std::nullopt;
    auto it = pf_map.find(token);
    return it == pf_map.end() ? std::nullopt : std::optional<int64_t>(it->second);
}

std::map<int64_t, int64_t> Portfolios::currentPfMap() const {
    std::lock_guard<std::recursive_mutex> g(lock_);
    std::map<int64_t, int64_t> out;
    for (int64_t t : tracked) {
        auto it = pf_map.find(t);
        if (it != pf_map.end()) out[t] = it->second;
    }
    return out;
}

std::vector<int64_t> Portfolios::currentPfs() const {
    std::vector<int64_t> out;
    for (const auto& [t, p] : currentPfMap()) out.push_back(p);
    return out;
}

std::vector<int64_t> Portfolios::allPfs() const {
    std::lock_guard<std::recursive_mutex> g(lock_);
    return std::vector<int64_t>(session_pfs.begin(), session_pfs.end());    // std::set is sorted
}

std::optional<int64_t> Portfolios::tokenOfPf(int64_t pf) const {
    for (const auto& [t, p] : currentPfMap())
        if (p == pf) return t;
    return std::nullopt;
}

std::optional<int64_t> Portfolios::ownerOf(int64_t token, const SymbolOf& symbol_of) const {
    auto current = currentPfMap();
    if (current.count(token)) return token;
    {
        std::lock_guard<std::recursive_mutex> g(lock_);
        for (const auto& [pf, opt] : subscribed)
            if (opt == token)
                for (const auto& [u, p] : current)
                    if (p == pf) return u;
    }
    const std::string name = firstWord(symbol_of(token));     // old leg of a retired PF: same stock = same checkbox
    for (const auto& [u, p] : current)
        if (firstWord(symbol_of(u)) == name) return u;
    return std::nullopt;
}

std::optional<int64_t> Portfolios::pfForRow(int64_t token, const SymbolOf& symbol_of) const {
    std::optional<int64_t> best;
    {
        std::lock_guard<std::recursive_mutex> g(lock_);
        for (const auto& [pf, opt] : subscribed)
            if (opt == token) best = best ? std::max(*best, pf) : pf;
    }
    if (best) return best;                                     // exact subscription (newest if several)
    auto owner = ownerOf(token, symbol_of);
    if (!owner) return std::nullopt;
    auto current = currentPfMap();
    auto it = current.find(*owner);
    return it == current.end() ? std::nullopt : std::optional<int64_t>(it->second);
}

// ---- numbering --------------------------------------------------------------------------------------------------
std::optional<int64_t> Portfolios::lastPf() const {
    std::lock_guard<std::recursive_mutex> g(lock_);
    std::optional<int64_t> hi;
    for (const auto& [t, p] : pf_map) hi = hi ? std::max(*hi, p) : p;
    for (const auto& [pf, opt] : subscribed) hi = hi ? std::max(*hi, pf) : pf;
    return hi;
}

std::vector<Claim> Portfolios::claimExisting() {
    if (!registry_ || !user_) return {};
    std::vector<int64_t> mine;
    {
        std::lock_guard<std::recursive_mutex> g(lock_);
        std::set<int64_t> s;
        for (const auto& [t, p] : pf_map) s.insert(p);
        for (const auto& [pf, opt] : subscribed) s.insert(pf);
        mine.assign(s.begin(), s.end());
    }
    std::vector<Claim> refused;
    for (int64_t pf : mine) {
        Claim c = registry_->registerPf(*user_, pf, owner_);
        if (!c.ok) refused.push_back(c);
    }
    return refused;
}

std::pair<std::optional<int64_t>, std::optional<int64_t>> Portfolios::plan(int64_t token, int64_t option) const {
    std::lock_guard<std::recursive_mutex> g(lock_);
    std::optional<int64_t> cur;
    if (tracked.count(token)) {
        auto it = pf_map.find(token);
        if (it != pf_map.end()) cur = it->second;
    }
    bool moves = false;
    if (cur) {
        auto sit = subscribed.find(*cur);
        moves = sit != subscribed.end() && sit->second != option;   // subscribed to a different option -> it moves
    }
    return {cur, moves ? cur : std::nullopt};
}

Portfolios::Assign Portfolios::assign(int64_t token, int64_t option, const std::string& name, const std::string& algo) {
    std::optional<int64_t> cur, old_pf;
    {
        std::lock_guard<std::recursive_mutex> g(lock_);
        std::tie(cur, old_pf) = plan(token, option);
        if (!cur || old_pf) {
            const int64_t pf = create(token, name, algo);           // (create takes the lock recursively)
            return {pf, old_pf, true};
        }
    }
    label(*cur, algo, name);                                        // the registry always knows who uses it
    return {*cur, std::nullopt, false};
}

void Portfolios::label(int64_t pf, const std::string& algo, const std::string& name) {
    if (registry_ && user_) registry_->registerPf(*user_, pf, owner_, "", algo, name);
}

int64_t Portfolios::create(int64_t token, const std::string& name, const std::string& algo) {
    int64_t pf;
    {
        std::lock_guard<std::recursive_mutex> g(lock_);            // two signals must never get the same number
        if (registry_ && user_) {
            pf = registry_->allocate(*user_, owner_, pf_start_, "", algo, name);
        } else {
            auto last = lastPf();
            pf = last ? *last + 1 : pf_start_;
        }
        pf_map[token] = pf;
        saveMap();
        session_pfs.insert(pf);
        caps.try_emplace(pf, 0);
    }
    changed();
    return pf;
}

void Portfolios::revert(int64_t token, int64_t old_pf, int64_t new_pf) {
    {
        std::lock_guard<std::recursive_mutex> g(lock_);
        pf_map[token] = old_pf;
        saveMap();
        session_pfs.erase(new_pf);
        caps.erase(new_pf);
    }
    changed();
}

// ---- mutations --------------------------------------------------------------------------------------------------
void Portfolios::subscribe(int64_t pf, int64_t option_token) {
    {
        std::lock_guard<std::recursive_mutex> g(lock_);
        subscribed[pf] = option_token;
        store(Which::Sub, subscribed);
    }
    changed();
}

void Portfolios::setPaused(int64_t pf, bool paused_on) {
    { std::lock_guard<std::recursive_mutex> g(lock_); if (paused_on) paused.insert(pf); else paused.erase(pf); }
    changed();
}

void Portfolios::setAllow(int64_t token, bool on) {
    { std::lock_guard<std::recursive_mutex> g(lock_); allow[token] = on; }
    changed();
}

void Portfolios::setLive(int64_t token, std::optional<std::string> leg, std::optional<int64_t> option) {
    std::lock_guard<std::recursive_mutex> g(lock_);
    live[token] = std::move(leg);
    if (!option) opt_of.erase(token);
    else opt_of[token] = *option;                                  // no changed(): live legs are not drawn
}

void Portfolios::saveMap() {
    std::lock_guard<std::recursive_mutex> g(lock_);
    store(Which::Map, pf_map);
}

// ---- where the two maps live: StateDB -------------------------------------------------------------------------
std::map<int64_t, int64_t> Portfolios::load(Which which) {
    if (!db_) return {};
    const std::string& book = (which == Which::Map) ? map_book_ : sub_book_;
    auto data = db_->loadInts(tableOf(which == Which::Map), book);
    return data ? *data : std::map<int64_t, int64_t>{};
}

void Portfolios::store(Which which, const std::map<int64_t, int64_t>& data) {
    if (!db_) return;
    const std::string& book = (which == Which::Map) ? map_book_ : sub_book_;
    db_->saveInts(tableOf(which == Which::Map), book, data);
}

}  // namespace algo
