#include "strategy/registry.h"

#include <algorithm>
#include <ctime>
#include <stdexcept>

namespace algo {
namespace {

std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

int64_t whole(int64_t x, const char* what) {
    if (x <= 0) throw std::invalid_argument(std::string(what) + " must be a whole number above 0");
    return x;
}

std::string owner_of(const std::string& x) {
    std::string out = trim(x);
    if (out.empty()) throw std::invalid_argument("owner must be a name, such as the book or screen that creates the portfolio");
    return out;
}

}  // namespace

std::string Claim::str() const {
    if (ok) return "user " + std::to_string(user) + " portfolio " + std::to_string(pf) + ": " + status + " for " + owner;
    return "user " + std::to_string(user) + " portfolio " + std::to_string(pf) + ": TAKEN by " + held_by +
           ", not available to " + owner;
}

Registry::Registry(StateDB* db) : db_(db) { load(); }

void Registry::load() {
    if (!db_) return;
    for (const auto& p : db_->loadPortfolios())
        data_[p.user][p.pf] = {p.owner, p.name, p.algo, p.created, p.note};
}

std::string Registry::stamp() const {
    std::time_t t = std::time(nullptr);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    char buf[32];
    std::strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%S", &tm);
    return buf;
}

RegRec Registry::put(int64_t user, int64_t pf, RegRec rec) {
    // store first, then persist; if the write fails, put things back as they were and let the caller see the error.
    auto& mine = data_[user];
    const bool existed = mine.count(pf) != 0;
    RegRec before = existed ? mine[pf] : RegRec{};
    mine[pf] = rec;
    if (db_) {
        try {
            db_->savePortfolio({user, pf, rec.owner, rec.name, rec.algo, rec.created, rec.note});
        } catch (...) {
            if (existed) mine[pf] = before;
            else { mine.erase(pf); if (mine.empty()) data_.erase(user); }
            throw;
        }
    }
    std::vector<std::pair<Token, Listener>> copy = cbs_;
    for (auto& [token, fn] : copy) {
        try { fn(user, pf, rec); } catch (...) { /* one bad listener never stops the rest */ }
    }
    return rec;
}

int64_t Registry::allocate(int64_t user, const std::string& owner_in, int64_t start,
                           const std::string& note, const std::string& algo, const std::string& name) {
    const int64_t u = whole(user, "user");
    const std::string owner = owner_of(owner_in);
    const int64_t s = whole(start, "start");
    std::lock_guard<std::recursive_mutex> g(lock_);
    auto& held = data_[u];
    int64_t highest_mine = 0;
    bool have_mine = false;
    for (const auto& [p, r] : held)
        if (r.owner == owner) { highest_mine = std::max(highest_mine, p); have_mine = true; }
    int64_t n = have_mine ? std::max(s, highest_mine + 1) : s;
    while (held.count(n)) ++n;
    put(u, n, {owner, trim(name), trim(algo), stamp(), trim(note)});
    return n;
}

int64_t Registry::create(int64_t user, const std::string& strategy, const std::string& name, int64_t start, const std::string& note) {
    return allocate(user, strategy, start, note, strategy, name);
}

Claim Registry::registerPf(int64_t user, int64_t pf, const std::string& owner_in,
                           const std::string& note, const std::string& algo, const std::string& name) {
    const int64_t u = whole(user, "user");
    const int64_t p = whole(pf, "portfolio");
    const std::string owner = owner_of(owner_in);
    std::lock_guard<std::recursive_mutex> g(lock_);
    auto uit = data_.find(u);
    const RegRec* have = (uit != data_.end() && uit->second.count(p)) ? &uit->second.at(p) : nullptr;
    if (!have) {
        put(u, p, {owner, trim(name), trim(algo), stamp(), trim(note)});
        return {true, "registered", u, p, owner, ""};
    }
    if (have->owner != owner) return {false, "taken", u, p, owner, have->owner};
    RegRec updated = *have;
    if (!trim(name).empty()) updated.name = trim(name);
    if (!trim(algo).empty()) updated.algo = trim(algo);
    if (!trim(note).empty()) updated.note = trim(note);
    if (updated.name == have->name && updated.algo == have->algo && updated.note == have->note)
        return {true, "exists", u, p, owner, ""};
    put(u, p, updated);
    return {true, "updated", u, p, owner, ""};
}

std::optional<std::string> Registry::ownerOf(int64_t user, int64_t pf) const {
    auto rec = get(user, pf);
    if (!rec) return std::nullopt;
    return rec->owner;
}

std::optional<RegRec> Registry::get(int64_t user, int64_t pf) const {
    std::lock_guard<std::recursive_mutex> g(lock_);
    auto uit = data_.find(user);
    if (uit == data_.end()) return std::nullopt;
    auto pit = uit->second.find(pf);
    if (pit == uit->second.end()) return std::nullopt;
    return pit->second;
}

std::vector<Portfolio> Registry::portfolios(std::optional<int64_t> user) const {
    std::lock_guard<std::recursive_mutex> g(lock_);
    std::vector<Portfolio> out;
    for (const auto& [u, pfs] : data_) {
        if (user && u != *user) continue;
        for (const auto& [p, r] : pfs) out.push_back({u, p, r.owner, r.name, r.algo, r.created, r.note});
    }
    return out;                                   // std::map keeps users and pfs sorted
}

std::pair<std::size_t, std::size_t> Registry::count() const {
    std::lock_guard<std::recursive_mutex> g(lock_);
    std::size_t pfs = 0;
    for (const auto& [u, m] : data_) pfs += m.size();
    return {data_.size(), pfs};
}

Registry::Token Registry::onChange(Listener fn) {
    std::lock_guard<std::recursive_mutex> g(lock_);
    const Token t = next_token_++;
    cbs_.emplace_back(t, std::move(fn));
    return t;
}

void Registry::removeListener(Token token) {
    std::lock_guard<std::recursive_mutex> g(lock_);
    for (auto it = cbs_.begin(); it != cbs_.end(); ++it)
        if (it->first == token) { cbs_.erase(it); return; }
}

}  // namespace algo
