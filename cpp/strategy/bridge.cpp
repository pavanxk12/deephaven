#include "strategy/bridge.h"

#include <chrono>
#include <stdexcept>
#include <thread>

namespace algo {
namespace {

double nowEpoch() {
    return std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
}

// Abort a sequence if a step did not come back ok (the workflow's try/catch turns this into a rollback).
Reply need(Reply r, const std::string& what) {
    if (!r.ok) throw std::runtime_error(what + ": " + r.status);
    return r;
}

}  // namespace

Bridge::Bridge(BridgeConfig cfg, std::string client) : cfg_(std::move(cfg)), client_(std::move(client)) {}

void Bridge::bind(NatsService* nats, Portfolios* book) {
    unbind();
    nats_ = nats;
    book_ = book;
    if (!nats_ || !book_) return;
    nats_->strategyOff(client_);                                 // every bind begins OFF
    listener_ = nats_->states().onChange([this](int64_t u, int64_t pf, const PfState& st) { onPf(u, pf, st); });
    // tell the shared states about the portfolios the book already had subscribed (so a restart re-adopts them)
    if (auto u = user()) {
        for (const auto& [pf, pf_token] : book_->subscribed)
            nats_->states().adopt(*u, pf, pf_token, book_->paused.count(pf) != 0);
    }
}

void Bridge::unbind() {
    if (nats_ && listener_) nats_->states().removeListener(listener_);
    listener_ = 0;
    if (nats_) { try { nats_->strategyOff(client_); } catch (...) {} }
}

// ---- the send switch --------------------------------------------------------------------------------------------
bool Bridge::sendOn() {
    if (!nats_ || !nats_->strategyOn(client_)) return false;
    if (book_) book_->changed();
    std::vector<std::pair<Token, std::function<void()>>> cbs;
    { std::lock_guard<std::mutex> g(cb_mu_); cbs = enable_cbs_; }
    for (auto& [t, cb] : cbs) cb();
    return true;
}

void Bridge::sendOff() {
    if (nats_) nats_->strategyOff(client_);
    if (book_) book_->changed();
}

bool Bridge::sending() const { return nats_ && nats_->allowed(client_); }
bool Bridge::dryRun() const { return nats_ && nats_->cfg().dry_run; }

std::string Bridge::state() const {
    if (!nats_) return "service_down";
    const std::string base = nats_->state();
    return (base == "on" && !nats_->isStrategyOn(client_)) ? "off" : base;
}

Bridge::Token Bridge::onEnable(std::function<void()> cb) {
    std::lock_guard<std::mutex> g(cb_mu_);
    const Token t = next_token_++;
    enable_cbs_.emplace_back(t, std::move(cb));
    return t;
}
Bridge::Token Bridge::onHalt(std::function<void()> cb) {
    std::lock_guard<std::mutex> g(cb_mu_);
    const Token t = next_token_++;
    halt_cbs_.emplace_back(t, std::move(cb));
    return t;
}
void Bridge::removeEnable(Token t) {
    std::lock_guard<std::mutex> g(cb_mu_);
    for (auto it = enable_cbs_.begin(); it != enable_cbs_.end(); ++it) if (it->first == t) { enable_cbs_.erase(it); return; }
}
void Bridge::removeHalt(Token t) {
    std::lock_guard<std::mutex> g(cb_mu_);
    for (auto it = halt_cbs_.begin(); it != halt_cbs_.end(); ++it) if (it->first == t) { halt_cbs_.erase(it); return; }
}

// ---- lookups ----------------------------------------------------------------------------------------------------
std::optional<int64_t> Bridge::user() const {
    if (cfg_.user) return cfg_.user;
    return book_ ? book_->userId() : std::nullopt;
}

std::optional<int64_t> Bridge::subscribedToken(int64_t pf) const {
    if (book_) {
        auto it = book_->subscribed.find(pf);
        if (it != book_->subscribed.end()) return it->second;
    }
    if (!nats_) return std::nullopt;
    auto st = nats_->pfState(pf, user());
    if (st && st->subscribed && st->pf_token) return st->pf_token;
    return std::nullopt;
}

Payload Bridge::sub(int64_t pf_token) const {
    Payload out = nats_ ? nats_->cfg().sub_payload : Payload{};
    for (const auto& [k, v] : cfg_.subscribe) out[k] = v;
    out["T1"] = pf_token;
    return out;
}

Reply Bridge::pauseStep(Call& call, int64_t pf, const std::string& action) {
    Reply r = call("pause_unpause", {{"Action", action}});
    if (r.status == "ok" && book_) book_->setPaused(pf, action == "Pause");
    return r;
}

bool Bridge::guard() const { return sending(); }

// ---- command sequences ------------------------------------------------------------------------------------------
bool Bridge::unsubscribeDeferred() const noexcept {
    return cfg_.unsubscribe_on_retire && cfg_.squareup_on_repoint && cfg_.squareup_settle_s > 0;
}

Reply Bridge::retireSteps(Call& call, int64_t old_pf, int64_t old_token) {
    need(pauseStep(call, old_pf, "Pause"), "pause of old PF not ok, new PF skipped");
    Reply last{true, "ok", old_pf, "pause_unpause", {}, 0};
    if (cfg_.squareup_on_repoint) last = need(call("squareup", {}), "squareup of old PF not ok, new PF skipped");
    if (cfg_.unsubscribe_on_retire && !unsubscribeDeferred())
        last = call("unsubscribe", sub(old_token));                // no squareup to settle: at once (not ok does not block)
    return last;
}

void Bridge::unsubscribeLater(int64_t pf, int64_t pf_token, std::optional<int64_t> u) {
    const bool started = nats_->go([this, pf, pf_token, u] {
        if (!nats_->waitUnlessStopped(cfg_.squareup_settle_s)) {   // the service is stopping: say so, do not hang it
            nats_->note(pf, "retire", "not_sent", "unsubscribe after squareup not sent: the service stopped during the wait", u.value_or(0));
            return;
        }
        try {
            const Reply r = nats_->lane(pf, [this, pf_token](Call& call) -> Reply { return call("unsubscribe", sub(pf_token)); }, u, client_);
            if (!r.ok) nats_->note(pf, "retire", "failed", "unsubscribe after squareup not ok: " + r.status, u.value_or(0));
        } catch (const std::exception& e) {
            nats_->note(pf, "retire", "failed", std::string("unsubscribe after squareup: ") + e.what(), u.value_or(0));
        }
    }, client_);
    if (!started)
        nats_->note(pf, "retire", "not_sent", "unsubscribe after squareup not scheduled (sending is off): the portfolio stays "
                    "subscribed, paused and squared", u.value_or(0));
}

bool Bridge::retire(int64_t old_pf) {
    if (!nats_) return false;
    const auto u = user();
    int64_t old_token = 0;
    if (auto it = book_->subscribed.find(old_pf); it != book_->subscribed.end()) old_token = it->second;
    return nats_->go([this, old_pf, old_token, u] {
        nats_->lane(old_pf, [this, old_pf, old_token](Call& call) -> Reply { return retireSteps(call, old_pf, old_token); }, u, client_);
        if (unsubscribeDeferred()) unsubscribeLater(old_pf, old_token, u);
    }, client_);
}

bool Bridge::swap(int64_t pf, int64_t token, int64_t pf_token, Payload params,
                  std::optional<std::string> prev_leg, std::optional<int64_t> prev_token, std::optional<int64_t> old_pf) {
    if (!nats_) return false;
    const auto u = user();
    return nats_->go([this, pf, token, pf_token, params = std::move(params), prev_leg, prev_token, old_pf, u]() mutable {
        auto job = [this, pf, pf_token, &params, u](Call& call) -> Reply {
            auto seen = nats_->pfState(pf, u);
            const bool held = seen && seen->paused && seen->by != client_;   // paused by the console/another algo
            if (book_->subscribed.find(pf) == book_->subscribed.end()) {     // new PF: subscribe it
                need(call("subscribe", sub(pf_token)), "subscribe not ok, apply skipped");
                book_->subscribe(pf, pf_token);
            }
            need(pauseStep(call, pf, "Pause"), "pause not ok, apply skipped");
            int64_t cap = cfg_.add_qty;
            auto cit = book_->caps.find(pf);
            if (cit != book_->caps.end()) cap += cit->second;
            Payload ap = params;
            ap["maxqty"] = cap;
            Reply applied = need(call("apply", ap), "apply not ok, left paused");
            book_->caps[pf] = cap;
            if (held) return applied;                                        // leave it paused
            need(pauseStep(call, pf, "UnPause"), "unpause not ok, may still be paused");
            return applied;
        };
        try {
            if (old_pf) {
                // retire the old PF first, on its own lane, before taking over the stock. Its unsubscribe (if any) follows
                // `squareup_settle_s` later, so its plugin can fill the closing order; the new PF does not wait for it.
                int64_t old_token = 0;
                if (auto it = book_->subscribed.find(*old_pf); it != book_->subscribed.end()) old_token = it->second;
                nats_->lane(*old_pf, [this, old_pf, old_token](Call& call) -> Reply { return retireSteps(call, *old_pf, old_token); }, u, client_);
                if (unsubscribeDeferred()) unsubscribeLater(*old_pf, old_token, u);
            }
            nats_->lane(pf, job, u, client_);
        } catch (const std::exception& e) {
            if (old_pf && book_->subscribed.find(pf) == book_->subscribed.end())
                book_->revert(token, *old_pf, pf);                           // the new PF never reached the bridge
            book_->setLive(token, prev_leg, prev_token);                    // allow a retry on a later update
            book_->last_fail[token] = nowEpoch();
            nats_->note(pf, "swap", "failed", e.what(), u.value_or(0));
        }
    }, client_);
}

// ---- operator actions -------------------------------------------------------------------------------------------
std::optional<std::future<Reply>> Bridge::pausePf(int64_t pf) {
    if (!guard()) return std::nullopt;
    if (dryRun()) { if (book_) book_->setPaused(pf, true); return std::nullopt; }
    return nats_->pause(pf, user(), client_);                // the book is brought in line by onPf when it confirms
}

std::optional<std::future<Reply>> Bridge::unpausePf(int64_t pf) {
    if (!guard()) return std::nullopt;
    if (dryRun()) { if (book_) book_->setPaused(pf, false); return std::nullopt; }
    return nats_->unpause(pf, user(), client_);
}

std::optional<std::future<Reply>> Bridge::squareupPf(int64_t pf) {
    if (!guard()) return std::nullopt;
    std::optional<std::future<Reply>> fut;
    if (!dryRun()) fut = nats_->squareup(pf, user(), client_);
    if (book_) { auto t = book_->tokenOfPf(pf); if (t) book_->setLive(*t, std::nullopt); }  // a later signal may start it again
    return fut;
}

void Bridge::pauseAll() {
    if (!guard() || !book_) return;
    for (int64_t pf : book_->allPfs()) if (!dryRun()) nats_->pause(pf, user(), client_);
}

void Bridge::unpauseAll() {
    if (!guard() || !book_) return;
    for (int64_t pf : book_->currentPfs()) if (!dryRun()) nats_->unpause(pf, user(), client_);
}

void Bridge::exitAll(bool pause) {
    if (!guard() || !book_) return;
    const auto u = user();
    for (int64_t pf : book_->allPfs()) {
        if (dryRun()) continue;
        nats_->submit(pf, [this, pf, pause](Call& call) -> Reply {
            if (pause) pauseStep(call, pf, "Pause");
            return call("squareup", {});
        }, u, client_, "exit");
    }
    for (int64_t t : std::vector<int64_t>(book_->tracked.begin(), book_->tracked.end())) book_->setLive(t, std::nullopt);
}

std::vector<std::future<Reply>> Bridge::unsubscribeAll(bool pause) {
    if (dryRun() || !sending() || !book_) return {};
    std::vector<std::pair<Token, std::function<void()>>> cbs;
    { std::lock_guard<std::mutex> g(cb_mu_); cbs = halt_cbs_; }
    for (auto& [t, cb] : cbs) cb();                          // strategies stop producing signals
    const auto u = user();
    std::vector<std::future<Reply>> futures;
    for (int64_t pf : book_->currentPfs())
        futures.push_back(nats_->submit(pf, unsubscribeJob(pf, pause), u, client_, "unsubscribe"));
    for (int64_t t : std::vector<int64_t>(book_->tracked.begin(), book_->tracked.end())) book_->setLive(t, std::nullopt);
    return futures;
}

Job Bridge::unsubscribeJob(int64_t pf, bool pause) {
    return [this, pf, pause](Call& call) -> Reply {
        auto pf_token = subscribedToken(pf);
        if (!pf_token) return Reply{true, "ok", pf, "", {}, 0};   // nothing to unsubscribe
        if (pause) { Reply r = pauseStep(call, pf, "Pause"); if (!r.ok) return r; }
        Reply sq = call("squareup", {});
        if (!sq.ok) return sq;
        if (cfg_.squareup_settle_s > 0 && !nats_->waitUnlessStopped(cfg_.squareup_settle_s))   // let the close fill first
            return Reply{false, "stopped", pf, "unsubscribe", {}, 0};    // the service is stopping: do not hang it
        return call("unsubscribe", sub(*pf_token));
    };
}

// ---- the pf-state listener --------------------------------------------------------------------------------------
void Bridge::onPf(int64_t u, int64_t pf, const PfState& st) {
    if (!book_) return;
    if (auto mine = user(); mine && u != *mine) return;
    auto key = book_->tokenOfPf(pf);
    const bool in_sub = book_->subscribed.count(pf) != 0;
    if (!key && !in_sub) return;                            // not one of ours
    try {
        auto it = book_->subscribed.find(pf);
        if (st.cmd == "subscribe" && st.subscribed && st.pf_token &&
            (it == book_->subscribed.end() || it->second != *st.pf_token))
            book_->subscribe(pf, *st.pf_token);               // whoever sent it: the book (and Postgres) records it
        if (st.cmd == "pause_unpause") book_->setPaused(pf, st.paused);
        else if (st.cmd == "squareup" && key) book_->setLive(*key, std::nullopt);
        else if ((st.cmd == "subscribe" || st.cmd == "unsubscribe") && st.by != client_ && key)
            book_->setAllow(*key, st.subscribed);           // a console tick/untick allows/blocks the slot
    } catch (...) {
        // a portfolio state that could not be applied: never let it break the listener
    }
}

}  // namespace algo
