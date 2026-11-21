#include "comms/nats_service.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <utility>

namespace algo {
namespace {

const std::set<std::string> kNoSender = {"subscribe", "unsubscribe"};

// Pull the pf-state fields out of a stamped payload (T1 = churn target, TOKEN = single-order field, Action = pause).
PfPayload toPfPayload(const Payload& p) {
    PfPayload out;
    auto asInt = [&](const char* k) -> std::optional<int64_t> {
        auto it = p.find(k);
        if (it == p.end()) return std::nullopt;
        if (std::holds_alternative<int64_t>(it->second)) return std::get<int64_t>(it->second);
        if (std::holds_alternative<double>(it->second)) return static_cast<int64_t>(std::get<double>(it->second));
        return std::nullopt;
    };
    out.t1 = asInt("T1");
    out.token = asInt("TOKEN");
    auto a = p.find("Action");
    if (a != p.end() && std::holds_alternative<std::string>(a->second)) out.action = std::get<std::string>(a->second);
    return out;
}

Payload merge(const Payload& base, const Payload& over, int64_t t1) {
    Payload out = base;
    for (const auto& [k, v] : over) out[k] = v;
    out["T1"] = t1;
    return out;
}

}  // namespace

NatsConfig::NatsConfig() {
    // _sub_payload() from nats_service.py: the subscribe/unsubscribe defaults; only T1 is replaced per portfolio.
    sub_payload = {
        {"T1", int64_t{0}}, {"gtw", int64_t{32395}}, {"PRNT", int64_t{1}}, {"code", std::string("")}, {"ival", int64_t{1}},
        {"jval", 0.75}, {"side", std::string("BUY")}, {"sval", int64_t{3}}, {"trp", int64_t{0}}, {"jqty1", int64_t{1}},
        {"jqty2", int64_t{1}}, {"openq", int64_t{0}}, {"maxqty", int64_t{0}}, {"target", int64_t{1000}},
        {"maxloss", int64_t{-1000}}, {"stoploss", int64_t{-1000}}, {"lowerrange", int64_t{0}}, {"upperrange", int64_t{10000}},
        {"max_unhedge", int64_t{1}}};
}

bool isRefused(const std::string& status) {
    return status == "stopped" || status == "full" || status == "no_user" || status == "user_not_allowed" || status == "blocked";
}

Reply Call::operator()(const std::string& cmd, const Payload& payload) {
    return svc_->call(pf_, user_, client_, cmd, payload);
}

NatsService::NatsService(NatsConfig cfg, TransportFactory factory)
    : cfg_(std::move(cfg)), factory_(std::move(factory)) {
    sending_ = cfg_.send_on;
}

NatsService::~NatsService() { stop(); }

// ---- lifecycle --------------------------------------------------------------------------------------------------
void NatsService::connect() {
    if (cfg_.dry_run) { stopped_ = false; return; }
    if (!factory_) throw std::runtime_error("NATS: no transport factory (the real cnats transport is not built yet; "
                                            "inject one, or use dry_run)");
    closing_ = false;
    stopped_ = false;
    transport_ = factory_();
    slots_ = cfg_.max_in_flight;                 // 0 => unlimited (acquireSlot is a no-op)
}

void NatsService::stop() {
    if (stopped_.exchange(true) && !transport_ && workflows_.empty()) return;
    closing_ = true;
    {
        std::lock_guard<std::mutex> g(mu_);      // whatever is still waiting is told it will not run
        for (auto& [key, lane] : lanes_) {
            while (!lane.empty()) {
                Waiting w = std::move(lane.front());
                lane.pop_front();
                queued_--;
                w.promise.set_value(Reply{false, "stopped", key.second, "", {}, w.user});
            }
        }
    }
    {   // wait for the running drains to finish their current job and exit
        std::unique_lock<std::mutex> g(mu_);
        drains_cv_.wait_for(g, std::chrono::seconds(5), [this] { return active_drains_ == 0; });
    }
    for (auto& t : workflows_) if (t.joinable()) t.join();
    workflows_.clear();
    if (transport_) { try { transport_->close(); } catch (...) {} transport_.reset(); }
}

// ---- the six commands -------------------------------------------------------------------------------------------
std::future<Reply> NatsService::subscribe(int64_t pf, int64_t option, const Payload& params, std::optional<int64_t> user, std::optional<std::string> client) {
    return send(pf, "subscribe", merge(cfg_.sub_payload, params, option), user, std::move(client));
}
std::future<Reply> NatsService::unsubscribe(int64_t pf, int64_t option, const Payload& params, std::optional<int64_t> user, std::optional<std::string> client) {
    return send(pf, "unsubscribe", merge(cfg_.sub_payload, params, option), user, std::move(client));
}
std::future<Reply> NatsService::pause(int64_t pf, std::optional<int64_t> user, std::optional<std::string> client) {
    return send(pf, "pause_unpause", {{"Action", std::string("Pause")}}, user, std::move(client));
}
std::future<Reply> NatsService::unpause(int64_t pf, std::optional<int64_t> user, std::optional<std::string> client) {
    return send(pf, "pause_unpause", {{"Action", std::string("UnPause")}}, user, std::move(client));
}
std::future<Reply> NatsService::squareup(int64_t pf, std::optional<int64_t> user, std::optional<std::string> client) {
    return send(pf, "squareup", {}, user, std::move(client));
}
std::future<Reply> NatsService::apply(int64_t pf, const Payload& params, std::optional<int64_t> user, std::optional<std::string> client) {
    return send(pf, "apply", params, user, std::move(client));
}

// ---- general entry points ---------------------------------------------------------------------------------------
std::future<Reply> NatsService::send(int64_t pf, const std::string& cmd, const Payload& payload, std::optional<int64_t> user, std::optional<std::string> client) {
    return submitInternal(pf, [cmd, payload](Call& call) { return call(cmd, payload); }, cmd, user, true, std::move(client));
}

std::future<Reply> NatsService::submit(int64_t pf, Job job, std::optional<int64_t> user, std::optional<std::string> client, const std::string& label) {
    return submitInternal(pf, std::move(job), label, user, true, std::move(client));
}

Reply NatsService::lane(int64_t pf, Job job, std::optional<int64_t> user, std::optional<std::string> client) {
    Reply r = submitInternal(pf, std::move(job), "job", user, false, std::move(client)).get();
    if (isRefused(r.status)) throw std::runtime_error("PF " + std::to_string(pf) + ": " + r.status);
    return r;
}

bool NatsService::go(std::function<void()> workflow, std::optional<std::string> client) {
    if (cfg_.dry_run || !allowed(client) || stopped_) return false;
    std::lock_guard<std::mutex> g(mu_);
    workflows_.emplace_back([workflow = std::move(workflow)] { try { workflow(); } catch (...) {} });
    return true;
}

std::future<Reply> NatsService::submitInternal(int64_t pf, Job job, const std::string& label, std::optional<int64_t> user, bool gated, std::optional<std::string> client) {
    std::promise<Reply> promise;
    std::future<Reply> fut = promise.get_future();
    auto [uid, why] = resolve(user);
    if (why.empty()) {
        std::lock_guard<std::mutex> g(mu_);
        if (blocked_users_.count(uid) || blocked_pfs_.count({uid, pf})) why = "blocked";
    }
    if (!why.empty()) {
        const std::string detail = why == "no_user" ? "several users are allowed: say which one"
                                 : why == "user_not_allowed" ? "that user is not allowed"
                                 : "this user or portfolio is blocked";
        note(pf, label, why, detail, uid);
        promise.set_value(Reply{false, why, pf, label, {{"detail", detail}}, uid});
        return fut;
    }
    if (cfg_.dry_run) { promise.set_value(Reply{true, "dry", pf, label, {}, uid}); return fut; }
    if (gated && !allowed(client)) {
        const std::string off = !sending_ ? "sending is off for everyone" : "this client has not switched sending on";
        promise.set_value(Reply{false, "off", pf, label, {{"detail", off}}, uid});
        return fut;
    }
    if (stopped_) { promise.set_value(Reply{false, "stopped", pf, label, {}, uid}); return fut; }
    enqueue(pf, uid, std::move(job), std::move(promise), std::move(client));
    return fut;
}

void NatsService::enqueue(int64_t pf, int64_t user, Job job, std::promise<Reply> promise, std::optional<std::string> client) {
    const std::pair<int64_t, int64_t> key{user, pf};
    std::unique_lock<std::mutex> g(mu_);
    auto it = lanes_.find(key);
    const std::size_t waiting = it == lanes_.end() ? 0 : it->second.size();
    const int64_t inside = queued_.load() + running_.load();
    if (static_cast<int32_t>(waiting) >= cfg_.lane_depth || (cfg_.max_queued && inside >= cfg_.max_queued)) {
        counts_.dropped++;
        const std::string why = static_cast<int32_t>(waiting) >= cfg_.lane_depth
            ? std::to_string(waiting) + " jobs already waiting for this portfolio"
            : std::to_string(inside) + " commands already inside the service";
        g.unlock();
        note(pf, "queue", "full", why, user);
        promise.set_value(Reply{false, "full", pf, "", {}, user});
        return;
    }
    const bool fresh = it == lanes_.end();
    auto& lane = fresh ? lanes_[key] : it->second;
    lane.push_back(Waiting{std::move(job), std::move(promise), user, std::move(client)});
    queued_++;
    if (fresh) {
        active_drains_++;
        std::thread(&NatsService::drain, this, key).detach();
    }
}

void NatsService::drain(std::pair<int64_t, int64_t> key) {
    for (;;) {
        Waiting w;
        {
            std::lock_guard<std::mutex> g(mu_);
            auto it = lanes_.find(key);
            if (it == lanes_.end() || it->second.empty()) {
                if (it != lanes_.end()) lanes_.erase(it);
                active_drains_--;
                drains_cv_.notify_all();
                return;
            }
            w = std::move(it->second.front());
            it->second.pop_front();
            queued_--;
            running_++;
        }
        Reply result;
        try {
            Call call(this, key.second, w.user, w.client);
            result = w.job(call);
        } catch (const std::exception& e) {
            result = Reply{false, "error", key.second, "", {{"error", e.what()}}, w.user};
        }
        running_--;
        w.promise.set_value(result);
    }
}

// ---- one request ------------------------------------------------------------------------------------------------
Reply NatsService::call(int64_t pf, int64_t user, const std::optional<std::string>& client, const std::string& cmd, const Payload& payload) {
    bool blocked;
    { std::lock_guard<std::mutex> g(mu_); blocked = blocked_users_.count(user) || blocked_pfs_.count({user, pf}); if (blocked) counts_.failed++; }
    if (blocked) {                                   // blocked in the middle of a sequence
        note(pf, cmd, "blocked", "this user or portfolio is blocked", user);
        return Reply{false, "blocked", pf, cmd, {}, user};
    }
    if (transport_ && !transport_->isConnected()) {
        { std::lock_guard<std::mutex> g(mu_); counts_.failed++; }
        note(pf, cmd, "disconnected", "not sent: no connection to NATS", user);
        return Reply{false, "disconnected", pf, cmd, {}, user};
    }
    Payload stamped = stamp(cmd, payload, user);
    std::string subject = cfg_.subject;
    if (auto pos = subject.find("{pf}"); pos != std::string::npos) subject.replace(pos, 4, std::to_string(pf));
    { std::lock_guard<std::mutex> g(mu_); counts_.sent++; }

    Reply reply;
    acquireSlot();
    in_flight_++;
    try {
        Response resp = transport_->request(subject, cmd, stamped, cfg_.timeout_s);
        const std::string status = resp.status.empty() ? "no_status" : resp.status;
        reply = Reply{status == "ok", status, pf, cmd, resp.body, user};
    } catch (const NatsTimeout&) {
        in_flight_--; releaseSlot();
        { std::lock_guard<std::mutex> g(mu_); counts_.timeout++; counts_.failed++; }
        note(pf, cmd, "timeout", "no answer: it is not known whether the bridge acted", user);
        return Reply{false, "timeout", pf, cmd, {}, user};
    } catch (const NatsNoResponders&) {
        in_flight_--; releaseSlot();
        { std::lock_guard<std::mutex> g(mu_); counts_.failed++; }
        note(pf, cmd, "no_responders", "nothing listens on this portfolio's subject", user);
        return Reply{false, "no_responders", pf, cmd, {}, user};
    } catch (const std::exception& e) {
        in_flight_--; releaseSlot();
        { std::lock_guard<std::mutex> g(mu_); counts_.failed++; }
        note(pf, cmd, "error", e.what(), user);
        return Reply{false, "error", pf, cmd, {{"error", e.what()}}, user};
    }
    in_flight_--;
    releaseSlot();
    {
        std::lock_guard<std::mutex> g(mu_);
        if (reply.ok) counts_.ok++; else counts_.failed++;
    }
    if (reply.ok) track(pf, user, cmd, stamped, client);
    else note(pf, cmd, reply.status, "", user);
    return reply;
}

Payload NatsService::stamp(const std::string& cmd, Payload payload, int64_t user) const {
    payload["USERIDENTIFIER"] = user;
    payload["user_id"] = user;
    if (!kNoSender.count(cmd)) payload["SENDER"] = cfg_.sender;
    return payload;
}

void NatsService::track(int64_t pf, int64_t user, const std::string& cmd, const Payload& payload, const std::optional<std::string>& client) {
    try { states_.record(user, pf, cmd, toPfPayload(payload), client ? *client : std::string()); }
    catch (...) { /* a pf-state listener failed: never let it break the send */ }
}

// ---- switches ---------------------------------------------------------------------------------------------------
bool NatsService::sendOn() {
    if (!transport_) return false;
    sending_ = true;
    return true;
}
void NatsService::sendOff() { sending_ = false; }

bool NatsService::enable(const std::string& client) {
    if (!transport_) return false;
    std::lock_guard<std::mutex> g(mu_);
    enabled_.insert(client);
    return true;
}
void NatsService::disable(const std::string& client) {
    std::lock_guard<std::mutex> g(mu_);
    enabled_.erase(client);
}
bool NatsService::isEnabled(const std::string& client) const {
    std::lock_guard<std::mutex> g(mu_);
    return enabled_.count(client) != 0;
}
bool NatsService::allowed(std::optional<std::string> client) const {
    if (!sending_) return false;
    if (!client) return true;
    std::lock_guard<std::mutex> g(mu_);
    return enabled_.count(*client) != 0;
}
void NatsService::addUsers(const std::vector<int64_t>& users) {
    std::lock_guard<std::mutex> g(mu_);
    for (int64_t u : users)
        if (std::find(cfg_.users.begin(), cfg_.users.end(), u) == cfg_.users.end()) cfg_.users.push_back(u);
}

// ---- blocks -----------------------------------------------------------------------------------------------------
void NatsService::block(std::optional<int64_t> user, const std::vector<int64_t>& pfs) {
    if (!user) throw std::invalid_argument("block(user=..): a portfolio number belongs to a user, so name the user");
    std::lock_guard<std::mutex> g(mu_);
    if (!pfs.empty()) for (int64_t p : pfs) blocked_pfs_.insert({*user, p});
    else blocked_users_.insert(*user);
}
void NatsService::unblock(std::optional<int64_t> user, const std::vector<int64_t>& pfs) {
    if (!user) throw std::invalid_argument("unblock(user=..): name the user");
    std::lock_guard<std::mutex> g(mu_);
    if (!pfs.empty()) for (int64_t p : pfs) blocked_pfs_.erase({*user, p});
    else { blocked_users_.erase(*user); for (auto it = blocked_pfs_.begin(); it != blocked_pfs_.end();) it = (it->first == *user) ? blocked_pfs_.erase(it) : std::next(it); }
}

// ---- state ------------------------------------------------------------------------------------------------------
bool NatsService::connected() const { return cfg_.dry_run || (transport_ && transport_->isConnected()); }

std::string NatsService::state() const {
    if (stopped_) return "service_down";
    if (cfg_.dry_run) return "dry";
    if (sending_ && connected()) return "on";
    return connected() ? "off" : "disconnected";
}

std::optional<PfState> NatsService::pfState(int64_t pf, std::optional<int64_t> user) const {
    auto [uid, why] = resolve(user);
    if (!why.empty()) return std::nullopt;
    return states_.get(uid, pf);
}

std::pair<int64_t, std::string> NatsService::resolve(std::optional<int64_t> user) const {
    std::lock_guard<std::mutex> g(mu_);
    const auto& users = cfg_.users;
    if (!user) return users.size() == 1 ? std::make_pair(users[0], std::string()) : std::make_pair(int64_t{0}, std::string("no_user"));
    return std::find(users.begin(), users.end(), *user) != users.end()
        ? std::make_pair(*user, std::string()) : std::make_pair(int64_t{0}, std::string("user_not_allowed"));
}

NatsService::Counts NatsService::counts() const { std::lock_guard<std::mutex> g(mu_); return counts_; }

std::vector<NatsService::Event> NatsService::events() const { std::lock_guard<std::mutex> g(mu_); return events_; }

void NatsService::note(int64_t pf, const std::string& cmd, const std::string& status, const std::string& detail, int64_t user) {
    std::lock_guard<std::mutex> g(mu_);
    events_.push_back({user, pf, cmd, status, detail.substr(0, 300)});
}

// ---- the in-flight cap ------------------------------------------------------------------------------------------
void NatsService::acquireSlot() {
    if (cfg_.max_in_flight <= 0) return;
    std::unique_lock<std::mutex> g(slot_mu_);
    slot_cv_.wait(g, [this] { return slots_ > 0; });
    slots_--;
}
void NatsService::releaseSlot() {
    if (cfg_.max_in_flight <= 0) return;
    { std::lock_guard<std::mutex> g(slot_mu_); slots_++; }
    slot_cv_.notify_one();
}

}  // namespace algo
