#include "strategy/guard.h"

#include <algorithm>
#include <cmath>

#include "comms/nats_service.h"
#include "strategy/bridge.h"
#include "strategy/registry.h"

namespace algo {
namespace {

// Group an integer rupee amount with thousands commas and an optional leading '+' (Python's ',.0f' / '+,.0f').
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

Guard::Guard(Limits& limits, NatsService* nats, Registry* registry, BridgeOf bridge_of)
    : limits_(limits), nats_(nats), registry_(registry), bridge_of_(std::move(bridge_of)) {}

void Guard::on_values(const std::string& strategy, std::optional<double> turnover, std::optional<double> m2m) {
    for (const auto& kind : limits_.update(strategy, turnover, m2m)) breach(strategy, kind);
}

std::string Guard::set_limit(const std::string& strategy, std::string_view kind, std::optional<double> value) {
    std::vector<std::string> newly;
    try {
        newly = limits_.setLimit(strategy, kind, value);
    } catch (const std::invalid_argument& e) {
        return e.what();
    }
    for (const auto& k : newly) breach(strategy, k);
    const std::string why = limits_.blocked(strategy);
    return (!why.empty() && newly.empty()) ? strategy + " is still blocked: " + why : "";
}

void Guard::breach(const std::string& strategy, const std::string& kind) {
    auto [turnover, m2m] = limits_.seen(strategy);
    const LimitRec r = limits_.get(strategy);
    std::string text;
    if (kind == "turnover")
        text = strategy + ": TURNOVER limit " + money(r.turnover.value_or(0), false) + " reached (" +
               (turnover ? money(*turnover, false) : std::string("-")) +
               "): sending switched OFF until the limit is changed";
    else
        text = strategy + ": M2M limit " + money(r.m2m.value_or(0), true) + " reached (" +
               (m2m ? money(*m2m, true) : std::string("-")) +
               "): squaring up and unsubscribing all portfolios, sending switched OFF until the limit is changed";

    Bridge* run = bridge_of_ ? bridge_of_(strategy) : nullptr;
    if (run) run->sendOff();                                    // first: the strategy sends nothing more of its own

    std::map<int64_t, int> users;
    if (kind == "m2m" && nats_ && nats_->state() != "service_down") users = squareAll(strategy);
    if (nats_) {
        const int64_t user = users.empty() ? 0 : users.begin()->first;
        nats_->note(0, "limit", kind + "_breached", text, user);
    }
    limits_.notify(strategy);                                   // screens redraw: the ON/OFF button reads the switch just turned off
}

std::map<int64_t, int> Guard::squareAll(const std::string& strategy) {
    std::map<int64_t, int> sent;
    if (!nats_) return sent;
    Bridge* bridge = bridge_of_ ? bridge_of_(strategy) : nullptr;
    for (const auto& [user, pf, st] : nats_->states().items()) {
        auto rec = registry_ ? registry_->get(user, pf) : std::nullopt;
        if (!st.subscribed || !rec || rec->algo != strategy) continue;

        Job job;
        if (bridge && bridge->subscribedOption(pf))
            job = bridge->unsubscribeJob(pf);                  // the bridge knows the option: pause -> squareup -> unsubscribe
        else
            job = [pf](Call& call) -> Reply { call("pause_unpause", {{"Action", std::string("Pause")}}); return call("squareup", {}); };

        nats_->submit(pf, std::move(job), user, std::nullopt, "m2m");   // fire and forget (an operator command)
        sent[user] += 1;
    }
    return sent;
}

}  // namespace algo
