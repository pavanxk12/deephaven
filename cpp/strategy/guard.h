// Enforces a strategy's turnover and M2M limits (the numbers and latches live in core/Limits). A port of
// algo/strategy/guard.py.
//   turnover breached -> the strategy's sending is switched OFF (its own switch; others untouched)
//   M2M breached      -> every subscribed portfolio of the strategy is paused, squared up and UNSUBSCRIBED, OFF
// The live values come from onValues (fed from the strategy's trades). setLimit changes a limit and acts at once if
// the strategy is already past the new one. Control-plane only.
//
// Python finds the pieces through app.get("nats") / app.get(strategy) / app["registry"]; that App string-lookup is
// not built yet (strategies land in Phase 3), so here the dependencies are injected: the shared Limits, the send
// service, the registry (to tell which portfolios belong to the strategy), and a lookup from a strategy name to its
// Bridge (to switch its sending off and reuse its unsubscribe sequence).
#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "core/limits.h"

namespace algo {

class NatsService;
class Registry;
class Bridge;

class Guard {
public:
    using BridgeOf = std::function<Bridge*(const std::string& strategy)>;   // a strategy name -> its Bridge, or nullptr

    Guard(Limits& limits, NatsService* nats, Registry* registry, BridgeOf bridge_of = {});

    // The live values (turnover in rupees, M2M in rupees). Acts on every limit this update breaches for the first time.
    void on_values(const std::string& strategy, std::optional<double> turnover, std::optional<double> m2m);

    // Change a limit. Returns "" or a sentence for the person (a bad value, or "still blocked ...").
    std::string set_limit(const std::string& strategy, std::string_view kind, std::optional<double> value);

private:
    void                   breach(const std::string& strategy, const std::string& kind);
    std::map<int64_t, int> squareAll(const std::string& strategy);   // user -> number of portfolios acted on

    Limits&      limits_;
    NatsService* nats_;
    Registry*    registry_;
    BridgeOf     bridge_of_;
};

}  // namespace algo
