// App: the one object that owns every running Plugin and every published value, the C++ counterpart of
// algo/core/app.py's App (there is no Deephaven session/`ns` dict here -- a value algod publishes for viewing goes
// out through the DH C++ client and/or the API later; `publish()`/`table()` here are the in-process handoff
// between plugins, same role as Python's `app.tables`). Running a plugin whose name is already running stops the
// old one first, so a hot-reload never leaves duplicate threads/subscriptions behind. Control-plane only: these
// calls happen at startup/shutdown/reload, never on the hot path.
#pragma once
#include <any>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/event_bus.h"
#include "core/market.h"
#include "core/plugin.h"
#include "core/process_config.h"

namespace algo {

class MissingDependency : public std::runtime_error {
public:
    explicit MissingDependency(const std::string& what) : std::runtime_error(what) {}
};

class App {
public:
    App(Market market, ProcessConfig config) : market_(std::move(market)), config_(std::move(config)) {}

    const Market&         market() const noexcept { return market_; }
    const ProcessConfig&  config() const noexcept { return config_; }
    EventBus&             bus() noexcept { return bus_; }

    // ---- plugins ------------------------------------------------------------------------------------------
    // Stops any plugin already running under the same name, checks `needs()`, binds and starts `plugin`. Throws
    // MissingDependency if a need is absent, or whatever start() throws (the plugin is torn down either way).
    Plugin& run(std::unique_ptr<Plugin> plugin);
    void    stop(const std::string& name);
    void    stopAll();                                   // reverse start order, same as Python's stop_all()

    Plugin* get(const std::string& name) const noexcept;  // nullptr if not running
    Plugin& at(const std::string& name) const;            // throws MissingDependency if not running
    bool    has(const std::string& name) const noexcept;  // a running plugin OR a published value named `name`

    // ---- published values (the in-process handoff between plugins) -----------------------------------------
    // Publishing an already-published name notifies that topic's EventBus subscribers with the new value
    // (Plugin::follow), so a listener can move to the new value -- e.g. candles restarted with a new bar size.
    void     publish(const std::string& name, std::any value);
    std::any table(const std::string& name) const;        // throws MissingDependency if absent

    std::vector<std::pair<std::string, std::string>> status() const;   // name, describe(), in start order

private:
    Market       market_;
    ProcessConfig config_;
    EventBus     bus_;
    std::vector<std::string>                          order_;      // plugin names, start order
    std::unordered_map<std::string, std::unique_ptr<Plugin>> plugins_;
    std::unordered_map<std::string, std::any>          published_;
};

}  // namespace algo
