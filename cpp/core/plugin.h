// Plugin: one independent component (master, candles, straddle, the NATS service, the book ...), the C++
// counterpart of algo/core/plugin.py's Plugin. It declares what it `needs()`, builds its output in `start()`, and
// everything it owns (threads, EventBus subscriptions) is torn down automatically when it is stopped or replaced.
// Lifecycle only; off the hot path (App::run/stop are control-plane calls, never called per tick).
#pragma once
#include <any>
#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "core/event_bus.h"

namespace algo {

class App;

class Plugin {
public:
    explicit Plugin(std::string name) : name_(std::move(name)) {}
    // Safety net only: App::stop()/stopAll() always call teardown() (via `App`, a friend) while the derived object
    // is still fully alive, so onStop() dispatches correctly. By the time this runs, teardown() has already run and
    // is a no-op (guarded by torn_down_) -- calling onStop() here instead would invoke Plugin's own onStop(), never
    // a derived override, because of how virtual dispatch works during destruction.
    virtual ~Plugin() { teardown(); }

    const std::string& name() const noexcept { return name_; }

    // Plugin / table names that must already exist in the App before start() runs.
    virtual const std::vector<std::string>& needs() const { static const std::vector<std::string> kNone; return kNone; }

    // ---- to implement ---------------------------------------------------------------------------------------
    virtual void start() = 0;             // build outputs, attach listeners/threads; runs once per App::run()
    virtual void onStop() {}              // extra cleanup beyond threads/subscriptions (close connections, ...)
    virtual std::string describe() const { return ""; }   // one line for App::status()

protected:
    // ---- available inside start() ----------------------------------------------------------------------------
    App& app() const;                                      // throws std::runtime_error if not currently running
    std::atomic<bool>& halted() noexcept { return halt_; }  // set when stopping; loops should exit when it is

    // Daemon-style thread, joined automatically on stop(). `fn` should return promptly once halted() is set.
    void spawn(std::function<void()> fn);

    // Subscribes to an EventBus topic (e.g. a table being republished); unsubscribed automatically on stop().
    void follow(const std::string& topic, EventBus::Callback cb);

private:
    friend class App;
    void bind(App& app) noexcept { app_ = &app; halt_.store(false); }
    void teardown();

    std::string          name_;
    App*                 app_ = nullptr;
    std::atomic<bool>    halt_{false};
    bool                 torn_down_ = false;     // guards against the double-teardown the destructor's safety net could cause
    std::vector<std::thread> threads_;
    std::vector<std::pair<std::string, EventBus::Token>> subscriptions_;
};

}  // namespace algo
