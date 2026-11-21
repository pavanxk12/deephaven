#include "core/plugin.h"

#include <stdexcept>

#include "core/app.h"

namespace algo {

App& Plugin::app() const {
    if (!app_) throw std::runtime_error("plugin '" + name_ + "' is not running");
    return *app_;
}

void Plugin::spawn(std::function<void()> fn) {
    threads_.emplace_back([fn = std::move(fn)]() {
        try { fn(); } catch (...) { /* the plugin owns reporting; a thread must never take the process down */ }
    });
}

void Plugin::follow(const std::string& topic, EventBus::Callback cb) {
    const auto token = app().bus().subscribe(topic, std::move(cb));
    subscriptions_.emplace_back(topic, token);
}

void Plugin::teardown() {
    if (torn_down_) return;
    torn_down_ = true;
    halt_.store(true);
    for (auto& t : threads_) if (t.joinable()) t.join();
    threads_.clear();
    if (app_) for (auto& [topic, token] : subscriptions_) app_->bus().unsubscribe(topic, token);
    subscriptions_.clear();
    try { onStop(); } catch (...) { /* best-effort cleanup; never throw out of teardown */ }
    app_ = nullptr;
}

}  // namespace algo
