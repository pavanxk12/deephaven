#include "core/event_bus.h"

#include <algorithm>
#include <exception>

namespace algo {

EventBus::Token EventBus::subscribe(const std::string& topic, Callback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    const Token token = next_token_++;
    subscribers_[topic].push_back({token, std::move(cb)});
    return token;
}

void EventBus::unsubscribe(const std::string& topic, Token token) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = subscribers_.find(topic);
    if (it == subscribers_.end()) return;
    auto& v = it->second;
    v.erase(std::remove_if(v.begin(), v.end(), [&](const Entry& e) { return e.token == token; }), v.end());
}

void EventBus::publish(const std::string& topic, std::any value) {
    std::vector<Callback> callbacks;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = subscribers_.find(topic);
        if (it == subscribers_.end()) return;
        for (auto& e : it->second) callbacks.push_back(e.cb);        // copy: a subscriber may unsubscribe mid-notify
    }
    for (auto& cb : callbacks) {
        try { cb(value); } catch (const std::exception&) { /* one bad subscriber must not stop the rest */ }
    }
}

}  // namespace algo
