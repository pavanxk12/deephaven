// A small topic -> subscribers board, the C++ counterpart of App.on_publish()/Plugin.follow() (algo/core/app.py,
// plugin.py): a plugin that republishes something (e.g. candles restarted with a new bar size) calls `publish()`,
// and anyone that called `subscribe()` on that topic is notified with the new value, synchronously, on the
// publisher's own thread -- exactly like the Python version's `cb(table)` calls. Off the hot path (control /
// housekeeping only), so a mutex is fine; payloads are `std::any` since what gets published varies by topic.
#pragma once
#include <any>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace algo {

class EventBus {
public:
    using Callback = std::function<void(const std::any&)>;
    using Token = uint64_t;

    // Registers `cb` for `topic`. Call the returned function (or `unsubscribe`) to stop listening.
    Token subscribe(const std::string& topic, Callback cb);
    void  unsubscribe(const std::string& topic, Token token);

    // Notifies every current subscriber of `topic`, in registration order. A subscriber that throws is caught and
    // dropped from this call (never stops the others), matching the Python side's per-callback try/except.
    void publish(const std::string& topic, std::any value);

private:
    struct Entry { Token token; Callback cb; };
    std::mutex mutex_;
    std::unordered_map<std::string, std::vector<Entry>> subscribers_;
    Token next_token_ = 1;
};

}  // namespace algo
