#include "core/app.h"

#include <algorithm>

namespace algo {

Plugin& App::run(std::unique_ptr<Plugin> plugin) {
    const std::string name = plugin->name();
    std::vector<std::string> missing;
    for (auto& need : plugin->needs()) if (!has(need)) missing.push_back(need);
    if (!missing.empty()) {
        std::string msg = name + " needs ";
        for (size_t i = 0; i < missing.size(); ++i) { if (i) msg += ", "; msg += missing[i]; }
        msg += ": run those first";
        throw MissingDependency(msg);
    }
    stop(name);                                    // a re-run of the same name replaces the old instance cleanly
    plugin->bind(*this);
    Plugin& ref = *plugin;
    try {
        ref.start();
    } catch (...) {
        plugin->teardown();
        throw;
    }
    plugins_[name] = std::move(plugin);
    order_.push_back(name);
    return ref;
}

void App::stop(const std::string& name) {
    auto it = plugins_.find(name);
    if (it == plugins_.end()) return;
    it->second->teardown();
    plugins_.erase(it);
    order_.erase(std::remove(order_.begin(), order_.end(), name), order_.end());
}

void App::stopAll() {
    while (!order_.empty()) stop(order_.back());
}

Plugin* App::get(const std::string& name) const noexcept {
    auto it = plugins_.find(name);
    return it == plugins_.end() ? nullptr : it->second.get();
}

Plugin& App::at(const std::string& name) const {
    Plugin* p = get(name);
    if (!p) throw MissingDependency("plugin '" + name + "' is not running");
    return *p;
}

bool App::has(const std::string& name) const noexcept {
    return plugins_.count(name) != 0 || published_.count(name) != 0;
}

void App::publish(const std::string& name, std::any value) {
    const bool replacing = published_.count(name) != 0;
    published_[name] = std::move(value);
    if (replacing) bus_.publish(name, published_[name]);
}

std::any App::table(const std::string& name) const {
    auto it = published_.find(name);
    if (it == published_.end()) throw MissingDependency("'" + name + "' has not been published");
    return it->second;
}

std::vector<std::pair<std::string, std::string>> App::status() const {
    std::vector<std::pair<std::string, std::string>> rows;
    for (auto& name : order_) rows.emplace_back(name, plugins_.at(name)->describe());
    return rows;
}

}  // namespace algo
