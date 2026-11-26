#include "data/master_service.h"

#include <any>
#include <cstdio>
#include <ctime>
#include <vector>

#include "core/app.h"
#include "core/process_config.h"

namespace algo {

std::string MasterService::today(const Market& mk) {
    const int64_t now = static_cast<int64_t>(std::time(nullptr));
    const int64_t local = now + mk.zone.offsetAt(now);
    int y; unsigned m, d;
    civilFromDays(local / 86400 - (local % 86400 < 0), y, m, d);
    char buf[16];
    std::snprintf(buf, sizeof buf, "%04d%02u%02u", y, m, d);
    return buf;
}

void MasterService::start() {
    App& a = app();
    const Market& mk = a.market();
    day_ = opt_.day.empty() ? today(mk) : opt_.day;
    if (day_.size() != 8) throw std::invalid_argument("master: day must be yyyymmdd, got \"" + day_ + "\"");
    const int32_t day_num = std::stoi(day_);

    std::string used, warn;
    auto data = std::make_shared<const MasterData>(
        loadMaster(mk, day_, a.config().market_data_dir, pgConninfo(a.config().pg), used, warn));
    source_ = used;
    warning_ = warn;

    // every contract token of the day, in the snapshot's own order
    std::vector<int64_t> tokens;
    tokens.reserve(data->index.size());
    for (uint32_t i = 0; i < data->index.size(); ++i) tokens.push_back(data->index.tokenOf(i));

    // the day's ids: reuse (append-only) when they are already for this day, otherwise start the day's map
    std::shared_ptr<TokenIds> ids;
    if (a.has("token_ids")) {
        try { ids = std::any_cast<std::shared_ptr<TokenIds>>(a.table("token_ids")); } catch (const std::bad_any_cast&) {}
    }
    added_ = refused_ = 0;
    if (ids && ids->day() == day_num) {
        for (int64_t t : tokens) {
            if (ids->find(t) != TokenIds::kNone) continue;
            if (ids->findOrAdd(t) == TokenIds::kNone) ++refused_;
            else ++added_;
        }
    } else {
        ids = std::make_shared<TokenIds>(day_num, tokens, static_cast<uint32_t>(tokens.size()) + opt_.overflow);
        a.publish("token_ids", ids);                              // a new day: followers rebuild their per-token arrays
    }
    data_ = data;
    ids_ = ids;
    a.publish("master", std::shared_ptr<const MasterData>(data));  // republishing tells everyone who follow()s "master"
}

std::string MasterService::describe() const {
    if (!data_ || !ids_) return "";
    char buf[320];
    std::snprintf(buf, sizeof buf, "%s %s from %s: %zu contracts (opt %zu, fut %zu, eq %zu, chain %zu); ids %u of %u%s%s",
                  "day", day_.c_str(), source_.c_str(), data_->contracts.size(), data_->opt.size(), data_->futs.size(),
                  data_->eqs.size(), data_->chain.size(), ids_->size(), ids_->capacity(),
                  added_ ? (", reload added " + std::to_string(added_)).c_str() : "",
                  refused_ ? (", REFUSED " + std::to_string(refused_) + " (ids full)").c_str() : "");
    return buf;
}

}  // namespace algo
