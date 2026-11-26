// MasterService: the "master" plugin, the C++ counterpart of the Python Master plugin / master_build(). It builds the
// day's contract master once (Postgres first or the .db3 file, as markets/<CODE>.ini [contracts] source says) and
// publishes two things every other service uses:
//
//   "master"     std::shared_ptr<const MasterData>   immutable snapshot: kind (FUT/OPT/EQ), expiry, strike, chain,
//                                                     tick, divisor, display text. Readers on any thread, no lock.
//   "token_ids"  std::shared_ptr<TokenIds>           the day's one token -> slot map (append-only). Created by the
//                                                     first build of a day; a later run the same day (a reload) only
//                                                     ADDS the tokens that are new, so no slot is ever renumbered.
//
// Running it again (App::run replaces the plugin) republishes "master", and everyone that follow()s it is told. A run
// for a different day starts a fresh "token_ids". Control-plane: runs on the caller's thread at startup / the 09:00
// schedule step, never on the tick path.
#pragma once
#include <cstdint>
#include <memory>
#include <string>

#include "core/plugin.h"
#include "data/master.h"
#include "data/token_ids.h"

namespace algo {

struct MasterServiceOptions {
    std::string day;                 // yyyymmdd; empty = today on the exchange's clock (markets/<CODE>.ini [market] tz)
    uint32_t    overflow = 4096;     // extra ids for tokens the master does not have (feed-only tokens, reload additions)
};

class MasterService final : public Plugin {
public:
    explicit MasterService(MasterServiceOptions opt = {}) : Plugin("master"), opt_(std::move(opt)) {}

    void start() override;
    std::string describe() const override;

    // what the last start() did, for status and tests
    const std::string& source() const noexcept { return source_; }
    const std::string& warning() const noexcept { return warning_; }
    uint32_t added() const noexcept { return added_; }        // tokens a reload appended to the day's ids
    uint32_t refused() const noexcept { return refused_; }    // tokens that did not fit (capacity full)

    static std::string today(const Market& mk);               // yyyymmdd on the exchange clock

private:
    MasterServiceOptions        opt_;
    std::string                 day_, source_, warning_;
    std::shared_ptr<const MasterData> data_;
    std::shared_ptr<TokenIds>   ids_;
    uint32_t                    added_ = 0, refused_ = 0;
};

}  // namespace algo
