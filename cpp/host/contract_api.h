// ContractApi: the SDK's contract questions answered from the day's master snapshot (the "master" the Master service
// publishes). A strategy names an underlying and asks for its future, cash, options, chain, ATM/OTM/ITM, a strike by
// value, the next / previous strike, the paired CE/PE -- with the same meaning as the Python lookups it replaces
// (algo/lookup.py: contract / expiries / strikes / nearest_strikes):
//
//   find(name)      EXACTLY the contract named: a token number, or its symbol (any case / spacing): "RELIANCE" is the
//                   cash stock, "NIFTY" the index, "NIFTY 24NOV26 25000 CE" the option. It never guesses a kind.
//   findFut / findSpot / findOpt   the underlying's future (by expiry index) / cash or index / option at a strike
//   expiry_index    0 = the nearest expiry that has NOT passed (today counts), 1 = the next ...; futures and options
//                   each have their own list
//   nearest / ATM   by distance from the price; on a tie the LOWER strike first (a stable sort of ascending strikes)
//   OTM / ITM       OTM is above ATM for a CE and below it for a PE; ITM the other way; `steps` strikes away
//
// Units: a price is in the minor units of the underlying's REFERENCE instrument (its nearest future, else its cash),
// i.e. what quote()/bars() give for it; a strike is in the OPTION's own minor units (strike x its divisor). Off the
// tick path (searches, std::string), so a strategy calls these in onStart / when a signal fires, not per tick.
#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "data/master.h"
#include "sdk/AlgoSDK.h"

namespace algo {

class ContractApi {
public:
    // today: yyyymmdd on the exchange clock (what "has not passed" means). default_div: the market's price_div, for a
    // contract whose file has no usable Divisor.
    ContractApi(std::shared_ptr<const MasterData> master, int32_t today, double default_div);

    int32_t  find(std::string_view name) const;
    bool     info(int32_t token, AlgoContract& out) const;
    uint32_t expiries(std::string_view underlying, AlgoKind kind, int32_t* out, uint32_t max) const;
    uint32_t strikes(std::string_view underlying, int32_t expiry_index, AlgoOptType t, int32_t* tokens, int64_t* strikes, uint32_t max) const;
    uint32_t nearest(std::string_view underlying, int64_t price, AlgoOptType t, int32_t expiry_index, uint32_t n, int32_t* out) const;
    int32_t  findFut(std::string_view underlying, int32_t expiry_index) const;
    int32_t  findSpot(std::string_view underlying) const;
    int32_t  findOpt(std::string_view underlying, int32_t expiry_index, AlgoOptType t, int64_t strike) const;
    int32_t  moneyness(std::string_view underlying, int64_t price, AlgoOptType t, int32_t expiry_index, int32_t steps) const;
    int32_t  stepStrike(int32_t option_token, int32_t steps) const;

    const MasterData& master() const noexcept { return *master_; }
    int32_t today() const noexcept { return today_; }

private:
    struct Chain { std::vector<double> strikes; std::vector<int64_t> tokens; };
    static std::string norm(std::string_view s);                        // upper-case, single spaces, trimmed
    static char typeChar(AlgoOptType t) noexcept { return t == AlgoOptType::CE ? 'C' : t == AlgoOptType::PE ? 'P' : 0; }
    std::vector<int32_t> unexpiredOptionExpiries(const std::string& und, char type) const;
    bool    chain(const std::string& und, int32_t expiry_index, char type, Chain& out) const;
    double  divisorOf(const Contract& c) const noexcept { return c.divisor > 0 ? c.divisor : default_div_; }
    bool    referencePrice(const std::string& und, int64_t price, double& out) const;   // minor units -> currency
    int64_t minorStrike(int64_t token, double strike) const;            // currency -> the option's own minor units
    int64_t futureFor(const Contract& c) const;                          // the future that goes with a contract

    std::shared_ptr<const MasterData> master_;
    int32_t today_;
    double  default_div_;
};

// The context algod hands a strategy's host functions (`void* ctx`). It grows as the host does (book, quotes ...);
// the contract functions only read `contracts`.
struct HostCtx {
    const ContractApi* contracts = nullptr;
};

// Point the contract functions of an AlgoHostAPI table at HostCtx::contracts (find, find_fut, find_spot, find_opt,
// info, expiries, strikes, nearest, moneyness, step_strike). The rest of the table is left as it is.
void wireContracts(AlgoHostAPI& api) noexcept;

}  // namespace algo
