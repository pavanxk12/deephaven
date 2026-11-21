// Contract master: the day's instruments from the dated SQLite (.db3) file or the Postgres table
// `resultset_<yyyymmdd>` (the same rows), classified into chain / options / futures / equities and indexed by
// token, exactly like algo/data/master.py's MasterData. A state-for-state port: `tools/master_dump.cpp` +
// `tests/parity/master_parity.py` check this against the Python output on the same real .db3 file.
//
// Storage: every contract with a usable Token gets one slot in `contracts_`, dense-indexed through `index`
// (built from every token in the file, same idea as the candle builder's TokenIndex over the master's tokens).
// `contracts_[dense_id]` is the cold per-token row (symbol, kind, expiry, strike, tick, lot, divisor, display
// text) -- read only at lookup time, never on the tick path. The grouped views (chain/opt/futs/eqs, the option
// strike index, the reverse symbol map) are built once from the same rows, matching Python's separate frames.
#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/market.h"
#include "data/token_index.h"

namespace algo {

enum class Kind : uint8_t { kNone = 0, kEq = 1, kFut = 2, kOpt = 3 };

struct Contract {
    int64_t     token = -1;
    std::string symbol;              // TckrSymb, upper-case
    Kind        kind = Kind::kNone;
    char        opt_type = 0;        // 'C' / 'P' / 0 (futures and equities)
    int32_t     expiry = -1;         // yyyymmdd, -1 = none (equities; an option/future whose expiry failed to parse)
    double      strike = 0;
    double      tick = 0;
    double      lot = 0;
    double      divisor = 0;
    std::string display;             // symbol_of text: "NIFTY 27OCT26 25000 CE" / "NIFTY 27OCT26 FUT" / "AAPL"
};

// One parsed+normalised contract row (the C++ counterpart of a row of Python's `normalise()` frame).
struct RawRow {
    int64_t     token = -1;
    std::string symbol;              // already upper-cased, trimmed
    std::string inst_type;           // upper-cased, trimmed
    std::string option_type;         // upper-cased, trimmed ("" if the column is absent)
    double      strike = 0;          // NaN if absent/unparsable
    double      tick = 0;
    double      divisor = 0;
    double      lot = 0;
    int32_t     expiry = -1;         // yyyymmdd, -1 = NaT (unparsable / absent)
};

// Chain row: one (symbol, expiry, strike) with both legs.
struct ChainRow {
    int64_t     ce_token = -1, pe_token = -1;
    std::string symbol;
    int32_t     expiry = -1;
    double      strike = 0;
};

class MasterData {
public:
    std::vector<ChainRow>  chain;
    std::vector<Contract>  opt, futs, eqs;     // kept rows of each kind, same filters as Python's opt/futs/eqs frames
    TokenIndex             index;              // every contract's token -> dense id (direct array or perfect hash)
    std::vector<Contract>  contracts;          // dense-indexed (parallel to `index`): the cold per-token row

    static MasterData build(std::vector<RawRow> rows);

    // ---- lookups (mirror algo.data.master.MasterData) ----------------------------------------------------
    const Contract* byToken(int64_t token) const noexcept;
    std::optional<int64_t> tokenOf(const std::string& exact_symbol_upper) const;     // reverse of `display`
    std::optional<double>  tickOf(int64_t token) const;
    std::optional<double>  divisorOf(int64_t token) const;
    std::optional<int64_t> spotOf(const std::string& underlying) const;              // cash token of an underlying
    std::optional<std::pair<std::string, int32_t>> futInfo(int64_t fut_token) const; // (underlying, expiry)
    std::optional<int64_t> nearestFuture(const std::string& symbol, int32_t today_yyyymmdd) const;
    std::vector<int32_t>   expiries(const std::string& symbol, char option_type) const;     // sorted, ascending
    bool strikes(const std::string& symbol, int32_t expiry, char option_type,
                std::vector<double>& strikes_out, std::vector<int64_t>& tokens_out) const;  // sorted by strike

    // Sorted token array of the chosen kind/symbols; `expiries_n` keeps, per underlying, only its n nearest
    // option expiries (nullopt = no restriction on either).
    std::vector<int64_t> tokensFor(const std::vector<std::string>* symbols, std::optional<Kind> kind,
                                   std::optional<int32_t> expiries_n) const;

private:
    std::unordered_map<std::string, int64_t>                         token_of_;   // display text -> token
    std::unordered_map<std::string, int64_t>                         spot_of_;    // underlying -> eq token (first)
    std::unordered_map<int64_t, std::pair<std::string, int32_t>>     fut_info_;
    std::unordered_map<int64_t, double>                              tick_by_token_, divisor_by_token_;
    struct OptKey { std::string symbol; int32_t expiry; char opt_type; bool operator==(const OptKey&) const noexcept; };
    struct OptKeyHash { size_t operator()(const OptKey&) const noexcept; };
    std::unordered_map<OptKey, std::pair<std::vector<double>, std::vector<int64_t>>, OptKeyHash> option_index_;
    struct ExpKey { std::string symbol; char opt_type; bool operator==(const ExpKey&) const noexcept; };
    struct ExpKeyHash { size_t operator()(const ExpKey&) const noexcept; };
    std::unordered_map<ExpKey, std::vector<int32_t>, ExpKeyHash> option_expiries_;
};

// ---- parsing (pure, ported from normalise()/classify()/build_symbol_map()) -----------------------------------
// `expiry_epoch` is "yyyy-mm-dd" (Market::expiry_epoch); the whole column decides numeric-vs-text once, like Python.
std::vector<int32_t> parseExpiryColumn(const std::vector<std::string>& raw_text, const std::string& expiry_epoch);
std::string expiryText(int32_t yyyymmdd);                 // "27OCT26" ("" for -1)
std::string strikeText(double strike);                    // "%.10g", same as Python's f"{v:.10g}"

// ---- files -----------------------------------------------------------------------------------------------
// The contract table of a .db3 file (auto-detected columns from `mk.columns`, in the order
// Token,Symbol,ExpiryDate,InstType,OptionType,StrikePrice,LotSize,TickSize,Divisor,Exch). Throws std::runtime_error.
std::vector<RawRow> readContractsDb3(const std::string& db_path, const Market& mk);

// Same rows from Postgres table `resultset_<yyyymmdd>` (day = "yyyymmdd"). Needs PG* environment / `conninfo`.
std::vector<RawRow> readContractsPg(const std::string& day, const Market& mk, const std::string& conninfo);

// `<dir>/<yyyymmdd>.db3` if present, else the last `*<yyyymmdd>*.db3` (same rule as Python's find_db).
std::string findDb(const std::string& directory, const std::string& day);

// `pattern` with every "<yyyymmdd>" replaced by `day` (the `[contracts] file_pattern` / `pg_table` of a market .ini).
std::string substituteDay(const std::string& pattern, const std::string& day);

// `<dir>/<file_pattern with day substituted>` if that exact file exists, else `findDb(dir, day)` (the generic glob,
// for a file whose real name does not match the pattern byte-for-byte, e.g. an extra tag before ".db3").
std::string findDbByPattern(const std::string& directory, const std::string& file_pattern, const std::string& day);

// Today's contracts, driven only by `mk` and `pg_conninfo` (built from algod.ini's [postgres] section + the named
// env var, never a hardcoded host or password): Postgres first when `mk.contracts_source` is "postgres" or "auto"
// and `pg_conninfo` is non-empty, the .db3 file under `market_data_dir` otherwise or when Postgres fails and the
// source is "auto" (never when it is "postgres" -- that failure is raised). Mirrors Python's Master.start().
// `used_source` becomes "postgres:<table>" or "file:<path>"; `warning` is set (and non-fatal) on an auto fallback.
MasterData loadMaster(const Market& mk, const std::string& day_yyyymmdd, const std::string& market_data_dir,
                      const std::string& pg_conninfo, std::string& used_source, std::string& warning);

}  // namespace algo
