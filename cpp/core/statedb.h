// The algo's own state in Postgres: the portfolio registry, each strategy's book (PF map + subscriptions) and the
// limits. A port of algo/core/statedb.py -- the SAME four tables with the SAME names and columns, so a Python algod
// and a C++ algod can hand state back and forth during cutover. Created if missing; nothing existing is touched.
//
//   algo_portfolios   user_id, pf, owner, name, algo, created, note, updated_at   pk (user_id, pf)   the registry
//   algo_pf_map       book, token, pf                                             pk (book, token)   a book's future token -> PF
//   algo_pf_sub       book, pf, option_token                                      pk (book, pf)      a book's PF -> option token
//   algo_limits       strategy, turnover, m2m, turnover_hit, m2m_hit, updated_at  pk (strategy)      limits + latches
//
// Control-plane only. Every call commits; a call that fails on a dropped connection is retried once on a fresh one
// (all statements are idempotent upserts / replace-all, so twice is safe), exactly as the Python wrapper does.
#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "core/sql.h"

namespace algo {

struct Portfolio {
    int64_t     user = 0;
    int64_t     pf = 0;
    std::string owner;
    std::string name;
    std::string algo;
    std::string created;
    std::string note;
};

struct Limit {
    std::string           strategy;
    std::optional<double> turnover;       // nullopt = no limit (an empty box)
    std::optional<double> m2m;
    bool                  turnover_hit = false;
    bool                  m2m_hit = false;
};

class StateDB {
public:
    enum class BookTable { Map, Sub };    // algo_pf_map (token->pf) or algo_pf_sub (pf->option_token)

    // `factory` makes a fresh connection on demand (for the first connect and for the retry after a drop). The
    // constructor runs the schema; it throws std::runtime_error if the store cannot be reached -- starting from
    // nothing would hand out portfolio numbers that are already in use.
    using Factory = std::function<std::unique_ptr<SqlConn>()>;
    explicit StateDB(Factory factory);

    // ---- the registry -------------------------------------------------------------------------------------------
    std::vector<Portfolio> loadPortfolios();                       // ordered by user_id, pf
    void savePortfolio(const Portfolio& p);
    void savePortfolios(const std::vector<Portfolio>& rows);       // import many

    // ---- a book -------------------------------------------------------------------------------------------------
    std::optional<std::map<int64_t, int64_t>> loadInts(BookTable table, const std::string& book);  // nullopt if no rows
    void saveInts(BookTable table, const std::string& book, const std::map<int64_t, int64_t>& data);  // replace-all

    // ---- the limits ---------------------------------------------------------------------------------------------
    std::vector<Limit> loadLimits();
    void saveLimits(const std::vector<Limit>& rows);

private:
    struct Stmt { std::string sql; std::vector<std::vector<DbValue>> rows; };  // one row -> execute; many -> executemany

    void             txWrite(const std::vector<Stmt>& statements);             // run in one transaction, retry once
    std::vector<Row> txRead(const std::string& sql, const std::vector<DbValue>& params);

    Factory                  factory_;
    std::unique_ptr<SqlConn> con_;
    std::mutex               lock_;
};

}  // namespace algo
