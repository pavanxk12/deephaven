#include "core/statedb.h"

#include <stdexcept>

namespace algo {
namespace {

// The schema and upserts are character-for-character the Python SCHEMA / UPSERT_* strings, with '?' placeholders
// (both backends bind by position). SQLite accepts the Postgres type names by affinity and supports the same
// ON CONFLICT ... DO UPDATE upsert, so one set of statements serves production and the tests.
const char* kSchema[] = {
    "CREATE TABLE IF NOT EXISTS algo_portfolios ("
    "user_id integer NOT NULL, pf bigint NOT NULL, owner text NOT NULL, name text, algo text, created text, note text, token bigint, "
    "updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (user_id, pf))",
    "CREATE TABLE IF NOT EXISTS algo_pf_map ("
    "book text NOT NULL, token bigint NOT NULL, pf bigint NOT NULL, PRIMARY KEY (book, token))",
    "CREATE TABLE IF NOT EXISTS algo_pf_sub ("
    "book text NOT NULL, pf bigint NOT NULL, token bigint NOT NULL, PRIMARY KEY (book, pf))",
    "CREATE TABLE IF NOT EXISTS algo_limits ("
    "strategy text NOT NULL, turnover double precision, m2m double precision, turnover_hit boolean NOT NULL DEFAULT false, "
    "m2m_hit boolean NOT NULL DEFAULT false, updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (strategy))",
};

// token: a write that does not know it (NULL) keeps the stored one, so nothing that saves a record without the token
// (an import, a rename) can erase it.
const char* kUpsertPortfolio =
    "INSERT INTO algo_portfolios (user_id, pf, owner, name, algo, created, note, token) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT (user_id, pf) DO UPDATE SET owner = EXCLUDED.owner, name = EXCLUDED.name, algo = EXCLUDED.algo, "
    "created = EXCLUDED.created, note = EXCLUDED.note, token = COALESCE(EXCLUDED.token, algo_portfolios.token), "
    "updated_at = CURRENT_TIMESTAMP";

const char* kUpsertLimit =
    "INSERT INTO algo_limits (strategy, turnover, m2m, turnover_hit, m2m_hit) VALUES (?, ?, ?, ?, ?) "
    "ON CONFLICT (strategy) DO UPDATE SET turnover = EXCLUDED.turnover, m2m = EXCLUDED.m2m, "
    "turnover_hit = EXCLUDED.turnover_hit, m2m_hit = EXCLUDED.m2m_hit, updated_at = CURRENT_TIMESTAMP";

DbValue tokenValue(const std::optional<int64_t>& t) { return t ? DbValue{*t} : DbValue{nullptr}; }

// The two int-map tables, by role: (table name, key column, value column).
struct BookCols { const char* table; const char* key; const char* val; };
BookCols cols(StateDB::BookTable t) {
    return t == StateDB::BookTable::Map ? BookCols{"algo_pf_map", "token", "pf"}
                                        : BookCols{"algo_pf_sub", "pf", "token"};
}

int64_t asInt(const std::optional<std::string>& cell) { return cell ? std::stoll(*cell) : 0; }
std::string asText(const std::optional<std::string>& cell) { return cell ? *cell : std::string(); }
std::optional<double> asNum(const std::optional<std::string>& cell) {
    if (!cell) return std::nullopt;
    try { return std::stod(*cell); } catch (...) { return std::nullopt; }
}
// Postgres returns booleans as "t"/"f"; SQLite as "1"/"0". Anything that isn't plainly false is false.
bool asBool(const std::optional<std::string>& cell) {
    if (!cell) return false;
    const std::string& s = *cell;
    return s == "t" || s == "true" || s == "1";
}

DbValue num(const std::optional<double>& v) { return v ? DbValue(*v) : DbValue(nullptr); }

}  // namespace

StateDB::StateDB(Factory factory) : factory_(std::move(factory)) {
    std::vector<Stmt> schema;
    for (const char* ddl : kSchema) schema.push_back({ddl, {{}}});  // one empty-param execution each
    try {
        txWrite(schema);
        // tables made before these columns had their current names (by the Python stack or an older algod) are brought
        // up to date, keeping every row. The checks read the database's catalogue, so they never raise an error.
        if (!hasColumn("algo_portfolios", "token"))
            txWrite({{"ALTER TABLE algo_portfolios ADD COLUMN token bigint", {{}}}});
        if (hasColumn("algo_pf_sub", "option_token"))                       // RENAME COLUMN: Postgres, and SQLite 3.25+
            txWrite({{"ALTER TABLE algo_pf_sub RENAME COLUMN option_token TO token", {{}}}});
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("the algo state store could not be initialised (") + e.what() +
                                 "); nothing was started -- portfolio numbers must not be handed out from nothing");
    }
}

void StateDB::txWrite(const std::vector<Stmt>& statements) {
    std::lock_guard<std::mutex> g(lock_);
    for (int attempt = 1; attempt <= 2; ++attempt) {
        try {
            if (!con_) con_ = factory_();
            con_->begin();
            try {
                for (const auto& s : statements)
                    for (const auto& params : s.rows) con_->exec(s.sql, params);
            } catch (...) {
                con_->rollback();
                throw;
            }
            con_->commit();
            return;
        } catch (...) {
            con_.reset();                     // drop the (possibly dead) connection; the retry rebuilds it
            if (attempt == 2) throw;
        }
    }
}

bool StateDB::hasColumn(const std::string& table, const std::string& column) {
    std::lock_guard<std::mutex> g(lock_);
    for (int attempt = 1; attempt <= 2; ++attempt) {
        try {
            if (!con_) con_ = factory_();
            return con_->hasColumn(table, column);
        } catch (...) {
            con_.reset();
            if (attempt == 2) throw;
        }
    }
    return false;
}

std::vector<Row> StateDB::txRead(const std::string& sql, const std::vector<DbValue>& params) {
    std::lock_guard<std::mutex> g(lock_);
    for (int attempt = 1; attempt <= 2; ++attempt) {
        try {
            if (!con_) con_ = factory_();
            return con_->query(sql, params);
        } catch (...) {
            con_.reset();
            if (attempt == 2) throw;
        }
    }
    return {};
}

std::vector<Portfolio> StateDB::loadPortfolios() {
    auto rows = txRead("SELECT user_id, pf, owner, name, algo, created, note, token FROM algo_portfolios ORDER BY user_id, pf", {});
    std::vector<Portfolio> out;
    out.reserve(rows.size());
    for (const auto& r : rows)
        out.push_back({asInt(r[0]), asInt(r[1]), asText(r[2]), asText(r[3]), asText(r[4]), asText(r[5]), asText(r[6]),
                       r[7] ? std::optional<int64_t>(asInt(r[7])) : std::nullopt});
    return out;
}

void StateDB::savePortfolio(const Portfolio& p) {
    txWrite({{kUpsertPortfolio, {{p.user, p.pf, p.owner, p.name, p.algo, p.created, p.note, tokenValue(p.token)}}}});
}

void StateDB::savePortfolios(const std::vector<Portfolio>& rows) {
    Stmt s{kUpsertPortfolio, {}};
    s.rows.reserve(rows.size());
    for (const auto& p : rows) s.rows.push_back({p.user, p.pf, p.owner, p.name, p.algo, p.created, p.note, tokenValue(p.token)});
    if (!s.rows.empty()) txWrite({s});
}

std::optional<std::map<int64_t, int64_t>> StateDB::loadInts(BookTable table, const std::string& book) {
    const BookCols c = cols(table);
    auto rows = txRead(std::string("SELECT ") + c.key + ", " + c.val + " FROM " + c.table + " WHERE book = ?", {book});
    if (rows.empty()) return std::nullopt;                 // the book has no rows at all
    std::map<int64_t, int64_t> out;
    for (const auto& r : rows) out[asInt(r[0])] = asInt(r[1]);
    return out;
}

void StateDB::saveInts(BookTable table, const std::string& book, const std::map<int64_t, int64_t>& data) {
    const BookCols c = cols(table);
    Stmt del{std::string("DELETE FROM ") + c.table + " WHERE book = ?", {{book}}};
    Stmt ins{std::string("INSERT INTO ") + c.table + " (book, " + c.key + ", " + c.val + ") VALUES (?, ?, ?)", {}};
    for (const auto& [k, v] : data) ins.rows.push_back({book, k, v});
    txWrite({del, ins});                                   // replace-all in one transaction
}

std::vector<Limit> StateDB::loadLimits() {
    auto rows = txRead("SELECT strategy, turnover, m2m, turnover_hit, m2m_hit FROM algo_limits", {});
    std::vector<Limit> out;
    out.reserve(rows.size());
    for (const auto& r : rows)
        out.push_back({asText(r[0]), asNum(r[1]), asNum(r[2]), asBool(r[3]), asBool(r[4])});
    return out;
}

void StateDB::saveLimits(const std::vector<Limit>& rows) {
    Stmt s{kUpsertLimit, {}};
    s.rows.reserve(rows.size());
    for (const auto& l : rows) s.rows.push_back({l.strategy, num(l.turnover), num(l.m2m), l.turnover_hit, l.m2m_hit});
    if (!s.rows.empty()) txWrite({s});
}

}  // namespace algo
