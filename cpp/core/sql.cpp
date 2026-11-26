#include "core/sql.h"

#include <cstdio>
#include <stdexcept>

#include <libpq-fe.h>
#include <sqlite3.h>

namespace algo {
namespace {

std::string toText(const DbValue& v) {
    if (std::holds_alternative<int64_t>(v)) return std::to_string(std::get<int64_t>(v));
    if (std::holds_alternative<bool>(v)) return std::get<bool>(v) ? "true" : "false";
    if (std::holds_alternative<double>(v)) {
        char buf[32];
        std::snprintf(buf, sizeof buf, "%.17g", std::get<double>(v));
        return buf;
    }
    return std::get<std::string>(v);
}

// '?' -> '$1', '$2', ... for libpq, leaving '?' inside single-quoted literals alone (StateDB uses none, but a
// literal '?' would otherwise shift the numbering). Returns the rewritten SQL.
std::string questionToDollar(const std::string& sql) {
    std::string out;
    out.reserve(sql.size() + 8);
    int n = 0;
    bool in_str = false;
    for (char c : sql) {
        if (c == '\'') in_str = !in_str;
        if (c == '?' && !in_str) { out += '$'; out += std::to_string(++n); }
        else out += c;
    }
    return out;
}

class PgConn final : public SqlConn {
public:
    explicit PgConn(const std::string& conninfo) : conn_(PQconnectdb(conninfo.c_str())) {
        if (PQstatus(conn_) != CONNECTION_OK) {
            const std::string err = conn_ ? PQerrorMessage(conn_) : "out of memory";
            if (conn_) PQfinish(conn_);
            conn_ = nullptr;
            throw std::runtime_error("Postgres connect failed: " + err);
        }
    }
    ~PgConn() override { if (conn_) PQfinish(conn_); }

    void begin() override { simple("BEGIN"); }
    void commit() override { simple("COMMIT"); }
    void rollback() override { simple("ROLLBACK"); }

    void exec(const std::string& sql, const std::vector<DbValue>& params) override { clear(run(sql, params, false)); }

    bool hasColumn(const std::string& table, const std::string& column) override {
        return !query("SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() "
                      "AND table_name = ? AND column_name = ?", {table, column}).empty();
    }

    std::vector<Row> query(const std::string& sql, const std::vector<DbValue>& params) override {
        PGresult* r = run(sql, params, true);
        const int rows = PQntuples(r), cols = PQnfields(r);
        std::vector<Row> out;
        out.reserve(rows);
        for (int i = 0; i < rows; ++i) {
            Row row;
            row.reserve(cols);
            for (int j = 0; j < cols; ++j)
                row.push_back(PQgetisnull(r, i, j) ? std::nullopt : std::optional<std::string>(PQgetvalue(r, i, j)));
            out.push_back(std::move(row));
        }
        PQclear(r);
        return out;
    }

private:
    void simple(const char* sql) {
        PGresult* r = PQexec(conn_, sql);
        const bool ok = PQresultStatus(r) == PGRES_COMMAND_OK;
        std::string err = ok ? "" : PQresultErrorMessage(r);
        PQclear(r);
        if (!ok) throw std::runtime_error(err);
    }
    PGresult* run(const std::string& sql, const std::vector<DbValue>& params, bool want_rows) {
        const std::string rewritten = questionToDollar(sql);
        std::vector<std::string> texts;     // owns the strings paramValues points into
        texts.reserve(params.size());
        std::vector<const char*> values;
        values.reserve(params.size());
        for (const auto& p : params) {
            if (std::holds_alternative<std::nullptr_t>(p)) { values.push_back(nullptr); texts.emplace_back(); }
            else { texts.push_back(toText(p)); values.push_back(nullptr); }
        }
        for (size_t i = 0; i < params.size(); ++i)
            if (!std::holds_alternative<std::nullptr_t>(params[i])) values[i] = texts[i].c_str();
        PGresult* r = PQexecParams(conn_, rewritten.c_str(), static_cast<int>(params.size()), nullptr,
                                   values.data(), nullptr, nullptr, 0);
        const ExecStatusType st = PQresultStatus(r);
        if (st != (want_rows ? PGRES_TUPLES_OK : PGRES_COMMAND_OK) && st != PGRES_TUPLES_OK) {
            const std::string err = PQresultErrorMessage(r);
            PQclear(r);
            throw std::runtime_error(err);
        }
        return r;
    }
    static void clear(PGresult* r) { PQclear(r); }

    PGconn* conn_;
};

class SqliteConn final : public SqlConn {
public:
    explicit SqliteConn(const std::string& path) {
        if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) {
            const std::string err = db_ ? sqlite3_errmsg(db_) : "out of memory";
            if (db_) sqlite3_close(db_);
            db_ = nullptr;
            throw std::runtime_error("SQLite open failed: " + err);
        }
    }
    ~SqliteConn() override { if (db_) sqlite3_close(db_); }

    void begin() override { simple("BEGIN"); }
    void commit() override { simple("COMMIT"); }
    void rollback() override { simple("ROLLBACK"); }

    void exec(const std::string& sql, const std::vector<DbValue>& params) override {
        sqlite3_stmt* st = prepare(sql, params);
        const int rc = sqlite3_step(st);
        if (rc != SQLITE_DONE && rc != SQLITE_ROW) { const std::string e = sqlite3_errmsg(db_); sqlite3_finalize(st); throw std::runtime_error(e); }
        sqlite3_finalize(st);
    }

    bool hasColumn(const std::string& table, const std::string& column) override {
        return !query("SELECT 1 FROM pragma_table_info(?) WHERE name = ?", {table, column}).empty();
    }

    std::vector<Row> query(const std::string& sql, const std::vector<DbValue>& params) override {
        sqlite3_stmt* st = prepare(sql, params);
        const int cols = sqlite3_column_count(st);
        std::vector<Row> out;
        int rc;
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            Row row;
            row.reserve(cols);
            for (int j = 0; j < cols; ++j) {
                if (sqlite3_column_type(st, j) == SQLITE_NULL) { row.push_back(std::nullopt); continue; }
                const unsigned char* t = sqlite3_column_text(st, j);
                row.emplace_back(t ? reinterpret_cast<const char*>(t) : "");
            }
            out.push_back(std::move(row));
        }
        if (rc != SQLITE_DONE) { const std::string e = sqlite3_errmsg(db_); sqlite3_finalize(st); throw std::runtime_error(e); }
        sqlite3_finalize(st);
        return out;
    }

private:
    void simple(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            const std::string msg = err ? err : "sqlite error";
            sqlite3_free(err);
            throw std::runtime_error(msg);
        }
    }
    sqlite3_stmt* prepare(const std::string& sql, const std::vector<DbValue>& params) {
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &st, nullptr) != SQLITE_OK)
            throw std::runtime_error(std::string("SQLite prepare: ") + sqlite3_errmsg(db_));
        for (size_t i = 0; i < params.size(); ++i) {
            const int k = static_cast<int>(i) + 1;
            const DbValue& p = params[i];
            int rc;
            if (std::holds_alternative<std::nullptr_t>(p)) rc = sqlite3_bind_null(st, k);
            else if (std::holds_alternative<int64_t>(p)) rc = sqlite3_bind_int64(st, k, std::get<int64_t>(p));
            else if (std::holds_alternative<bool>(p)) rc = sqlite3_bind_int(st, k, std::get<bool>(p) ? 1 : 0);
            else if (std::holds_alternative<double>(p)) rc = sqlite3_bind_double(st, k, std::get<double>(p));
            else rc = sqlite3_bind_text(st, k, std::get<std::string>(p).c_str(), -1, SQLITE_TRANSIENT);
            if (rc != SQLITE_OK) { const std::string e = sqlite3_errmsg(db_); sqlite3_finalize(st); throw std::runtime_error(e); }
        }
        return st;
    }

    sqlite3* db_ = nullptr;
};

}  // namespace

std::unique_ptr<SqlConn> makePgConn(const std::string& conninfo) { return std::make_unique<PgConn>(conninfo); }
std::unique_ptr<SqlConn> makeSqliteConn(const std::string& path) { return std::make_unique<SqliteConn>(path); }

}  // namespace algo
