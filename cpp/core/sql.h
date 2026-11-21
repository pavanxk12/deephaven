// A tiny SQL seam so one piece of logic (StateDB) runs on two backends without caring which: libpq Postgres in
// production, SQLite in tests. This is the C++ counterpart of algo/core/statedb.py's injectable `connect` +
// `paramstyle` -- the Python tests point it at SQLite the same way. Control-plane only (startup/command handling),
// never the hot path: std::string, exceptions and virtual calls are all fine here, the same as master.cpp.
//
// Statements are written with '?' placeholders (SQLite's native style). The Postgres backend rewrites the i-th '?'
// to '$i' for PQexecParams; both bind by position, so one template serves both. Every value comes back as text
// (PQgetvalue's model), which the caller parses -- SQL NULL is std::nullopt.
#pragma once
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace algo {

// A bound parameter. nullptr_t is SQL NULL; bool binds as Postgres 'true'/'false' / SQLite 0|1.
using DbValue = std::variant<std::nullptr_t, int64_t, double, bool, std::string>;

// One result row: a cell is std::nullopt for SQL NULL, else the value as text.
using Row = std::vector<std::optional<std::string>>;

// One open connection. Not thread-safe on its own; StateDB serialises access. A method throws std::runtime_error on
// any database error, so the caller's retry logic can drop and rebuild the connection (statements are idempotent).
class SqlConn {
public:
    virtual ~SqlConn() = default;
    virtual void begin() = 0;
    virtual void commit() = 0;
    virtual void rollback() = 0;
    virtual void exec(const std::string& sql, const std::vector<DbValue>& params) = 0;
    virtual std::vector<Row> query(const std::string& sql, const std::vector<DbValue>& params) = 0;
};

// A libpq connection from a conninfo string (see pgConninfo). Throws if it cannot connect.
std::unique_ptr<SqlConn> makePgConn(const std::string& conninfo);

// A SQLite connection to `path` (":memory:" for tests). Throws if it cannot open.
std::unique_ptr<SqlConn> makeSqliteConn(const std::string& path);

}  // namespace algo
