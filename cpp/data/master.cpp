#include "data/master.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <tuple>

#include <libpq-fe.h>
#include <sqlite3.h>

#include "core/ini.h"
#include "core/tz.h"

namespace algo {

namespace fs = std::filesystem;
namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

std::string upper(std::string s) {
    for (auto& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

bool tryParseDouble(const std::string& text, double& out) {
    const std::string s = Ini::trim(text);
    if (s.empty()) return false;
    try {
        size_t pos = 0;
        out = std::stod(s, &pos);
        while (pos < s.size() && std::isspace(static_cast<unsigned char>(s[pos]))) ++pos;
        return pos == s.size();
    } catch (...) {
        return false;
    }
}

int32_t yyyymmddFromDigits(double v) {
    if (!std::isfinite(v)) return -1;
    const int64_t n = static_cast<int64_t>(v);
    const int m = static_cast<int>(n / 100 % 100), d = static_cast<int>(n % 100);
    if (m < 1 || m > 12 || d < 1 || d > 31) return -1;
    return static_cast<int32_t>(n);
}

bool parseIsoDate(const std::string& s, int32_t& out) {    // "yyyy-mm-dd"
    if (s.size() != 10 || s[4] != '-' || s[7] != '-') return false;
    for (size_t i : {0, 1, 2, 3, 5, 6, 8, 9}) if (!std::isdigit(static_cast<unsigned char>(s[i]))) return false;
    const int y = std::stoi(s.substr(0, 4)), m = std::stoi(s.substr(5, 2)), d = std::stoi(s.substr(8, 2));
    if (m < 1 || m > 12 || d < 1 || d > 31) return false;
    out = static_cast<int32_t>(y * 10000 + m * 100 + d);
    return true;
}

// Fallback for a column that is mostly non-numeric text (rare for a real contract file: the real .db3/Postgres
// ExpiryDate is always numeric). Covers "yyyy-mm-dd", "yyyymmdd" and "dd-mm-yyyy"; anything else is NaT, same as an
// unparsable value would be to pandas.
bool parseDateText(const std::string& raw, int32_t& out) {
    const std::string s = Ini::trim(raw);
    if (parseIsoDate(s, out)) return true;
    if (s.size() == 8 && std::all_of(s.begin(), s.end(), [](char c) { return std::isdigit(static_cast<unsigned char>(c)); })) {
        const int y = std::stoi(s.substr(0, 4)), m = std::stoi(s.substr(4, 2)), d = std::stoi(s.substr(6, 2));
        if (m >= 1 && m <= 12 && d >= 1 && d <= 31) { out = static_cast<int32_t>(y * 10000 + m * 100 + d); return true; }
    }
    if (s.size() == 10 && s[2] == '-' && s[5] == '-') {
        const int d = std::stoi(s.substr(0, 2)), m = std::stoi(s.substr(3, 2)), y = std::stoi(s.substr(6, 4));
        if (m >= 1 && m <= 12 && d >= 1 && d <= 31) { out = static_cast<int32_t>(y * 10000 + m * 100 + d); return true; }
    }
    return false;
}

void checkExchange(const std::vector<std::string>& exch_values, const Market& mk, const std::string& where) {
    if (exch_values.empty()) return;
    const std::set<std::string> found(exch_values.begin(), exch_values.end());
    const bool wrong = (mk.code == "NSE" && std::any_of(found.begin(), found.end(), [](const std::string& s) { return s != "NSE"; })) ||
                       (mk.code == "US" && found.count("NSE") != 0);
    if (wrong)
        throw std::runtime_error(where + ": lists an exchange that is not " + mk.code +
                                 "; it looks like the other market's contract file for this day");
}

// One row of normalise(): everything except `expiry`, which the whole column decides together (see parseExpiryColumn).
RawRow makeRow(int64_t token, const std::string& symbol, const std::string& inst, const std::string& option_type_text,
              std::optional<double> strike_raw, std::optional<double> tick_raw, std::optional<double> lot_raw,
              std::optional<double> divisor_raw, const Market& mk) {
    RawRow r;
    r.token = token;
    r.symbol = upper(Ini::trim(symbol));
    r.inst_type = upper(Ini::trim(inst));
    r.option_type = option_type_text.empty() ? std::string() : upper(Ini::trim(option_type_text));
    const double div_eff = (divisor_raw && std::isfinite(*divisor_raw) && *divisor_raw > 0) ? *divisor_raw : kNaN;
    r.strike = strike_raw ? (*strike_raw) / (std::isfinite(div_eff) ? div_eff : mk.strike_div) : kNaN;
    r.tick = tick_raw ? (*tick_raw) / (std::isfinite(div_eff) ? div_eff : mk.tick_div) : kNaN;
    r.divisor = std::isfinite(div_eff) ? div_eff : mk.price_div;
    r.lot = lot_raw ? *lot_raw : kNaN;
    r.expiry = -1;        // filled in afterwards, once for the whole column
    return r;
}

}  // namespace

// =================================================================================================
// parsing (pure)
// =================================================================================================
std::vector<int32_t> parseExpiryColumn(const std::vector<std::string>& raw_text, const std::string& expiry_epoch) {
    const size_t n = raw_text.size();
    std::vector<double> parsed(n, kNaN);
    size_t numeric_count = 0;
    for (size_t i = 0; i < n; ++i) if (tryParseDouble(raw_text[i], parsed[i])) ++numeric_count;

    std::vector<int32_t> out(n, -1);
    if (n > 0 && static_cast<double>(numeric_count) / static_cast<double>(n) > 0.5) {
        std::vector<double> finite;
        finite.reserve(numeric_count);
        for (double v : parsed) if (std::isfinite(v)) finite.push_back(v);
        std::sort(finite.begin(), finite.end());
        const double median = finite.empty() ? 0.0
            : (finite.size() % 2 ? finite[finite.size() / 2]
                                 : (finite[finite.size() / 2 - 1] + finite[finite.size() / 2]) / 2.0);
        if (median >= 19000101.0 && median <= 21001231.0) {
            for (size_t i = 0; i < n; ++i) if (std::isfinite(parsed[i])) out[i] = yyyymmddFromDigits(parsed[i]);
        } else {
            int32_t epoch_ymd = 0;
            if (!parseIsoDate(expiry_epoch, epoch_ymd)) throw std::runtime_error("bad expiry_epoch (want yyyy-mm-dd): " + expiry_epoch);
            const int64_t epoch_days = daysFromCivil(epoch_ymd / 10000, static_cast<unsigned>(epoch_ymd / 100 % 100), static_cast<unsigned>(epoch_ymd % 100));
            const double unit_s = median > 1e11 ? 1e-3 : 1.0;    // ms vs s, same 1e11 cutoff as the Python converter
            for (size_t i = 0; i < n; ++i) {
                if (!std::isfinite(parsed[i])) continue;
                const int64_t total_days = epoch_days + static_cast<int64_t>(std::floor(parsed[i] * unit_s / 86400.0));
                int y; unsigned m, d;
                civilFromDays(total_days, y, m, d);
                out[i] = static_cast<int32_t>(y * 10000 + static_cast<int32_t>(m) * 100 + static_cast<int32_t>(d));
            }
        }
    } else {
        for (size_t i = 0; i < n; ++i) { int32_t v; if (parseDateText(raw_text[i], v)) out[i] = v; }
    }
    return out;
}

std::string expiryText(int32_t yyyymmdd) {
    if (yyyymmdd < 0) return "";
    static const char* kMon[] = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};
    const int y = yyyymmdd / 10000, m = (yyyymmdd / 100) % 100, d = yyyymmdd % 100;
    if (m < 1 || m > 12) return "";
    char buf[16];
    std::snprintf(buf, sizeof buf, "%02d%s%02d", d, kMon[m - 1], y % 100);
    return buf;
}

std::string strikeText(double strike) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.10g", strike);
    return buf;
}

// =================================================================================================
// classify (pure, ported from classify()/build_symbol_map())
// =================================================================================================
MasterData MasterData::build(std::vector<RawRow> rows) {
    MasterData m;
    std::vector<Contract> all;
    all.reserve(rows.size());

    static const std::set<std::string> kEqTypes = {"EQUITY", "EQ", "STK", "ETF", "INDEX"};
    for (auto& r : rows) {
        if (r.token < 0) continue;
        const char first = r.option_type.empty() ? 0 : r.option_type[0];
        const char opt_type = (first == 'C') ? 'C' : (first == 'P') ? 'P' : 0;
        const bool dated = r.expiry >= 0;
        const bool starts_opt = r.inst_type.rfind("OPT", 0) == 0;
        const bool starts_fut = r.inst_type.rfind("FUT", 0) == 0;
        const bool is_opt = (starts_opt || opt_type != 0) && dated;
        const bool is_fut = starts_fut && !is_opt && dated;
        const bool is_eq = kEqTypes.count(r.inst_type) != 0 && !(starts_opt || opt_type != 0) && !starts_fut;

        Contract c;
        c.token = r.token;
        c.symbol = r.symbol;
        if (is_opt) {
            if (opt_type == 0 || !(r.strike > 0)) continue;             // opt = is_opt & OptionType.notna() & strike>0
            c.kind = Kind::kOpt; c.opt_type = opt_type; c.expiry = r.expiry; c.strike = r.strike;
            c.tick = r.tick; c.lot = r.lot; c.divisor = r.divisor;
            c.display = c.symbol + " " + expiryText(c.expiry) + " " + strikeText(c.strike) + " " + (opt_type == 'C' ? "CE" : "PE");
            m.opt.push_back(c);
        } else if (is_fut) {
            c.kind = Kind::kFut; c.expiry = r.expiry; c.tick = r.tick; c.lot = r.lot; c.divisor = r.divisor;
            c.display = c.symbol + " " + expiryText(c.expiry) + " FUT";
            m.futs.push_back(c);
        } else if (is_eq) {
            c.kind = Kind::kEq; c.tick = r.tick; c.divisor = r.divisor;
            c.display = c.symbol;
            m.eqs.push_back(c);
        } else {
            continue;                                                   // matches none of opt/futs/eqs: dropped, like Python
        }
        all.push_back(c);
    }

    // chain: CE/PE pairs sharing (symbol, expiry, strike). First occurrence of each leg wins; this only differs from
    // Python's inner-merge + drop_duplicates when the SAME key has more than one CE or more than one PE row, which a
    // real contract file (Token is its primary key) never does.
    std::map<std::tuple<std::string, int32_t, double>, int64_t> ce_first, pe_first;
    for (auto& c : m.opt) {
        const auto key = std::make_tuple(c.symbol, c.expiry, c.strike);
        (c.opt_type == 'C' ? ce_first : pe_first).emplace(key, c.token);
    }
    for (auto& [key, ce_tok] : ce_first) {
        auto it = pe_first.find(key);
        if (it == pe_first.end()) continue;
        ChainRow row;
        row.ce_token = ce_tok; row.pe_token = it->second;
        row.symbol = std::get<0>(key); row.expiry = std::get<1>(key); row.strike = std::get<2>(key);
        m.chain.push_back(std::move(row));
    }

    // token -> dense id over every kept contract, and the dense (cold) rows the host reads at lookup time
    std::vector<int64_t> toks;
    toks.reserve(all.size());
    for (auto& c : all) toks.push_back(c.token);
    m.index.build(toks, 0);
    m.contracts.assign(m.index.size(), Contract{});
    for (auto& c : all) m.contracts[static_cast<size_t>(m.index.find(c.token))] = c;

    // reverse display map: same precedence as Python's symbol_of dict (opt, then futs, then eqs)
    for (auto& c : m.opt)  m.token_of_[c.display] = c.token;
    for (auto& c : m.futs) m.token_of_[c.display] = c.token;
    for (auto& c : m.eqs)  m.token_of_[c.display] = c.token;

    for (auto& c : m.eqs) m.spot_of_.try_emplace(c.symbol, c.token);
    for (auto& c : m.futs) m.fut_info_[c.token] = {c.symbol, c.expiry};

    for (auto& c : m.futs) if (std::isfinite(c.tick) && c.tick > 0) m.tick_by_token_[c.token] = c.tick;
    for (auto& c : m.opt)  if (std::isfinite(c.tick) && c.tick > 0) m.tick_by_token_[c.token] = c.tick;
    for (auto& c : m.eqs)  if (std::isfinite(c.tick) && c.tick > 0) m.tick_by_token_[c.token] = c.tick;
    std::unordered_map<std::string, double> fut_tick_by_symbol;              // an equity falls back to its future's tick
    for (auto& c : m.futs) if (std::isfinite(c.tick)) fut_tick_by_symbol.try_emplace(c.symbol, c.tick);
    for (auto& c : m.eqs) {
        if (m.tick_by_token_.count(c.token)) continue;
        auto it = fut_tick_by_symbol.find(c.symbol);
        if (it != fut_tick_by_symbol.end() && it->second > 0) m.tick_by_token_[c.token] = it->second;
    }
    for (auto& c : m.futs) if (std::isfinite(c.divisor) && c.divisor > 0) m.divisor_by_token_[c.token] = c.divisor;
    for (auto& c : m.opt)  if (std::isfinite(c.divisor) && c.divisor > 0) m.divisor_by_token_[c.token] = c.divisor;
    for (auto& c : m.eqs)  if (std::isfinite(c.divisor) && c.divisor > 0) m.divisor_by_token_[c.token] = c.divisor;

    std::map<std::tuple<std::string, int32_t, char>, std::vector<std::pair<double, int64_t>>> groups;
    for (auto& c : m.opt) groups[{c.symbol, c.expiry, c.opt_type}].emplace_back(c.strike, c.token);
    for (auto& [key, items] : groups) {
        auto sorted_items = items;
        std::sort(sorted_items.begin(), sorted_items.end());
        std::vector<double> strikes; std::vector<int64_t> toks2;
        strikes.reserve(sorted_items.size()); toks2.reserve(sorted_items.size());
        for (auto& [s, t] : sorted_items) { strikes.push_back(s); toks2.push_back(t); }
        m.option_index_[OptKey{std::get<0>(key), std::get<1>(key), std::get<2>(key)}] = {std::move(strikes), std::move(toks2)};
        m.option_expiries_[ExpKey{std::get<0>(key), std::get<2>(key)}].push_back(std::get<1>(key));
    }
    for (auto& [k, v] : m.option_expiries_) std::sort(v.begin(), v.end());

    return m;
}

bool MasterData::OptKey::operator==(const OptKey& o) const noexcept { return symbol == o.symbol && expiry == o.expiry && opt_type == o.opt_type; }
size_t MasterData::OptKeyHash::operator()(const OptKey& k) const noexcept {
    size_t h = std::hash<std::string>{}(k.symbol);
    h ^= std::hash<int32_t>{}(k.expiry) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
    h ^= std::hash<int>{}(k.opt_type) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
    return h;
}
bool MasterData::ExpKey::operator==(const ExpKey& o) const noexcept { return symbol == o.symbol && opt_type == o.opt_type; }
size_t MasterData::ExpKeyHash::operator()(const ExpKey& k) const noexcept {
    size_t h = std::hash<std::string>{}(k.symbol);
    h ^= std::hash<int>{}(k.opt_type) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
    return h;
}

// =================================================================================================
// lookups
// =================================================================================================
const Contract* MasterData::byToken(int64_t token) const noexcept {
    const int32_t id = index.find(token);
    return id == TokenIndex::kNone ? nullptr : &contracts[static_cast<size_t>(id)];
}

std::optional<int64_t> MasterData::tokenOf(const std::string& exact_symbol_upper) const {
    auto it = token_of_.find(exact_symbol_upper);
    return it == token_of_.end() ? std::nullopt : std::optional<int64_t>(it->second);
}

std::optional<double> MasterData::tickOf(int64_t token) const {
    auto it = tick_by_token_.find(token);
    return it == tick_by_token_.end() ? std::nullopt : std::optional<double>(it->second);
}

std::optional<double> MasterData::divisorOf(int64_t token) const {
    auto it = divisor_by_token_.find(token);
    return it == divisor_by_token_.end() ? std::nullopt : std::optional<double>(it->second);
}

std::optional<int64_t> MasterData::spotOf(const std::string& underlying) const {
    auto it = spot_of_.find(underlying);
    return it == spot_of_.end() ? std::nullopt : std::optional<int64_t>(it->second);
}

std::optional<std::pair<std::string, int32_t>> MasterData::futInfo(int64_t fut_token) const {
    auto it = fut_info_.find(fut_token);
    return it == fut_info_.end() ? std::nullopt : std::optional<std::pair<std::string, int32_t>>(it->second);
}

std::optional<int64_t> MasterData::nearestFuture(const std::string& symbol, int32_t today_yyyymmdd) const {
    std::optional<int64_t> best;
    int32_t best_exp = std::numeric_limits<int32_t>::max();
    for (auto& c : futs)
        if (c.symbol == symbol && c.expiry >= today_yyyymmdd && c.expiry < best_exp) { best_exp = c.expiry; best = c.token; }
    return best;
}

std::vector<int32_t> MasterData::expiries(const std::string& symbol, char option_type) const {
    auto it = option_expiries_.find(ExpKey{symbol, option_type});
    return it == option_expiries_.end() ? std::vector<int32_t>{} : it->second;
}

bool MasterData::strikes(const std::string& symbol, int32_t expiry, char option_type,
                         std::vector<double>& strikes_out, std::vector<int64_t>& tokens_out) const {
    auto it = option_index_.find(OptKey{symbol, expiry, option_type});
    if (it == option_index_.end()) return false;
    strikes_out = it->second.first;
    tokens_out = it->second.second;
    return true;
}

std::vector<int64_t> MasterData::tokensFor(const std::vector<std::string>* symbols, std::optional<Kind> kind,
                                           std::optional<int32_t> expiries_n) const {
    std::optional<std::set<std::string>> want;
    if (symbols) {
        std::set<std::string> s;
        for (auto& x : *symbols) s.insert(upper(x));
        want = std::move(s);
    }
    std::vector<int64_t> out;
    auto collectOpt = [&]() {
        if (!expiries_n) { for (auto& c : opt) if (!want || want->count(c.symbol)) out.push_back(c.token); return; }
        std::map<std::string, std::vector<int32_t>> exp_by_sym;
        for (auto& c : opt) if (!want || want->count(c.symbol)) exp_by_sym[c.symbol].push_back(c.expiry);
        std::map<std::string, std::set<int32_t>> keep;
        for (auto& [sym, exps] : exp_by_sym) {
            std::vector<int32_t> u = exps;
            std::sort(u.begin(), u.end());
            u.erase(std::unique(u.begin(), u.end()), u.end());
            if (static_cast<int32_t>(u.size()) > *expiries_n) u.resize(static_cast<size_t>(*expiries_n));
            keep[sym] = std::set<int32_t>(u.begin(), u.end());
        }
        for (auto& c : opt) {
            if (want && !want->count(c.symbol)) continue;
            auto it = keep.find(c.symbol);
            if (it != keep.end() && it->second.count(c.expiry)) out.push_back(c.token);
        }
    };
    if (!kind || *kind == Kind::kFut) for (auto& c : futs) if (!want || want->count(c.symbol)) out.push_back(c.token);
    if (!kind || *kind == Kind::kOpt) collectOpt();
    if (!kind || *kind == Kind::kEq)  for (auto& c : eqs)  if (!want || want->count(c.symbol)) out.push_back(c.token);
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    if (want && out.empty()) throw std::runtime_error("no instruments in master for the given symbols");
    return out;
}

// =================================================================================================
// files
// =================================================================================================
std::string findDb(const std::string& directory, const std::string& day) {
    const std::string exact = directory + "/" + day + ".db3";
    std::vector<std::string> others;
    if (fs::exists(directory)) {
        for (auto& e : fs::directory_iterator(directory)) {
            if (!e.is_regular_file()) continue;
            const std::string name = e.path().filename().string();
            if (name.size() >= 4 && name.substr(name.size() - 4) == ".db3" && name.find(day) != std::string::npos &&
                e.path().string() != exact)
                others.push_back(e.path().string());
        }
    }
    std::sort(others.begin(), others.end());
    if (fs::exists(exact)) return exact;
    if (!others.empty()) return others.back();
    throw std::runtime_error("no .db3 for " + day + " in " + directory);
}

std::vector<RawRow> readContractsDb3(const std::string& db_path, const Market& mk) {
    if (mk.columns.size() < 9) throw std::runtime_error("market .ini [contracts] columns needs at least 9 names (token..divisor)");
    const std::string &col_token = mk.columns[0], &col_symbol = mk.columns[1], &col_expiry = mk.columns[2],
                      &col_inst = mk.columns[3], &col_opt = mk.columns[4], &col_strike = mk.columns[5],
                      &col_lot = mk.columns[6], &col_tick = mk.columns[7], &col_divisor = mk.columns[8];
    const std::string col_exch = mk.columns.size() > 9 ? mk.columns[9] : std::string();

    sqlite3* db = nullptr;
    const std::string uri = "file:" + db_path + "?mode=ro";
    if (sqlite3_open_v2(uri.c_str(), &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, nullptr) != SQLITE_OK) {
        const std::string err = db ? sqlite3_errmsg(db) : "cannot open";
        if (db) sqlite3_close(db);
        throw std::runtime_error("cannot open " + db_path + ": " + err);
    }

    auto tableColumns = [&](const std::string& table) {
        std::set<std::string> have;
        sqlite3_stmt* p = nullptr;
        sqlite3_prepare_v2(db, ("PRAGMA table_info(\"" + table + "\")").c_str(), -1, &p, nullptr);
        while (sqlite3_step(p) == SQLITE_ROW) have.insert(reinterpret_cast<const char*>(sqlite3_column_text(p, 1)));
        sqlite3_finalize(p);
        return have;
    };

    std::string table;
    {
        sqlite3_stmt* st = nullptr;
        sqlite3_prepare_v2(db, "SELECT name FROM sqlite_master WHERE type IN ('table','view')", -1, &st, nullptr);
        std::vector<std::string> names;
        while (sqlite3_step(st) == SQLITE_ROW) names.emplace_back(reinterpret_cast<const char*>(sqlite3_column_text(st, 0)));
        sqlite3_finalize(st);
        for (auto& n : names) {
            const auto have = tableColumns(n);
            if (have.count(col_token) && have.count(col_symbol) && have.count(col_expiry) && have.count(col_inst)) { table = n; break; }
        }
    }
    if (table.empty()) { sqlite3_close(db); throw std::runtime_error("no table with columns " + col_token + "," + col_symbol + "," + col_expiry + "," + col_inst + " in " + db_path); }

    const auto have = tableColumns(table);
    auto wantCol = [&](const std::string& c) { return (!c.empty() && have.count(c)) ? c : std::string(); };
    const std::string q_opt = wantCol(col_opt), q_strike = wantCol(col_strike), q_lot = wantCol(col_lot),
                      q_tick = wantCol(col_tick), q_divisor = wantCol(col_divisor), q_exch = wantCol(col_exch);

    std::vector<std::string> select_cols = {col_token, col_symbol, col_expiry, col_inst};
    for (auto& c : {q_opt, q_strike, q_lot, q_tick, q_divisor, q_exch}) if (!c.empty()) select_cols.push_back(c);
    std::string sql = "SELECT ";
    for (size_t i = 0; i < select_cols.size(); ++i) { if (i) sql += ", "; sql += "\"" + select_cols[i] + "\""; }
    sql += " FROM \"" + table + "\"";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) {
        const std::string err = sqlite3_errmsg(db);
        sqlite3_close(db);
        throw std::runtime_error("query failed on " + db_path + ": " + err);
    }
    std::unordered_map<std::string, int> idx;
    for (size_t i = 0; i < select_cols.size(); ++i) idx[select_cols[i]] = static_cast<int>(i);

    auto getText = [&](const std::string& col) -> std::string {
        auto it = idx.find(col);
        if (it == idx.end()) return "";
        const unsigned char* t = sqlite3_column_text(st, it->second);
        return t ? reinterpret_cast<const char*>(t) : "";
    };
    auto getDouble = [&](const std::string& col) -> std::optional<double> {
        auto it = idx.find(col);
        if (it == idx.end() || sqlite3_column_type(st, it->second) == SQLITE_NULL) return std::nullopt;
        return sqlite3_column_double(st, it->second);
    };

    std::vector<RawRow> rows;
    std::vector<std::string> expiry_text, exch_values;
    while (sqlite3_step(st) == SQLITE_ROW) {
        auto it = idx.find(col_token);
        const int64_t token = sqlite3_column_type(st, it->second) == SQLITE_NULL ? -1 : sqlite3_column_int64(st, it->second);
        rows.push_back(makeRow(token, getText(col_symbol), getText(col_inst), q_opt.empty() ? std::string() : getText(q_opt),
                               q_strike.empty() ? std::nullopt : getDouble(q_strike), q_tick.empty() ? std::nullopt : getDouble(q_tick),
                               q_lot.empty() ? std::nullopt : getDouble(q_lot), q_divisor.empty() ? std::nullopt : getDouble(q_divisor), mk));
        expiry_text.push_back(getText(col_expiry));
        if (!q_exch.empty()) exch_values.push_back(upper(Ini::trim(getText(q_exch))));
    }
    sqlite3_finalize(st);
    sqlite3_close(db);

    const auto expiry = parseExpiryColumn(expiry_text, mk.expiry_epoch);
    for (size_t i = 0; i < rows.size(); ++i) rows[i].expiry = expiry[i];
    checkExchange(exch_values, mk, db_path);
    return rows;
}

std::vector<RawRow> readContractsPg(const std::string& day, const Market& mk, const std::string& conninfo) {
    if (day.size() != 8 || !std::all_of(day.begin(), day.end(), [](char c) { return std::isdigit(static_cast<unsigned char>(c)); }))
        throw std::runtime_error("the day must be yyyymmdd, got " + day);
    if (mk.pg_table.empty()) throw std::runtime_error("market .ini has no [contracts] pg_table pattern");
    const std::string table = substituteDay(mk.pg_table, day);

    PGconn* conn = PQconnectdb(conninfo.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(conn);
        PQfinish(conn);
        throw std::runtime_error("Postgres connect failed: " + err);
    }
    auto run = [&](const std::string& sql) -> PGresult* {
        PGresult* r = PQexec(conn, sql.c_str());
        if (PQresultStatus(r) != PGRES_TUPLES_OK) {
            const std::string err = PQresultErrorMessage(r);
            PQclear(r);
            PQfinish(conn);
            throw std::runtime_error(err);
        }
        return r;
    };

    PGresult* cols_res = run("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '" + table + "'");
    std::unordered_map<std::string, std::string> have;     // lower-case -> actual column name
    for (int i = 0; i < PQntuples(cols_res); ++i) {
        const std::string actual = PQgetvalue(cols_res, i, 0);
        have[upper(actual)] = actual;                       // compare case-insensitively via upper(); see lowerKey below
    }
    PQclear(cols_res);
    if (have.empty()) { PQfinish(conn); throw std::runtime_error("Postgres has no table " + table); }

    if (mk.columns.size() < 9) { PQfinish(conn); throw std::runtime_error("market .ini [contracts] columns needs at least 9 names"); }
    const std::string &col_token = mk.columns[0], &col_symbol = mk.columns[1], &col_expiry = mk.columns[2], &col_inst = mk.columns[3],
                      &col_opt = mk.columns[4], &col_strike = mk.columns[5], &col_lot = mk.columns[6], &col_tick = mk.columns[7],
                      &col_divisor = mk.columns[8];
    const std::string col_exch = mk.columns.size() > 9 ? mk.columns[9] : std::string();

    for (auto* req : {&col_token, &col_symbol, &col_expiry, &col_inst})
        if (!have.count(upper(*req))) { PQfinish(conn); throw std::runtime_error(table + " lacks column " + *req); }
    auto wantCol = [&](const std::string& c) -> std::string { return (!c.empty() && have.count(upper(c))) ? c : std::string(); };
    const std::string q_opt = wantCol(col_opt), q_strike = wantCol(col_strike), q_lot = wantCol(col_lot),
                      q_tick = wantCol(col_tick), q_divisor = wantCol(col_divisor), q_exch = wantCol(col_exch);

    std::vector<std::string> want = {col_token, col_symbol, col_expiry, col_inst};
    for (auto& c : {q_opt, q_strike, q_lot, q_tick, q_divisor, q_exch}) if (!c.empty()) want.push_back(c);
    std::string sql = "SELECT ";
    for (size_t i = 0; i < want.size(); ++i) { if (i) sql += ", "; sql += "\"" + have[upper(want[i])] + "\" AS \"" + want[i] + "\""; }
    sql += " FROM \"" + table + "\"";

    PGresult* r = run(sql);
    const int n = PQntuples(r);
    if (n == 0) { PQclear(r); PQfinish(conn); throw std::runtime_error(table + " is empty (a market holiday?)"); }

    std::unordered_map<std::string, int> fidx;
    for (int i = 0; i < PQnfields(r); ++i) fidx[PQfname(r, i)] = i;
    auto getText = [&](int row, const std::string& col) -> std::string {
        auto it = fidx.find(col);
        if (it == fidx.end() || PQgetisnull(r, row, it->second)) return "";
        return PQgetvalue(r, row, it->second);
    };
    auto getDouble = [&](int row, const std::string& col) -> std::optional<double> {
        auto it = fidx.find(col);
        if (it == fidx.end() || PQgetisnull(r, row, it->second)) return std::nullopt;
        double v;
        if (!tryParseDouble(PQgetvalue(r, row, it->second), v)) return std::nullopt;
        return v;
    };

    std::vector<RawRow> rows;
    rows.reserve(static_cast<size_t>(n));
    std::vector<std::string> expiry_text, exch_values;
    for (int i = 0; i < n; ++i) {
        const std::string tok_text = getText(i, col_token);
        int64_t token = -1;
        if (!tok_text.empty()) { try { token = std::stoll(tok_text); } catch (...) { token = -1; } }
        rows.push_back(makeRow(token, getText(i, col_symbol), getText(i, col_inst), q_opt.empty() ? std::string() : getText(i, q_opt),
                               q_strike.empty() ? std::nullopt : getDouble(i, q_strike), q_tick.empty() ? std::nullopt : getDouble(i, q_tick),
                               q_lot.empty() ? std::nullopt : getDouble(i, q_lot), q_divisor.empty() ? std::nullopt : getDouble(i, q_divisor), mk));
        expiry_text.push_back(getText(i, col_expiry));
        if (!q_exch.empty()) exch_values.push_back(upper(Ini::trim(getText(i, q_exch))));
    }
    PQclear(r);
    PQfinish(conn);

    const auto expiry = parseExpiryColumn(expiry_text, mk.expiry_epoch);
    for (size_t i = 0; i < rows.size(); ++i) rows[i].expiry = expiry[i];
    checkExchange(exch_values, mk, "Postgres " + table);
    return rows;
}

std::string substituteDay(const std::string& pattern, const std::string& day) {
    std::string out = pattern;
    const std::string token = "<yyyymmdd>";
    size_t pos = 0;
    while ((pos = out.find(token, pos)) != std::string::npos) { out.replace(pos, token.size(), day); pos += day.size(); }
    return out;
}

std::string findDbByPattern(const std::string& directory, const std::string& file_pattern, const std::string& day) {
    if (!file_pattern.empty()) {
        const std::string exact = directory + "/" + substituteDay(file_pattern, day);
        if (fs::exists(exact)) return exact;
    }
    return findDb(directory, day);
}

MasterData loadMaster(const Market& mk, const std::string& day_yyyymmdd, const std::string& market_data_dir,
                      const std::string& pg_conninfo, std::string& used_source, std::string& warning) {
    used_source.clear();
    warning.clear();
    const bool want_pg = (mk.contracts_source == "postgres" || mk.contracts_source == "auto") && !pg_conninfo.empty();
    if (want_pg) {
        try {
            auto rows = readContractsPg(day_yyyymmdd, mk, pg_conninfo);
            used_source = "postgres:" + substituteDay(mk.pg_table, day_yyyymmdd);
            return MasterData::build(std::move(rows));
        } catch (const std::exception& e) {
            if (mk.contracts_source == "postgres") throw;    // source pinned to Postgres: the failure is the caller's to see
            warning = std::string("the contracts were not read from Postgres (") + e.what() + "); using the .db3 file";
        }
    }
    const std::string path = findDbByPattern(market_data_dir, mk.file_pattern, day_yyyymmdd);
    auto rows = readContractsDb3(path, mk);
    used_source = "file:" + path;
    return MasterData::build(std::move(rows));
}

}  // namespace algo
