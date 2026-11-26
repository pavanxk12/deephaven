#include "host/contract_api.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <limits>
#include <numeric>

namespace algo {

namespace {
int32_t asSdkToken(int64_t t) noexcept {
    return (t > 0 && t <= std::numeric_limits<int32_t>::max()) ? static_cast<int32_t>(t) : 0;
}
void copyText(char* dst, size_t n, const std::string& src) noexcept {
    std::strncpy(dst, src.c_str(), n - 1);
    dst[n - 1] = '\0';
}
}  // namespace

ContractApi::ContractApi(std::shared_ptr<const MasterData> master, int32_t today, double default_div)
    : master_(std::move(master)), today_(today), default_div_(default_div > 0 ? default_div : 100) {}

std::string ContractApi::norm(std::string_view s) {
    std::string out;
    bool space = false;
    for (char ch : s) {
        if (std::isspace(static_cast<unsigned char>(ch))) { space = !out.empty(); continue; }
        if (space) { out += ' '; space = false; }
        out += static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    return out;
}

int32_t ContractApi::find(std::string_view name) const {
    const std::string text = norm(name);
    if (text.empty()) return 0;
    if (std::all_of(text.begin(), text.end(), [](char c) { return c >= '0' && c <= '9'; })) {   // a token number
        if (text.size() > 18) return 0;
        const int64_t t = std::stoll(text);
        return master_->byToken(t) ? asSdkToken(t) : 0;
    }
    auto t = master_->tokenOf(text);                                   // exactly the contract of that symbol, nothing guessed
    return t ? asSdkToken(*t) : 0;
}

int32_t ContractApi::findSpot(std::string_view underlying) const {
    auto s = master_->spotOf(norm(underlying));                        // the cash stock or the index of that name
    return s ? asSdkToken(*s) : 0;
}

int64_t ContractApi::futureFor(const Contract& c) const {
    const auto& fs = master_->futures(c.symbol);
    if (c.kind == Kind::kFut) return c.token;
    // an option: the nearest future expiring on or after it (weekly options use the monthly future); anything else: the
    // nearest unexpired one. Same rule as the Python straddle's reference future.
    const int32_t from = (c.kind == Kind::kOpt && c.expiry > 0) ? c.expiry : today_;
    for (const auto& [exp, tok] : fs) if (exp >= from) return tok;
    return 0;
}

bool ContractApi::info(int32_t token, AlgoContract& out) const {
    out = AlgoContract{};
    const Contract* c = master_->byToken(token);
    if (!c || c->kind == Kind::kNone) return false;
    const double div = divisorOf(*c);
    out.token = token;
    out.kind = c->kind == Kind::kOpt ? AlgoKind::OPT : c->kind == Kind::kFut ? AlgoKind::FUT
             : c->is_index ? AlgoKind::INDEX : AlgoKind::EQ;
    out.opt_type = c->opt_type == 'C' ? AlgoOptType::CE : c->opt_type == 'P' ? AlgoOptType::PE : AlgoOptType::NONE;
    out.expiry = c->expiry > 0 ? c->expiry : 0;
    out.strike = c->kind == Kind::kOpt ? std::llround(c->strike * div) : 0;
    const auto tick = master_->tickOf(c->token);
    out.tick = tick ? std::llround(*tick * div) : 0;
    out.lot_size = static_cast<int32_t>(std::llround(c->lot));
    out.divisor = static_cast<int32_t>(std::llround(div));
    out.future_token = asSdkToken(futureFor(*c));
    if (auto s = master_->spotOf(c->symbol)) out.spot_token = asSdkToken(*s);
    if (auto a = master_->altOf(c->token)) out.alt_token = asSdkToken(*a);
    copyText(out.symbol, sizeof out.symbol, c->display);
    copyText(out.underlying, sizeof out.underlying, c->symbol);
    return true;
}

std::vector<int32_t> ContractApi::unexpiredOptionExpiries(const std::string& und, char type) const {
    std::vector<int32_t> out;
    for (int32_t e : master_->expiries(und, type)) if (e >= today_) out.push_back(e);
    return out;                                                         // master keeps them sorted
}

uint32_t ContractApi::expiries(std::string_view underlying, AlgoKind kind, int32_t* out, uint32_t max) const {
    const std::string und = norm(underlying);
    std::vector<int32_t> all;
    if (kind == AlgoKind::FUT) {
        for (const auto& [exp, tok] : master_->futures(und)) if (exp >= today_) all.push_back(exp);
    } else if (kind == AlgoKind::OPT) {
        for (char t : {'C', 'P'}) for (int32_t e : unexpiredOptionExpiries(und, t)) all.push_back(e);
    }
    std::sort(all.begin(), all.end());
    all.erase(std::unique(all.begin(), all.end()), all.end());
    const uint32_t n = static_cast<uint32_t>(std::min<size_t>(all.size(), max));
    std::copy_n(all.begin(), n, out);
    return n;
}

bool ContractApi::chain(const std::string& und, int32_t expiry_index, char type, Chain& out) const {
    if (!type || expiry_index < 0) return false;
    const auto exps = unexpiredOptionExpiries(und, type);
    if (static_cast<size_t>(expiry_index) >= exps.size()) return false;
    return master_->strikes(und, exps[static_cast<size_t>(expiry_index)], type, out.strikes, out.tokens);
}

int64_t ContractApi::minorStrike(int64_t token, double strike) const {
    const Contract* c = master_->byToken(token);
    return std::llround(strike * (c ? divisorOf(*c) : default_div_));
}

uint32_t ContractApi::strikes(std::string_view underlying, int32_t expiry_index, AlgoOptType t, int32_t* tokens, int64_t* strikes_out, uint32_t max) const {
    Chain ch;
    if (!chain(norm(underlying), expiry_index, typeChar(t), ch)) return 0;
    const uint32_t n = static_cast<uint32_t>(std::min<size_t>(ch.tokens.size(), max));
    for (uint32_t i = 0; i < n; ++i) {
        if (tokens) tokens[i] = asSdkToken(ch.tokens[i]);
        if (strikes_out) strikes_out[i] = minorStrike(ch.tokens[i], ch.strikes[i]);
    }
    return n;
}

bool ContractApi::referencePrice(const std::string& und, int64_t price, double& out) const {
    std::optional<int64_t> ref = master_->nearestFuture(und, today_);
    if (!ref) ref = master_->tokenOf(und);
    if (!ref) ref = master_->spotOf(und);
    const Contract* c = ref ? master_->byToken(*ref) : nullptr;
    out = static_cast<double>(price) / (c ? divisorOf(*c) : default_div_);
    return price > 0;
}

uint32_t ContractApi::nearest(std::string_view underlying, int64_t price, AlgoOptType t, int32_t expiry_index, uint32_t n, int32_t* out) const {
    const std::string und = norm(underlying);
    Chain ch;
    double px = 0;
    if (!referencePrice(und, price, px) || !chain(und, expiry_index, typeChar(t), ch)) return 0;
    std::vector<size_t> order(ch.strikes.size());
    std::iota(order.begin(), order.end(), size_t(0));
    std::stable_sort(order.begin(), order.end(),
                     [&](size_t a, size_t b) { return std::fabs(ch.strikes[a] - px) < std::fabs(ch.strikes[b] - px); });
    const uint32_t k = static_cast<uint32_t>(std::min<size_t>(order.size(), std::max<uint32_t>(n, 1)));
    for (uint32_t i = 0; i < k; ++i) out[i] = asSdkToken(ch.tokens[order[i]]);
    return k;
}

int32_t ContractApi::findFut(std::string_view underlying, int32_t expiry_index) const {
    if (expiry_index < 0) return 0;
    int32_t k = 0;
    for (const auto& [exp, tok] : master_->futures(norm(underlying))) {
        if (exp < today_) continue;
        if (k++ == expiry_index) return asSdkToken(tok);
    }
    return 0;
}

int32_t ContractApi::findOpt(std::string_view underlying, int32_t expiry_index, AlgoOptType t, int64_t strike) const {
    Chain ch;
    if (!chain(norm(underlying), expiry_index, typeChar(t), ch)) return 0;
    for (size_t i = 0; i < ch.tokens.size(); ++i)
        if (minorStrike(ch.tokens[i], ch.strikes[i]) == strike) return asSdkToken(ch.tokens[i]);
    return 0;
}

int32_t ContractApi::moneyness(std::string_view underlying, int64_t price, AlgoOptType t, int32_t expiry_index, int32_t steps) const {
    const std::string und = norm(underlying);
    Chain ch;
    double px = 0;
    if (!referencePrice(und, price, px) || !chain(und, expiry_index, typeChar(t), ch) || ch.strikes.empty()) return 0;
    size_t atm = 0;                                                      // the nearest; the first (lower) one on a tie
    for (size_t i = 1; i < ch.strikes.size(); ++i)
        if (std::fabs(ch.strikes[i] - px) < std::fabs(ch.strikes[atm] - px)) atm = i;
    const int64_t at = static_cast<int64_t>(atm) + (t == AlgoOptType::CE ? steps : -steps);   // OTM: CE up, PE down
    if (at < 0 || at >= static_cast<int64_t>(ch.tokens.size())) return 0;
    return asSdkToken(ch.tokens[static_cast<size_t>(at)]);
}

int32_t ContractApi::stepStrike(int32_t option_token, int32_t steps) const {
    const Contract* c = master_->byToken(option_token);
    if (!c || c->kind != Kind::kOpt) return 0;
    std::vector<double> s;
    std::vector<int64_t> toks;
    if (!master_->strikes(c->symbol, c->expiry, c->opt_type, s, toks)) return 0;
    const auto it = std::find(toks.begin(), toks.end(), static_cast<int64_t>(option_token));
    if (it == toks.end()) return 0;
    const int64_t at = (it - toks.begin()) + steps;
    if (at < 0 || at >= static_cast<int64_t>(toks.size())) return 0;
    return asSdkToken(toks[static_cast<size_t>(at)]);
}

// ---- the SDK function table ------------------------------------------------------------------------------------
namespace {
const ContractApi* C(void* ctx) noexcept { return ctx ? static_cast<HostCtx*>(ctx)->contracts : nullptr; }

int32_t  tFind(void* x, const char* n) noexcept { try { auto* c = C(x); return c && n ? c->find(n) : 0; } catch (...) { return 0; } }
bool     tInfo(void* x, int32_t t, AlgoContract* o) noexcept { try { auto* c = C(x); return c && o && c->info(t, *o); } catch (...) { return false; } }
uint32_t tExpiries(void* x, const char* u, AlgoKind k, int32_t* o, uint32_t m) noexcept {
    try { auto* c = C(x); return c && u && o ? c->expiries(u, k, o, m) : 0; } catch (...) { return 0; }
}
uint32_t tStrikes(void* x, const char* u, int32_t e, AlgoOptType t, int32_t* toks, int64_t* s, uint32_t m) noexcept {
    try { auto* c = C(x); return c && u ? c->strikes(u, e, t, toks, s, m) : 0; } catch (...) { return 0; }
}
uint32_t tNearest(void* x, const char* u, int64_t p, AlgoOptType t, int32_t e, uint32_t n, int32_t* o) noexcept {
    try { auto* c = C(x); return c && u && o ? c->nearest(u, p, t, e, n, o) : 0; } catch (...) { return 0; }
}
int32_t  tFindFut(void* x, const char* u, int32_t e) noexcept { try { auto* c = C(x); return c && u ? c->findFut(u, e) : 0; } catch (...) { return 0; } }
int32_t  tFindSpot(void* x, const char* u) noexcept { try { auto* c = C(x); return c && u ? c->findSpot(u) : 0; } catch (...) { return 0; } }
int32_t  tFindOpt(void* x, const char* u, int32_t e, AlgoOptType t, int64_t k) noexcept {
    try { auto* c = C(x); return c && u ? c->findOpt(u, e, t, k) : 0; } catch (...) { return 0; }
}
int32_t  tMoney(void* x, const char* u, int64_t p, AlgoOptType t, int32_t e, int32_t s) noexcept {
    try { auto* c = C(x); return c && u ? c->moneyness(u, p, t, e, s) : 0; } catch (...) { return 0; }
}
int32_t  tStep(void* x, int32_t t, int32_t s) noexcept { try { auto* c = C(x); return c ? c->stepStrike(t, s) : 0; } catch (...) { return 0; } }
}  // namespace

void wireContracts(AlgoHostAPI& api) noexcept {
    api.find = tFind; api.info = tInfo; api.expiries = tExpiries; api.strikes = tStrikes; api.nearest = tNearest;
    api.find_fut = tFindFut; api.find_spot = tFindSpot; api.find_opt = tFindOpt; api.moneyness = tMoney; api.step_strike = tStep;
}

}  // namespace algo
