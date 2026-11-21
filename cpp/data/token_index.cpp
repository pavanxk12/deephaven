#include "data/token_index.h"

#include <algorithm>

namespace algo {

namespace {
constexpr int64_t kEmpty = INT64_MIN;
uint32_t bitsFor(uint64_t n) {                       // table of 2^bits slots holding n keys at load <= 1/2
    uint32_t b = 4;
    while ((1ull << b) < 2 * n) ++b;
    return b;
}
}  // namespace

int32_t TokenIndex::probe(const std::vector<int64_t>& keys, const std::vector<int32_t>& vals, uint32_t bits, int64_t token) const noexcept {
    if (bits == 0) return kNone;
    const uint32_t mask = (1u << bits) - 1;
    for (uint32_t i = slotOf(token, bits);; i = (i + 1) & mask) {
        if (keys[i] == token) return vals[i];
        if (keys[i] == kEmpty) return kNone;
    }
}

bool TokenIndex::insert(std::vector<int64_t>& keys, std::vector<int32_t>& vals, uint32_t bits, int64_t token, int32_t id) noexcept {
    const uint32_t mask = (1u << bits) - 1;
    for (uint32_t i = slotOf(token, bits), n = 0; n <= mask; i = (i + 1) & mask, ++n) {
        if (keys[i] == kEmpty) { keys[i] = token; vals[i] = id; return true; }
        if (keys[i] == token) return false;
    }
    return false;
}

void TokenIndex::build(std::span<const int64_t> master_tokens, uint32_t overflow) {
    std::vector<int64_t> toks(master_tokens.begin(), master_tokens.end());
    std::sort(toks.begin(), toks.end());
    toks.erase(std::unique(toks.begin(), toks.end()), toks.end());
    toks.erase(std::remove_if(toks.begin(), toks.end(), [](int64_t t) { return t == kEmpty; }), toks.end());

    master_ = static_cast<uint32_t>(toks.size());
    capacity_ = master_ + overflow;
    token_of_.clear();
    token_of_.reserve(capacity_);
    max_token_ = toks.empty() ? -1 : toks.back();
    direct_ = !toks.empty() && toks.front() >= 0 && static_cast<uint64_t>(max_token_) <= 4ull * toks.size() + 1024;

    dense_of_.clear(); keys_.clear(); vals_.clear(); bits_ = 0;
    if (direct_) {
        dense_of_.assign(static_cast<size_t>(max_token_) + 1, kNone);
        for (int64_t t : toks) { dense_of_[static_cast<size_t>(t)] = static_cast<int32_t>(token_of_.size()); token_of_.push_back(t); }
    } else if (!toks.empty()) {
        bits_ = bitsFor(toks.size());
        keys_.assign(1ull << bits_, kEmpty);
        vals_.assign(1ull << bits_, kNone);
        for (int64_t t : toks) { insert(keys_, vals_, bits_, t, static_cast<int32_t>(token_of_.size())); token_of_.push_back(t); }
    }
    obits_ = overflow ? bitsFor(overflow) : 0;
    okeys_.assign(obits_ ? (1ull << obits_) : 0, kEmpty);
    ovals_.assign(obits_ ? (1ull << obits_) : 0, kNone);
}

int32_t TokenIndex::find(int64_t token) const noexcept {
    if (direct_) {
        if (token >= 0 && token <= max_token_) { const int32_t id = dense_of_[static_cast<size_t>(token)]; if (id != kNone) return id; }
    } else {
        const int32_t id = probe(keys_, vals_, bits_, token);
        if (id != kNone) return id;
    }
    return probe(okeys_, ovals_, obits_, token);
}

int32_t TokenIndex::findOrAdd(int64_t token) noexcept {
    const int32_t id = find(token);
    if (id != kNone || token == kEmpty) return id;
    if (token_of_.size() >= capacity_ || obits_ == 0) return kNone;
    const int32_t nid = static_cast<int32_t>(token_of_.size());
    if (!insert(okeys_, ovals_, obits_, token, nid)) return kNone;
    token_of_.push_back(token);                         // capacity reserved in build(): no reallocation
    return nid;
}

}  // namespace algo
