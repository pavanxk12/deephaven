// Exchange token -> dense index (0..size-1), done ONCE per tick by the feed thread; every later stage indexes flat arrays
// with the dense id. Built once per day from the master:
//   * direct array `dense_of[token]` when tokens are dense (max_token <= 4 x count): one load, no hash;
//   * otherwise an open-addressed table, Fibonacci hashing (one multiply, one shift), load factor <= 1/2.
// Tokens the master does not know (Python still builds their candles, with no symbol) get ids from a preallocated
// overflow range. Only the feed thread adds them; when the range is full they are refused (and counted by the caller).
// Ids are stable for the day: a reload may only append.
#pragma once
#include <cstdint>
#include <span>
#include <vector>

namespace algo {

class TokenIndex {
public:
    static constexpr int32_t kNone = -1;

    // master_tokens: today's contracts (any order, duplicates ignored). overflow: ids reserved for unknown tokens.
    void build(std::span<const int64_t> master_tokens, uint32_t overflow);

    int32_t find(int64_t token) const noexcept;
    int32_t findOrAdd(int64_t token) noexcept;            // feed thread only

    uint32_t size() const noexcept { return static_cast<uint32_t>(token_of_.size()); }   // ids in use
    uint32_t capacity() const noexcept { return capacity_; }                            // master + overflow
    uint32_t masterCount() const noexcept { return master_; }
    int64_t  tokenOf(uint32_t id) const noexcept { return token_of_[id]; }
    bool     direct() const noexcept { return direct_; }

private:
    static uint32_t slotOf(int64_t token, uint32_t bits) noexcept {
        return static_cast<uint32_t>((static_cast<uint64_t>(token) * 0x9E3779B97F4A7C15ull) >> (64 - bits));
    }
    int32_t probe(const std::vector<int64_t>& keys, const std::vector<int32_t>& vals, uint32_t bits, int64_t token) const noexcept;
    bool    insert(std::vector<int64_t>& keys, std::vector<int32_t>& vals, uint32_t bits, int64_t token, int32_t id) noexcept;

    bool                 direct_ = true;
    int64_t              max_token_ = -1;
    std::vector<int32_t> dense_of_;                       // direct mode
    std::vector<int64_t> keys_;  std::vector<int32_t> vals_;  uint32_t bits_ = 0;     // hashed mode (master)
    std::vector<int64_t> okeys_; std::vector<int32_t> ovals_; uint32_t obits_ = 0;    // overflow (unknown tokens)
    std::vector<int64_t> token_of_;
    uint32_t             master_ = 0, capacity_ = 0;
};

}  // namespace algo
