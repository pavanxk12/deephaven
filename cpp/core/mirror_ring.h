// Double-mapped ("magic") broadcast ring over shared memory: one writer, any number of readers in any process.
//
// The data pages are mapped twice, back to back, so a run of messages that wraps the end of the ring is still one
// contiguous span (zero copy, no split at the wrap). The writer constructs messages in place and publishes them with one
// release store of `head`. Each reader keeps its own cursor; the writer never looks at readers, so a slow reader can
// never hold it up. A reader that falls a whole ring behind is told it was lapped and must resync from the latest state.
//
// Layout of the shared object: one header page (magic, version, capacity, slot size, head), then the data pages.
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <type_traits>

namespace algo {

class MirrorMap {
public:
    MirrorMap() = default;
    MirrorMap(const MirrorMap&) = delete;
    MirrorMap& operator=(const MirrorMap&) = delete;
    ~MirrorMap();

    // name empty: anonymous (in-process). create = true makes (or truncates) the object; false attaches to it.
    // data_bytes must be a multiple of the page size.
    bool open(const std::string& name, size_t data_bytes, bool create, std::string& error);
    void close() noexcept;

    void*  header() const noexcept { return header_; }
    char*  data() const noexcept { return data_; }        // valid for 2 * dataBytes()
    size_t dataBytes() const noexcept { return data_bytes_; }
    static size_t pageSize() noexcept;

private:
    void*  header_ = nullptr;
    char*  data_ = nullptr;
    size_t data_bytes_ = 0, header_bytes_ = 0;
    int    fd_ = -1;
};

template <typename T>
class BroadcastRing {
    static_assert(std::is_trivially_copyable_v<T>, "ring slots hold trivially copyable messages");

    struct alignas(64) Header {
        uint64_t              magic;
        uint32_t              version;
        uint32_t              slot_size;
        uint64_t              capacity;
        alignas(64) std::atomic<uint64_t> head;
    };
    static constexpr uint64_t kMagic = 0x414c474f52494e47ull;   // "ALGORING"
    static constexpr uint32_t kVersion = 1;

public:
    enum class Read { OK, EMPTY, LAPPED };

    // capacity: messages (power of two); capacity * sizeof(T) must be a multiple of the page size.
    bool create(const std::string& name, uint64_t capacity, std::string& error) { return init(name, capacity, true, error); }
    bool attach(const std::string& name, uint64_t capacity, std::string& error) { return init(name, capacity, false, error); }

    // writer ---------------------------------------------------------------------------
    T* claim() noexcept { return slot(head_local_); }
    void publish() noexcept { hdr()->head.store(++head_local_, std::memory_order_release); }
    void publishN(uint64_t n) noexcept { head_local_ += n; hdr()->head.store(head_local_, std::memory_order_release); }
    T* claimAt(uint64_t k) noexcept { return slot(head_local_ + k); }   // the k-th slot after the next one, for batch writes

    // reader ---------------------------------------------------------------------------
    class Reader {
    public:
        // Everything published since the last call, as one contiguous span (in place). After using the span call
        // `stillValid()`: false means the writer lapped this reader while it read, and the span must be discarded.
        Read next(std::span<const T>& out) noexcept {
            const uint64_t h = ring_->hdr()->head.load(std::memory_order_acquire);
            if (h == cursor_) { out = {}; return Read::EMPTY; }
            if (h - cursor_ > ring_->cap_) { cursor_ = h; out = {}; return Read::LAPPED; }
            out = std::span<const T>(ring_->slot(cursor_), static_cast<size_t>(h - cursor_));
            from_ = cursor_;
            cursor_ = h;
            return Read::OK;
        }
        bool stillValid() const noexcept {
            const uint64_t h = ring_->hdr()->head.load(std::memory_order_acquire);
            return h - from_ <= ring_->cap_;
        }
        uint64_t cursor() const noexcept { return cursor_; }

    private:
        friend class BroadcastRing;
        const BroadcastRing* ring_ = nullptr;
        uint64_t cursor_ = 0, from_ = 0;
    };
    // A new reader starts at the newest message (it does not replay the past).
    Reader reader() const noexcept { Reader r; r.ring_ = this; r.cursor_ = hdr()->head.load(std::memory_order_acquire); r.from_ = r.cursor_; return r; }

    uint64_t capacity() const noexcept { return cap_; }

private:
    bool init(const std::string& name, uint64_t capacity, bool create, std::string& error) {
        if (capacity < 2 || (capacity & (capacity - 1))) { error = "ring capacity must be a power of two"; return false; }
        const size_t bytes = static_cast<size_t>(capacity) * sizeof(T);
        if (bytes % MirrorMap::pageSize()) { error = "ring bytes (capacity x slot size) must be a multiple of the page size"; return false; }
        if (!map_.open(name, bytes, create, error)) return false;
        cap_ = capacity;
        mask_ = capacity - 1;
        Header* h = hdr();
        if (create) {
            h->magic = kMagic; h->version = kVersion; h->slot_size = sizeof(T); h->capacity = capacity;
            h->head.store(0, std::memory_order_release);
            head_local_ = 0;
        } else {
            if (h->magic != kMagic || h->version != kVersion || h->slot_size != sizeof(T) || h->capacity != capacity) {
                error = "ring \"" + name + "\" has a different layout (version, slot size or capacity)";
                return false;
            }
            head_local_ = h->head.load(std::memory_order_acquire);
        }
        return true;
    }
    Header* hdr() const noexcept { return static_cast<Header*>(map_.header()); }
    T* slot(uint64_t seq) const noexcept { return reinterpret_cast<T*>(map_.data()) + (seq & mask_); }

    MirrorMap map_;
    uint64_t  cap_ = 0, mask_ = 0, head_local_ = 0;
};

}  // namespace algo
