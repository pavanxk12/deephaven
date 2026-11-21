#include "core/mirror_ring.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>

namespace algo {

size_t MirrorMap::pageSize() noexcept {
    static const size_t p = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    return p;
}

MirrorMap::~MirrorMap() { close(); }

void MirrorMap::close() noexcept {
    if (data_) munmap(data_, 2 * data_bytes_);
    if (header_) munmap(header_, header_bytes_);
    if (fd_ >= 0) ::close(fd_);
    data_ = nullptr; header_ = nullptr; fd_ = -1; data_bytes_ = header_bytes_ = 0;
}

bool MirrorMap::open(const std::string& name, size_t data_bytes, bool create, std::string& error) {
    close();
    const size_t page = pageSize();
    if (data_bytes == 0 || data_bytes % page) { error = "mirror size must be a positive multiple of the page size"; return false; }
    header_bytes_ = page;
    const size_t total = header_bytes_ + data_bytes;

    std::string shm = name;
    if (shm.empty()) {                                    // anonymous: a unique name, unlinked at once
        static std::atomic<unsigned> seq{0};
        char buf[64];
        std::snprintf(buf, sizeof buf, "/algo.anon.%d.%u", static_cast<int>(getpid()), seq.fetch_add(1));
        shm = buf;
        create = true;
    } else if (shm.front() != '/') {
        shm = "/" + shm;
    }
    const int flags = create ? (O_RDWR | O_CREAT) : O_RDWR;
    fd_ = shm_open(shm.c_str(), flags, 0600);
    if (fd_ < 0) { error = "shm_open " + shm + ": " + std::strerror(errno); return false; }
    if (name.empty()) shm_unlink(shm.c_str());
    if (create && ftruncate(fd_, static_cast<off_t>(total)) != 0) { error = "ftruncate " + shm + ": " + std::strerror(errno); close(); return false; }

    header_ = mmap(nullptr, header_bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (header_ == MAP_FAILED) { header_ = nullptr; error = std::string("mmap header: ") + std::strerror(errno); close(); return false; }

    // reserve 2x the data range, then map the same data pages into both halves
    void* base = mmap(nullptr, 2 * data_bytes, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (base == MAP_FAILED) { error = std::string("mmap reserve: ") + std::strerror(errno); close(); return false; }
    char* b = static_cast<char*>(base);
    void* lo = mmap(b, data_bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd_, static_cast<off_t>(header_bytes_));
    void* hi = mmap(b + data_bytes, data_bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd_, static_cast<off_t>(header_bytes_));
    if (lo != b || hi != b + data_bytes) {
        error = std::string("mmap mirror: ") + std::strerror(errno);
        munmap(base, 2 * data_bytes);
        close();
        return false;
    }
    data_ = b;
    data_bytes_ = data_bytes;
    return true;
}

}  // namespace algo
