// object_pool.h -- fixed-capacity free-list allocator for a single type.
//
// This is NOT a general-purpose malloc replacement, and it is not claimed to
// "avoid malloc syscalls" -- malloc does not syscall per allocation; it
// amortises via arenas and only touches brk/mmap when an arena runs dry.
//
// What this pool actually buys, and why it is shaped this way:
//
//   * ONE INSTANCE PER EVENT-LOOP THREAD, so there is no lock on the hot path.
//     This is the whole point. A single globally mutex-protected free list is
//     usually SLOWER than glibc malloc or tcmalloc under concurrency, because
//     those already keep per-thread caches and this would add a contended
//     cache line that every thread has to bounce.
//   * Connection objects are ~8 KB and uniform, so a free list gives O(1)
//     acquire/release with no size-class search and no fragmentation.
//   * It DEGRADES instead of dying. When the pool is exhausted it falls back
//     to the heap. The previous design threw std::bad_alloc from a worker
//     thread with no catch, which means std::terminate -- a traffic spike
//     would take the whole server down.
//
// Build with -DCWS_NO_POOL=1 to route every allocation through new/delete so
// you can measure whether the pool is buying anything at all. If it is not,
// that is a real finding worth writing up -- see README "Benchmarking".

#pragma once

#include <cstddef>
#include <cstdint>
#include <new>
#include <utility>

namespace cws {

template <typename T, std::size_t Capacity>
class ObjectPool {
  static_assert(Capacity > 0, "capacity must be non-zero");

  // A slot must be able to hold either a live T or a free-list next pointer.
  struct alignas(alignof(T) > alignof(void*) ? alignof(T) : alignof(void*))
      Slot {
    unsigned char bytes[sizeof(T) > sizeof(void*) ? sizeof(T) : sizeof(void*)];
  };

 public:
  ObjectPool() {
#if !CWS_NO_POOL
    slots_ = static_cast<Slot*>(::operator new(sizeof(Slot) * Capacity));
    // Thread the free list back to front so that the first few acquires walk
    // forwards through memory, which is friendlier to the prefetcher.
    for (std::size_t i = Capacity; i-- > 0;) {
      Slot* s = slots_ + i;
      *reinterpret_cast<Slot**>(s) = free_;
      free_ = s;
    }
#endif
  }

  ~ObjectPool() {
#if !CWS_NO_POOL
    ::operator delete(slots_);
#endif
  }

  ObjectPool(const ObjectPool&) = delete;
  ObjectPool& operator=(const ObjectPool&) = delete;

  template <typename... Args>
  T* acquire(Args&&... args) {
#if CWS_NO_POOL
    ++heap_allocs_;
    ++live_;
    if (live_ > peak_live_) peak_live_ = live_;
    return new T(std::forward<Args>(args)...);
#else
    if (!free_) {
      ++heap_allocs_;  // pool exhausted: degrade, do not die
      ++live_;
      if (live_ > peak_live_) peak_live_ = live_;
      return new T(std::forward<Args>(args)...);
    }
    Slot* s = free_;
    free_ = *reinterpret_cast<Slot**>(s);
    ++live_;
    if (live_ > peak_live_) peak_live_ = live_;
    return new (static_cast<void*>(s)) T(std::forward<Args>(args)...);
#endif
  }

  void release(T* p) {
    if (!p) return;
    --live_;
#if CWS_NO_POOL
    delete p;
#else
    if (owns(p)) {
      p->~T();
      Slot* s = reinterpret_cast<Slot*>(p);
      *reinterpret_cast<Slot**>(s) = free_;
      free_ = s;
    } else {
      delete p;  // came from the heap fallback above
    }
#endif
  }

  std::size_t live() const { return live_; }
  std::size_t peak_live() const { return peak_live_; }
  std::size_t heap_fallbacks() const { return heap_allocs_; }

  static constexpr std::size_t capacity() { return Capacity; }
  static constexpr std::size_t slot_bytes() { return sizeof(Slot); }
  static constexpr std::size_t arena_bytes() { return sizeof(Slot) * Capacity; }

 private:
  // Pointer-range check to tell pooled objects from heap-fallback ones.
  bool owns(const T* p) const {
    if (!slots_) return false;
    auto a = reinterpret_cast<std::uintptr_t>(p);
    auto lo = reinterpret_cast<std::uintptr_t>(slots_);
    return a >= lo && a < lo + sizeof(Slot) * Capacity;
  }

  Slot* slots_ = nullptr;
  Slot* free_ = nullptr;
  std::size_t live_ = 0;
  std::size_t peak_live_ = 0;
  std::size_t heap_allocs_ = 0;
};

}  // namespace cws
