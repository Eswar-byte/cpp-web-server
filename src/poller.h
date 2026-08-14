// poller.h -- thin, edge-triggered abstraction over kqueue (BSD/macOS) and
// epoll (Linux).
//
// Design note: we register READ and WRITE interest ONCE at accept time and
// never modify the registration afterwards. That is a deliberate trade:
//
//   * Modifying interest costs a syscall per state transition, and on kqueue
//     it also forces you to track which filters are currently armed (deleting
//     an unarmed filter returns ENOENT, which then has to be absorbed out of
//     the eventlist).
//   * Because both backends are edge-triggered, an always-armed write filter
//     costs exactly one spurious wakeup per connection: the socket is writable
//     the moment it is accepted, that edge fires once, we ignore it. After
//     that the filter only fires when the send buffer transitions from full
//     back to writable -- which is precisely the event we care about for
//     resuming a partial write.
//
// One spurious wakeup per connection is far cheaper than two syscalls per
// request. Closing an fd removes it from the kqueue/epoll set automatically,
// so there is no de-registration on the teardown path either.

#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <unistd.h>

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || \
    defined(__OpenBSD__) || defined(__DragonFly__)
#define CWS_USE_KQUEUE 1
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#elif defined(__linux__)
#define CWS_USE_EPOLL 1
#include <sys/epoll.h>
#else
#error "poller.h supports kqueue (BSD/macOS) and epoll (Linux) only"
#endif

namespace cws {

enum : uint32_t {
  kPollRead = 1u << 0,
  kPollWrite = 1u << 1,
};

struct PollEvent {
  void* data = nullptr;    // udata / data.ptr -- nullptr means "the listener"
  uint32_t events = 0;     // kPollRead | kPollWrite
  bool hup = false;        // peer closed at least one direction
  bool error = false;      // socket error pending
};

class Poller {
 public:
  explicit Poller(int max_events = 512) : backing_(max_events) {
#ifdef CWS_USE_KQUEUE
    fd_ = ::kqueue();
#else
    fd_ = ::epoll_create1(0);
#endif
  }

  ~Poller() {
    if (fd_ >= 0) ::close(fd_);
  }

  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;

  bool valid() const { return fd_ >= 0; }
  int fd() const { return fd_; }

  // Register `fd` edge-triggered. `data` is handed back verbatim in
  // PollEvent::data.
  bool add(int fd, uint32_t interest, void* data) {
#ifdef CWS_USE_KQUEUE
    struct kevent ch[2];
    int n = 0;
    if (interest & kPollRead) {
      EV_SET(&ch[n++], fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, data);
    }
    if (interest & kPollWrite) {
      EV_SET(&ch[n++], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, data);
    }
    if (n == 0) return true;
    // nevents == 0: the changelist cannot fail for a valid fd, and passing a
    // non-empty eventlist here would risk swallowing real pending events.
    return ::kevent(fd_, ch, n, nullptr, 0, nullptr) != -1;
#else
    struct epoll_event ev;
    ev.events = EPOLLET | EPOLLRDHUP;
    if (interest & kPollRead) ev.events |= EPOLLIN;
    if (interest & kPollWrite) ev.events |= EPOLLOUT;
    ev.data.ptr = data;
    return ::epoll_ctl(fd_, EPOLL_CTL_ADD, fd, &ev) == 0;
#endif
  }

  // Only needed when an fd must leave the set while staying open; close()
  // de-registers on its own.
  void del(int fd) {
#ifdef CWS_USE_KQUEUE
    struct kevent ch[2];
    EV_SET(&ch[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&ch[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ::kevent(fd_, ch, 2, nullptr, 0, nullptr);  // ENOENT is expected + benign
#else
    ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr);
#endif
  }

  // Returns the number of events written to `out`, 0 on timeout/EINTR,
  // -1 on a real failure. timeout_ms < 0 blocks indefinitely.
  int wait(PollEvent* out, int max, int timeout_ms) {
    if (max > static_cast<int>(backing_.size())) {
      max = static_cast<int>(backing_.size());
    }
#ifdef CWS_USE_KQUEUE
    struct timespec ts;
    struct timespec* pts = nullptr;
    if (timeout_ms >= 0) {
      ts.tv_sec = timeout_ms / 1000;
      ts.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1000000L;
      pts = &ts;
    }
    int n = ::kevent(fd_, nullptr, 0, backing_.data(), max, pts);
    if (n < 0) return (errno == EINTR) ? 0 : -1;
    for (int i = 0; i < n; ++i) {
      const struct kevent& e = backing_[i];
      out[i].data = e.udata;
      out[i].events = (e.filter == EVFILT_WRITE) ? kPollWrite : kPollRead;
      out[i].hup = (e.flags & EV_EOF) != 0;
      out[i].error = (e.flags & EV_ERROR) != 0;
    }
    return n;
#else
    int n = ::epoll_wait(fd_, backing_.data(), max, timeout_ms);
    if (n < 0) return (errno == EINTR) ? 0 : -1;
    for (int i = 0; i < n; ++i) {
      const struct epoll_event& e = backing_[i];
      out[i].data = e.data.ptr;
      out[i].events = 0;
      if (e.events & (EPOLLIN | EPOLLHUP | EPOLLERR)) out[i].events |= kPollRead;
      if (e.events & (EPOLLOUT | EPOLLHUP | EPOLLERR)) {
        out[i].events |= kPollWrite;
      }
      out[i].hup = (e.events & (EPOLLHUP | EPOLLRDHUP)) != 0;
      out[i].error = (e.events & EPOLLERR) != 0;
    }
    return n;
#endif
  }

  static const char* backend() {
#ifdef CWS_USE_KQUEUE
    return "kqueue";
#else
    return "epoll";
#endif
  }

 private:
  int fd_ = -1;
#ifdef CWS_USE_KQUEUE
  std::vector<struct kevent> backing_;
#else
  std::vector<struct epoll_event> backing_;
#endif
};

}  // namespace cws
