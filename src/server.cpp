// server.cpp -- N independent event loops over one shared listening socket.
//
// THREADING MODEL
// ---------------
// There is no thread pool and no shared task queue. Each thread owns a poller,
// a connection pool and its own connections; nothing is shared except the
// (immutable) file cache and the listening fd. That removes the mutex +
// condition_variable handoff from the hot path entirely -- under load, a single
// shared queue is a contended cache line that every worker fights over, and the
// handoff also costs a context switch per request.
//
// The listening socket is registered in every loop's poller. All loops wake on
// a new connection (a small thundering herd); the losers get EAGAIN from
// accept() and go back to sleep. For N in the 4-16 range this is cheaper than
// the alternatives. Beyond that, SO_REUSEPORT with a listener per thread
// (Linux) or EPOLLEXCLUSIVE is the next step -- deliberately not done here
// because plain SO_REUSEPORT does not load-balance on macOS/BSD, and silently
// sending every connection to one thread would be worse than the herd.
//
// PER-REQUEST ALLOCATIONS: ZERO
// -----------------------------
// Response headers are precomputed per file at startup, bodies are already
// resident, the Date line is regenerated once a second, and the connection
// object comes from a per-thread free list. Responding is a writev() of three
// pointers. This is what makes the memory pool meaningful -- in the previous
// version the pool saved one allocation while the handler did ~8 more via
// std::string and std::stringstream, plus two full copies of the body.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include "file_cache.h"
#include "http.h"
#include "object_pool.h"
#include "poller.h"

namespace cws {

// ---------------------------------------------------------------- tunables
static constexpr std::size_t kReadBufSize = 8192;    // max request head size

#ifndef CWS_POOL_CAPACITY
#define CWS_POOL_CAPACITY 1024
#endif
static constexpr std::size_t kPoolCapacity = CWS_POOL_CAPACITY;

static constexpr int kMaxEventsPerWait = 512;
static constexpr int kPollTimeoutMs = 500;           // also drives idle sweep
static constexpr int64_t kIdleTimeoutMs = 15000;     // keep-alive reaper
static constexpr int64_t kLingerMs = 1000;           // lingering-close budget
static constexpr std::size_t kMaxConnPerLoop = 4096;
static constexpr int kMaxIov = 3;

static std::atomic<bool> g_running{true};

static void handle_signal(int) {
  // Only async-signal-safe work here: a relaxed store on a lock-free atomic.
  g_running.store(false, std::memory_order_relaxed);
}

// ------------------------------------------------------------- error pages
struct ErrorPage {
  std::string header;  // minus Date + blank line
  std::string body;
};

static ErrorPage make_error(const char* status, const char* title,
                            const char* detail) {
  ErrorPage p;
  p.body = std::string("<!doctype html><html><head><meta charset=\"utf-8\">"
                       "<title>") + title +
           "</title></head><body style=\"font-family:system-ui,sans-serif;"
           "max-width:34rem;margin:4rem auto;padding:0 1rem\"><h1>" +
           title + "</h1><p>" + detail + "</p><hr><p><small>cws/0.2</small>"
           "</p></body></html>";
  p.header = std::string("HTTP/1.1 ") + status +
             "\r\nServer: cws/0.2\r\nContent-Type: text/html; charset=utf-8"
             "\r\nContent-Length: " + std::to_string(p.body.size()) +
             "\r\nConnection: close\r\n";
  return p;
}

struct ErrorPages {
  ErrorPage bad_request =
      make_error("400 Bad Request", "400 Bad Request",
                 "The request could not be parsed.");
  ErrorPage not_found =
      make_error("404 Not Found", "404 Not Found",
                 "No resource is mapped to that path.");
  ErrorPage method_not_allowed =
      make_error("405 Method Not Allowed", "405 Method Not Allowed",
                 "This server implements GET and HEAD only.");
  ErrorPage headers_too_large =
      make_error("431 Request Header Fields Too Large",
                 "431 Header Fields Too Large",
                 "The request head exceeded the read buffer.");
};

// ------------------------------------------------------------- connection
struct Connection {
  int fd = -1;

  // Read side. Deliberately NOT zero-initialised: memset-ing 8 KB per
  // connection is pure waste, and in_len bounds every read of it.
  char in[kReadBufSize];
  std::uint32_t in_len = 0;
  std::size_t scan_pos = 0;  // resume point for the CRLFCRLF search

  // Write side.
  struct iovec iov[kMaxIov];
  int iov_count = 0;
  std::size_t sent = 0;
  std::size_t total = 0;
  bool writing = false;

  char date_hdr[48];  // private copy: the loop's buffer is rewritten each sec
  std::uint8_t date_len = 0;

  bool keep_alive = true;
  bool close_after_write = false;
  bool peer_closed = false;
  bool read_paused = false;
  bool lingering = false;  // half-closed, draining the peer's leftovers
  bool closed = false;     // set on teardown; freed after the event batch

  std::int64_t linger_until = 0;
  std::int64_t last_active = 0;
  Connection* lru_prev = nullptr;
  Connection* lru_next = nullptr;
};

static std::int64_t now_ms() {
  struct timespec ts;
  ::clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<std::int64_t>(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

static bool set_non_blocking(int fd) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) return false;
  return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

// Rebuilds the not-yet-sent tail of a response as an iovec array.
static int remaining_iov(struct iovec* dst, const struct iovec* src, int n,
                         std::size_t off) {
  int k = 0;
  for (int i = 0; i < n; ++i) {
    if (off >= src[i].iov_len) {
      off -= src[i].iov_len;
      continue;
    }
    dst[k].iov_base = static_cast<char*>(src[i].iov_base) + off;
    dst[k].iov_len = src[i].iov_len - off;
    off = 0;
    ++k;
  }
  return k;
}

struct Stats {
  std::uint64_t accepted = 0;
  std::uint64_t requests = 0;
  std::uint64_t responses = 0;
  std::uint64_t bytes_in = 0;
  std::uint64_t bytes_out = 0;
  std::uint64_t partial_writes = 0;
  std::uint64_t idle_reaped = 0;
  std::uint64_t accept_stalls = 0;
};

class EventLoop {
 public:
  EventLoop(int listen_fd, const FileCache& cache, const ErrorPages& errors)
      : listen_fd_(listen_fd), cache_(cache), errors_(errors),
        poller_(kMaxEventsPerWait) {}

  bool valid() const { return poller_.valid(); }
  const Stats& stats() const { return stats_; }
  std::size_t pool_peak() const { return pool_.peak_live(); }
  std::size_t pool_fallbacks() const { return pool_.heap_fallbacks(); }

  void run() {
    if (!poller_.add(listen_fd_, kPollRead, nullptr)) return;

    std::vector<PollEvent> evs(kMaxEventsPerWait);
    while (g_running.load(std::memory_order_relaxed)) {
      int n = poller_.wait(evs.data(), kMaxEventsPerWait, kPollTimeoutMs);
      if (n < 0) break;

      std::int64_t now = now_ms();
      refresh_date();

      for (int i = 0; i < n; ++i) {
        if (evs[i].data == nullptr) {
          accept_batch(now);
        } else {
          service(static_cast<Connection*>(evs[i].data), evs[i], now);
        }
      }

      sweep_lingering(now);
      maintenance(now);

      // Connections are only freed here, never mid-batch. kqueue can hand back
      // a READ and a WRITE event for the same fd in one wait(); if the read
      // handler freed the object, the write event's pointer would dangle.
      drain_pending_free();
    }
    shutdown_all();
  }

 private:
  // ----------------------------------------------------------- accept path
  void accept_batch(std::int64_t now) {
    for (;;) {
      if (conn_count_ >= kMaxConnPerLoop) {
        accept_stalled_ = true;
        ++stats_.accept_stalls;
        return;
      }

      struct sockaddr_in addr;
      socklen_t alen = sizeof(addr);
      int fd;
#ifdef __linux__
      fd = ::accept4(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr),
                     &alen, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
      fd = ::accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr),
                    &alen);
#endif
      if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          accept_stalled_ = false;
          return;  // drained: correct exit for an edge-triggered listener
        }
        if (errno == EINTR || errno == ECONNABORTED) continue;
        if (errno == EMFILE || errno == ENFILE || errno == ENOMEM ||
            errno == ENOBUFS) {
          // Out of descriptors. Under edge triggering, returning here would
          // mean never being notified about the listener again -- the accept
          // loop would wedge permanently. The poll timeout plus this flag is
          // what gets us retried. (A production server also keeps a spare fd
          // in reserve to close, accept, and immediately hang up, so the
          // client gets a reset instead of a hang.)
          accept_stalled_ = true;
          ++stats_.accept_stalls;
          return;
        }
        return;
      }

#ifndef __linux__
      if (!set_non_blocking(fd)) {
        ::close(fd);
        continue;
      }
      ::fcntl(fd, F_SETFD, FD_CLOEXEC);
#endif
      // Latency, not throughput, is the goal: without this, Nagle holds a
      // small response back waiting to coalesce with data that never comes.
      int one = 1;
      ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

      Connection* c = pool_.acquire();
      c->fd = fd;
      c->last_active = now;
      lru_push_front(c);
      ++conn_count_;
      ++stats_.accepted;

      if (!poller_.add(fd, kPollRead | kPollWrite, c)) {
        close_connection(c);
        continue;
      }
      // Data may already be waiting; with edge triggering we must not wait for
      // an event that has already fired.
      service_new(c, now);
    }
  }

  void service_new(Connection* c, std::int64_t now) {
    PollEvent ev;
    ev.data = c;
    ev.events = kPollRead;
    ev.hup = false;
    ev.error = false;
    service(c, ev, now);
  }

  // ------------------------------------------------------------ event path
  void service(Connection* c, const PollEvent& ev, std::int64_t now) {
    if (c->closed) return;  // freed later this batch; ignore stale events

    if (c->lingering) {
      if (ev.error || !discard_input(c)) close_connection(c);
      return;
    }

    c->last_active = now;
    lru_touch(c);

    if (ev.error) {
      close_connection(c);
      return;
    }
    if ((ev.events & kPollRead) && !drain_read(c)) {
      close_connection(c);
      return;
    }
    if ((ev.events & kPollWrite) && c->writing && !flush(c)) {
      close_connection(c);
      return;
    }
    if (c->read_paused && !c->writing && !drain_read(c)) {
      close_connection(c);
      return;
    }
    if (!process(c)) {
      close_connection(c);
      return;
    }
    if (!c->writing && (c->close_after_write || c->peer_closed ||
                        !c->keep_alive)) {
      if (c->peer_closed) {
        close_connection(c);  // peer is already gone; nothing to linger for
      } else {
        enter_linger(c, now);
      }
    }
  }

  // Closing a socket that still holds unread data in its receive buffer makes
  // the kernel emit RST rather than FIN -- and an RST discards data the peer
  // has not read yet, so the response we just wrote is thrown away. That is
  // precisely how a 431 for an oversized header, or a 405 for a POST with a
  // body, turns into ECONNRESET at the client. Half-close, drain whatever the
  // client still had in flight, then close for real.
  void enter_linger(Connection* c, std::int64_t now) {
    if (c->lingering) return;
    ::shutdown(c->fd, SHUT_WR);
    c->lingering = true;
    c->linger_until = now + kLingerMs;
    lingering_.push_back(c);
    if (!discard_input(c)) close_connection(c);
  }

  // Reads and throws away input. Returns false on EOF or a hard error.
  bool discard_input(Connection* c) {
    char sink[4096];
    for (;;) {
      ssize_t n = ::read(c->fd, sink, sizeof(sink));
      if (n > 0) continue;
      if (n == 0) return false;
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
      return false;
    }
  }

  // Bounded: a client that neither reads nor closes cannot pin a descriptor
  // for longer than kLingerMs.
  void sweep_lingering(std::int64_t now) {
    for (std::size_t i = 0; i < lingering_.size();) {
      Connection* c = lingering_[i];
      if (now >= c->linger_until) {
        close_connection(c);  // swap-erases lingering_[i]; do not advance
      } else {
        ++i;
      }
    }
  }

  // Reads until EAGAIN. Draining fully is mandatory under edge triggering:
  // leave bytes in the socket buffer and no further event is delivered.
  bool drain_read(Connection* c) {
    c->read_paused = false;
    for (;;) {
      if (c->in_len >= kReadBufSize) {
        c->read_paused = true;  // resume once the current response is flushed
        return true;
      }
      ssize_t n = ::read(c->fd, c->in + c->in_len, kReadBufSize - c->in_len);
      if (n > 0) {
        c->in_len += static_cast<std::uint32_t>(n);
        stats_.bytes_in += static_cast<std::uint64_t>(n);
        continue;
      }
      if (n == 0) {
        c->peer_closed = true;  // orderly FIN; finish any pending write first
        return true;
      }
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
      return false;
    }
  }

  // Parses and answers as many complete requests as the buffer holds.
  // Pipelined requests are handled one at a time so responses stay in order.
  bool process(Connection* c) {
    while (!c->writing && !c->close_after_write) {
      Request req;
      std::size_t consumed = 0;
      Parse r = parse_request(c->in, c->in_len, kReadBufSize, &c->scan_pos,
                              &req, &consumed);

      if (r == Parse::kIncomplete) return true;

      if (r != Parse::kOk) {
        const ErrorPage* page = &errors_.bad_request;
        if (r == Parse::kUnsupportedMethod) page = &errors_.method_not_allowed;
        else if (r == Parse::kHeadersTooLarge) page = &errors_.headers_too_large;
        // Do not consume: the connection is closing, and on a malformed or
        // over-long head we cannot trust the framing enough to resync.
        c->in_len = 0;
        c->scan_pos = 0;
        send_error(c, *page);
        return flush(c);
      }

      consume(c, consumed);
      ++stats_.requests;
      c->keep_alive = req.keep_alive;

      const Resource* res = cache_.find(req.path);
      if (!res) {
        send_error(c, errors_.not_found);
      } else {
        send_resource(c, *res, req);
      }
      if (!flush(c)) return false;
    }
    return true;
  }

  void consume(Connection* c, std::size_t n) {
    if (n >= c->in_len) {
      c->in_len = 0;
    } else {
      std::memmove(c->in, c->in + n, c->in_len - n);
      c->in_len -= static_cast<std::uint32_t>(n);
    }
    c->scan_pos = 0;
  }

  void stamp_date(Connection* c) {
    std::memcpy(c->date_hdr, date_buf_, date_len_);
    c->date_len = static_cast<std::uint8_t>(date_len_);
  }

  void send_resource(Connection* c, const Resource& res, const Request& req) {
    const std::string& hdr =
        c->keep_alive ? res.header_keepalive : res.header_close;
    stamp_date(c);

    c->iov[0].iov_base = const_cast<char*>(hdr.data());
    c->iov[0].iov_len = hdr.size();
    c->iov[1].iov_base = c->date_hdr;
    c->iov[1].iov_len = c->date_len;
    c->iov_count = 2;

    // HEAD sends the headers of the equivalent GET, including the real
    // Content-Length, but no body.
    if (req.method == Request::kGet && !res.body.empty()) {
      c->iov[2].iov_base = const_cast<char*>(res.body.data());
      c->iov[2].iov_len = res.body.size();
      c->iov_count = 3;
    }

    c->sent = 0;
    c->total = 0;
    for (int i = 0; i < c->iov_count; ++i) c->total += c->iov[i].iov_len;
  }

  void send_error(Connection* c, const ErrorPage& page) {
    stamp_date(c);
    c->iov[0].iov_base = const_cast<char*>(page.header.data());
    c->iov[0].iov_len = page.header.size();
    c->iov[1].iov_base = c->date_hdr;
    c->iov[1].iov_len = c->date_len;
    c->iov[2].iov_base = const_cast<char*>(page.body.data());
    c->iov[2].iov_len = page.body.size();
    c->iov_count = 3;
    c->sent = 0;
    c->total = c->iov[0].iov_len + c->iov[1].iov_len + c->iov[2].iov_len;
    c->keep_alive = false;
    c->close_after_write = true;
  }

  // The socket is non-blocking, so a large body WILL short-write once the
  // send buffer fills. Ignoring the return value of write() (as the original
  // did) silently truncates every response bigger than ~64 KB. Here the
  // unsent tail stays pending and the always-armed write filter wakes us when
  // the buffer drains.
  bool flush(Connection* c) {
    while (c->sent < c->total) {
      struct iovec tmp[kMaxIov];
      int k = remaining_iov(tmp, c->iov, c->iov_count, c->sent);
      ssize_t n = ::writev(c->fd, tmp, k);
      if (n > 0) {
        c->sent += static_cast<std::size_t>(n);
        stats_.bytes_out += static_cast<std::uint64_t>(n);
        continue;
      }
      if (n < 0 && errno == EINTR) continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (!c->writing) ++stats_.partial_writes;
        c->writing = true;
        return true;
      }
      return false;  // EPIPE/ECONNRESET -- peer is gone
    }
    c->writing = false;
    c->iov_count = 0;
    c->sent = 0;
    c->total = 0;
    ++stats_.responses;
    return true;
  }

  // ------------------------------------------------------------ housekeeping
  void refresh_date() {
    std::time_t t = std::time(nullptr);
    if (t == date_sec_) return;
    date_sec_ = t;
    struct tm tmv;
    ::gmtime_r(&t, &tmv);
    date_len_ = std::strftime(date_buf_, sizeof(date_buf_),
                              "Date: %a, %d %b %Y %H:%M:%S GMT\r\n\r\n", &tmv);
  }

  void maintenance(std::int64_t now) {
    if (accept_stalled_) accept_batch(now);  // retry after EMFILE / conn cap
    if (now - last_sweep_ < 1000) return;
    last_sweep_ = now;

    // Idle keep-alive connections must be reaped or a slowloris-style client
    // parks descriptors indefinitely. The LRU list is ordered by last
    // activity, so this walks only the expired tail.
    while (lru_tail_ && now - lru_tail_->last_active > kIdleTimeoutMs) {
      Connection* victim = lru_tail_;
      ++stats_.idle_reaped;
      close_connection(victim);
    }
  }

  void close_connection(Connection* c) {
    if (c->closed) return;
    c->closed = true;
    if (c->lingering) {
      c->lingering = false;
      for (std::size_t i = 0; i < lingering_.size(); ++i) {
        if (lingering_[i] == c) {
          lingering_[i] = lingering_.back();
          lingering_.pop_back();
          break;
        }
      }
    }
    lru_unlink(c);
    if (c->fd >= 0) {
      ::close(c->fd);  // also removes it from the kqueue/epoll set
      c->fd = -1;
    }
    --conn_count_;
    pending_free_.push_back(c);
  }

  void drain_pending_free() {
    for (Connection* c : pending_free_) pool_.release(c);
    pending_free_.clear();
  }

  void shutdown_all() {
    Connection* c = lru_head_;
    while (c) {
      Connection* next = c->lru_next;
      close_connection(c);
      c = next;
    }
    drain_pending_free();
  }

  // ------------------------------------------------------------- LRU list
  void lru_push_front(Connection* c) {
    c->lru_prev = nullptr;
    c->lru_next = lru_head_;
    if (lru_head_) lru_head_->lru_prev = c;
    lru_head_ = c;
    if (!lru_tail_) lru_tail_ = c;
  }

  void lru_unlink(Connection* c) {
    if (c->lru_prev) c->lru_prev->lru_next = c->lru_next;
    else if (lru_head_ == c) lru_head_ = c->lru_next;
    if (c->lru_next) c->lru_next->lru_prev = c->lru_prev;
    else if (lru_tail_ == c) lru_tail_ = c->lru_prev;
    c->lru_prev = c->lru_next = nullptr;
  }

  void lru_touch(Connection* c) {
    if (lru_head_ == c) return;
    lru_unlink(c);
    lru_push_front(c);
  }

  int listen_fd_;
  const FileCache& cache_;
  const ErrorPages& errors_;
  Poller poller_;
  ObjectPool<Connection, kPoolCapacity> pool_;
  std::vector<Connection*> pending_free_;
  std::vector<Connection*> lingering_;
  Connection* lru_head_ = nullptr;
  Connection* lru_tail_ = nullptr;
  std::size_t conn_count_ = 0;
  bool accept_stalled_ = false;
  std::int64_t last_sweep_ = 0;
  std::time_t date_sec_ = 0;
  char date_buf_[48] = {0};
  std::size_t date_len_ = 0;
  Stats stats_;
};

}  // namespace cws

// ---------------------------------------------------------------------- main
namespace cws {

static int make_listener(int port, int backlog) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;

  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(port));

  if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return -1;
  }
  if (::listen(fd, backlog) < 0) {
    ::close(fd);
    return -1;
  }
  if (!set_non_blocking(fd)) {
    ::close(fd);
    return -1;
  }
  return fd;
}

}  // namespace cws

int main(int argc, char** argv) {
  using namespace cws;

  int port = (argc > 1) ? std::atoi(argv[1]) : 8080;
  std::string docroot = (argc > 2) ? argv[2] : "www";
  int threads = (argc > 3) ? std::atoi(argv[3])
                           : static_cast<int>(std::thread::hardware_concurrency());
  if (threads < 1) threads = 1;
  if (threads > 32) threads = 32;
  if (port <= 0 || port > 65535) {
    std::fprintf(stderr, "invalid port\n");
    return 1;
  }

  // Without this, writing to a socket the peer already closed raises SIGPIPE
  // and the default disposition kills the process. One misbehaving client
  // should not be able to terminate the server.
  ::signal(SIGPIPE, SIG_IGN);
  ::signal(SIGINT, handle_signal);
  ::signal(SIGTERM, handle_signal);

  FileCache cache;
  if (!cache.load(docroot)) {
    std::fprintf(stderr, "cws: cannot read docroot '%s'\n", docroot.c_str());
    return 1;
  }
  if (cache.entry_count() == 0) {
    std::fprintf(stderr, "cws: docroot '%s' is empty\n", docroot.c_str());
    return 1;
  }

  ErrorPages errors;

  int listen_fd = make_listener(port, 4096);
  if (listen_fd < 0) {
    std::fprintf(stderr, "cws: bind/listen on port %d failed: %s\n", port,
                 std::strerror(errno));
    return 1;
  }

  std::printf("cws/0.2  backend=%s  port=%d  threads=%d\n", Poller::backend(),
              port, threads);
  std::printf("  docroot   %s (%zu entries, %zu bytes preloaded)\n",
              cache.docroot().c_str(), cache.entry_count(), cache.byte_count());
#if CWS_NO_POOL
  std::printf("  conn pool DISABLED (-DCWS_NO_POOL=1) -- new/delete per conn\n");
#else
  std::printf("  conn pool %zu slots x %zu B = %.1f MiB per thread\n",
              ObjectPool<Connection, kPoolCapacity>::capacity(),
              ObjectPool<Connection, kPoolCapacity>::slot_bytes(),
              ObjectPool<Connection, kPoolCapacity>::arena_bytes() /
                  (1024.0 * 1024.0));
#endif
  std::printf("  ctrl-c to stop\n\n");
  std::fflush(stdout);

  std::vector<EventLoop*> loops;
  loops.reserve(threads);
  for (int i = 0; i < threads; ++i) {
    loops.push_back(new EventLoop(listen_fd, cache, errors));
    if (!loops.back()->valid()) {
      std::fprintf(stderr, "cws: poller init failed\n");
      return 1;
    }
  }

  std::vector<std::thread> workers;
  for (int i = 1; i < threads; ++i) {
    workers.emplace_back([&loops, i] { loops[i]->run(); });
  }
  loops[0]->run();
  for (auto& t : workers) t.join();

  Stats total;
  std::size_t peak = 0, fallbacks = 0;
  for (EventLoop* l : loops) {
    const Stats& s = l->stats();
    total.accepted += s.accepted;
    total.requests += s.requests;
    total.responses += s.responses;
    total.bytes_in += s.bytes_in;
    total.bytes_out += s.bytes_out;
    total.partial_writes += s.partial_writes;
    total.idle_reaped += s.idle_reaped;
    total.accept_stalls += s.accept_stalls;
    peak += l->pool_peak();
    fallbacks += l->pool_fallbacks();
  }

  std::printf("\n-- shutdown --------------------------------\n");
  std::printf("  connections accepted   %" PRIu64 "\n", total.accepted);
  std::printf("  requests parsed        %" PRIu64 "\n", total.requests);
  std::printf("  responses completed    %" PRIu64 "\n", total.responses);
  std::printf("  bytes in / out         %" PRIu64 " / %" PRIu64 "\n",
              total.bytes_in, total.bytes_out);
  std::printf("  partial writes         %" PRIu64 "\n", total.partial_writes);
  std::printf("  idle conns reaped      %" PRIu64 "\n", total.idle_reaped);
  std::printf("  accept stalls          %" PRIu64 "\n", total.accept_stalls);
  std::printf("  pool peak (all loops)  %zu\n", peak);
  std::printf("  pool heap fallbacks    %zu\n", fallbacks);

  for (EventLoop* l : loops) delete l;
  ::close(listen_fd);
  return 0;
}
