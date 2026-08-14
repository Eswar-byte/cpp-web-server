# cws — a static HTTP/1.1 server in C++17

An event-driven static file server written from scratch. No dependencies, no
framework. Builds on macOS/BSD (kqueue) and Linux (epoll) from the same source.

The interesting part is not that it serves files — it's the set of failure
modes an event-driven server has to get right to survive contact with a real
client, and the fact that the numbers below were measured rather than assumed.

```bash
make          # -> ./server
make run      # http://localhost:8080
make test     # correctness suite
make bench    # throughput + latency
```

---

## Architecture

**N independent event loops, no shared queue.** Each thread owns a poller, a
connection pool, and its own connections. Nothing is shared except the
immutable file cache and the listening descriptor. There is no mutex or
condition variable on the request path — a single shared task queue is a
contended cache line every worker fights over, plus a context switch per
request to hand the work off.

The listening socket is registered in every loop. All loops wake on a new
connection; the losers get `EAGAIN` and go back to sleep. For 4–16 threads
that beats the alternatives. `SO_REUSEPORT` per-thread listeners would remove
the herd, but plain `SO_REUSEPORT` does not load-balance on macOS/BSD, and
silently routing every connection to one thread would be worse.

**Zero heap allocations per request.** Response headers are precomputed per
file at startup (two variants, keep-alive and close), bodies are already
resident, and the `Date` line is regenerated once per second. Serving a request
is a `writev()` of three pointers.

**Edge-triggered, register-once.** Read and write interest are registered at
accept time and never modified. Under edge triggering an always-armed write
filter costs exactly one spurious wakeup per connection; after that it only
fires when the send buffer drains, which is precisely when a stalled write
should resume. That's cheaper than two `kevent`/`epoll_ctl` calls per request.

```
src/poller.h       kqueue/epoll abstraction
src/object_pool.h  per-thread free list for connection objects
src/http.h         incremental request-line + header parser
src/file_cache.h   startup preload, precomputed headers
src/server.cpp     event loop + connection state machine
```

---

## What it gets right

These are the things that separate a server that works against `curl` from one
that works under load. Each is covered by `bench/edge_cases.py`.

| | |
|---|---|
| **Partial reads** | A request is accumulated across reads until `CRLFCRLF`. Never assumes the head arrived in one segment — it doesn't, for large headers or slow clients. |
| **Partial writes** | Sockets are non-blocking, so a large body *will* short-write once the send buffer fills. The unsent tail stays pending and resumes on the write edge. Ignoring `write()`'s return value silently truncates every response over ~64 KB. |
| **Path traversal** | Structurally impossible: serving is a lookup in a map built by walking the docroot, not a string concatenation into a filesystem path. `GET /../../etc/passwd` is a key that was never inserted. |
| **`EMFILE`** | Descriptor exhaustion is distinguished from `EAGAIN`. Under edge triggering, treating them alike means the accept loop never gets notified again and wedges permanently. |
| **`SIGPIPE`** | Ignored. Otherwise writing to a socket the peer already closed terminates the process — one rude client kills the server. |
| **Lingering close** | Closing a socket with unread data in its receive buffer makes the kernel send RST instead of FIN, and RST discards data the peer hasn't read — so the error response you just wrote is lost. Half-close, drain, then close. |
| **Idle connections** | Keep-alive connections are reaped from an LRU list after 15s, so a slowloris-style client can't park descriptors indefinitely. |
| **Deferred free** | kqueue can return a read and a write event for the same fd in one `wait()`. Connections are freed after the batch, never mid-batch, or the second event dereferences freed memory. |
| **Pipelining** | Multiple requests in one segment are answered in order. |
| **Blocking disk I/O** | None on the request path. Files are read once at startup; an `open()`/`read()` inside a handler stalls every connection on that loop behind it. |

---

## Benchmarks

**Measured, not estimated.** Read the methodology before quoting any of this.

4-core Linux container, loopback, `-O2`, 4 server threads. The load generator
shares the same 4 cores as the server, so these are *contended* numbers.

### Keep-alive

| Connections | Throughput | p50 | p99 | p99.9 |
|---|---|---|---|---|
| 50 | 387,630 req/s | 114 µs | 431 µs | 1.91 ms |
| 200 | 308,316 req/s | 460 µs | 2.33 ms | 3.54 ms |

### New connection per request

| Connections | Throughput | p50 | p99 | p99.9 |
|---|---|---|---|---|
| 50 | 85,492 req/s | 403 µs | 3.17 ms | 36.7 ms |

**Methodology and caveats — these matter more than the numbers:**

- Loopback has no NIC, no driver, and effectively no RTT. This measures the
  server's own overhead and nothing else. It is not a production figure.
- The load generator competes with the server for the same 4 cores.
- Every configuration was run 3+ times alternating. Run-to-run spread on this
  box is roughly ±15%, and the first binary in any sequence is consistently
  penalized by warmup. Single-run comparisons here are worthless — I got a
  "45% improvement" from one such pair that vanished entirely on repetition.
- Latency is reported as percentiles because the mean hides the tail, and the
  tail is what users experience.

### The connection pool does not measurably help

The most interesting result. `make nopool` compiles the pool out and routes
every connection through `new`/`delete`:

| Workload | Pooled | No pool |
|---|---|---|
| keep-alive, 50c | 366k / 388k req/s | 332k / 386k req/s |
| connection per request, 50c | 85.5k / 85.3k req/s | 96.3k / 76.8k req/s |

The two builds are indistinguishable inside the noise floor, and under churn
the no-pool build won as often as it lost. Why:

1. **Under keep-alive the allocator is off the hot path entirely.** 50
   connections served 4.5M requests — about 250 allocations total. Nothing that
   runs 250 times can matter across millions of requests.
2. **glibc `malloc` is already good at this.** It keeps per-thread caches, so a
   freed 8 KB chunk is handed straight back on the next allocation, still hot
   in cache. There is no syscall per allocation to eliminate — `malloc`
   amortises via arenas and only touches `brk`/`mmap` when one runs dry.

The 4.5× gap between the two workloads is the real finding: what costs is the
**connection lifecycle** — TCP handshake, `accept`, `close`, and the syscalls
around them — not object allocation. Optimizing the allocator was optimizing
the wrong thing.

The pool is kept because it does buy two things the benchmark doesn't show:
a bounded memory ceiling under load, and graceful degradation instead of
`std::bad_alloc` from a worker thread with no catch (which is `std::terminate`,
i.e. the whole server dies on a traffic spike). Both are correctness
properties, not speed. Tune with `-DCWS_POOL_CAPACITY=N`.

---

## Correctness

```bash
make test          # 7 edge-case groups, all must pass
make asan          # AddressSanitizer + UBSan
make tsan          # ThreadSanitizer
```

Last run: **ASan + UBSan clean** over 18,106 requests / 6,062 connections
(0 errors, 0 leaks). **TSan clean** over 19,200 requests across 6 threads
(0 races). Sanitizer builds are worth more than the benchmark here — a
use-after-free on the connection teardown path is the single easiest way to
get an event-driven server wrong, and it won't show up in `curl`.

```bash
python3 bench/edge_cases.py 8080
```
covers dribbled requests, pipelining, slow readers forcing short writes, 100
requests on one keep-alive socket, six classes of malformed request, oversized
headers, and 50 abrupt RSTs.

---

## Usage

```bash
./server [port] [docroot] [threads]     # defaults: 8080 www <hardware_concurrency>
```

```bash
curl -i http://localhost:8080/
curl -i http://localhost:8080/nope                            # 404
curl -i -X POST http://localhost:8080/                        # 405
curl -i --path-as-is http://localhost:8080/../../etc/passwd   # 400
```

Ctrl-C prints connection, request, byte, partial-write and pool-occupancy
counters.

---

## Deliberately not implemented

Being explicit about scope is more useful than pretending the gaps aren't
there:

- **HTTPS.** TLS termination belongs in front of this.
- **Request bodies.** GET and HEAD only; anything else is a 405.
- **Chunked transfer encoding**, compression, range requests, caching
  validators (`ETag` / `If-Modified-Since`).
- **HTTP/2, HTTP/3.**
- **Mutable docroot.** Content is snapshotted at startup; changes need a
  restart. `mmap` plus an fd cache with `inotify`/kqueue invalidation is the
  next step for a large or changing docroot.
- **`sendfile`/`splice`.** Bodies are copied from user space. For large static
  files `sendfile` would remove that copy — worth measuring, but the numbers
  above say the connection lifecycle dominates, so it likely wouldn't move
  keep-alive throughput much.

## Next

- Port the benchmark off loopback onto two machines with a real NIC.
- `perf` the churn workload to confirm the syscall hypothesis rather than
  inferring it from the pool A/B.
- `SO_REUSEPORT` listeners on Linux, measured against the current shared
  listener rather than assumed better.
