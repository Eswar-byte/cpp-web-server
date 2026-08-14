// loadgen.cpp -- closed-loop HTTP keep-alive load generator.
//
// Exists so the benchmark in the README is reproducible without installing
// wrk. One thread per connection, blocking sockets, one outstanding request
// each: concurrency is exactly the connection count, and every sample is a
// full round trip.
//
//   c++ -std=c++17 -O2 -pthread -o loadgen bench/loadgen.cpp
//   ./loadgen 127.0.0.1 8080 / 200 10           # keep-alive (default)
//   ./loadgen 127.0.0.1 8080 / 200 10 close     # new connection per request
//
// The two modes measure different things and it matters which one you quote.
// Keep-alive measures the request path. "close" measures the CONNECTION path
// -- accept, allocate, TCP handshake, teardown -- which is the only workload
// where a connection allocator can possibly show up in the numbers.
//
// Reports throughput and the latency tail. The mean is close to useless for a
// server -- p99/p99.9 is what a user actually experiences, and it is where
// head-of-line blocking and GC-like pauses show up.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

static std::atomic<bool> g_stop{false};

struct Result {
  std::vector<double> lat_us;
  long long requests = 0;
  long long errors = 0;
  long long bytes = 0;
};

static void worker(const std::string& host, int port, const std::string& path,
                   bool reconnect, Result* out) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

  const std::string req = "GET " + path + " HTTP/1.1\r\nHost: " + host +
                          (reconnect ? "\r\nConnection: close\r\n\r\n"
                                     : "\r\nConnection: keep-alive\r\n\r\n");
  std::vector<char> buf(65536);
  out->lat_us.reserve(1 << 16);

  int fd = -1;
  auto open_conn = [&]() -> bool {
    fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
      ::close(fd);
      fd = -1;
      return false;
    }
    return true;
  };

  if (!reconnect && !open_conn()) {
    ++out->errors;
    return;
  }

  while (!g_stop.load(std::memory_order_relaxed)) {
    auto t0 = std::chrono::steady_clock::now();

    // In "close" mode the handshake is part of the measured request, which is
    // the point: it is what a client without keep-alive actually pays.
    if (reconnect && !open_conn()) {
      ++out->errors;
      continue;
    }

    if (::send(fd, req.data(), req.size(), 0) != (ssize_t)req.size()) {
      ++out->errors;
      ::close(fd);
      fd = -1;
      if (reconnect) continue;
      break;
    }

    // Read headers, then exactly Content-Length bytes of body.
    std::string head;
    long clen = -1;
    size_t body_seen = 0;
    bool done = false;
    while (!done) {
      ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
      if (n <= 0) {
        ++out->errors;
        done = true;
        break;
      }
      out->bytes += n;
      if (clen < 0) {
        head.append(buf.data(), static_cast<size_t>(n));
        size_t hp = head.find("\r\n\r\n");
        if (hp == std::string::npos) continue;
        size_t cp = head.find("Content-Length: ");
        if (cp == std::string::npos) {
          ++out->errors;
          done = true;
          break;
        }
        clen = std::strtol(head.c_str() + cp + 16, nullptr, 10);
        body_seen = head.size() - (hp + 4);
      } else {
        body_seen += static_cast<size_t>(n);
      }
      if (clen >= 0 && body_seen >= static_cast<size_t>(clen)) done = true;
    }

    if (reconnect) {
      if (fd >= 0) ::close(fd);
      fd = -1;
    }
    if (clen < 0) {
      if (reconnect) continue;
      break;
    }

    auto t1 = std::chrono::steady_clock::now();
    out->lat_us.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
    ++out->requests;
  }
  if (fd >= 0) ::close(fd);
}

static double pct(std::vector<double>& v, double p) {
  if (v.empty()) return 0;
  size_t i = static_cast<size_t>(p / 100.0 * (v.size() - 1));
  return v[i];
}

int main(int argc, char** argv) {
  std::string host = argc > 1 ? argv[1] : "127.0.0.1";
  int port = argc > 2 ? std::atoi(argv[2]) : 8080;
  std::string path = argc > 3 ? argv[3] : "/";
  int conns = argc > 4 ? std::atoi(argv[4]) : 100;
  int secs = argc > 5 ? std::atoi(argv[5]) : 10;
  bool reconnect = argc > 6 && std::strcmp(argv[6], "close") == 0;

  std::printf("%d connections -> http://%s:%d%s for %ds  [%s]\n", conns,
              host.c_str(), port, path.c_str(), secs,
              reconnect ? "connection per request" : "keep-alive");

  std::vector<Result> results(static_cast<size_t>(conns));
  std::vector<std::thread> threads;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < conns; ++i) {
    threads.emplace_back(worker, host, port, path, reconnect,
                         &results[static_cast<size_t>(i)]);
  }
  std::this_thread::sleep_for(std::chrono::seconds(secs));
  g_stop.store(true);
  for (auto& t : threads) t.join();
  double elapsed =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
          .count();

  std::vector<double> all;
  long long reqs = 0, errs = 0, bytes = 0;
  for (auto& r : results) {
    all.insert(all.end(), r.lat_us.begin(), r.lat_us.end());
    reqs += r.requests;
    errs += r.errors;
    bytes += r.bytes;
  }
  std::sort(all.begin(), all.end());

  double mean = 0;
  for (double d : all) mean += d;
  if (!all.empty()) mean /= static_cast<double>(all.size());

  std::printf("\n  requests      %lld  (%lld errors)\n", reqs, errs);
  std::printf("  throughput    %.0f req/s\n", reqs / elapsed);
  std::printf("  transfer      %.1f MiB/s\n",
              bytes / elapsed / (1024.0 * 1024.0));
  std::printf("\n  latency (us)\n");
  std::printf("    mean        %8.1f\n", mean);
  std::printf("    p50         %8.1f\n", pct(all, 50));
  std::printf("    p90         %8.1f\n", pct(all, 90));
  std::printf("    p99         %8.1f\n", pct(all, 99));
  std::printf("    p99.9       %8.1f\n", pct(all, 99.9));
  std::printf("    max         %8.1f\n", all.empty() ? 0 : all.back());
  return 0;
}
