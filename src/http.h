// http.h -- incremental HTTP/1.1 request-line + header parsing.
//
// The important property here versus a naive parser: this NEVER assumes the
// request arrived in one read(). TCP is a byte stream; a GET can and does get
// split across segments, and a slow client can dribble it a byte at a time.
// parse_request() is called repeatedly on a growing buffer and returns
// kIncomplete until it has actually seen the CRLFCRLF terminator.
//
// `scan_pos` is carried across calls so re-scanning is O(new bytes) rather
// than O(buffer) on every wakeup.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace cws {

enum class Parse {
  kIncomplete,         // need more bytes
  kOk,                 // req/consumed are populated
  kBadRequest,         // malformed -> 400
  kUnsupportedMethod,  // not GET/HEAD -> 405
  kHeadersTooLarge,    // buffer full, still no terminator -> 431
};

struct Request {
  enum Method { kGet, kHead };
  Method method = kGet;
  std::string path;        // percent-decoded, query stripped
  bool keep_alive = true;  // after applying version default + Connection hdr
};

namespace detail {

inline int hex_val(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

inline char lower(char c) {
  return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

inline bool iequals(const char* a, std::size_t alen, const char* lit) {
  std::size_t i = 0;
  for (; i < alen; ++i) {
    if (lit[i] == '\0') return false;
    if (lower(a[i]) != lit[i]) return false;
  }
  return lit[i] == '\0';
}

// Case-insensitive substring search, used on the Connection header value.
inline bool icontains(const char* hay, std::size_t n, const char* needle) {
  std::size_t m = std::strlen(needle);
  if (m == 0 || n < m) return false;
  for (std::size_t i = 0; i + m <= n; ++i) {
    std::size_t j = 0;
    while (j < m && lower(hay[i + j]) == needle[j]) ++j;
    if (j == m) return true;
  }
  return false;
}

inline bool percent_decode(const char* s, std::size_t n, std::string* out) {
  out->clear();
  out->reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    if (s[i] != '%') {
      out->push_back(s[i]);
      continue;
    }
    if (i + 2 >= n) return false;
    int hi = hex_val(s[i + 1]);
    int lo = hex_val(s[i + 2]);
    if (hi < 0 || lo < 0) return false;
    char c = static_cast<char>(hi * 16 + lo);
    if (c == '\0') return false;  // embedded NUL: reject outright
    out->push_back(c);
    i += 2;
  }
  return true;
}

}  // namespace detail

// Parses one request out of buf[0, len). On kOk, *consumed is the number of
// bytes the caller should drop from the front of the buffer.
inline Parse parse_request(const char* buf, std::size_t len, std::size_t cap,
                           std::size_t* scan_pos, Request* req,
                           std::size_t* consumed) {
  // ---- locate end of headers (CRLFCRLF), resuming where we left off -------
  std::size_t start = *scan_pos;
  if (start >= 3) {
    start -= 3;  // a terminator could straddle the previous boundary
  } else {
    start = 0;
  }

  std::size_t hdr_end = 0;  // index just past CRLFCRLF
  for (std::size_t i = start; i + 3 < len; ++i) {
    if (buf[i] == '\r' && buf[i + 1] == '\n' && buf[i + 2] == '\r' &&
        buf[i + 3] == '\n') {
      hdr_end = i + 4;
      break;
    }
  }
  if (hdr_end == 0) {
    *scan_pos = len;
    return (len >= cap) ? Parse::kHeadersTooLarge : Parse::kIncomplete;
  }
  *scan_pos = 0;
  *consumed = hdr_end;

  // ---- request line ------------------------------------------------------
  const char* line_end = static_cast<const char*>(
      std::memchr(buf, '\r', hdr_end));  // first CR ends the request line
  if (!line_end) return Parse::kBadRequest;
  std::size_t line_len = static_cast<std::size_t>(line_end - buf);

  const char* sp1 =
      static_cast<const char*>(std::memchr(buf, ' ', line_len));
  if (!sp1) return Parse::kBadRequest;
  std::size_t method_len = static_cast<std::size_t>(sp1 - buf);

  const char* after = sp1 + 1;
  std::size_t rest = line_len - method_len - 1;
  const char* sp2 = static_cast<const char*>(std::memchr(after, ' ', rest));
  if (!sp2) return Parse::kBadRequest;  // HTTP/0.9 style, not supported
  std::size_t target_len = static_cast<std::size_t>(sp2 - after);
  if (target_len == 0) return Parse::kBadRequest;

  const char* version = sp2 + 1;
  std::size_t version_len = line_len - method_len - 1 - target_len - 1;

  // ---- method ------------------------------------------------------------
  if (method_len == 3 && std::memcmp(buf, "GET", 3) == 0) {
    req->method = Request::kGet;
  } else if (method_len == 4 && std::memcmp(buf, "HEAD", 4) == 0) {
    req->method = Request::kHead;
  } else {
    return Parse::kUnsupportedMethod;
  }

  // ---- version -> default persistence ------------------------------------
  bool keep_alive;
  if (version_len == 8 && std::memcmp(version, "HTTP/1.1", 8) == 0) {
    keep_alive = true;
  } else if (version_len == 8 && std::memcmp(version, "HTTP/1.0", 8) == 0) {
    keep_alive = false;
  } else {
    return Parse::kBadRequest;
  }

  // ---- target: strip query/fragment, then percent-decode -----------------
  std::size_t plen = target_len;
  for (std::size_t i = 0; i < target_len; ++i) {
    if (after[i] == '?' || after[i] == '#') {
      plen = i;
      break;
    }
  }
  if (plen == 0 || after[0] != '/') return Parse::kBadRequest;
  if (!detail::percent_decode(after, plen, &req->path)) {
    return Parse::kBadRequest;
  }
  // Defence in depth. Traversal is already structurally impossible because
  // serving is a lookup in a prebuilt map (see file_cache.h) rather than a
  // filesystem concatenation -- the worst case is a 404. We still reject dot
  // segments so the rejection is explicit rather than incidental.
  if (req->path.find("..") != std::string::npos) return Parse::kBadRequest;

  // ---- headers: we only care about Connection ----------------------------
  std::size_t pos = static_cast<std::size_t>(line_end - buf) + 2;
  while (pos + 1 < hdr_end) {
    if (buf[pos] == '\r' && buf[pos + 1] == '\n') break;  // final CRLF
    const char* eol = static_cast<const char*>(
        std::memchr(buf + pos, '\r', hdr_end - pos));
    if (!eol) return Parse::kBadRequest;
    std::size_t nlen = static_cast<std::size_t>(eol - (buf + pos));

    const char* colon =
        static_cast<const char*>(std::memchr(buf + pos, ':', nlen));
    if (colon) {
      std::size_t name_len = static_cast<std::size_t>(colon - (buf + pos));
      if (detail::iequals(buf + pos, name_len, "connection")) {
        const char* v = colon + 1;
        std::size_t vlen = static_cast<std::size_t>(eol - v);
        while (vlen && (*v == ' ' || *v == '\t')) {
          ++v;
          --vlen;
        }
        if (detail::icontains(v, vlen, "close")) {
          keep_alive = false;
        } else if (detail::icontains(v, vlen, "keep-alive")) {
          keep_alive = true;
        }
      }
    }
    pos += nlen + 2;
  }

  req->keep_alive = keep_alive;
  return Parse::kOk;
}

}  // namespace cws
