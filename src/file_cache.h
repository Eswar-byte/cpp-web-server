// file_cache.h -- the document root is walked once at startup, every file is
// read into memory, and its response headers are precomputed.
//
// Two things fall out of this that matter more than they might look:
//
// 1. PATH TRAVERSAL BECOMES STRUCTURALLY IMPOSSIBLE. Serving a request is a
//    lookup in a map whose keys were built by walking the docroot. There is no
//    string concatenation of user input into a filesystem path, so there is
//    nothing for "GET /../../etc/passwd" to escape into -- it is simply a key
//    that was never inserted, i.e. a 404. This is a much stronger guarantee
//    than sanitising the path with realpath() and hoping the check is airtight.
//
// 2. THE EVENT LOOP NEVER BLOCKS ON DISK. open()/read() are blocking syscalls.
//    Doing them inside a request handler stalls that thread for the whole
//    duration of an I/O, which defeats the point of having an event loop --
//    a handful of slow reads and every connection on that loop is stuck
//    behind them. Paying the I/O once at startup removes it from the hot path
//    entirely.
//
// The cache is fully built before any worker thread starts and is const
// afterwards, so it is shared across threads with no locking at all.
//
// Trade-off, stated plainly: memory scales with docroot size, and content
// changes require a restart. That is the correct trade for a static asset
// server; it is the wrong one for a large or mutable docroot, where mmap +
// an fd cache with inotify/kqueue invalidation is the next step.

#pragma once

#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace cws {

struct Resource {
  // Everything except the Date header and the terminating blank line, which
  // the loop appends per response. Two variants so that neither the header
  // nor the body is ever rebuilt per request -- responding is a writev() of
  // three pointers with zero allocation on the hot path.
  std::string header_keepalive;
  std::string header_close;
  std::vector<char> body;
};

class FileCache {
 public:
  bool load(const std::string& docroot) {
    docroot_ = docroot;
    struct stat st;
    if (::stat(docroot.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
      return false;
    }
    return walk(docroot, "");
  }

  // nullptr when absent. Safe to call concurrently: const after load().
  const Resource* find(const std::string& path) const {
    auto it = map_.find(path);
    return it == map_.end() ? nullptr : &it->second;
  }

  std::size_t entry_count() const { return map_.size(); }
  std::size_t byte_count() const { return bytes_; }
  const std::string& docroot() const { return docroot_; }

  std::vector<std::string> keys() const {
    std::vector<std::string> k;
    k.reserve(map_.size());
    for (const auto& kv : map_) k.push_back(kv.first);
    return k;
  }

  static std::string mime_for(const std::string& path) {
    // Match on the actual trailing extension. Using find(".html") anywhere in
    // the string means "notes.html.txt" is served as text/html, which is both
    // wrong and a small XSS foothold on a server that accepts uploads.
    std::size_t dot = path.rfind('.');
    std::size_t slash = path.rfind('/');
    if (dot == std::string::npos ||
        (slash != std::string::npos && dot < slash)) {
      return "application/octet-stream";
    }
    std::string ext = path.substr(dot + 1);
    for (char& c : ext) {
      if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    }
    if (ext == "html" || ext == "htm") return "text/html; charset=utf-8";
    if (ext == "css") return "text/css; charset=utf-8";
    if (ext == "js" || ext == "mjs") return "application/javascript; charset=utf-8";
    if (ext == "json") return "application/json";
    if (ext == "txt" || ext == "md") return "text/plain; charset=utf-8";
    if (ext == "svg") return "image/svg+xml";
    if (ext == "png") return "image/png";
    if (ext == "jpg" || ext == "jpeg") return "image/jpeg";
    if (ext == "gif") return "image/gif";
    if (ext == "webp") return "image/webp";
    if (ext == "ico") return "image/x-icon";
    if (ext == "woff2") return "font/woff2";
    if (ext == "woff") return "font/woff";
    if (ext == "wasm") return "application/wasm";
    if (ext == "pdf") return "application/pdf";
    return "application/octet-stream";
  }

 private:
  bool walk(const std::string& dir, const std::string& prefix) {
    DIR* d = ::opendir(dir.c_str());
    if (!d) return false;
    bool ok = true;
    struct dirent* ent;
    while ((ent = ::readdir(d)) != nullptr) {
      const char* name = ent->d_name;
      if (name[0] == '.') continue;  // skip ., .. and dotfiles

      std::string disk = dir + "/" + name;
      std::string url = prefix + "/" + name;

      struct stat st;
      if (::stat(disk.c_str(), &st) != 0) continue;

      if (S_ISDIR(st.st_mode)) {
        if (!walk(disk, url)) ok = false;
      } else if (S_ISREG(st.st_mode)) {
        if (!add(disk, url, static_cast<std::size_t>(st.st_size))) ok = false;
      }
    }
    ::closedir(d);

    // "/dir/" and "/dir" both resolve to "/dir/index.html" if it exists.
    std::string index = prefix + "/index.html";
    if (map_.count(index)) {
      map_[prefix.empty() ? "/" : prefix + "/"] = map_[index];
      if (!prefix.empty()) map_[prefix] = map_[index];
    }
    return ok;
  }

  bool add(const std::string& disk, const std::string& url, std::size_t size) {
    std::FILE* f = std::fopen(disk.c_str(), "rb");
    if (!f) return false;

    Resource r;
    r.body.resize(size);
    std::size_t got = size ? std::fread(r.body.data(), 1, size, f) : 0;
    std::fclose(f);
    if (got != size) return false;

    std::string mime = mime_for(url);
    std::string common = "HTTP/1.1 200 OK\r\nServer: cws/0.2\r\nContent-Type: " +
                         mime + "\r\nContent-Length: " +
                         std::to_string(size) + "\r\n";
    r.header_keepalive = common + "Connection: keep-alive\r\n";
    r.header_close = common + "Connection: close\r\n";

    bytes_ += size;
    map_[url] = std::move(r);
    return true;
  }

  std::string docroot_;
  std::unordered_map<std::string, Resource> map_;
  std::size_t bytes_ = 0;
};

}  // namespace cws
