# High-Performance C++ Web Server 🚀

A highly concurrent, multithreaded HTTP/1.1 web server built from scratch in C++. 
Engineered to handle thousands of concurrent connections by bypassing standard OS memory allocation and utilizing the BSD kernel event notification subsystem.

## 🧠 Core Systems Architecture

* **Custom Slab Allocator (Memory Pool):** Overloaded the `new` and `delete` operators for incoming requests to pull from a pre-allocated 8KB memory pool. This eliminates slow `malloc`/`brk` system calls and prevents heap fragmentation during high traffic.
* **macOS `kqueue` Event Loop:** Replaced the standard blocking `accept()` loop with BSD `kqueue` using Edge-Triggered (`EV_CLEAR`) and `EV_ONESHOT` flags. This ensures zero CPU cycles are wasted on idle connections and guarantees lock-free event scaling.
* **Thread-Safe Task Queue:** Implemented a custom Thread Pool using `std::mutex` and `std::condition_variable` to synchronize socket file descriptors, preventing race conditions across worker threads.
* **Zero-Dependency HTTP Parser:** Manually parses TCP payloads at the Application Layer to extract HTTP GET methods, URIs, and construct valid HTTP responses with dynamic MIME types.

## 🛠️ Quick Start

### Build and Run

```bash
# Clone the repository
git clone [https://github.com/Eswar-byte/cpp-web-server.git](https://github.com/Eswar-byte/cpp-web-server.git)
cd cpp-web-server

# Compile using the Makefile
make

# Start the server (runs on port 8080 by default)
make run
