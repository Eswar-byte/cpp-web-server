#include <iostream>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <new>
#include <fstream>
#include <sstream>
#include <fcntl.h>
#include <sys/event.h>
#include <sys/time.h>

#define MAX_EVENTS 1000

// ==========================================
// UTILITIES
// ==========================================
void set_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
}

std::string get_mime_type(const std::string& path) {
    if (path.find(".html") != std::string::npos) return "text/html";
    if (path.find(".css") != std::string::npos) return "text/css";
    if (path.find(".js") != std::string::npos) return "application/javascript";
    if (path.find(".png") != std::string::npos) return "image/png";
    return "text/plain";
}

// ==========================================
// 1. CUSTOM MEMORY ALLOCATOR (Slab Pool)
// ==========================================
class MemoryPool {
private:
    struct FreeNode { FreeNode* next; };
    FreeNode* head;
    char* arena;
    std::mutex pool_mutex;

public:
    MemoryPool(size_t block_size, size_t num_blocks) {
        arena = new char[block_size * num_blocks];
        head = reinterpret_cast<FreeNode*>(arena);
        FreeNode* current = head;
        for (size_t i = 1; i < num_blocks; ++i) {
            current->next = reinterpret_cast<FreeNode*>(arena + i * block_size);
            current = current->next;
        }
        current->next = nullptr;
    }
    
    ~MemoryPool() { delete[] arena; }
    
    void* allocate() {
        std::lock_guard<std::mutex> lock(pool_mutex);
        if (!head) return nullptr;
        FreeNode* block = head;
        head = head->next;
        return block;
    }
    
    void deallocate(void* block) {
        std::lock_guard<std::mutex> lock(pool_mutex);
        FreeNode* node = static_cast<FreeNode*>(block);
        node->next = head;
        head = node;
    }
};

MemoryPool requestPool(8192, 10000);

class HttpRequest {
public:
    char buffer[8100]; 
    int socket_fd;     

    void* operator new(size_t size) {
        void* ptr = requestPool.allocate();
        if (!ptr) throw std::bad_alloc();
        return ptr;
    }
    void operator delete(void* ptr) {
        requestPool.deallocate(ptr);
    }
};

// ==========================================
// 2. THREAD POOL & APPLICATION LAYER
// ==========================================
class ThreadPool {
private:
    std::vector<std::thread> workers;
    std::queue<int> task_queue;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;

public:
    ThreadPool(size_t threads) : stop(false) {
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                while (true) {
                    int client_socket;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this] { 
                            return this->stop || !this->task_queue.empty(); 
                        });
                        
                        if (this->stop && this->task_queue.empty()) return;
                        
                        client_socket = this->task_queue.front();
                        this->task_queue.pop();
                    }
                    handle_connection(client_socket);
                }
            });
        }
    }

    void enqueue(int client_socket) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            task_queue.push(client_socket);
        }
        condition.notify_one();
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread &worker : workers) worker.join();
    }

private:
    void handle_connection(int client_socket) {
        HttpRequest* req = new HttpRequest();
        req->socket_fd = client_socket;

        int bytes_read = read(req->socket_fd, req->buffer, sizeof(req->buffer) - 1);
        if (bytes_read > 0) {
            req->buffer[bytes_read] = '\0';
            std::string request(req->buffer);

            size_t method_end = request.find(' ');
            size_t path_end = request.find(' ', method_end + 1);
            
            if (method_end != std::string::npos && path_end != std::string::npos) {
                std::string path = request.substr(method_end + 1, path_end - method_end - 1);
                if (path == "/") path = "/index.html";
                std::string filepath = "www" + path; 

                std::ifstream file(filepath, std::ios::binary);
                if (file.is_open()) {
                    std::stringstream buffer;
                    buffer << file.rdbuf();
                    std::string body = buffer.str();

                    std::string response = 
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: " + get_mime_type(filepath) + "\r\n"
                        "Content-Length: " + std::to_string(body.size()) + "\r\n"
                        "Connection: close\r\n\r\n" + body;

                    write(req->socket_fd, response.c_str(), response.size());
                } else {
                    std::string body = "<html><body><h1>404 Not Found</h1></body></html>";
                    std::string response = 
                        "HTTP/1.1 404 Not Found\r\n"
                        "Content-Type: text/html\r\n"
                        "Content-Length: " + std::to_string(body.size()) + "\r\n"
                        "Connection: close\r\n\r\n" + body;

                    write(req->socket_fd, response.c_str(), response.size());
                }
            }
        }
        close(req->socket_fd);
        delete req; 
    }
};

// ==========================================
// 3. NETWORKING (kqueue Event Loop)
// ==========================================
int main() {
    int server_fd;
    struct sockaddr_in address;
    int port = 8080;

    ThreadPool pool(4);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) exit(EXIT_FAILURE);
    
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));
    set_non_blocking(server_fd);

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) exit(EXIT_FAILURE);
    if (listen(server_fd, 1000) < 0) exit(EXIT_FAILURE);

    int kq = kqueue();
    if (kq == -1) exit(EXIT_FAILURE);

    struct kevent evSet;
    struct kevent evList[MAX_EVENTS];

    EV_SET(&evSet, server_fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, NULL);
    kevent(kq, &evSet, 1, NULL, 0, NULL);

    std::cout << "High-Performance kqueue Server listening on port " << port << "...\n";

    while (true) {
        int num_events = kevent(kq, NULL, 0, evList, MAX_EVENTS, NULL);

        for (int i = 0; i < num_events; i++) {
            if (evList[i].ident == static_cast<uintptr_t>(server_fd)) {
                while (true) {
                    struct sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int client_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
                    
                    if (client_socket == -1) break; 

                    set_non_blocking(client_socket);

                    EV_SET(&evSet, client_socket, EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, NULL);
                    kevent(kq, &evSet, 1, NULL, 0, NULL);
                }
            } else {
                pool.enqueue(evList[i].ident);
            }
        }
    }

    close(server_fd);
    return 0;
}