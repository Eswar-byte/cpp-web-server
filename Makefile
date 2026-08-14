CXX      ?= c++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -Wpedantic -Wshadow -pthread
LDFLAGS  ?= -pthread

TARGET := server
SRC    := src/server.cpp
HDRS   := src/poller.h src/object_pool.h src/http.h src/file_cache.h

PORT    ?= 8080
DOCROOT ?= www
THREADS ?=
CONNS   ?= 50
SECS    ?= 10

.PHONY: all run test bench debug asan tsan nopool clean

all: $(TARGET)

$(TARGET): $(SRC) $(HDRS)
	$(CXX) $(CXXFLAGS) -o $@ $(SRC) $(LDFLAGS)

loadgen: bench/loadgen.cpp
	$(CXX) -std=c++17 -O2 -pthread -o $@ bench/loadgen.cpp

run: $(TARGET)
	./$(TARGET) $(PORT) $(DOCROOT) $(THREADS)

# Correctness suite: partial reads, pipelining, slow readers, malformed input,
# oversized headers, abrupt resets. Starts and stops the server itself.
test: $(TARGET)
	@./bench/run.sh test $(PORT)

# Throughput + latency percentiles, keep-alive and connection-per-request.
bench: $(TARGET) loadgen
	@./bench/run.sh bench $(PORT) $(CONNS) $(SECS)

# -O0 -g for stepping through the connection state machine.
debug: CXXFLAGS := -std=c++17 -O0 -g3 -Wall -Wextra -Wpedantic -Wshadow -pthread
debug: clean $(TARGET)

# Run the correctness suite against this before believing any benchmark.
# Catches use-after-free on the teardown path, which is where an event-driven
# server is easiest to get wrong and hardest to notice.
asan: CXXFLAGS := -std=c++17 -O1 -g -fsanitize=address,undefined \
                  -fno-omit-frame-pointer -Wall -Wextra -pthread
asan: LDFLAGS := -fsanitize=address,undefined -pthread
asan: clean $(TARGET)

# Data races across the event loops.
tsan: CXXFLAGS := -std=c++17 -O1 -g -fsanitize=thread \
                  -fno-omit-frame-pointer -Wall -Wextra -pthread
tsan: LDFLAGS := -fsanitize=thread -pthread
tsan: clean $(TARGET)

# Connection pool compiled out, so you can A/B it. See README "Benchmarks".
nopool: CXXFLAGS += -DCWS_NO_POOL=1
nopool: clean $(TARGET)

clean:
	rm -f $(TARGET) loadgen
	rm -rf $(TARGET).dSYM loadgen.dSYM
