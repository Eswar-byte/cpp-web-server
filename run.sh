#!/usr/bin/env bash
# Starts the server, runs a suite, stops it.
#
#   ./bench/run.sh test  [port]
#   ./bench/run.sh bench [port] [conns] [secs]
#
# Benchmark caveat, worth repeating because it is the thing people get wrong:
# loopback has no NIC, no driver and effectively no RTT, and the load generator
# competes with the server for the same cores. These numbers measure the
# server's own overhead. They are not production figures, and a single run of
# anything here is noise -- run each configuration at least three times.
set -euo pipefail

MODE="${1:-test}"
PORT="${2:-8080}"
CONNS="${3:-50}"
SECS="${4:-10}"
HOST=127.0.0.1
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

[ -x ./server ] || { echo "build first: make" >&2; exit 1; }

ulimit -n 65535 2>/dev/null || true

./server "$PORT" www 4 > /tmp/cws-run.log 2>&1 &
SRV=$!
cleanup() { kill -INT "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true; }
trap cleanup EXIT

sleep 1
kill -0 "$SRV" 2>/dev/null || { echo "server failed to start:"; cat /tmp/cws-run.log; exit 1; }

case "$MODE" in
  test)
    python3 bench/edge_cases.py "$PORT"
    ;;

  bench)
    [ -x ./loadgen ] || { echo "build first: make loadgen" >&2; exit 1; }
    # A large file has to exist for the short-write path to be exercised.
    [ -f www/big.bin ] || head -c 8388608 /dev/urandom > www/big.bin

    echo "=============================================================="
    echo " keep-alive"
    echo "=============================================================="
    for C in "$CONNS" $((CONNS * 4)); do
      ./loadgen "$HOST" "$PORT" / "$C" "$SECS"
      echo
    done

    echo "=============================================================="
    echo " new connection per request"
    echo "=============================================================="
    ./loadgen "$HOST" "$PORT" / "$CONNS" "$SECS" close
    echo

    echo "Run this 3+ times before believing any of it. Compare against"
    echo "'make nopool' to check whether the connection pool earns its place."
    ;;

  *)
    echo "usage: $0 {test|bench} [port] [conns] [secs]" >&2
    exit 1
    ;;
esac

echo
echo "--- server counters ---"
kill -INT "$SRV" 2>/dev/null || true
sleep 1
sed -n '/shutdown/,$p' /tmp/cws-run.log
