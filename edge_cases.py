#!/usr/bin/env python3
"""Edge cases that plain curl will not exercise.

These are the four failure modes that separate a server that works on a
laptop from one that works under load:

  1. A request split across TCP segments (a single read() sees a fragment).
  2. Pipelined requests (two requests arrive in one segment).
  3. A slow reader (forces short writes, so the unsent tail must be tracked).
  4. Idle keep-alive connections (must be reaped, or descriptors leak).

Usage: python3 bench/edge_cases.py [port]
"""
import socket
import sys
import time

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
HOST = "127.0.0.1"
fails = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond:
        fails.append(name)


def conn(timeout=10):
    s = socket.create_connection((HOST, PORT), timeout=timeout)
    s.settimeout(timeout)
    return s


def read_all(s, limit=None):
    buf = b""
    while True:
        try:
            b = s.recv(65536)
        except socket.timeout:
            break
        if not b:
            break
        buf += b
        if limit and len(buf) >= limit:
            break
    return buf


print("\n1. request split across segments (byte-at-a-time head)")
s = conn()
req = b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
for i, byte in enumerate(req):
    s.sendall(bytes([byte]))
    if i % 7 == 0:
        time.sleep(0.002)  # force distinct segments
data = read_all(s)
s.close()
check("dribbled request answered", data.startswith(b"HTTP/1.1 200"), data[:24].decode(errors="replace"))

print("\n2. pipelined requests in one segment")
s = conn()
s.sendall(
    b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"
    b"GET /style.css HTTP/1.1\r\nHost: x\r\n\r\n"
    b"GET /nope HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
)
data = read_all(s)
s.close()
n200 = data.count(b"HTTP/1.1 200 OK")
n404 = data.count(b"HTTP/1.1 404")
check("3 pipelined -> 3 responses in order", n200 == 2 and n404 == 1, f"200x{n200} 404x{n404}")
check("responses ordered (404 last)", data.rfind(b"HTTP/1.1 404") > data.rfind(b"HTTP/1.1 200 OK"))

print("\n3. slow reader on 8 MB body (forces short writes)")
s = conn(timeout=30)
s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)
s.sendall(b"GET /big.bin HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
total, chunks = 0, 0
while True:
    try:
        b = s.recv(4096)
    except socket.timeout:
        break
    if not b:
        break
    total += len(b)
    chunks += 1
    if chunks % 64 == 0:
        time.sleep(0.004)  # stall so the send buffer backs up
s.close()
hdr_end = 0
expect = 8 * 1024 * 1024
check("full 8 MB delivered to slow reader", total >= expect, f"{total} bytes in {chunks} recvs")

print("\n4. keep-alive: 100 requests on one socket")
s = conn()
ok = 0
for _ in range(100):
    s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
    buf = b""
    while b"\r\n\r\n" not in buf:
        buf += s.recv(65536)
    # drain the body using Content-Length
    clen = int(buf.split(b"Content-Length: ")[1].split(b"\r\n")[0])
    head = buf.index(b"\r\n\r\n") + 4
    while len(buf) - head < clen:
        buf += s.recv(65536)
    if buf.startswith(b"HTTP/1.1 200"):
        ok += 1
s.close()
check("100/100 served on one connection", ok == 100, f"{ok}/100")

print("\n5. malformed requests are rejected, not crashed on")
cases = {
    "no version": b"GET /\r\n\r\n",
    "empty line": b"\r\n\r\n",
    "garbage": b"\x00\x01\x02\xff\r\n\r\n",
    "bad percent": b"GET /%zz HTTP/1.1\r\n\r\n",
    "no leading slash": b"GET x HTTP/1.1\r\n\r\n",
    "http/0.9": b"GET /\r\n",
}
for name, payload in cases.items():
    try:
        s = conn(timeout=3)
        s.sendall(payload)
        d = read_all(s)
        s.close()
        check(f"{name}", d.startswith(b"HTTP/1.1 4") or d == b"", d[:20].decode(errors="replace"))
    except Exception as e:
        check(f"{name}", False, str(e))

print("\n6. oversized header -> 431, no overflow")
s = conn()
s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + b"A" * 16384 + b"\r\n\r\n")
d = read_all(s)
s.close()
check("16 KB header rejected", d.startswith(b"HTTP/1.1 431"), d[:24].decode(errors="replace"))

print("\n7. abrupt disconnect mid-request (RST path)")
for _ in range(50):
    s = conn(timeout=3)
    s.sendall(b"GET / HTTP/1.1\r\nHost:")
    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
    s.close()  # RST
time.sleep(0.3)
try:
    s = conn(timeout=3)
    s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    d = read_all(s)
    s.close()
    check("server alive after 50 resets", d.startswith(b"HTTP/1.1 200"))
except Exception as e:
    check("server alive after 50 resets", False, str(e))

print()
if fails:
    print(f"FAILED: {len(fails)} -> {', '.join(fails)}")
    sys.exit(1)
print("all edge cases passed")
