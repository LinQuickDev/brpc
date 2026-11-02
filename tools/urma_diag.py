#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read URMA snapshots from brpc's existing builtin-services listener."""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def fetch(server, timeout, socket_id=None):
    if "://" not in server:
        server = "http://" + server
    url = urllib.parse.urlsplit(server)
    if url.scheme not in ("http", "https") or not url.netloc:
        raise ValueError("server must be an HTTP(S) address")
    if socket_id is not None and not socket_id.isdecimal():
        raise ValueError("socket ID must be a decimal integer")
    path = "/sockets/" + socket_id if socket_id is not None else "/sockets/urma"
    request_url = urllib.parse.urlunsplit(
        (url.scheme, url.netloc, url.path.rstrip("/") + path,
         "format=json" if socket_id is not None else "", "")
    )
    with urllib.request.urlopen(request_url, timeout=timeout) as response:
        snapshot = json.load(response)
    if not isinstance(snapshot, dict):
        raise ValueError("expected a URMA JSON object")
    if socket_id is not None:
        return {"schema_version": 1, "pool": {}, "connections": [snapshot], "truncated": False}
    if snapshot.get("schema_version") != 1:
        raise ValueError("unsupported URMA snapshot schema")
    return snapshot


def delta(previous, current, elapsed):
    """Compute rates only within the same SocketId and connection generation."""
    old = {item["socket_id"]: item for item in previous["connections"]}
    result = []
    for item in current["connections"]:
        before = old.get(item["socket_id"])
        row = {"socket_id": item["socket_id"], "state": item["state"]}
        reset = before is None or before["generation"] != item["generation"]
        row["reset"] = reset
        row["rates"] = {} if reset else {
            key + "_per_second": max(0, value - before["counters"].get(key, 0)) / elapsed
            for key, value in item["counters"].items()
        }
        result.append(row)
    return result


def select(snapshot, args):
    if args.command == "pool":
        return snapshot["pool"]
    connections = snapshot["connections"]
    if args.command in ("socket", "trace"):
        connections = [item for item in connections if item["socket_id"] == args.socket_id]
        if not connections:
            suffix = " (inventory truncated)" if snapshot.get("truncated") else ""
            raise ValueError("socket not present in snapshot" + suffix)
    if args.command == "trace":
        traces = connections[0]["traces"]
        if args.trace_id:
            traces = [item for item in traces if item["trace_id"] == args.trace_id]
        return traces
    return connections[0] if args.command == "socket" else {
        "connections": connections, "truncated": snapshot.get("truncated", False)
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, help="builtin-services host:port or URL")
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--json", action="store_true", help="compact JSON, one object per sample")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("connections")
    commands.add_parser("pool")
    socket = commands.add_parser("socket")
    socket.add_argument("socket_id")
    trace = commands.add_parser("trace")
    trace.add_argument("socket_id")
    trace.add_argument("--trace-id")
    watch = commands.add_parser("watch")
    watch.add_argument("--interval", type=float, default=1)
    watch.add_argument("--count", type=int, default=0, help="0 runs until interrupted")
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    if args.command == "watch" and (args.interval <= 0 or args.count < 0):
        parser.error("interval must be positive and count must be nonnegative")

    def emit(value):
        print(json.dumps(value, indent=None if args.json else 2), flush=True)

    try:
        previous = fetch(args.server, args.timeout, getattr(args, "socket_id", None))
        if args.command != "watch":
            emit(select(previous, args))
            return 0
        previous_time = time.monotonic()
        sample = 0
        while args.count == 0 or sample < args.count:
            time.sleep(args.interval)
            current = fetch(args.server, args.timeout)
            current_time = time.monotonic()
            emit({"elapsed_seconds": current_time - previous_time,
                  "connections": delta(previous, current, max(current_time - previous_time, 1e-6)),
                  "pool": current["pool"], "truncated": current.get("truncated", False)})
            previous, previous_time = current, current_time
            sample += 1
        return 0
    except (ValueError, OSError, urllib.error.URLError) as error:
        print("urma_diag: " + str(error), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
