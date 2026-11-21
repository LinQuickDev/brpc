#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run mixed-size URMA regression tests with independent live connections."""

import argparse
import os
from pathlib import Path
import resource
import signal
import subprocess
import tempfile
import xml.etree.ElementTree as ET


def positive(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--connections", type=positive, default=1024)
    parser.add_argument("--rounds", type=positive, default=100)
    parser.add_argument("--timeout", type=positive, default=1800)
    parser.add_argument("--device", default="")
    args = parser.parse_args()
    # Each connection has client/server TCP fds and two CQ event fds.
    needed = args.connections * 8 + 1024
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < needed:
        if hard != resource.RLIM_INFINITY and hard < needed:
            parser.error(f"RLIMIT_NOFILE must be at least {needed}")
        resource.setrlimit(resource.RLIMIT_NOFILE, (needed, hard))
    binary = str(args.binary.resolve(strict=True))
    with tempfile.TemporaryDirectory(prefix="urma-stability-") as directory:
        for polling, inline in ((False, False), (True, False), (True, True)):
            report = Path(directory) / "result.xml"
            command = [
                binary, "--urma_stability_datapath",
                "--gtest_filter=UrmaStabilityDatapathTest.mixed_sizes_on_independent_live_connections",
                f"--gtest_output=xml:{report}",
                f"--urma_stability_connections={args.connections}",
                f"--urma_stability_rounds={args.rounds}",
                f"--urma_buffer_count={max(4096, args.connections * 40 + 4096)}",
                f"--urma_use_polling={str(polling).lower()}",
                f"--urma_disable_bthread={str(inline).lower()}",
            ]
            if args.device:
                command.append(f"--urma_device={args.device}")
            print(f"connections={args.connections} rounds={args.rounds} "
                  f"polling={polling} inline={inline}", flush=True)
            with subprocess.Popen(command, start_new_session=True) as process:
                try:
                    code = process.wait(timeout=args.timeout)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise
            if code:
                raise RuntimeError(f"URMA test process exited with status {code}")
            result = ET.parse(report).getroot()
            if int(result.get("tests", "0")) != 1 or result.findall(".//skipped"):
                raise RuntimeError("URMA mixed-size test did not run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
