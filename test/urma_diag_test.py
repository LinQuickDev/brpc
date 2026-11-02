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

import contextlib
import copy
import http.server
import importlib.util
import io
import json
import pathlib
import threading
import unittest

spec = importlib.util.spec_from_file_location(
    "urma_diag", pathlib.Path(__file__).resolve().parents[1] / "tools/urma_diag.py")
diag = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diag)

SNAPSHOT = {"schema_version": 1, "pool": {"capacity": 8, "in_use": 2},
            "truncated": False, "connections": [
                {"socket_id": "18446744073709551614", "state": "ESTABLISHED",
                 "generation": 1, "counters": {"send_bytes": 100},
                 "traces": [{"trace_id": "42", "span_id": "17"}]}]}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/sockets/urma":
            body = SNAPSHOT
        elif self.path == "/sockets/" + SNAPSHOT["connections"][0]["socket_id"] + "?format=json":
            body = SNAPSHOT["connections"][0]
        else:
            self.send_error(404, "socket not present")
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, *_):
        pass


class UrmaDiagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever)
        cls.thread.start()
        cls.address = "127.0.0.1:" + str(cls.server.server_port)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def run_cli(self, *args):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = diag.main(["--server", self.address, "--json", *args])
        self.assertEqual(0, code)
        return [json.loads(line) for line in output.getvalue().splitlines()]

    def test_inventory_pool_socket_and_trace(self):
        self.assertEqual(SNAPSHOT["connections"], self.run_cli("connections")[0]["connections"])
        self.assertEqual(SNAPSHOT["pool"], self.run_cli("pool")[0])
        sid = SNAPSHOT["connections"][0]["socket_id"]
        self.assertEqual(sid, self.run_cli("socket", sid)[0]["socket_id"])
        self.assertEqual("42", self.run_cli("trace", sid, "--trace-id", "42")[0][0]["trace_id"])

    def test_delta_and_connection_reuse(self):
        current = copy.deepcopy(SNAPSHOT)
        current["connections"][0]["counters"]["send_bytes"] += 80
        self.assertEqual(40, diag.delta(SNAPSHOT, current, 2)[0]["rates"]["send_bytes_per_second"])
        current["connections"][0]["generation"] += 1
        self.assertTrue(diag.delta(SNAPSHOT, current, 2)[0]["reset"])
        self.assertEqual({}, diag.delta(SNAPSHOT, current, 2)[0]["rates"])

    def test_watch_is_finite(self):
        self.assertEqual(2, len(self.run_cli("watch", "--interval", "0.001", "--count", "2")))

    def test_missing_socket_returns_error(self):
        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            self.assertEqual(1, diag.main(["--server", self.address, "socket", "123"]))
        self.assertIn("404", error.getvalue())

    def test_invalid_scheme(self):
        with self.assertRaisesRegex(ValueError, "HTTP"):
            diag.fetch("file:///etc/passwd", 1)


if __name__ == "__main__":
    unittest.main()
