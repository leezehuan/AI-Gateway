#!/usr/bin/env python3
import http.client
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


GATEWAY_KEY = ""
RPM_KEY = ""
CONCURRENCY_KEY = ""
BUDGET_KEY = ""
TENANT_B_KEY = ""
EXPIRED_KEY = ""
DISABLED_KEY = ""
DISABLED_TENANT_KEY = ""
DISABLED_POLICY_KEY = ""
PROTOCOL_DENY_KEY = ""
MODEL_DENY_KEY = ""
PROVIDER_DENY_KEY = ""
PROVIDER_KEY = "provider-test-key-0123456789-abcdefghijklmnopqrstuvwxyz"
PROVIDER_KEY_B = "provider-b-test-key-0123456789-abcdefghijklmnopqrstuv"


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class MockProviderState:
    def __init__(self):
        self.lock = threading.Lock()
        self.scenario = "success"
        self.requests = []
        self.stream_finished = threading.Event()
        self.provider_disconnected = threading.Event()
        self.active_streams = 0
        self.stream_events_sent = 0
        self.stream_events_total = 0
        self.health_status = 200
        self.health_requests = []

    def reset(self, scenario="success"):
        with self.lock:
            self.scenario = scenario
            self.requests.clear()
            self.stream_finished.clear()
            self.provider_disconnected.clear()
            self.active_streams = 0
            self.stream_events_sent = 0
            self.stream_events_total = 0

    def snapshot(self):
        with self.lock:
            return self.scenario, list(self.requests)

    def active_count(self):
        with self.lock:
            return self.active_streams

    def stream_progress(self):
        with self.lock:
            return self.stream_events_sent, self.stream_events_total

    def set_health(self, status):
        with self.lock:
            self.health_status = status
            self.health_requests.clear()

    def health_snapshot(self):
        with self.lock:
            return self.health_status, list(self.health_requests)


class MockProviderHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path != "/health":
            self.send_json(404, {"error": "not found"})
            return
        with self.server.state.lock:
            status = self.server.state.health_status
            self.server.state.health_requests.append(time.monotonic())
        self.send_json(status, {"ok": status == 200})

    def do_POST(self):
        if self.path == "/control":
            size = int(self.headers.get("content-length", "0"))
            control = json.loads(self.rfile.read(size))
            self.server.state.reset(control["scenario"])
            self.send_json(200, {"ok": True})
            return

        size = int(self.headers.get("content-length", "0"))
        raw = self.rfile.read(size)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = None
        scenario, _ = self.server.state.snapshot()
        record = {
            "headers": {key.lower(): value for key, value in self.headers.items()},
            "payload": payload,
            "body": raw.decode("utf-8", errors="replace"),
        }
        with self.server.state.lock:
            self.server.state.requests.append(record)

        if scenario == "slow":
            time.sleep(1.2)
        if scenario == "slow_success":
            time.sleep(0.3)
        if scenario == "stream_split_crlf":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            parts = [
                b": keepalive\r",
                b"\n\r",
                b"\nretry: 1000\r",
                b"\n\r",
                b"\nevent: response.created\r",
                b"\nfoo: ignored-extension\r",
                b"\ndata: {\r",
                b'\ndata: "type": "response.created",\r',
                b'\ndata: "sequence_number": 0}\r',
                b"\n\r",
                b"\nevent: response.completed\r\n",
                b'data: {"type":"response.completed","sequence_number":1}\r\n\r\n',
                b"data: [DONE]\r\n\r\n",
            ]
            for part in parts:
                self.wfile.write(part)
                self.wfile.flush()
                time.sleep(0.01)
            self.close_connection = True
        elif scenario == "stream_error_first":
            self.send_raw(
                200,
                b'event: error\ndata: {"type":"error","code":"provider_secret","message":"provider detail"}\n\n',
                "text/event-stream",
            )
        elif scenario == "stream_wrong_content_type":
            self.send_json(200, {"error": {"message": "provider detail", "secret": PROVIDER_KEY}})
        elif scenario == "stream_malformed_first":
            self.send_raw(200, b"event: response.created\ndata: not-json\n\n", "text/event-stream")
        elif scenario == "stream_oversized_first":
            oversized = json.dumps({"type": "response.created", "sequence_number": 0, "padding": "x" * 1100})
            self.send_raw(
                200,
                ("event: response.created\ndata: " + oversized + "\n\n").encode("utf-8"),
                "text/event-stream",
            )
        elif scenario in (
            "stream_close_after_commit",
            "stream_invalid_after_commit",
            "stream_error_after_commit",
        ):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            first = b'event: response.created\ndata: {"type":"response.created","sequence_number":0}\n\n'
            self.wfile.write(first)
            self.wfile.flush()
            if scenario == "stream_invalid_after_commit":
                self.wfile.write(b"event: response.output_text.delta\ndata: not-json\n\n")
                self.wfile.flush()
            elif scenario == "stream_error_after_commit":
                self.wfile.write(
                    b'event: error\ndata: {"type":"error","code":"provider_secret","message":"provider detail"}\n\n'
                )
                self.wfile.flush()
            self.close_connection = True
        elif scenario == "stream_trickled_incomplete_first":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            try:
                for part in (b"event", b": response", b".created", b"\ndata", b": {"):
                    self.wfile.write(part)
                    self.wfile.flush()
                    time.sleep(0.22)
            except (BrokenPipeError, ConnectionResetError):
                pass
            self.close_connection = True
        elif scenario in ("stream_disconnect_before_first", "stream_disconnect_after_first"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            with self.server.state.lock:
                self.server.state.active_streams += 1
            try:
                if scenario == "stream_disconnect_after_first":
                    self.wfile.write(
                        b'event: response.created\ndata: {"type":"response.created","sequence_number":0}\n\n'
                    )
                    self.wfile.flush()
                self.connection.settimeout(0.1)
                deadline = time.time() + 2
                while time.time() < deadline:
                    try:
                        pending = self.connection.recv(1, socket.MSG_PEEK)
                        if not pending:
                            self.server.state.provider_disconnected.set()
                            break
                    except socket.timeout:
                        continue
                    except OSError:
                        self.server.state.provider_disconnected.set()
                        break
            finally:
                with self.server.state.lock:
                    self.server.state.active_streams -= 1
            self.close_connection = True
        elif scenario in (
            "stream_idle_after_commit",
            "stream_max_duration",
            "stream_response_too_large",
        ):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            first = b'event: response.created\ndata: {"type":"response.created","sequence_number":0}\n\n'
            try:
                self.wfile.write(first)
                self.wfile.flush()
                time.sleep(0.1)
                if scenario == "stream_idle_after_commit":
                    time.sleep(1.0)
                elif scenario == "stream_max_duration":
                    for _ in range(20):
                        self.wfile.write(b": keepalive\n\n")
                        self.wfile.flush()
                        time.sleep(0.1)
                else:
                    oversized = json.dumps(
                        {"type": "response.output_text.delta", "sequence_number": 1, "delta": "x" * 2400}
                    )
                    self.wfile.write(
                        ("event: response.output_text.delta\ndata: " + oversized + "\n\n").encode("utf-8")
                    )
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            self.close_connection = True
        elif scenario == "stream_backpressure":
            total_deltas = 1600
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            with self.server.state.lock:
                self.server.state.stream_events_total = total_deltas + 2
            try:
                frames = [
                    b'event: response.created\ndata: {"type":"response.created","sequence_number":0}\n\n'
                ]
                for sequence in range(1, total_deltas + 1):
                    data = json.dumps(
                        {
                            "type": "response.output_text.delta",
                            "sequence_number": sequence,
                            "delta": "x" * 8192,
                        },
                        separators=(",", ":"),
                    )
                    frames.append(("event: response.output_text.delta\ndata: " + data + "\n\n").encode())
                frames.append(
                    (
                        "event: response.completed\ndata: "
                        + json.dumps(
                            {"type": "response.completed", "sequence_number": total_deltas + 1},
                            separators=(",", ":"),
                        )
                        + "\n\n"
                    ).encode()
                )
                for frame in frames:
                    self.wfile.write(frame)
                    self.wfile.flush()
                    with self.server.state.lock:
                        self.server.state.stream_events_sent += 1
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                self.server.state.stream_finished.set()
            except (BrokenPipeError, ConnectionResetError):
                pass
            self.close_connection = True
        elif scenario == "stream_concurrent_slow":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(
                b'event: response.created\ndata: {"type":"response.created","sequence_number":0}\n\n'
            )
            self.wfile.flush()
            time.sleep(0.3)
            self.wfile.write(
                b'event: response.completed\ndata: {"type":"response.completed","sequence_number":1}\n\n'
                b"data: [DONE]\n\n"
            )
            self.wfile.flush()
            self.close_connection = True
        elif scenario == "stream_sensitive_success":
            self.send_raw(
                200,
                b'event: response.created\ndata: {"type":"response.created","sequence_number":0}\n\n'
                b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","sequence_number":1,"delta":"event-body-never-log"}\n\n'
                b'event: response.completed\ndata: {"type":"response.completed","sequence_number":2}\n\n'
                b"data: [DONE]\n\n",
                "text/event-stream",
            )
        elif scenario == "stream_minimal_success":
            self.send_raw(
                200,
                b'event: response.created\ndata: {"type":"response.created","sequence_number":0}\n\n'
                b'event: response.completed\ndata: {"type":"response.completed","sequence_number":1}\n\n'
                b"data: [DONE]\n\n",
                "text/event-stream",
            )
        elif scenario == "stream_success":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            events = [
                ("response.created", {"type": "response.created", "sequence_number": 0}),
                (
                    "response.output_item.added",
                    {"type": "response.output_item.added", "sequence_number": 1, "item": {}},
                ),
                (
                    "response.content_part.added",
                    {"type": "response.content_part.added", "sequence_number": 2, "part": {}},
                ),
                (
                    "response.output_text.delta",
                    {"type": "response.output_text.delta", "sequence_number": 3, "delta": "hello"},
                ),
                (
                    "response.output_text.done",
                    {"type": "response.output_text.done", "sequence_number": 4, "text": "hello"},
                ),
                (
                    "response.content_part.done",
                    {"type": "response.content_part.done", "sequence_number": 5, "part": {}},
                ),
                (
                    "response.output_item.done",
                    {"type": "response.output_item.done", "sequence_number": 6, "item": {}},
                ),
                ("response.completed", {"type": "response.completed", "sequence_number": 7}),
            ]
            for index, (event, data) in enumerate(events):
                frame = f"event: {event}\ndata: {json.dumps(data)}\n\n".encode("utf-8")
                self.wfile.write(frame)
                self.wfile.flush()
                if index == 0:
                    time.sleep(0.35)
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.server.state.stream_finished.set()
            self.close_connection = True
        elif scenario == "stream_usage_exact":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("X-Request-ID", "provider-request-stream-usage-1")
            self.send_header("Connection", "close")
            self.end_headers()
            completed = {
                "type": "response.completed", "sequence_number": 1,
                "response": {"id": "resp_stream_usage", "usage": {
                    "input_tokens": 1100,
                    "input_tokens_details": {"cached_tokens": 100},
                    "output_tokens": 250,
                    "total_tokens": 1350,
                }},
            }
            self.wfile.write(
                b'event: response.created\ndata: {"type":"response.created","sequence_number":0}\n\n'
                + ("event: response.completed\ndata: "
                   + json.dumps(completed, separators=(",", ":")) + "\n\n").encode()
                + b"data: [DONE]\n\n"
            )
            self.wfile.flush()
            self.close_connection = True
        elif scenario == "status400":
            self.send_json(400, {"error": {"message": "provider detail"}})
        elif scenario == "status429":
            self.send_json(429, {"error": {"message": "provider detail"}}, {"Retry-After": "1"})
        elif scenario == "status500":
            self.send_json(500, {"error": {"message": "provider detail"}})
        elif scenario == "invalid_json":
            self.send_raw(200, b"not-json", "application/json")
        elif scenario == "too_large":
            self.send_raw(200, b"{" + b"x" * 3000 + b"}", "application/json")
        elif scenario == "usage_exact":
            self.send_json(
                200,
                {"id": "resp_usage", "object": "response", "model": "provider-model",
                 "output": [], "usage": {
                     "input_tokens": 1100,
                     "input_tokens_details": {"cached_tokens": 100},
                     "output_tokens": 250,
                     "total_tokens": 1350,
                 }},
                {"X-Request-ID": "provider-request-usage-1"},
            )
        elif scenario == "slow":
            try:
                self.send_json(200, {"id": "resp_late", "object": "response", "output": []})
            except (BrokenPipeError, ConnectionResetError):
                pass
        else:
            self.send_json(
                200,
                {
                    "id": "resp_mock",
                    "object": "response",
                    "model": "provider-model",
                    "output": [],
                    "input_echo": payload.get("input") if isinstance(payload, dict) else None,
                },
                {"Set-Cookie": "provider-secret=do-not-forward", "X-Provider-Secret": PROVIDER_KEY},
            )

    def send_json(self, status, payload, headers=None):
        self.send_raw(status, json.dumps(payload).encode("utf-8"), "application/json", headers)

    def send_raw(self, status, body, content_type, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def log_message(self, *_args):
        pass


class GatewayIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global GATEWAY_KEY, TENANT_B_KEY, EXPIRED_KEY, DISABLED_KEY
        global RPM_KEY, CONCURRENCY_KEY, BUDGET_KEY
        global DISABLED_TENANT_KEY, DISABLED_POLICY_KEY
        global PROTOCOL_DENY_KEY, MODEL_DENY_KEY, PROVIDER_DENY_KEY
        cls.gateway_binary = sys.argv[1]
        cls.admin_binary = sys.argv[2]
        cls.repo = sys.argv[3]
        cls.provider_state = MockProviderState()
        cls.provider_port = free_port()
        cls.provider = ThreadingHTTPServer(("127.0.0.1", cls.provider_port), MockProviderHandler)
        cls.provider.state = cls.provider_state
        cls.provider_thread = threading.Thread(target=cls.provider.serve_forever, daemon=True)
        cls.provider_thread.start()
        cls.provider_secondary_state = MockProviderState()
        cls.provider_secondary_port = free_port()
        cls.provider_secondary = ThreadingHTTPServer(
            ("127.0.0.1", cls.provider_secondary_port), MockProviderHandler
        )
        cls.provider_secondary.state = cls.provider_secondary_state
        cls.provider_secondary_thread = threading.Thread(
            target=cls.provider_secondary.serve_forever, daemon=True
        )
        cls.provider_secondary_thread.start()

        cls.database_temp = tempfile.TemporaryDirectory(prefix="aigw-http-")
        cls.secret_dir = os.path.join(cls.database_temp.name, "secrets")
        os.mkdir(cls.secret_dir)
        cls.database_dir = os.path.join(cls.database_temp.name, "data")
        cls.database_socket = os.path.join(cls.database_temp.name, "mariadb.sock")
        cls.database_port = free_port()
        subprocess.run(
            ["mariadb-install-db", "--no-defaults", f"--datadir={cls.database_dir}",
             "--auth-root-authentication-method=normal", "--skip-test-db"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        cls.database = subprocess.Popen(
            ["mariadbd", "--no-defaults", f"--datadir={cls.database_dir}",
             f"--socket={cls.database_socket}", f"--port={cls.database_port}",
             "--bind-address=127.0.0.1", "--skip-name-resolve",
             f"--pid-file={cls.database_temp.name}/mariadb.pid",
             f"--log-error={cls.database_temp.name}/mariadb.log"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            probe = subprocess.run(
                ["mariadb", "--no-defaults", f"--socket={cls.database_socket}",
                 "-u", "root", "-e", "SELECT 1"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if probe.returncode == 0:
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("temporary MariaDB did not start")
        subprocess.run(
            ["mariadb", "--no-defaults", f"--socket={cls.database_socket}", "-u", "root", "-e",
             "CREATE DATABASE ai_gateway; CREATE USER 'gateway'@'127.0.0.1' IDENTIFIED BY 'test-db-password'; "
             "GRANT ALL ON ai_gateway.* TO 'gateway'@'127.0.0.1'; FLUSH PRIVILEGES;"],
            check=True,
        )
        cls.redis_port = free_port()
        cls.redis = subprocess.Popen(
            ["redis-server", "--bind", "127.0.0.1", "--port", str(cls.redis_port),
             "--save", "", "--appendonly", "no", "--dir", cls.database_temp.name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            probe = subprocess.run(
                ["redis-cli", "-h", "127.0.0.1", "-p", str(cls.redis_port), "PING"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            )
            if probe.returncode == 0 and probe.stdout.strip() == "PONG":
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("temporary Redis did not start")
        cls.run_admin("migrate", "--dir", os.path.join(cls.repo, "migrations", "gateway"))
        config = cls.phase3_config()
        config_path = os.path.join(cls.database_temp.name, "config.json")
        with open(config_path, "w", encoding="utf-8") as output:
            json.dump(config, output)
        cls.run_admin("apply-config", "--file", config_path)
        GATEWAY_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "default", "--name", "phase2-regression"
        ).stdout.strip()
        RPM_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "default", "--name", "rpm-key",
            "--quota", "rpm-two"
        ).stdout.strip()
        CONCURRENCY_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "default",
            "--name", "concurrency-key", "--quota", "concurrency-one"
        ).stdout.strip()
        BUDGET_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "default",
            "--name", "budget-key", "--quota", "budget-fifty-cents"
        ).stdout.strip()
        TENANT_B_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-b", "--policy", "default", "--name", "tenant-b-key"
        ).stdout.strip()
        EXPIRED_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "default", "--name", "expired",
            "--expires-at", "2000-01-01T00:00:00Z"
        ).stdout.strip()
        DISABLED_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "default", "--name", "disabled"
        ).stdout.strip()
        disabled_key_id = cls.database_query(
            f"SELECT key_id FROM api_keys WHERE display_prefix='{DISABLED_KEY[5:17]}'"
        ).strip()
        cls.run_admin("set-key-status", "--key-id", disabled_key_id, "--status", "disabled")
        DISABLED_TENANT_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-disabled", "--policy", "default", "--name", "disabled-tenant"
        ).stdout.strip()
        DISABLED_POLICY_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-policy", "--policy", "disabled", "--name", "disabled-policy"
        ).stdout.strip()
        PROTOCOL_DENY_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "protocol-deny", "--name", "protocol-deny"
        ).stdout.strip()
        MODEL_DENY_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "model-deny", "--name", "model-deny"
        ).stdout.strip()
        PROVIDER_DENY_KEY = cls.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "provider-deny", "--name", "provider-deny"
        ).stdout.strip()

        cls.gateway_port = free_port()
        environment = cls.gateway_environment(cls.gateway_port)
        environment.update(
            {
                "AI_GATEWAY_MAX_BODY_BYTES": "1024",
                "AI_GATEWAY_MAX_RESPONSE_BYTES": "2048",
                "AI_GATEWAY_UPSTREAM_TIMEOUT_MS": "500",
                "AI_GATEWAY_IO_THREADS": "2",
                "AI_GATEWAY_STREAM_PREFETCH_BYTES": "1024",
                "AI_GATEWAY_STREAM_BUFFER_HIGH_WATER_BYTES": "8192",
                "AI_GATEWAY_STREAM_BUFFER_LOW_WATER_BYTES": "2048",
                "AI_GATEWAY_STREAM_IDLE_TIMEOUT_MS": "500",
                "AI_GATEWAY_STREAM_MAX_DURATION_MS": "1000",
                "AI_GATEWAY_CIRCUIT_FAILURE_THRESHOLD": "100",
            }
        )
        cls.gateway = subprocess.Popen(
            [cls.gateway_binary],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        cls.logs = []

        def drain():
            for line in cls.gateway.stdout:
                cls.logs.append(line)

        cls.log_thread = threading.Thread(target=drain, daemon=True)
        cls.log_thread.start()
        cls.wait_for_gateway()

        cls.gateway_secondary_port = free_port()
        cls.gateway_secondary = subprocess.Popen(
            [cls.gateway_binary],
            env=cls.gateway_environment(cls.gateway_secondary_port),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        cls.secondary_logs = []

        def drain_secondary():
            for line in cls.gateway_secondary.stdout:
                cls.secondary_logs.append(line)

        cls.secondary_log_thread = threading.Thread(target=drain_secondary, daemon=True)
        cls.secondary_log_thread.start()
        cls.wait_for_gateway_port(cls.gateway_secondary_port)

    @classmethod
    def gateway_environment(cls, listen_port):
        environment = {
            key: value for key, value in os.environ.items()
            if not key.startswith("AI_GATEWAY_")
        }
        environment.update({
            "AI_GATEWAY_LISTEN_ADDRESS": "127.0.0.1",
            "AI_GATEWAY_LISTEN_PORT": str(listen_port),
            "AI_GATEWAY_DB_HOST": "127.0.0.1",
            "AI_GATEWAY_DB_PORT": str(cls.database_port),
            "AI_GATEWAY_DB_USER": "gateway",
            "AI_GATEWAY_DB_PASSWORD": "test-db-password",
            "AI_GATEWAY_DB_NAME": "ai_gateway",
            "AI_GATEWAY_DB_CONNECT_TIMEOUT_SECONDS": "1",
            "AI_GATEWAY_DB_POOL_SIZE": "2",
            "AI_GATEWAY_DB_WORKERS": "2",
            "AI_GATEWAY_DB_QUEUE_SIZE": "128",
            "AI_GATEWAY_AUTH_CACHE_TTL_SECONDS": "30",
            "AI_GATEWAY_AUTH_CACHE_MAX_ENTRIES": "100",
            "AI_GATEWAY_CONFIG_POLL_INTERVAL_MS": "200",
            "AI_GATEWAY_API_KEY_HMAC_PEPPER": "phase3-test-pepper-that-is-at-least-32-bytes",
            "AI_GATEWAY_SECRET_DIR": cls.secret_dir,
            "AI_GATEWAY_REDIS_HOST": "127.0.0.1",
            "AI_GATEWAY_REDIS_PORT": str(cls.redis_port),
            "AI_GATEWAY_REDIS_WORKERS": "2",
            "AI_GATEWAY_REDIS_QUEUE_SIZE": "256",
            "AI_GATEWAY_REDIS_CONNECT_TIMEOUT_MS": "200",
            "AI_GATEWAY_REDIS_COMMAND_TIMEOUT_MS": "200",
            "TEST_PROVIDER_SECRET_A": PROVIDER_KEY,
            "TEST_PROVIDER_SECRET_B": PROVIDER_KEY_B,
            "TEST_PROVIDER_SECRET_FAILOVER": "provider-failover-secret-never-store",
        })
        return environment

    @classmethod
    def run_admin(cls, *arguments, check=True):
        result = subprocess.run(
            [cls.admin_binary, *arguments], env=cls.gateway_environment(1), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if check and result.returncode != 0:
            raise RuntimeError(f"AiGatewayAdmin failed: {result.stderr}")
        return result

    @classmethod
    def database_query(cls, sql):
        result = subprocess.run(
            ["mariadb", "--no-defaults", "-h", "127.0.0.1", "-P", str(cls.database_port),
             "-u", "gateway", "-ptest-db-password", "-N", "ai_gateway", "-e", sql],
            check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        return result.stdout

    @classmethod
    def apply_config_patch(cls, config, filename="patch.json"):
        path = os.path.join(cls.database_temp.name, filename)
        with open(path, "w", encoding="utf-8") as output:
            json.dump(config, output)
        return cls.run_admin("apply-config", "--file", path)

    @classmethod
    def public_key_id(cls, key):
        return cls.database_query(
            f"SELECT key_id FROM api_keys WHERE display_prefix='{key[5:17]}'"
        ).strip()

    @classmethod
    def wait_for_model(cls, model="gateway-model", timeout=3):
        deadline = time.time() + timeout
        while time.time() < deadline:
            status, _, body = cls.request(
                "GET", "/v1/models", headers={"Authorization": f"Bearer {GATEWAY_KEY}"}
            )
            if status == 200 and model in [entry["id"] for entry in json.loads(body)["data"]]:
                return
            time.sleep(0.05)
        raise AssertionError(f"model {model} did not become available")

    @classmethod
    def phase3_config(cls):
        endpoint = f"http://127.0.0.1:{cls.provider_port}/v1/responses"
        secondary_endpoint = f"http://127.0.0.1:{cls.provider_secondary_port}/v1/responses"
        return {
            "tenants": [
                {"slug": "tenant-a", "name": "Tenant A", "status": "active"},
                {"slug": "tenant-b", "name": "Tenant B", "status": "active"},
                {"slug": "tenant-disabled", "name": "Disabled Tenant", "status": "disabled"},
                {"slug": "tenant-policy", "name": "Policy Tenant", "status": "active"},
            ],
            "quota_policies": [
                {"tenant": "tenant-a", "slug": "rpm-two", "name": "RPM Two",
                 "rpm": 2, "status": "active"},
                {"tenant": "tenant-a", "slug": "concurrency-one",
                 "name": "Concurrency One", "rpm": 100, "concurrency": 1,
                 "status": "active"},
                {"tenant": "tenant-a", "slug": "credential-one",
                 "name": "Credential One", "rpm": 1, "status": "active"},
                {"tenant": "tenant-a", "slug": "budget-fifty-cents",
                 "name": "Budget Fifty Cents", "daily_budget_usd": "0.50",
                 "reservation_per_attempt_usd": "0.10", "status": "active"},
            ],
            "providers": [
                {"tenant": "tenant-a", "slug": "provider-a", "name": "Provider A",
                 "status": "active",
                 "endpoints": [{"name": "responses", "protocol": "responses",
                                "url": endpoint, "status": "active"}],
                 "credentials": [{"name": "default", "secret_ref": "env:TEST_PROVIDER_SECRET_A",
                                  "status": "active"}]},
                {"tenant": "tenant-b", "slug": "provider-b", "name": "Provider B",
                 "status": "active",
                 "endpoints": [{"name": "responses", "protocol": "responses",
                                "url": endpoint, "status": "active"}],
                 "credentials": [{"name": "default", "secret_ref": "env:TEST_PROVIDER_SECRET_B",
                                  "status": "active"}]},
                {"tenant": "tenant-a", "slug": "provider-secondary",
                 "name": "Provider Secondary", "status": "active",
                 "endpoints": [{"name": "responses", "protocol": "responses",
                                "url": secondary_endpoint, "status": "active"}],
                 "credentials": [{"name": "default",
                                  "secret_ref": "env:TEST_PROVIDER_SECRET_FAILOVER",
                                  "status": "active"}]},
            ],
            "logical_models": [
                {"tenant": "tenant-a", "protocol": "responses", "name": "gateway-model",
                 "status": "active"},
                {"tenant": "tenant-b", "protocol": "responses", "name": "gateway-model",
                 "status": "active"},
                {"tenant": "tenant-b", "protocol": "responses", "name": "tenant-b-only",
                 "status": "active"},
            ],
            "policies": [
                {"tenant": "tenant-a", "slug": "default", "name": "Default A",
                 "status": "active", "protocols": ["responses"],
                 "models": ["gateway-model"],
                 "providers": ["provider-a", "provider-secondary"]},
                {"tenant": "tenant-b", "slug": "default", "name": "Default B",
                 "status": "active", "protocols": ["responses"],
                 "models": ["gateway-model", "tenant-b-only"], "providers": ["provider-b"]},
                {"tenant": "tenant-a", "slug": "protocol-deny", "name": "Protocol Deny",
                 "status": "active", "protocols": [{"name": "responses", "enabled": False}],
                 "models": ["gateway-model"], "providers": ["provider-a"]},
                {"tenant": "tenant-a", "slug": "model-deny", "name": "Model Deny",
                 "status": "active", "protocols": ["responses"],
                 "models": [{"name": "gateway-model", "enabled": False}],
                 "providers": ["provider-a"]},
                {"tenant": "tenant-a", "slug": "provider-deny", "name": "Provider Deny",
                 "status": "active", "protocols": ["responses"], "models": ["gateway-model"],
                 "providers": [{"name": "provider-a", "enabled": False}]},
                {"tenant": "tenant-disabled", "slug": "default", "name": "Default",
                 "status": "active"},
                {"tenant": "tenant-policy", "slug": "disabled", "name": "Disabled",
                 "status": "disabled"},
            ],
            "mappings": [
                {"tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
                 "name": "primary", "provider": "provider-a", "endpoint": "responses",
                 "credential": "default", "upstream_model": "provider-model", "status": "active"},
                {"tenant": "tenant-b", "protocol": "responses", "logical_model": "gateway-model",
                 "name": "primary", "provider": "provider-b", "endpoint": "responses",
                 "credential": "default", "upstream_model": "provider-model-b", "status": "active"},
                {"tenant": "tenant-b", "protocol": "responses", "logical_model": "tenant-b-only",
                 "name": "primary", "provider": "provider-b", "endpoint": "responses",
                 "credential": "default", "upstream_model": "provider-model-b-only", "status": "active"},
            ],
            "model_prices": [
                {"tenant": "tenant-a", "provider": "provider-a",
                 "upstream_model": "provider-model", "version": "2026-08-01",
                 "effective_at": "2026-08-01T00:00:00Z",
                 "input_per_million_usd": "1.00",
                 "cached_input_per_million_usd": "0.10",
                 "output_per_million_usd": "4.00", "status": "active"},
            ],
        }

    @classmethod
    def tearDownClass(cls):
        cls.gateway_secondary.terminate()
        try:
            cls.gateway_secondary.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls.gateway_secondary.kill()
            cls.gateway_secondary.wait(timeout=5)
        cls.gateway_secondary.stdout.close()
        cls.secondary_log_thread.join(timeout=1)
        cls.gateway.terminate()
        try:
            cls.gateway.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls.gateway.kill()
            cls.gateway.wait(timeout=5)
        cls.gateway.stdout.close()
        cls.log_thread.join(timeout=1)
        cls.provider.shutdown()
        cls.provider.server_close()
        cls.provider_secondary.shutdown()
        cls.provider_secondary.server_close()
        cls.redis.terminate()
        cls.redis.wait(timeout=5)
        cls.database.terminate()
        cls.database.wait(timeout=5)
        cls.database_temp.cleanup()

    @classmethod
    def wait_for_gateway(cls):
        cls.wait_for_gateway_port(cls.gateway_port)

    @classmethod
    def wait_for_gateway_port(cls, port):
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                status, _, _ = cls.request_at(port, "GET", "/healthz")
                if status == 200:
                    ready, _, _ = cls.request_at(port, "GET", "/readyz")
                    if ready == 200:
                        return
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("AiGateway did not start")

    @classmethod
    def request(cls, method, path, body=None, headers=None, timeout=3):
        return cls.request_at(cls.gateway_port, method, path, body, headers, timeout)

    @classmethod
    def request_at(cls, port, method, path, body=None, headers=None, timeout=3):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
        encoded = None
        request_headers = dict(headers or {})
        if body is not None:
            encoded = body if isinstance(body, bytes) else body.encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")
        connection.request(method, path, encoded, request_headers)
        response = connection.getresponse()
        data = response.read()
        result = (response.status, dict(response.getheaders()), data)
        connection.close()
        return result

    @classmethod
    def start_redis(cls):
        process = subprocess.Popen(
            ["redis-server", "--bind", "127.0.0.1", "--port", str(cls.redis_port),
             "--save", "", "--appendonly", "no", "--dir", cls.database_temp.name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            probe = subprocess.run(
                ["redis-cli", "-h", "127.0.0.1", "-p", str(cls.redis_port), "PING"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            )
            if probe.returncode == 0 and probe.stdout.strip() == "PONG":
                return process
            time.sleep(0.05)
        process.terminate()
        process.wait(timeout=5)
        raise RuntimeError("temporary Redis did not restart")

    def setUp(self):
        self.provider_state.reset()

    def test_health_ready_and_models(self):
        status, headers, body = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], "ok")
        self.assertIn("x-request-id", {key.lower() for key in headers})

        status, _, body = self.request("GET", "/readyz")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], "ready")

        status, _, body = self.request(
            "GET", "/v1/models", headers={"Authorization": f"Bearer {GATEWAY_KEY}"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["data"][0]["id"], "gateway-model")

    def test_phase5_rpm_is_shared_across_gateway_nodes(self):
        headers = {"Authorization": f"Bearer {RPM_KEY}", "Content-Type": "application/json"}
        payload = json.dumps({"model": "gateway-model", "input": "quota", "stream": False})
        first = self.request_at(self.gateway_port, "POST", "/v1/responses", payload, headers)
        second = self.request_at(
            self.gateway_secondary_port, "POST", "/v1/responses", payload, headers
        )
        denied = self.request_at(self.gateway_port, "POST", "/v1/responses", payload, headers)
        self.assertEqual(first[0], 200)
        self.assertEqual(second[0], 200)
        self.assertEqual(denied[0], 429)
        self.assertEqual(json.loads(denied[2])["error"]["code"], "rate_limit_exceeded")
        self.assertIn("retry-after", {name.lower() for name in denied[1]})

    def test_phase5_concurrency_lease_is_shared_and_released(self):
        self.provider_state.reset("stream_concurrent_slow")
        headers = {"Authorization": f"Bearer {CONCURRENCY_KEY}",
                   "Content-Type": "application/json"}
        payload = json.dumps({"model": "gateway-model", "input": "hold", "stream": True})
        result = []
        active = threading.Thread(
            target=lambda: result.append(
                self.request_at(self.gateway_port, "POST", "/v1/responses",
                                payload, headers, timeout=3)
            )
        )
        active.start()
        time.sleep(0.08)
        denied = self.request_at(
            self.gateway_secondary_port, "POST", "/v1/responses", payload, headers
        )
        self.assertEqual(denied[0], 429)
        self.assertEqual(
            json.loads(denied[2])["error"]["code"], "concurrency_limit_exceeded"
        )
        active.join(timeout=3)
        self.assertFalse(active.is_alive())
        self.assertEqual(result[0][0], 200)

        self.provider_state.reset()
        deadline = time.time() + 2
        released = None
        while time.time() < deadline:
            released = self.request_at(
                self.gateway_secondary_port, "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "released", "stream": False}),
                headers,
            )
            if released[0] == 200:
                break
            self.assertEqual(
                json.loads(released[2])["error"]["code"], "concurrency_limit_exceeded"
            )
            time.sleep(0.01)
        self.assertEqual(released[0], 200)

    def test_phase5_credential_quota_skips_candidates_and_fails_without_provider_call(self):
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "credential-backup", "provider": "provider-secondary",
            "endpoint": "responses", "credential": "default",
            "upstream_model": "provider-secondary-model", "priority": 200,
            "status": "active",
        }
        providers = [
            {"tenant": "tenant-a", "slug": "provider-a", "name": "Provider A",
             "status": "active", "credentials": [
                 {"name": "default", "secret_ref": "env:TEST_PROVIDER_SECRET_A",
                  "quota_policy": "credential-one", "status": "active"}
             ]},
            {"tenant": "tenant-a", "slug": "provider-secondary",
             "name": "Provider Secondary", "status": "active", "credentials": [
                 {"name": "default", "secret_ref": "env:TEST_PROVIDER_SECRET_FAILOVER",
                  "quota_policy": "credential-one", "status": "active"}
             ]},
        ]
        try:
            self.apply_config_patch(
                {"providers": providers, "mappings": [secondary]},
                "credential-quota-enable.json",
            )
            time.sleep(0.4)
            headers = {"Authorization": f"Bearer {GATEWAY_KEY}",
                       "Content-Type": "application/json"}
            payload = json.dumps({"model": "gateway-model", "input": "credential quota"})

            self.provider_state.reset()
            self.provider_secondary_state.reset()
            first = self.request("POST", "/v1/responses", payload, headers)
            self.assertEqual(first[0], 200)
            self.assertEqual(len(self.provider_state.snapshot()[1]), 1)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 0)

            self.provider_state.reset()
            self.provider_secondary_state.reset()
            second = self.request_at(
                self.gateway_secondary_port, "POST", "/v1/responses", payload, headers
            )
            self.assertEqual(second[0], 200)
            self.assertEqual(len(self.provider_state.snapshot()[1]), 0)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 1)

            self.provider_state.reset()
            self.provider_secondary_state.reset()
            denied = self.request("POST", "/v1/responses", payload, headers)
            self.assertEqual(denied[0], 429)
            self.assertEqual(
                json.loads(denied[2])["error"]["code"], "credential_quota_exceeded"
            )
            self.assertEqual(len(self.provider_state.snapshot()[1]), 0)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 0)
        finally:
            for provider in providers:
                provider["credentials"][0]["quota_policy"] = None
            secondary["status"] = "disabled"
            self.apply_config_patch(
                {"providers": providers, "mappings": [secondary]},
                "credential-quota-disable.json",
            )
            time.sleep(0.4)

    def test_phase5_budget_reservation_is_atomic_and_refunds_unused_attempts(self):
        headers = {"Authorization": f"Bearer {BUDGET_KEY}",
                   "Content-Type": "application/json"}
        payload = json.dumps({"model": "gateway-model", "input": "budget"})
        self.provider_state.reset("slow_success")
        active_result = []
        active = threading.Thread(
            target=lambda: active_result.append(
                self.request("POST", "/v1/responses", payload, headers, timeout=3)
            )
        )
        active.start()
        deadline = time.time() + 2
        while time.time() < deadline and not self.provider_state.snapshot()[1]:
            time.sleep(0.01)
        self.assertTrue(self.provider_state.snapshot()[1])

        denied_in_flight = self.request_at(
            self.gateway_secondary_port, "POST", "/v1/responses", payload, headers
        )
        self.assertEqual(denied_in_flight[0], 429)
        self.assertEqual(
            json.loads(denied_in_flight[2])["error"]["code"], "budget_exceeded"
        )
        active.join(timeout=3)
        self.assertFalse(active.is_alive())
        self.assertEqual(active_result[0][0], 200)
        key_id = self.public_key_id(BUDGET_KEY)
        deadline = time.time() + 2
        while time.time() < deadline:
            completed = self.database_query(
                "SELECT COUNT(*) FROM usage_records WHERE state='succeeded' "
                f"AND api_key_id=(SELECT id FROM api_keys WHERE key_id='{key_id}')"
            ).strip()
            if completed == "1":
                break
            time.sleep(0.01)
        self.assertEqual(completed, "1")

        self.provider_state.reset()
        second = self.request("POST", "/v1/responses", payload, headers)
        third = self.request_at(
            self.gateway_secondary_port, "POST", "/v1/responses", payload, headers
        )
        self.assertEqual(second[0], 200)
        self.assertEqual(third[0], 200)

        calls_before_denial = len(self.provider_state.snapshot()[1])
        denied_spent = self.request("POST", "/v1/responses", payload, headers)
        self.assertEqual(denied_spent[0], 429)
        self.assertEqual(json.loads(denied_spent[2])["error"]["type"], "insufficient_quota")
        self.assertEqual(json.loads(denied_spent[2])["error"]["code"], "budget_exceeded")
        self.assertEqual(len(self.provider_state.snapshot()[1]), calls_before_denial)

        usage = self.database_query(
            "SELECT state, attempt_count, cost_microusd, cost_quality FROM usage_records "
            f"WHERE api_key_id=(SELECT id FROM api_keys WHERE key_id='{key_id}') "
            "ORDER BY id"
        ).strip().splitlines()
        self.assertEqual(usage, [
            "succeeded\t1\t100000\testimated",
            "succeeded\t1\t100000\testimated",
            "succeeded\t1\t100000\testimated",
        ])
        period = self.database_query(
            "SELECT reserved_microusd, settled_microusd FROM budget_periods "
            f"WHERE scope_type='api_key' AND scope_id=(SELECT id FROM api_keys WHERE key_id='{key_id}') "
            "AND period_kind='day' AND period_start=UTC_DATE()"
        ).strip()
        self.assertEqual(period, "0\t300000")

    def test_phase5_non_stream_usage_uses_effective_price_and_exact_integer_cost(self):
        self.provider_state.reset("usage_exact")
        status, headers, body = self.request(
            "POST", "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "priced"}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["usage"]["input_tokens"], 1100)
        request_id = {key.lower(): value for key, value in headers.items()}["x-request-id"]
        deadline = time.time() + 2
        while time.time() < deadline:
            usage = self.database_query(
                "SELECT state, input_tokens, cached_input_tokens, output_tokens, "
                "usage_quality, cost_microusd, cost_quality FROM usage_records "
                f"WHERE request_id='{request_id}'"
            ).strip()
            if usage.startswith("succeeded"):
                break
            time.sleep(0.01)
        self.assertEqual(usage, "succeeded\t1100\t100\t250\texact\t2010\texact")
        attempt = self.database_query(
            "SELECT provider_request_id, first_byte_ms IS NOT NULL, input_tokens, "
            "cached_input_tokens, output_tokens, usage_quality, mp.version, "
            "ra.cost_microusd, ra.cost_quality FROM request_attempts ra "
            "LEFT JOIN model_prices mp ON mp.id=ra.model_price_id "
            f"WHERE ra.request_id='{request_id}'"
        ).strip()
        self.assertEqual(
            attempt,
            "provider-request-usage-1\t1\t1100\t100\t250\texact\t2026-08-01\t2010\texact",
        )

    def test_phase5_stream_usage_is_read_from_response_completed(self):
        self.provider_state.reset("stream_usage_exact")
        status, headers, body = self.request(
            "POST", "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "priced stream", "stream": True}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
        )
        self.assertEqual(status, 200)
        self.assertIn(b"event: response.completed", body)
        self.assertTrue(body.endswith(b"data: [DONE]\n\n"))
        request_id = {key.lower(): value for key, value in headers.items()}["x-request-id"]
        deadline = time.time() + 2
        while time.time() < deadline:
            usage = self.database_query(
                "SELECT state, input_tokens, cached_input_tokens, output_tokens, "
                "usage_quality, cost_microusd, cost_quality FROM usage_records "
                f"WHERE request_id='{request_id}'"
            ).strip()
            if usage.startswith("succeeded"):
                break
            time.sleep(0.01)
        self.assertEqual(usage, "succeeded\t1100\t100\t250\texact\t2010\texact")
        attempt = self.database_query(
            "SELECT provider_request_id, input_tokens, cached_input_tokens, output_tokens, "
            "cost_microusd, cost_quality FROM request_attempts "
            f"WHERE request_id='{request_id}'"
        ).strip()
        self.assertEqual(
            attempt, "provider-request-stream-usage-1\t1100\t100\t250\t2010\texact"
        )

    def test_phase5_metrics_are_prometheus_text_and_exclude_sensitive_dimensions(self):
        self.provider_state.reset("usage_exact")
        prompt = "metrics-prompt-must-not-appear"
        status, headers, _ = self.request(
            "POST", "/v1/responses",
            json.dumps({"model": "gateway-model", "input": prompt}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
        )
        self.assertEqual(status, 200)
        request_id = {key.lower(): value for key, value in headers.items()}["x-request-id"]
        deadline = time.time() + 2
        metrics = b""
        while time.time() < deadline:
            metric_status, metric_headers, metrics = self.request("GET", "/metrics")
            if metric_status == 200 and b'ai_gateway_input_tokens_total' in metrics:
                break
            time.sleep(0.01)
        self.assertEqual(metric_status, 200)
        self.assertTrue(
            {key.lower(): value for key, value in metric_headers.items()}["content-type"].startswith(
                "text/plain"
            )
        )
        text = metrics.decode()
        self.assertIn("# TYPE ai_gateway_requests_total counter", text)
        self.assertIn('tenant="tenant-a"', text)
        self.assertIn('logical_model="gateway-model"', text)
        self.assertIn('provider="provider-a"', text)
        self.assertIn('credential="default"', text)
        self.assertIn("ai_gateway_input_tokens_total", text)
        self.assertIn("ai_gateway_cost_microusd_total", text)
        self.assertIn("ai_gateway_attempt_first_byte_ms", text)
        for sensitive in (
            GATEWAY_KEY, PROVIDER_KEY, prompt, request_id,
            f"http://127.0.0.1:{self.provider_port}/v1/responses",
        ):
            self.assertNotIn(sensitive, text)

    def test_phase5_two_nodes_share_health_probe_and_exclude_then_restore_candidate(self):
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "health-backup", "provider": "provider-secondary",
            "endpoint": "responses", "credential": "default",
            "upstream_model": "provider-secondary-model", "priority": 200,
            "status": "active",
        }
        health = {
            "tenant": "tenant-a", "provider": "provider-a", "name": "non-billable",
            "endpoint": "responses", "credential": "default", "method": "GET",
            "url": f"http://127.0.0.1:{self.provider_port}/health",
            "interval_ms": 1000, "timeout_ms": 200, "status": "active",
        }
        try:
            usage_before = self.database_query("SELECT COUNT(*) FROM usage_records").strip()
            attempts_before = self.database_query("SELECT COUNT(*) FROM request_attempts").strip()
            self.provider_state.set_health(500)
            self.apply_config_patch(
                {"mappings": [secondary], "health_checks": [health]},
                "health-probe-enable.json",
            )
            deadline = time.time() + 5
            while time.time() < deadline:
                _, probes = self.provider_state.health_snapshot()
                if len(probes) >= 3:
                    break
                time.sleep(0.05)
            self.assertGreaterEqual(len(probes), 3)
            for previous, current in zip(probes, probes[1:]):
                self.assertGreater(current - previous, 0.65)
            self.assertEqual(
                self.database_query("SELECT COUNT(*) FROM usage_records").strip(), usage_before
            )
            self.assertEqual(
                self.database_query("SELECT COUNT(*) FROM request_attempts").strip(),
                attempts_before,
            )

            self.provider_state.reset()
            self.provider_secondary_state.reset()
            status, _, body = self.request(
                "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "health excluded"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, 200, body)
            self.assertEqual(len(self.provider_state.snapshot()[1]), 0)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 1)

            self.provider_state.set_health(200)
            deadline = time.time() + 5
            while time.time() < deadline:
                _, recovered = self.provider_state.health_snapshot()
                if recovered:
                    break
                time.sleep(0.05)
            self.assertTrue(
                recovered,
                "primary logs:\n" + "".join(self.logs[-20:])
                + "\nsecondary logs:\n" + "".join(self.secondary_logs[-20:]),
            )
            time.sleep(0.2)
            self.provider_state.reset()
            self.provider_secondary_state.reset()
            status, _, body = self.request_at(
                self.gateway_secondary_port, "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "health restored"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, 200, body)
            self.assertEqual(len(self.provider_state.snapshot()[1]), 1)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 0)
        finally:
            health["status"] = "disabled"
            secondary["status"] = "disabled"
            self.apply_config_patch(
                {"mappings": [secondary], "health_checks": [health]},
                "health-probe-disable.json",
            )
            time.sleep(0.4)

    def test_phase5_coordinator_abandons_expired_usage_and_releases_budget(self):
        key_id = self.public_key_id(BUDGET_KEY)
        fixture_request_id = "req_phase5_abandoned_fixture"
        self.database_query(
            "INSERT INTO usage_records(request_id, tenant_id, api_key_id, logical_model_id, "
            "protocol, stream, state) SELECT '" + fixture_request_id + "', ak.tenant_id, ak.id, "
            "lm.id, 'responses', 0, 'started' FROM api_keys ak JOIN logical_models lm "
            "ON lm.tenant_id=ak.tenant_id AND lm.name='gateway-model' "
            f"WHERE ak.key_id='{key_id}'; "
            "SET @usage_id=LAST_INSERT_ID(); "
            "INSERT INTO budget_periods(scope_type, scope_id, period_kind, period_start, "
            "limit_microusd, reserved_microusd) SELECT 'api_key', id, 'day', UTC_DATE(), "
            f"500000, 100000 FROM api_keys WHERE key_id='{key_id}' "
            "ON DUPLICATE KEY UPDATE reserved_microusd=reserved_microusd+100000; "
            "INSERT INTO budget_reservations(usage_record_id, scope_type, scope_id, period_kind, "
            "period_start, reserved_microusd, lease_expires_at) SELECT @usage_id, 'api_key', id, "
            f"'day', UTC_DATE(), 100000, UTC_TIMESTAMP(6)-INTERVAL 1 SECOND FROM api_keys WHERE key_id='{key_id}'"
        )
        deadline = time.time() + 3
        while time.time() < deadline:
            state = self.database_query(
                "SELECT ur.state, ur.usage_quality, ur.cost_quality, br.state "
                "FROM usage_records ur JOIN budget_reservations br ON br.usage_record_id=ur.id "
                f"WHERE ur.request_id='{fixture_request_id}'"
            ).strip()
            if state.startswith("abandoned"):
                break
            time.sleep(0.05)
        self.assertEqual(state, "abandoned\tunknown\tunknown\treleased")
        reserved = self.database_query(
            "SELECT reserved_microusd FROM budget_periods "
            f"WHERE scope_type='api_key' AND scope_id=(SELECT id FROM api_keys WHERE key_id='{key_id}') "
            "AND period_kind='day' AND period_start=UTC_DATE()"
        ).strip()
        self.assertEqual(reserved, "0")

    def test_phase3_tenants_resolve_the_same_name_independently(self):
        self.provider_state.reset()
        status, _, _ = self.request(
            "POST", "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "tenant b"}),
            {"Authorization": f"Bearer {TENANT_B_KEY}"},
        )
        self.assertEqual(status, 200)
        _, requests = self.provider_state.snapshot()
        self.assertEqual(requests[-1]["payload"]["model"], "provider-model-b")
        self.assertEqual(requests[-1]["headers"]["authorization"], f"Bearer {PROVIDER_KEY_B}")
        self.assertNotEqual(requests[-1]["headers"]["authorization"], f"Bearer {TENANT_B_KEY}")

        status, _, body = self.request(
            "POST", "/v1/responses",
            json.dumps({"model": "tenant-b-only", "input": "cross tenant"}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body)["error"]["code"], "model_not_allowed")

        status, _, body = self.request(
            "GET", "/v1/models", headers={"Authorization": f"Bearer {TENANT_B_KEY}"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            [model["id"] for model in json.loads(body)["data"]],
            ["gateway-model", "tenant-b-only"],
        )

    def test_phase3_key_status_and_policy_grants_fail_closed(self):
        unknown_key = GATEWAY_KEY[:-1] + ("A" if GATEWAY_KEY[-1] != "A" else "B")
        for key in ("malformed", unknown_key, DISABLED_KEY, EXPIRED_KEY):
            status, _, body = self.request(
                "GET", "/v1/models", headers={"Authorization": f"Bearer {key}"}
            )
            self.assertEqual(status, 401, key[:20])
            self.assertEqual(json.loads(body)["error"]["code"], "invalid_api_key")

        for key in (DISABLED_TENANT_KEY, DISABLED_POLICY_KEY):
            status, _, body = self.request(
                "GET", "/v1/models", headers={"Authorization": f"Bearer {key}"}
            )
            self.assertEqual(status, 403)
            self.assertEqual(json.loads(body)["error"]["code"], "access_disabled")

        status, _, body = self.request(
            "POST", "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "denied"}),
            {"Authorization": f"Bearer {PROTOCOL_DENY_KEY}"},
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body)["error"]["code"], "protocol_not_allowed")

        for key in (MODEL_DENY_KEY, PROVIDER_DENY_KEY):
            status, _, body = self.request(
                "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "denied"}),
                {"Authorization": f"Bearer {key}"},
            )
            self.assertEqual(status, 403)
            self.assertEqual(json.loads(body)["error"]["code"], "model_not_allowed")

        status, _, body = self.request(
            "GET", "/v1/models", headers={"Authorization": f"Bearer {PROVIDER_DENY_KEY}"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["data"], [])

    def test_phase3_database_outage_fails_cached_auth_closed_and_recovers(self):
        self.assertEqual(
            self.request("GET", "/v1/models",
                         headers={"Authorization": f"Bearer {GATEWAY_KEY}"})[0],
            200,
        )
        self.database.terminate()
        self.database.wait(timeout=5)
        deadline = time.time() + 4
        while time.time() < deadline:
            status, _, _ = self.request("GET", "/readyz")
            if status == 503:
                break
            time.sleep(0.05)
        self.assertEqual(status, 503)

        started = time.time()
        health_status, _, _ = self.request("GET", "/healthz")
        self.assertEqual(health_status, 200)
        self.assertLess(time.time() - started, 0.5)
        status, _, body = self.request(
            "GET", "/v1/models", headers={"Authorization": f"Bearer {GATEWAY_KEY}"}
        )
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body)["error"]["code"], "authorization_unavailable")

        type(self).database = subprocess.Popen(
            ["mariadbd", "--no-defaults", f"--datadir={self.database_dir}",
             f"--socket={self.database_socket}", f"--port={self.database_port}",
             "--bind-address=127.0.0.1", "--skip-name-resolve",
             f"--pid-file={self.database_temp.name}/mariadb.pid",
             f"--log-error={self.database_temp.name}/mariadb.log"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 8
        while time.time() < deadline:
            try:
                status, _, _ = self.request("GET", "/readyz")
                if status == 200:
                    break
            except OSError:
                pass
            time.sleep(0.05)
        self.assertEqual(status, 200)
        self.assertEqual(
            self.request("GET", "/v1/models",
                         headers={"Authorization": f"Bearer {GATEWAY_KEY}"})[0],
            200,
        )

    def test_phase3_schema_mismatch_fails_readiness_and_recovers(self):
        self.database_query(
            "INSERT INTO schema_migrations(version, checksum) VALUES "
            "('9999_future_schema', REPEAT('0', 64))"
        )
        try:
            deadline = time.time() + 3
            while time.time() < deadline:
                status, _, _ = self.request("GET", "/readyz")
                if status == 503:
                    break
                time.sleep(0.05)
            self.assertEqual(status, 503)
            self.assertEqual(self.request("GET", "/healthz")[0], 200)
        finally:
            self.database_query(
                "DELETE FROM schema_migrations WHERE version='9999_future_schema'"
            )
        deadline = time.time() + 3
        while time.time() < deadline:
            status, _, _ = self.request("GET", "/readyz")
            if status == 200:
                break
            time.sleep(0.05)
        self.assertEqual(status, 200)

    def test_phase3_slow_database_auth_does_not_block_health(self):
        locker = subprocess.Popen(
            ["mariadb", "--no-defaults", "-h", "127.0.0.1", "-P", str(self.database_port),
             "-u", "gateway", "-ptest-db-password", "ai_gateway", "-e",
             "LOCK TABLES api_keys WRITE; DO SLEEP(1.2); UNLOCK TABLES"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(0.1)
        unknown_key = "aigw_ABCDEFGHIJKL_" + "A" * 43
        result = {}

        def authenticate():
            result["response"] = self.request(
                "GET", "/v1/models",
                headers={"Authorization": f"Bearer {unknown_key}"}, timeout=3,
            )

        thread = threading.Thread(target=authenticate)
        thread.start()
        time.sleep(0.1)
        started = time.time()
        status, _, _ = self.request("GET", "/healthz")
        elapsed = time.time() - started
        thread.join(timeout=4)
        locker.wait(timeout=4)
        self.assertEqual(status, 200)
        self.assertLess(elapsed, 0.5)
        self.assertEqual(result["response"][0], 401)

    def test_phase3_database_worker_queue_saturation_returns_503(self):
        port = free_port()
        environment = self.gateway_environment(port)
        environment.update({
            "AI_GATEWAY_DB_POOL_SIZE": "1",
            "AI_GATEWAY_DB_WORKERS": "1",
            "AI_GATEWAY_DB_QUEUE_SIZE": "1",
            "AI_GATEWAY_CONFIG_POLL_INTERVAL_MS": "60000",
        })
        process = subprocess.Popen(
            [self.gateway_binary], env=environment,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        locker = None
        try:
            deadline = time.time() + 5
            while time.time() < deadline:
                try:
                    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request("GET", "/readyz")
                    response = connection.getresponse()
                    response.read()
                    connection.close()
                    if response.status == 200:
                        break
                except OSError:
                    pass
                time.sleep(0.05)
            self.assertEqual(response.status, 200)

            locker = subprocess.Popen(
                ["mariadb", "--no-defaults", "-h", "127.0.0.1", "-P", str(self.database_port),
                 "-u", "gateway", "-ptest-db-password", "ai_gateway", "-e",
                 "LOCK TABLES api_keys WRITE; DO SLEEP(1.5); UNLOCK TABLES"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            time.sleep(0.1)
            results = []

            def authenticate(index):
                prefix = chr(ord("A") + index) * 12
                key = f"aigw_{prefix}_{'A' * 43}"
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=4)
                connection.request("GET", "/v1/models", headers={"Authorization": f"Bearer {key}"})
                response = connection.getresponse()
                body = response.read()
                results.append((response.status, body))
                connection.close()

            threads = []
            for index in range(3):
                thread = threading.Thread(target=authenticate, args=(index,))
                thread.start()
                threads.append(thread)
                time.sleep(0.05)

            started = time.time()
            health = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
            health.request("GET", "/healthz")
            health_response = health.getresponse()
            health_response.read()
            health.close()
            elapsed = time.time() - started
            for thread in threads:
                thread.join(timeout=4)
            self.assertEqual(health_response.status, 200)
            self.assertLess(elapsed, 0.5)
            self.assertEqual(len(results), 3)
            unavailable = [body for status, body in results if status == 503]
            self.assertTrue(unavailable)
            self.assertTrue(all(
                json.loads(body)["error"]["code"] == "authorization_unavailable"
                for body in unavailable
            ))
        finally:
            if locker is not None:
                locker.wait(timeout=4)
            process.terminate()
            process.wait(timeout=5)

    def test_phase3_prepared_admin_input_and_database_are_secret_free(self):
        injected_name = "key'); DROP TABLE tenants; --"
        key = self.run_admin(
            "issue-key", "--tenant", "tenant-a", "--policy", "default", "--name", injected_name
        ).stdout.strip()
        status, _, _ = self.request(
            "GET", "/v1/models", headers={"Authorization": f"Bearer {key}"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(self.database_query("SELECT COUNT(*) FROM tenants").strip(), "4")
        dump = subprocess.run(
            ["mariadb-dump", "--no-defaults", "-h", "127.0.0.1", "-P", str(self.database_port),
             "-u", "gateway", "-ptest-db-password", "--skip-comments", "ai_gateway"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ).stdout
        self.assertNotIn(key.encode(), dump)
        self.assertNotIn(GATEWAY_KEY.encode(), dump)
        self.assertNotIn(TENANT_B_KEY.encode(), dump)
        self.assertNotIn(PROVIDER_KEY.encode(), dump)
        self.assertNotIn(PROVIDER_KEY_B.encode(), dump)
        self.assertNotIn(b"prompt-never-log", dump)
        self.assertNotIn(b"event-body-never-log", dump)

    def test_phase3_version_invalidation_updates_key_and_mapping_without_restart(self):
        key_id = self.public_key_id(GATEWAY_KEY)
        self.assertEqual(
            self.request("GET", "/v1/models",
                         headers={"Authorization": f"Bearer {GATEWAY_KEY}"})[0],
            200,
        )
        try:
            self.run_admin("set-key-status", "--key-id", key_id, "--status", "disabled")
            deadline = time.time() + 2
            while time.time() < deadline:
                status, _, body = self.request(
                    "GET", "/v1/models", headers={"Authorization": f"Bearer {GATEWAY_KEY}"}
                )
                if status == 401:
                    break
                time.sleep(0.05)
            self.assertEqual(status, 401)
            self.assertEqual(json.loads(body)["error"]["code"], "invalid_api_key")
        finally:
            self.run_admin("set-key-status", "--key-id", key_id, "--status", "active")

        deadline = time.time() + 2
        while time.time() < deadline:
            status, _, _ = self.request(
                "GET", "/v1/models", headers={"Authorization": f"Bearer {GATEWAY_KEY}"}
            )
            if status == 200:
                break
            time.sleep(0.05)
        self.assertEqual(status, 200)

        mapping = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "primary", "provider": "provider-a", "endpoint": "responses",
            "credential": "default", "upstream_model": "provider-model-updated", "status": "active",
        }
        before = int(self.database_query(
            "SELECT version FROM gateway_config_versions WHERE singleton_id=1"
        ).strip())
        try:
            self.apply_config_patch({"mappings": [mapping]}, "mapping-update.json")
            after = int(self.database_query(
                "SELECT version FROM gateway_config_versions WHERE singleton_id=1"
            ).strip())
            self.assertEqual(after, before + 1)
            deadline = time.time() + 2
            observed = None
            while time.time() < deadline:
                self.provider_state.reset()
                status, _, _ = self.request(
                    "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": "version"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                if status == 200:
                    _, requests = self.provider_state.snapshot()
                    if requests:
                        observed = requests[-1]["payload"]["model"]
                if observed == "provider-model-updated":
                    break
                time.sleep(0.05)
            self.assertEqual(observed, "provider-model-updated")
        finally:
            mapping["upstream_model"] = "provider-model"
            self.apply_config_patch({"mappings": [mapping]}, "mapping-restore.json")
            self.wait_for_model()

    def test_phase4_missing_secret_is_unavailable_and_fixed_order_accepts_multiple_mappings(self):
        provider = {
            "tenant": "tenant-a", "slug": "provider-a", "name": "Provider A",
            "status": "active", "credentials": [
                {"name": "default", "secret_ref": "env:MISSING_PROVIDER_SECRET",
                 "status": "active"}
            ],
        }
        try:
            self.apply_config_patch({"providers": [provider]}, "missing-secret.json")
            deadline = time.time() + 2
            while time.time() < deadline:
                status, _, body = self.request(
                    "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": "secret"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                if status == 503:
                    break
                time.sleep(0.05)
            self.assertEqual(status, 503)
            self.assertEqual(json.loads(body)["error"]["code"], "model_unavailable")
            self.assertNotIn("MISSING_PROVIDER_SECRET", body.decode())
        finally:
            provider["credentials"][0]["secret_ref"] = "env:TEST_PROVIDER_SECRET_A"
            self.apply_config_patch({"providers": [provider]}, "secret-restore.json")
            self.wait_for_model()

        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "secondary", "provider": "provider-a", "endpoint": "responses",
            "credential": "default", "upstream_model": "duplicate-provider-model",
            "priority": 200, "status": "active",
        }
        try:
            self.apply_config_patch({"mappings": [secondary]}, "duplicate-mapping.json")
            time.sleep(0.4)
            deadline = time.time() + 2
            observed = None
            while time.time() < deadline:
                self.provider_state.reset()
                status, _, body = self.request(
                    "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": "duplicate"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                if status == 200:
                    _, requests = self.provider_state.snapshot()
                    if requests:
                        observed = requests[-1]["payload"]["model"]
                    break
                time.sleep(0.05)
            self.assertEqual(status, 200, body)
            self.assertEqual(observed, "provider-model")
        finally:
            secondary["status"] = "disabled"
            self.apply_config_patch({"mappings": [secondary]}, "duplicate-restore.json")
            self.wait_for_model()

    def test_phase3_file_secret_rejects_traversal_symlinks_and_oversized_files(self):
        secret_path = os.path.join(self.secret_dir, "provider.key")
        with open(secret_path, "w", encoding="ascii") as output:
            output.write("provider-key-from-file\n")
        provider = {
            "tenant": "tenant-a", "slug": "provider-a", "name": "Provider A",
            "status": "active", "credentials": [
                {"name": "default", "secret_ref": "file:provider.key", "status": "active"}
            ],
        }
        try:
            self.apply_config_patch({"providers": [provider]}, "file-secret.json")
            deadline = time.time() + 2
            observed = None
            while time.time() < deadline:
                self.provider_state.reset()
                status, _, _ = self.request(
                    "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": "file secret"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                _, requests = self.provider_state.snapshot()
                if status == 200 and requests:
                    observed = requests[-1]["headers"].get("authorization")
                if observed == "Bearer provider-key-from-file":
                    break
                time.sleep(0.05)
            self.assertEqual(observed, "Bearer provider-key-from-file")

            traversal = dict(provider)
            traversal["credentials"] = [
                {"name": "default", "secret_ref": "file:../provider.key", "status": "active"}
            ]
            path = os.path.join(self.database_temp.name, "traversal.json")
            with open(path, "w", encoding="utf-8") as output:
                json.dump({"providers": [traversal]}, output)
            rejected = self.run_admin("apply-config", "--file", path, check=False)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("secret_ref", rejected.stderr)

            os.symlink(secret_path, os.path.join(self.secret_dir, "linked.key"))
            provider["credentials"][0]["secret_ref"] = "file:linked.key"
            self.apply_config_patch({"providers": [provider]}, "symlink-secret.json")
            deadline = time.time() + 2
            while time.time() < deadline:
                status, _, body = self.request(
                    "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": "symlink"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                if status == 503:
                    break
                time.sleep(0.05)
            self.assertEqual(status, 503)
            self.assertEqual(json.loads(body)["error"]["code"], "model_unavailable")

            with open(os.path.join(self.secret_dir, "oversized.key"), "wb") as output:
                output.write(b"x" * (64 * 1024 + 1))
            provider["credentials"][0]["secret_ref"] = "file:oversized.key"
            self.apply_config_patch({"providers": [provider]}, "oversized-secret.json")
            deadline = time.time() + 2
            while time.time() < deadline:
                status, _, body = self.request(
                    "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": "oversized"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                if status == 503:
                    break
                time.sleep(0.05)
            self.assertEqual(status, 503)
            self.assertEqual(json.loads(body)["error"]["code"], "model_unavailable")
        finally:
            provider["credentials"][0]["secret_ref"] = "env:TEST_PROVIDER_SECRET_A"
            self.apply_config_patch({"providers": [provider]}, "file-secret-restore.json")
            self.wait_for_model()

    def test_readiness_fails_closed_when_configuration_is_missing(self):
        port = free_port()
        environment = self.gateway_environment(port)
        environment["AI_GATEWAY_DB_PORT"] = str(free_port())
        process = subprocess.Popen(
            [self.gateway_binary],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.time() + 3
            while True:
                try:
                    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request("GET", "/readyz")
                    response = connection.getresponse()
                    body = response.read()
                    connection.close()
                    break
                except OSError:
                    if time.time() >= deadline:
                        self.fail("not-ready AiGateway did not start")
                    time.sleep(0.05)
            self.assertEqual(response.status, 503)
            self.assertEqual(json.loads(body)["status"], "not_ready")
        finally:
            process.terminate()
            process.wait(timeout=5)

    def test_invalid_stream_watermarks_fail_at_startup(self):
        base = self.gateway_environment(free_port())
        for values, message in [
            (
                {
                    "AI_GATEWAY_STREAM_BUFFER_LOW_WATER_BYTES": "2048",
                    "AI_GATEWAY_STREAM_BUFFER_HIGH_WATER_BYTES": "2048",
                },
                "must be less than",
            ),
            (
                {
                    "AI_GATEWAY_STREAM_PREFETCH_BYTES": "4096",
                    "AI_GATEWAY_STREAM_BUFFER_HIGH_WATER_BYTES": "2048",
                    "AI_GATEWAY_STREAM_BUFFER_LOW_WATER_BYTES": "1024",
                },
                "must not exceed",
            ),
        ]:
            environment = dict(base)
            environment.update(values)
            process = subprocess.run(
                [self.gateway_binary],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=3,
                check=False,
            )
            self.assertNotEqual(process.returncode, 0)
            self.assertIn(message, process.stdout)
            self.assertNotIn(GATEWAY_KEY, process.stdout)
            self.assertNotIn(PROVIDER_KEY, process.stdout)

    def test_authentication_and_protocol_errors(self):
        status, _, body = self.request("GET", "/v1/models")
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(body)["error"]["code"], "invalid_api_key")

        status, _, body = self.request(
            "GET", "/v1/models", headers={"Authorization": "Bearer wrong"}
        )
        self.assertEqual(status, 401)

        common = {"Authorization": f"Bearer {GATEWAY_KEY}"}
        for body, expected_status, code in [
            ("not-json", 400, "invalid_json"),
            (json.dumps({"input": "missing model"}), 400, "invalid_model"),
            (json.dumps({"model": "other-model"}), 403, "model_not_allowed"),
        ]:
            status, _, response_body = self.request("POST", "/v1/responses", body, common)
            self.assertEqual(status, expected_status)
            self.assertEqual(json.loads(response_body)["error"]["code"], code)

        status, _, response_body = self.request(
            "POST",
            "/v1/responses",
            json.dumps({"model": "gateway-model"}),
            {"Authorization": f"Bearer {GATEWAY_KEY}", "Content-Type": "text/plain"},
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(response_body)["error"]["code"], "invalid_content_type")

    def test_success_rewrites_model_and_filters_credentials(self):
        request_body = json.dumps({"model": "gateway-model", "input": "hello", "store": False})
        status, response_headers, body = self.request(
            "POST",
            "/v1/responses",
            request_body,
            {
                "Authorization": f"Bearer {GATEWAY_KEY}",
                "X-Client-Request-Id": "client-123",
                "X-Codex-Turn-Metadata": "drop-private-metadata",
                "X-Evil": "drop-me",
                "Proxy-Authorization": "drop-me-too",
                "Host": "client-controlled.invalid",
                "Connection": "keep-alive, x-hop-secret",
                "X-Hop-Secret": "drop-hop-header",
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["model"], "gateway-model")
        self.assertIn("x-request-id", {key.lower() for key in response_headers})

        _, requests = self.provider_state.snapshot()
        self.assertEqual(len(requests), 1)
        provider_request = requests[0]
        self.assertEqual(provider_request["payload"]["model"], "provider-model")
        self.assertEqual(provider_request["headers"]["authorization"], f"Bearer {PROVIDER_KEY}")
        self.assertNotIn(GATEWAY_KEY, provider_request["body"])
        self.assertEqual(provider_request["headers"]["x-client-request-id"], "client-123")
        self.assertNotIn("x-evil", provider_request["headers"])
        self.assertNotIn("x-codex-turn-metadata", provider_request["headers"])
        self.assertNotIn("proxy-authorization", provider_request["headers"])
        self.assertNotEqual(provider_request["headers"]["host"], "client-controlled.invalid")
        self.assertNotIn("x-hop-secret", provider_request["headers"])
        self.assertNotIn("set-cookie", {key.lower() for key in response_headers})
        self.assertNotIn("x-provider-secret", {key.lower() for key in response_headers})

    def test_non_streaming_response_keeps_content_length_and_connection_reuse(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.gateway_port, timeout=3)
        connection.request(
            "POST",
            "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "hello", "stream": False}),
            {"Authorization": f"Bearer {GATEWAY_KEY}", "Content-Type": "application/json"},
        )
        response = connection.getresponse()
        body = response.read()
        self.assertEqual(response.status, 200)
        self.assertEqual(int(response.getheader("Content-Length")), len(body))
        self.assertIsNone(response.getheader("Transfer-Encoding"))
        self.assertFalse(response.will_close)

        connection.request("GET", "/healthz")
        health = connection.getresponse()
        health.read()
        self.assertEqual(health.status, 200)
        connection.close()

    def test_streaming_response_is_incremental_and_preserves_event_order(self):
        self.provider_state.reset("stream_success")
        connection = http.client.HTTPConnection("127.0.0.1", self.gateway_port, timeout=3)
        connection.request(
            "POST",
            "/v1/responses",
            json.dumps(
                {
                    "model": "gateway-model",
                    "input": "hello",
                    "instructions": "answer briefly",
                    "stream": True,
                    "store": False,
                    "parallel_tool_calls": True,
                    "tools": [],
                    "tool_choice": "auto",
                }
            ),
            {
                "Authorization": f"Bearer {GATEWAY_KEY}",
                "Content-Type": "application/json",
                "X-Client-Request-Id": "stream-client-123",
                "X-Codex-Turn-Metadata": "drop-private-metadata",
                "Proxy-Authorization": "drop-me",
            },
        )
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertTrue(response.getheader("Content-Type").startswith("text/event-stream"))
        self.assertEqual(response.getheader("Transfer-Encoding"), "chunked")
        self.assertIsNone(response.getheader("Content-Length"))
        self.assertEqual(response.getheader("Cache-Control"), "no-store")
        self.assertEqual(response.getheader("X-Accel-Buffering"), "no")

        started = time.time()
        first_event = response.readline() + response.readline() + response.readline()
        self.assertLess(time.time() - started, 0.25)
        self.assertIn(b"event: response.created", first_event)
        self.assertFalse(self.provider_state.stream_finished.is_set())

        remaining = response.read()
        connection.close()
        stream = first_event + remaining
        event_names = [
            line.removeprefix("event: ")
            for line in stream.decode("utf-8").splitlines()
            if line.startswith("event: ")
        ]
        self.assertEqual(
            event_names,
            [
                "response.created",
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta",
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.completed",
            ],
        )
        self.assertTrue(stream.endswith(b"data: [DONE]\n\n"))
        _, requests = self.provider_state.snapshot()
        provider_request = requests[0]
        self.assertEqual(provider_request["payload"]["model"], "provider-model")
        self.assertTrue(provider_request["payload"]["stream"])
        self.assertEqual(provider_request["payload"]["instructions"], "answer briefly")
        self.assertFalse(provider_request["payload"]["store"])
        self.assertEqual(provider_request["payload"]["tools"], [])
        self.assertEqual(provider_request["headers"]["authorization"], f"Bearer {PROVIDER_KEY}")
        self.assertEqual(provider_request["headers"]["accept"], "text/event-stream")
        self.assertEqual(provider_request["headers"]["x-client-request-id"], "stream-client-123")
        self.assertNotIn("x-codex-turn-metadata", provider_request["headers"])
        self.assertNotIn("proxy-authorization", provider_request["headers"])

    @unittest.skipUnless(shutil.which("nginx"), "nginx is not installed")
    def test_nginx_example_passes_config_test_and_does_not_buffer_sse(self):
        self.provider_state.reset("stream_success")
        nginx_port = free_port()
        nginx_dir = os.path.join(self.database_temp.name, "nginx")
        os.makedirs(nginx_dir, exist_ok=True)
        example_path = os.path.join(
            self.repo, "deploy", "nginx", "ai-gateway.conf.example"
        )
        with open(example_path, encoding="utf-8") as source:
            site = source.read()
        self.assertIn("proxy_buffering off;", site)
        self.assertIn("proxy_request_buffering off;", site)
        site = site.replace(
            "server 127.0.0.1:8080;",
            f"server 127.0.0.1:{self.gateway_port};",
        ).replace("listen 8081;", f"listen {nginx_port};")
        config_path = os.path.join(nginx_dir, "nginx.conf")
        with open(config_path, "w", encoding="utf-8") as output:
            output.write(
                "worker_processes 1;\n"
                f"pid {nginx_dir}/nginx.pid;\n"
                f"error_log {nginx_dir}/error.log notice;\n"
                "events { worker_connections 64; }\n"
                "http {\n"
                "access_log off;\n"
                f"{site}\n"
                "}\n"
            )

        config_test = subprocess.run(
            ["nginx", "-t", "-p", nginx_dir, "-c", config_path],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        self.assertEqual(config_test.returncode, 0, config_test.stdout)
        nginx = subprocess.Popen(
            ["nginx", "-p", nginx_dir, "-c", config_path, "-g", "daemon off;"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.time() + 5
            while time.time() < deadline:
                try:
                    probe = http.client.HTTPConnection("127.0.0.1", nginx_port, timeout=0.5)
                    probe.request("GET", "/healthz")
                    ready = probe.getresponse()
                    ready.read()
                    probe.close()
                    if ready.status == 200:
                        break
                except OSError:
                    time.sleep(0.05)
            else:
                self.fail("nginx did not become ready")

            metrics = http.client.HTTPConnection("127.0.0.1", nginx_port, timeout=1)
            metrics.request("GET", "/metrics")
            metrics_response = metrics.getresponse()
            metrics_response.read()
            metrics.close()
            self.assertEqual(metrics_response.status, 404)

            connection = http.client.HTTPConnection("127.0.0.1", nginx_port, timeout=3)
            connection.request(
                "POST",
                "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "hello", "stream": True}),
                {
                    "Authorization": f"Bearer {GATEWAY_KEY}",
                    "Content-Type": "application/json",
                },
            )
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(response.getheader("Content-Type").startswith("text/event-stream"))
            started = time.time()
            first_event = response.readline() + response.readline() + response.readline()
            self.assertLess(time.time() - started, 0.25)
            self.assertIn(b"event: response.created", first_event)
            self.assertFalse(self.provider_state.stream_finished.is_set())
            remaining = response.read()
            self.assertTrue((first_event + remaining).endswith(b"data: [DONE]\n\n"))
            connection.close()
        finally:
            nginx.terminate()
            try:
                nginx.wait(timeout=5)
            except subprocess.TimeoutExpired:
                nginx.kill()
                nginx.wait(timeout=5)

    @unittest.skipUnless(
        shutil.which("nginx") and os.environ.get("LINGSUAN_API_KEY")
        and os.environ.get("AI_GATEWAY_RUN_LIVE_TESTS") == "1",
        "Nginx, LINGSUAN_API_KEY, and AI_GATEWAY_RUN_LIVE_TESTS=1 are required",
    )
    def test_lingsuan_gpt_5_6_terra_live_through_nginx(self):
        self.run_admin(
            "apply-config", "--file",
            os.path.join(self.repo, "config", "ai-gateway.lingsuan.json"),
        )
        live_key = self.run_admin(
            "issue-key", "--tenant", "lingsuan", "--policy", "default",
            "--name", "phase5-live-test",
        ).stdout.strip()
        time.sleep(0.4)

        nginx_port = free_port()
        nginx_dir = os.path.join(self.database_temp.name, "nginx-lingsuan-live")
        os.makedirs(nginx_dir, exist_ok=True)
        with open(
            os.path.join(self.repo, "deploy", "nginx", "ai-gateway.conf.example"),
            encoding="utf-8",
        ) as source:
            site = source.read()
        site = site.replace(
            "server 127.0.0.1:8080;",
            f"server 127.0.0.1:{self.gateway_secondary_port};",
        ).replace("listen 8081;", f"listen {nginx_port};")
        config_path = os.path.join(nginx_dir, "nginx.conf")
        with open(config_path, "w", encoding="utf-8") as output:
            output.write(
                "worker_processes 1;\n"
                f"pid {nginx_dir}/nginx.pid;\n"
                f"error_log {nginx_dir}/error.log notice;\n"
                "events { worker_connections 64; }\n"
                "http { access_log off;\n"
                f"{site}\n"
                "}\n"
            )
        config_test = subprocess.run(
            ["nginx", "-t", "-p", nginx_dir, "-c", config_path],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
        )
        self.assertEqual(config_test.returncode, 0, config_test.stdout)
        nginx = subprocess.Popen(
            ["nginx", "-p", nginx_dir, "-c", config_path, "-g", "daemon off;"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.time() + 5
            while time.time() < deadline:
                try:
                    status, _, _ = self.request_at(nginx_port, "GET", "/healthz")
                    if status == 200:
                        break
                except OSError:
                    pass
                time.sleep(0.05)
            else:
                self.fail("live-test Nginx did not become ready")

            environment = dict(os.environ)
            environment.update({
                "AI_GATEWAY_NGINX_URL": f"http://127.0.0.1:{nginx_port}",
                "AI_GATEWAY_API_KEY": live_key,
                "AI_GATEWAY_LIVE_TEST_MODEL": "gpt-5.6-terra",
            })
            live = subprocess.run(
                [sys.executable, os.path.join(self.repo, "scripts", "test-lingsuan-nginx.py")],
                env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                check=False, timeout=180,
            )
            self.assertEqual(live.returncode, 0, live.stderr)
            result = json.loads(live.stdout)
            self.assertEqual(result["model"], "gpt-5.6-terra")
            self.assertEqual(result["non_streaming_status"], 200)
            self.assertEqual(result["streaming_status"], 200)
            self.assertTrue(result["response_completed"] or result["done_sentinel"])
        finally:
            nginx.terminate()
            try:
                nginx.wait(timeout=5)
            except subprocess.TimeoutExpired:
                nginx.kill()
                nginx.wait(timeout=5)

    def test_streaming_accepts_crlf_split_across_chunks_comments_and_multiline_data(self):
        self.provider_state.reset("stream_split_crlf")
        status, _, body = self.request(
            "POST",
            "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "hello", "stream": True}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
        )
        self.assertEqual(status, 200)
        self.assertTrue(body.startswith(b": keepalive\r\n"))
        self.assertIn(b"retry: 1000\r\n\r\n", body)
        self.assertIn(b"event: response.created\r\n", body)
        self.assertIn(b"foo: ignored-extension\r\n", body)
        self.assertIn(b"data: [DONE]\r\n\r\n", body)
        self.assertNotIn(b"event: error", body)

    def test_streaming_commit_gate_returns_sanitized_json_for_bad_first_response(self):
        for scenario, expected_status, expected_code in [
            ("stream_wrong_content_type", 502, "upstream_invalid_stream"),
            ("stream_error_first", 502, "upstream_stream_error"),
            ("stream_malformed_first", 502, "upstream_invalid_stream"),
            ("stream_oversized_first", 502, "upstream_prefetch_too_large"),
            ("status400", 400, "upstream_rejected_request"),
            ("status429", 429, "upstream_rate_limited"),
            ("status500", 502, "upstream_unavailable"),
        ]:
            self.provider_state.reset(scenario)
            status, headers, body = self.request(
                "POST",
                "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "secret prompt", "stream": True}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, expected_status, scenario)
            self.assertEqual(json.loads(body)["error"]["code"], expected_code)
            self.assertNotIn(b"event:", body)
            self.assertNotIn(PROVIDER_KEY.encode(), body)
            self.assertNotIn(b"provider detail", body)
            normalized = {key.lower(): value for key, value in headers.items()}
            self.assertEqual(normalized.get("content-type"), "application/json")

    def test_phase4_stream_failover_stops_after_downstream_commit(self):
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "stream-failover", "provider": "provider-secondary",
            "endpoint": "responses", "credential": "default",
            "upstream_model": "provider-secondary-model", "priority": 200,
            "status": "active",
        }
        try:
            self.apply_config_patch({"mappings": [secondary]}, "stream-failover-enable.json")
            time.sleep(0.4)

            for scenario in (
                "stream_error_first",
                "stream_wrong_content_type",
                "stream_malformed_first",
                "stream_oversized_first",
            ):
                subprocess.run(
                    ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "FLUSHDB"],
                    check=True, stdout=subprocess.DEVNULL,
                )
                self.provider_state.reset(scenario)
                self.provider_secondary_state.reset(
                    "stream_minimal_success" if scenario == "stream_oversized_first"
                    else "stream_success"
                )
                status, _, body = self.request(
                    "POST", "/v1/responses",
                    json.dumps({
                        "model": "gateway-model", "input": "before commit", "stream": True,
                    }),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"}, timeout=4,
                )
                self.assertEqual(status, 200, (scenario, body))
                self.assertTrue(body.endswith(b"data: [DONE]\n\n"), scenario)
                self.assertNotIn(b"event: error", body, scenario)
                self.assertEqual(len(self.provider_state.snapshot()[1]), 1, scenario)
                self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 1, scenario)

            subprocess.run(
                ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "FLUSHDB"],
                check=True, stdout=subprocess.DEVNULL,
            )
            self.provider_state.reset("stream_close_after_commit")
            self.provider_secondary_state.reset("stream_success")
            status, _, body = self.request(
                "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "after commit", "stream": True}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"}, timeout=4,
            )
            self.assertEqual(status, 200, body)
            self.assertEqual(body.count(b"event: error"), 1)
            self.assertFalse(body.endswith(b"data: [DONE]\n\n"))
            self.assertEqual(len(self.provider_state.snapshot()[1]), 1)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 0)
        finally:
            secondary["status"] = "disabled"
            self.apply_config_patch({"mappings": [secondary]}, "stream-failover-disable.json")
            time.sleep(0.4)

    def test_streaming_failure_after_commit_is_one_terminal_error_event(self):
        for scenario in (
            "stream_close_after_commit",
            "stream_invalid_after_commit",
            "stream_error_after_commit",
        ):
            self.provider_state.reset(scenario)
            status, headers, body = self.request(
                "POST",
                "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "hello", "stream": True}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, 200, scenario)
            normalized = {key.lower(): value for key, value in headers.items()}
            self.assertTrue(normalized["content-type"].startswith("text/event-stream"))
            self.assertEqual(body.count(b"event: error\n"), 1)
            self.assertIn(b'"code":"upstream_stream_error"', body)
            self.assertNotIn(b"data: [DONE]", body)
            self.assertEqual(body.count(b'"type":"error"'), 1)
            self.assertNotIn(b"provider detail", body)
            self.assertNotIn(b"provider_secret", body)

    def test_streaming_first_event_timeout_is_not_extended_by_partial_bytes(self):
        self.provider_state.reset("stream_trickled_incomplete_first")
        started = time.time()
        status, _, body = self.request(
            "POST",
            "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "hello", "stream": True}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
            timeout=3,
        )
        elapsed = time.time() - started
        self.assertEqual(status, 502)
        self.assertEqual(json.loads(body)["error"]["code"], "upstream_timeout")
        self.assertLess(elapsed, 0.9)

    def test_streaming_limits_after_commit_use_the_terminal_error_protocol(self):
        for scenario in (
            "stream_idle_after_commit",
            "stream_max_duration",
            "stream_response_too_large",
        ):
            self.provider_state.reset(scenario)
            status, _, body = self.request(
                "POST",
                "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "hello", "stream": True}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
                timeout=3,
            )
            self.assertEqual(status, 200, scenario)
            self.assertEqual(body.count(b"event: error\n"), 1, scenario)
            self.assertIn(b'"code":"upstream_stream_error"', body, scenario)
            self.assertNotIn(b"data: [DONE]", body, scenario)

    def test_client_disconnect_cancels_provider_before_and_after_stream_commit(self):
        for scenario in ("stream_disconnect_before_first", "stream_disconnect_after_first"):
            self.provider_state.reset(scenario)
            connection = http.client.HTTPConnection("127.0.0.1", self.gateway_port, timeout=3)
            connection.request(
                "POST",
                "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "hello", "stream": True}),
                {"Authorization": f"Bearer {GATEWAY_KEY}", "Content-Type": "application/json"},
            )
            if scenario == "stream_disconnect_after_first":
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                first = response.readline() + response.readline() + response.readline()
                self.assertIn(b"event: response.created", first)
            else:
                deadline = time.time() + 1
                while time.time() < deadline and not self.provider_state.snapshot()[1]:
                    time.sleep(0.01)
            connection.close()
            self.assertTrue(self.provider_state.provider_disconnected.wait(1.5), scenario)
            deadline = time.time() + 1
            while time.time() < deadline and self.provider_state.active_count() != 0:
                time.sleep(0.01)
            self.assertEqual(self.provider_state.active_count(), 0, scenario)
            provider_requests = self.provider_state.snapshot()[1]
            request_id = provider_requests[-1]["headers"]["x-request-id"]
            deadline = time.time() + 2
            attempt_state = ""
            while time.time() < deadline:
                attempt_state = self.database_query(
                    "SELECT state FROM request_attempts "
                    f"WHERE request_id='{request_id}' ORDER BY attempt_number DESC LIMIT 1"
                ).strip()
                if attempt_state == "cancelled":
                    break
                time.sleep(0.05)
            self.assertEqual(attempt_state, "cancelled", scenario)

    def test_slow_client_backpressures_and_resumes_without_event_loss(self):
        self.provider_state.reset("stream_backpressure")
        gateway_port = free_port()
        environment = self.gateway_environment(gateway_port)
        environment.update(
            {
                "AI_GATEWAY_MAX_BODY_BYTES": "1024",
                "AI_GATEWAY_MAX_RESPONSE_BYTES": str(20 * 1024 * 1024),
                "AI_GATEWAY_UPSTREAM_TIMEOUT_MS": "1000",
                "AI_GATEWAY_IO_THREADS": "2",
                "AI_GATEWAY_STREAM_PREFETCH_BYTES": "1024",
                "AI_GATEWAY_STREAM_BUFFER_HIGH_WATER_BYTES": "1024",
                "AI_GATEWAY_STREAM_BUFFER_LOW_WATER_BYTES": "128",
                "AI_GATEWAY_STREAM_IDLE_TIMEOUT_MS": "2000",
                "AI_GATEWAY_STREAM_MAX_DURATION_MS": "10000",
            }
        )
        process = subprocess.Popen(
            [self.gateway_binary],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        logs = []
        log_thread = threading.Thread(
            target=lambda: logs.extend(iter(process.stdout.readline, "")), daemon=True
        )
        log_thread.start()
        connection = None
        try:
            deadline = time.time() + 5
            while True:
                try:
                    probe = http.client.HTTPConnection("127.0.0.1", gateway_port, timeout=1)
                    probe.request("GET", "/readyz")
                    ready_response = probe.getresponse()
                    ready_response.read()
                    probe.close()
                    if ready_response.status == 200:
                        break
                except OSError:
                    if time.time() >= deadline:
                        self.fail("backpressure AiGateway did not start")
                    time.sleep(0.05)

            connection = http.client.HTTPConnection("127.0.0.1", gateway_port, timeout=10)
            connection.request(
                "POST",
                "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "hello", "stream": True}),
                {"Authorization": f"Bearer {GATEWAY_KEY}", "Content-Type": "application/json"},
            )
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            time.sleep(0.6)
            sent, total = self.provider_state.stream_progress()
            self.assertGreater(sent, 0)
            self.assertLess(sent, total)

            body = response.read()
            connection.close()
            connection = None
            self.assertTrue(self.provider_state.stream_finished.wait(3))
            sequences = []
            for line in body.splitlines():
                if line.startswith(b"data: {"):
                    payload = json.loads(line.removeprefix(b"data: "))
                    sequences.append(payload["sequence_number"])
            self.assertEqual(sequences, list(range(total)))
            self.assertTrue(body.endswith(b"data: [DONE]\n\n"))

            deadline = time.time() + 2
            completion = None
            while time.time() < deadline and completion is None:
                for line in logs:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if record.get("event") == "request_completed" and record.get("stream") == "true":
                        completion = record
                time.sleep(0.01)
            self.assertIsNotNone(completion)
            self.assertGreater(int(completion["backpressure_pauses"]), 0)
            self.assertEqual(int(completion["response_bytes"]), len(body))
        finally:
            if connection is not None:
                connection.close()
            process.terminate()
            process.wait(timeout=5)
            process.stdout.close()
            log_thread.join(timeout=1)

    def test_concurrent_slow_streams_do_not_block_health_or_each_other(self):
        self.provider_state.reset("stream_concurrent_slow")
        results = []

        def consume_stream():
            results.append(
                self.request(
                    "POST",
                    "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": "hello", "stream": True}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                    timeout=3,
                )
            )

        threads = [threading.Thread(target=consume_stream) for _ in range(3)]
        for thread in threads:
            thread.start()
        deadline = time.time() + 1
        while time.time() < deadline and len(self.provider_state.snapshot()[1]) < 3:
            time.sleep(0.01)

        started = time.time()
        status, _, _ = self.request("GET", "/healthz")
        health_elapsed = time.time() - started
        for thread in threads:
            thread.join(timeout=3)

        self.assertEqual(status, 200)
        self.assertLess(health_elapsed, 0.5)
        self.assertEqual(len(results), 3)
        self.assertTrue(all(result[0] == 200 for result in results))
        self.assertTrue(
            all(result[2].endswith(b"data: [DONE]\n\n") for result in results),
            repr([(result[0], result[2]) for result in results]),
        )

    def test_body_and_upstream_limits(self):
        status, _, body = self.request(
            "POST",
            "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "x" * 1100}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
        )
        self.assertEqual(status, 413)
        self.assertEqual(json.loads(body)["error"]["code"], "request_too_large")

        for scenario, code in [("invalid_json", "upstream_invalid_response"), ("too_large", "upstream_response_too_large")]:
            self.provider_state.reset(scenario)
            status, _, body = self.request(
                "POST",
                "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "hello"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, 502)
            self.assertEqual(json.loads(body)["error"]["code"], code)

    def test_phase4_precommit_5xx_fails_over_and_audits_each_attempt(self):
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "failover", "provider": "provider-secondary", "endpoint": "responses",
            "credential": "default", "upstream_model": "provider-secondary-model",
            "priority": 200, "status": "active",
        }
        try:
            self.apply_config_patch({"mappings": [secondary]}, "failover-enable.json")
            time.sleep(0.4)
            self.provider_state.reset("status500")
            self.provider_secondary_state.reset()
            status, headers, body = self.request(
                "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "failover-audit"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, 200, body)
            self.assertEqual(len(self.provider_state.snapshot()[1]), 1)
            secondary_requests = self.provider_secondary_state.snapshot()[1]
            self.assertEqual(len(secondary_requests), 1)
            self.assertEqual(secondary_requests[0]["payload"]["model"], "provider-secondary-model")
            self.assertEqual(
                secondary_requests[0]["headers"]["authorization"],
                "Bearer provider-failover-secret-never-store",
            )

            request_id = {key.lower(): value for key, value in headers.items()}["x-request-id"]
            deadline = time.time() + 2
            attempts = ""
            while time.time() < deadline:
                attempts = self.database_query(
                    "SELECT attempt_number, state, provider_status, error_class, retryable, "
                    "possible_duplicate_cost FROM request_attempts "
                    f"WHERE request_id='{request_id}' ORDER BY attempt_number"
                ).strip()
                if len(attempts.splitlines()) == 2:
                    break
                time.sleep(0.05)
            self.assertEqual(
                attempts.splitlines(),
                ["1\tfailed\t500\tupstream_unavailable\t1\t1",
                 "2\tsucceeded\t200\tsuccess\t0\t0"],
            )
            completion = None
            deadline = time.time() + 2
            while time.time() < deadline and completion is None:
                for line in self.logs:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (record.get("event") == "request_completed" and
                            record.get("request_id") == request_id):
                        completion = record
                        break
                time.sleep(0.01)
            self.assertIsNotNone(completion)
            self.assertEqual(completion["failover_count"], "1")
            attempt_logs = []
            for line in self.logs:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (record.get("event") == "attempt_completed" and
                        record.get("request_id") == request_id):
                    attempt_logs.append(record)
            self.assertEqual(len(attempt_logs), 2)
            for record in attempt_logs:
                self.assertEqual(record["tenant_slug"], "tenant-a")
                self.assertEqual(record["api_key_id"], self.public_key_id(GATEWAY_KEY))
                self.assertIn("attempt_id", record)
                self.assertIn("candidate", record)
                self.assertIn("retryable", record)
                self.assertIn("possible_duplicate_cost", record)
            audit_dump = self.database_query(
                "SELECT * FROM request_attempts "
                f"WHERE request_id='{request_id}' ORDER BY attempt_number"
            )
            for sensitive in (
                GATEWAY_KEY, PROVIDER_KEY, "failover-audit",
                f"http://127.0.0.1:{self.provider_port}/v1/responses",
            ):
                self.assertNotIn(sensitive, audit_dump)
        finally:
            secondary["status"] = "disabled"
            self.apply_config_patch({"mappings": [secondary]}, "failover-disable.json")
            time.sleep(0.4)

    def test_phase4_other_retryable_precommit_failures_fail_over(self):
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "failure-matrix", "provider": "provider-secondary", "endpoint": "responses",
            "credential": "default", "upstream_model": "provider-secondary-model",
            "priority": 200, "status": "active",
        }
        try:
            self.apply_config_patch({"mappings": [secondary]}, "failure-matrix-enable.json")
            time.sleep(0.4)
            for scenario in ("status429", "invalid_json", "too_large"):
                subprocess.run(
                    ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "FLUSHDB"],
                    check=True, stdout=subprocess.DEVNULL,
                )
                self.provider_state.reset(scenario)
                self.provider_secondary_state.reset()
                status, _, body = self.request(
                    "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": f"failure-{scenario}"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                self.assertEqual(status, 200, (scenario, body))
                self.assertEqual(len(self.provider_state.snapshot()[1]), 1, scenario)
                self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 1, scenario)
        finally:
            secondary["status"] = "disabled"
            self.apply_config_patch({"mappings": [secondary]}, "failure-matrix-disable.json")
            time.sleep(0.4)

    def test_phase4_connection_failure_fails_over_without_duplicate_cost_risk(self):
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "connection-backup", "provider": "provider-secondary",
            "endpoint": "responses", "credential": "default",
            "upstream_model": "provider-secondary-model", "priority": 200, "status": "active",
        }
        provider = {
            "tenant": "tenant-a", "slug": "provider-a", "name": "Provider A",
            "status": "active", "endpoints": [{
                "name": "responses", "protocol": "responses",
                "url": f"http://127.0.0.1:{free_port()}/v1/responses", "status": "active",
            }],
        }
        try:
            self.apply_config_patch(
                {"providers": [provider], "mappings": [secondary]},
                "connection-failure-enable.json",
            )
            time.sleep(0.4)
            self.provider_state.reset()
            self.provider_secondary_state.reset()
            status, headers, body = self.request(
                "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "connection failover"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, 200, body)
            self.assertEqual(len(self.provider_state.snapshot()[1]), 0)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 1)
            request_id = next(
                value for key, value in headers.items() if key.lower() == "x-request-id"
            )
            audit = self.database_query(
                "SELECT error_class, retryable, possible_duplicate_cost FROM request_attempts "
                f"WHERE request_id='{request_id}' AND attempt_number=1"
            ).strip()
            self.assertEqual(audit, "upstream_connection_failure\t1\t0")
        finally:
            provider["endpoints"][0]["url"] = (
                f"http://127.0.0.1:{self.provider_port}/v1/responses"
            )
            secondary["status"] = "disabled"
            self.apply_config_patch(
                {"providers": [provider], "mappings": [secondary]},
                "connection-failure-disable.json",
            )
            time.sleep(0.4)

    def test_phase4_first_byte_timeout_fails_over_with_duplicate_cost_risk(self):
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "timeout-backup", "provider": "provider-secondary", "endpoint": "responses",
            "credential": "default", "upstream_model": "provider-secondary-model",
            "priority": 200, "status": "active",
        }
        process = None
        try:
            self.apply_config_patch({"mappings": [secondary]}, "timeout-failover-enable.json")
            time.sleep(0.4)
            subprocess.run(
                ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "FLUSHDB"],
                check=True, stdout=subprocess.DEVNULL,
            )
            port = free_port()
            environment = self.gateway_environment(port)
            environment["AI_GATEWAY_UPSTREAM_TIMEOUT_MS"] = "150"
            process = subprocess.Popen(
                [self.gateway_binary], env=environment,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            self.wait_for_gateway_port(port)
            self.provider_state.reset("slow")
            self.provider_secondary_state.reset()
            status, headers, body = self.request_at(
                port, "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "timeout failover"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"}, timeout=3,
            )
            self.assertEqual(status, 200, body)
            self.assertEqual(len(self.provider_state.snapshot()[1]), 1)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 1)
            request_id = next(
                value for key, value in headers.items() if key.lower() == "x-request-id"
            )
            audit = self.database_query(
                "SELECT error_class, retryable, possible_duplicate_cost FROM request_attempts "
                f"WHERE request_id='{request_id}' AND attempt_number=1"
            ).strip()
            self.assertEqual(audit, "upstream_timeout\t1\t1")
        finally:
            if process is not None:
                process.terminate()
                process.wait(timeout=5)
            secondary["status"] = "disabled"
            self.apply_config_patch({"mappings": [secondary]}, "timeout-failover-disable.json")
            time.sleep(0.4)

    def test_phase4_cache_affinity_is_shared_between_gateway_nodes(self):
        model = {
            "tenant": "tenant-a", "protocol": "responses", "name": "gateway-model",
            "status": "active", "routing": {"mode": "cache_affinity", "max_attempts": 3},
        }
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "affinity", "provider": "provider-secondary", "endpoint": "responses",
            "credential": "default", "upstream_model": "provider-secondary-model",
            "priority": 100, "status": "active",
        }
        session_id = "codex-session-must-never-enter-redis"
        try:
            self.apply_config_patch(
                {"logical_models": [model], "mappings": [secondary]},
                "affinity-enable.json",
            )
            time.sleep(1.0)
            headers = {
                "Authorization": f"Bearer {GATEWAY_KEY}",
                "originator": "codex",
                "session-id": session_id,
            }
            self.provider_state.reset()
            self.provider_secondary_state.reset()
            status, _, body = self.request_at(
                self.gateway_port, "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "affinity first"}), headers,
            )
            self.assertEqual(status, 200, body)
            first = "primary" if self.provider_state.snapshot()[1] else "secondary"
            self.assertEqual(
                len(self.provider_state.snapshot()[1]) +
                len(self.provider_secondary_state.snapshot()[1]),
                1,
            )

            self.provider_state.reset()
            self.provider_secondary_state.reset()
            status, _, body = self.request_at(
                self.gateway_secondary_port, "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "affinity second"}), headers,
            )
            self.assertEqual(status, 200, body)
            second = "primary" if self.provider_state.snapshot()[1] else "secondary"
            self.assertEqual(second, first)

            keys = subprocess.run(
                ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "--scan"],
                check=True, text=True, stdout=subprocess.PIPE,
            ).stdout
            redis_contents = keys
            for key in keys.splitlines():
                value_type = subprocess.run(
                    ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port),
                     "--raw", "TYPE", key],
                    check=True, text=True, stdout=subprocess.PIPE,
                ).stdout.strip()
                command = "HGETALL" if value_type == "hash" else "GET"
                redis_contents += subprocess.run(
                    ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port),
                     "--raw", command, key],
                    check=True, text=True, stdout=subprocess.PIPE,
                ).stdout
            for sensitive in (
                session_id, GATEWAY_KEY, PROVIDER_KEY,
                "affinity first", "affinity second",
            ):
                self.assertNotIn(sensitive, redis_contents)
        finally:
            model["routing"] = {"mode": "fixed_order", "max_attempts": 3}
            secondary["status"] = "disabled"
            self.apply_config_patch(
                {"logical_models": [model], "mappings": [secondary]},
                "affinity-disable.json",
            )
            time.sleep(1.0)

    def test_phase4_redis_outage_fails_proxy_closed_and_recovers(self):
        self.redis.terminate()
        self.redis.wait(timeout=5)
        try:
            deadline = time.time() + 3
            statuses = (200, 200)
            while time.time() < deadline:
                statuses = (
                    self.request_at(self.gateway_port, "GET", "/readyz")[0],
                    self.request_at(self.gateway_secondary_port, "GET", "/readyz")[0],
                )
                if statuses == (503, 503):
                    break
                time.sleep(0.05)
            self.assertEqual(statuses, (503, 503))
            self.assertEqual(self.request("GET", "/healthz")[0], 200)
            self.assertEqual(
                self.request(
                    "GET", "/v1/models", headers={"Authorization": f"Bearer {GATEWAY_KEY}"}
                )[0],
                200,
            )
            status, _, body = self.request(
                "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "redis unavailable"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, 503)
            self.assertEqual(json.loads(body)["error"]["code"], "governance_unavailable")
        finally:
            self.redis = self.start_redis()
            self.wait_for_gateway_port(self.gateway_port)
            self.wait_for_gateway_port(self.gateway_secondary_port)

    def test_phase4_load_balance_distributes_equal_priority_candidates(self):
        model = {
            "tenant": "tenant-a", "protocol": "responses", "name": "gateway-model",
            "status": "active", "routing": {"mode": "load_balance", "max_attempts": 2},
        }
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "balanced", "provider": "provider-secondary", "endpoint": "responses",
            "credential": "default", "upstream_model": "provider-secondary-model",
            "priority": 100, "status": "active",
        }
        try:
            self.apply_config_patch(
                {"logical_models": [model], "mappings": [secondary]},
                "load-balance-enable.json",
            )
            time.sleep(1.0)
            self.provider_state.reset()
            self.provider_secondary_state.reset()
            for index in range(20):
                port = self.gateway_port if index % 2 == 0 else self.gateway_secondary_port
                status, _, body = self.request_at(
                    port, "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": f"balanced-{index}"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                self.assertEqual(status, 200, body)
            primary_count = len(self.provider_state.snapshot()[1])
            secondary_count = len(self.provider_secondary_state.snapshot()[1])
            self.assertGreater(primary_count, 0)
            self.assertGreater(secondary_count, 0)
            self.assertEqual(primary_count + secondary_count, 20)
        finally:
            model["routing"] = {"mode": "fixed_order", "max_attempts": 3}
            secondary["status"] = "disabled"
            self.apply_config_patch(
                {"logical_models": [model], "mappings": [secondary]},
                "load-balance-disable.json",
            )
            time.sleep(1.0)

    def test_phase4_candidate_identity_includes_the_model_mapping(self):
        model = {
            "tenant": "tenant-a", "protocol": "responses", "name": "gateway-model",
            "status": "active", "routing": {"mode": "load_balance", "max_attempts": 2},
        }
        duplicate_target = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "duplicate-target", "provider": "provider-a", "endpoint": "responses",
            "credential": "default", "upstream_model": "provider-model",
            "priority": 100, "status": "active",
        }
        try:
            self.apply_config_patch(
                {"logical_models": [model], "mappings": [duplicate_target]},
                "candidate-identity-enable.json",
            )
            time.sleep(1.0)
            subprocess.run(
                ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "FLUSHDB"],
                check=True, stdout=subprocess.DEVNULL,
            )
            request_ids = []
            for index in range(24):
                status, headers, body = self.request(
                    "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": f"candidate-{index}"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                self.assertEqual(status, 200, body)
                request_ids.append(
                    next(value for key, value in headers.items() if key.lower() == "x-request-id")
                )
            quoted_ids = ",".join(f"'{request_id}'" for request_id in request_ids)
            selected_mappings = self.database_query(
                "SELECT COUNT(DISTINCT mapping_id) FROM request_attempts "
                f"WHERE request_id IN ({quoted_ids})"
            ).strip()
            self.assertEqual(selected_mappings, "2")
        finally:
            model["routing"] = {"mode": "fixed_order", "max_attempts": 3}
            duplicate_target["status"] = "disabled"
            self.apply_config_patch(
                {"logical_models": [model], "mappings": [duplicate_target]},
                "candidate-identity-disable.json",
            )
            time.sleep(1.0)

    def test_phase4_circuit_state_and_half_open_probe_are_shared(self):
        model = {
            "tenant": "tenant-a", "protocol": "responses", "name": "gateway-model",
            "status": "active", "routing": {"mode": "fixed_order", "max_attempts": 2},
        }
        secondary = {
            "tenant": "tenant-a", "protocol": "responses", "logical_model": "gateway-model",
            "name": "circuit-backup", "provider": "provider-secondary",
            "endpoint": "responses", "credential": "default",
            "upstream_model": "provider-secondary-model", "priority": 200,
            "status": "active",
        }
        circuit_gateway = None
        try:
            self.apply_config_patch(
                {"logical_models": [model], "mappings": [secondary]},
                "circuit-enable.json",
            )
            time.sleep(1.0)
            subprocess.run(
                ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "FLUSHDB"],
                check=True, stdout=subprocess.DEVNULL,
            )
            circuit_port = free_port()
            environment = self.gateway_environment(circuit_port)
            environment.update({
                "AI_GATEWAY_CIRCUIT_FAILURE_THRESHOLD": "3",
                "AI_GATEWAY_CIRCUIT_OPEN_MS": "300",
                "AI_GATEWAY_CIRCUIT_PROBE_LEASE_MS": "1000",
            })
            circuit_gateway = subprocess.Popen(
                [self.gateway_binary], env=environment,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            self.wait_for_gateway_port(circuit_port)

            for index in range(3):
                self.provider_state.reset("status500")
                self.provider_secondary_state.reset()
                status, _, body = self.request_at(
                    circuit_port, "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": f"open-{index}"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"},
                )
                self.assertEqual(status, 200, body)
                self.assertEqual(len(self.provider_state.snapshot()[1]), 1)
                self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 1)

            self.provider_state.reset()
            self.provider_secondary_state.reset()
            status, _, body = self.request_at(
                self.gateway_secondary_port, "POST", "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "shared-open"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, 200, body)
            self.assertEqual(len(self.provider_state.snapshot()[1]), 0)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 1)

            time.sleep(0.4)
            self.provider_state.reset("slow_success")
            self.provider_secondary_state.reset()
            results = []
            barrier = threading.Barrier(3)

            def request_after_cooldown(port):
                barrier.wait()
                results.append(self.request_at(
                    port, "POST", "/v1/responses",
                    json.dumps({"model": "gateway-model", "input": "half-open"}),
                    {"Authorization": f"Bearer {GATEWAY_KEY}"}, timeout=3,
                ))

            threads = [
                threading.Thread(target=request_after_cooldown, args=(self.gateway_port,)),
                threading.Thread(
                    target=request_after_cooldown, args=(self.gateway_secondary_port,)
                ),
            ]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join(timeout=4)
            self.assertEqual([result[0] for result in results], [200, 200])
            self.assertEqual(len(self.provider_state.snapshot()[1]), 1)
            self.assertEqual(len(self.provider_secondary_state.snapshot()[1]), 1)
        finally:
            if circuit_gateway is not None:
                circuit_gateway.terminate()
                circuit_gateway.wait(timeout=5)
            model["routing"] = {"mode": "fixed_order", "max_attempts": 3}
            secondary["status"] = "disabled"
            self.apply_config_patch(
                {"logical_models": [model], "mappings": [secondary]},
                "circuit-disable.json",
            )
            time.sleep(1.0)

    def test_upstream_statuses_are_sanitized(self):
        for scenario, expected_status, expected_code in [
            ("status400", 400, "upstream_rejected_request"),
            ("status429", 429, "upstream_rate_limited"),
            ("status500", 502, "upstream_unavailable"),
        ]:
            self.provider_state.reset(scenario)
            status, headers, body = self.request(
                "POST",
                "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "hello"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
            )
            self.assertEqual(status, expected_status)
            self.assertEqual(json.loads(body)["error"]["code"], expected_code)
            self.assertNotIn("provider detail", body.decode())
            if expected_status == 429:
                normalized_headers = {key.lower(): value for key, value in headers.items()}
                self.assertEqual(normalized_headers.get("retry-after"), "1")

    def test_timeout_does_not_block_health(self):
        self.provider_state.reset("slow")
        result = {}

        def slow_request():
            result["value"] = self.request(
                "POST",
                "/v1/responses",
                json.dumps({"model": "gateway-model", "input": "hello"}),
                {"Authorization": f"Bearer {GATEWAY_KEY}"},
                timeout=3,
            )

        thread = threading.Thread(target=slow_request)
        thread.start()
        time.sleep(0.08)
        started = time.time()
        status, _, _ = self.request("GET", "/healthz")
        elapsed = time.time() - started
        thread.join(timeout=3)
        self.assertEqual(status, 200)
        self.assertLess(elapsed, 0.5)
        self.assertEqual(result["value"][0], 502)

    def test_logs_are_metadata_only(self):
        self.provider_state.reset()
        self.request(
            "POST",
            "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "prompt-never-log"}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
        )
        time.sleep(0.05)
        logs = "".join(self.logs)
        self.assertNotIn(GATEWAY_KEY, logs)
        self.assertNotIn(TENANT_B_KEY, logs)
        self.assertNotIn(PROVIDER_KEY, logs)
        self.assertNotIn(PROVIDER_KEY_B, logs)
        self.assertNotIn("prompt-never-log", logs)
        self.assertIn('"request_id"', logs)

        self.provider_state.reset("stream_sensitive_success")
        status, headers, stream_body = self.request(
            "POST",
            "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "stream-prompt-never-log", "stream": True}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
        )
        self.assertEqual(status, 200)
        self.assertIn(b"event-body-never-log", stream_body)
        request_id = next(value for key, value in headers.items() if key.lower() == "x-request-id")
        deadline = time.time() + 1
        completion = None
        while time.time() < deadline and completion is None:
            for line in self.logs:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("request_id") == request_id and record.get("event") == "request_completed":
                    completion = record
            time.sleep(0.01)
        self.assertIsNotNone(completion)
        self.assertEqual(completion["stream"], "true")
        self.assertEqual(completion["provider_result"], "success")
        self.assertEqual(completion["tenant_slug"], "tenant-a")
        self.assertEqual(completion["api_key_id"], self.public_key_id(GATEWAY_KEY))
        self.assertEqual(int(completion["response_bytes"]), len(stream_body))
        self.assertIn("backpressure_pauses", completion)
        logs = "".join(self.logs)
        self.assertNotIn("stream-prompt-never-log", logs)
        self.assertNotIn("event-body-never-log", logs)
        digest = self.database_query(
            f"SELECT HEX(key_hmac) FROM api_keys WHERE display_prefix='{GATEWAY_KEY[5:17]}'"
        ).strip()
        self.assertNotIn(digest, logs)
        self.assertNotIn(digest.lower(), logs)

    def test_failed_stream_log_counts_the_terminal_error_bytes(self):
        self.provider_state.reset("stream_close_after_commit")
        status, headers, body = self.request(
            "POST",
            "/v1/responses",
            json.dumps({"model": "gateway-model", "input": "hello", "stream": True}),
            {"Authorization": f"Bearer {GATEWAY_KEY}"},
        )
        self.assertEqual(status, 200)
        request_id = next(value for key, value in headers.items() if key.lower() == "x-request-id")
        deadline = time.time() + 1
        completion = None
        while time.time() < deadline and completion is None:
            for line in self.logs:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("request_id") == request_id and record.get("event") == "request_completed":
                    completion = record
            time.sleep(0.01)
        self.assertIsNotNone(completion)
        self.assertEqual(completion["provider_result"], "upstream_stream_incomplete")
        self.assertEqual(int(completion["response_bytes"]), len(body))


if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise SystemExit("usage: test_gateway.py /path/to/AiGateway /path/to/AiGatewayAdmin /repo [filters]")
    unittest.main(argv=[sys.argv[0], *sys.argv[4:]])
