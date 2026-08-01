#!/usr/bin/env python3
import http.client
import json
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


GATEWAY_KEY = "gateway-test-key-0123456789-abcdefghijklmnopqrstuvwxyz"
PROVIDER_KEY = "provider-test-key-0123456789-abcdefghijklmnopqrstuvwxyz"


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


class MockProviderHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

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
        cls.gateway_binary = sys.argv[1]
        cls.provider_state = MockProviderState()
        cls.provider_port = free_port()
        cls.provider = ThreadingHTTPServer(("127.0.0.1", cls.provider_port), MockProviderHandler)
        cls.provider.state = cls.provider_state
        cls.provider_thread = threading.Thread(target=cls.provider.serve_forever, daemon=True)
        cls.provider_thread.start()

        cls.gateway_port = free_port()
        environment = os.environ.copy()
        environment.update(
            {
                "AI_GATEWAY_LISTEN_ADDRESS": "127.0.0.1",
                "AI_GATEWAY_LISTEN_PORT": str(cls.gateway_port),
                "AI_GATEWAY_API_KEY": GATEWAY_KEY,
                "AI_GATEWAY_PROVIDER_RESPONSES_URL": f"http://127.0.0.1:{cls.provider_port}/v1/responses",
                "AI_GATEWAY_PROVIDER_API_KEY": PROVIDER_KEY,
                "AI_GATEWAY_LOGICAL_MODEL": "gateway-model",
                "AI_GATEWAY_UPSTREAM_MODEL": "provider-model",
                "AI_GATEWAY_MAX_BODY_BYTES": "1024",
                "AI_GATEWAY_MAX_RESPONSE_BYTES": "2048",
                "AI_GATEWAY_UPSTREAM_TIMEOUT_MS": "500",
                "AI_GATEWAY_IO_THREADS": "2",
                "AI_GATEWAY_STREAM_PREFETCH_BYTES": "1024",
                "AI_GATEWAY_STREAM_BUFFER_HIGH_WATER_BYTES": "8192",
                "AI_GATEWAY_STREAM_BUFFER_LOW_WATER_BYTES": "2048",
                "AI_GATEWAY_STREAM_IDLE_TIMEOUT_MS": "500",
                "AI_GATEWAY_STREAM_MAX_DURATION_MS": "1000",
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

    @classmethod
    def tearDownClass(cls):
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

    @classmethod
    def wait_for_gateway(cls):
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                status, _, _ = cls.request("GET", "/healthz")
                if status == 200:
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("AiGateway did not start")

    @classmethod
    def request(cls, method, path, body=None, headers=None, timeout=3):
        connection = http.client.HTTPConnection("127.0.0.1", cls.gateway_port, timeout=timeout)
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

    def test_readiness_fails_closed_when_configuration_is_missing(self):
        port = free_port()
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("AI_GATEWAY_")
        }
        environment.update(
            {
                "AI_GATEWAY_LISTEN_ADDRESS": "127.0.0.1",
                "AI_GATEWAY_LISTEN_PORT": str(port),
            }
        )
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
        base = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("AI_GATEWAY_")
        }
        base.update(
            {
                "AI_GATEWAY_API_KEY": GATEWAY_KEY,
                "AI_GATEWAY_PROVIDER_RESPONSES_URL": f"http://127.0.0.1:{self.provider_port}/v1/responses",
                "AI_GATEWAY_PROVIDER_API_KEY": PROVIDER_KEY,
                "AI_GATEWAY_LOGICAL_MODEL": "gateway-model",
                "AI_GATEWAY_UPSTREAM_MODEL": "provider-model",
            }
        )
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

    def test_slow_client_backpressures_and_resumes_without_event_loss(self):
        self.provider_state.reset("stream_backpressure")
        gateway_port = free_port()
        environment = os.environ.copy()
        environment.update(
            {
                "AI_GATEWAY_LISTEN_ADDRESS": "127.0.0.1",
                "AI_GATEWAY_LISTEN_PORT": str(gateway_port),
                "AI_GATEWAY_API_KEY": GATEWAY_KEY,
                "AI_GATEWAY_PROVIDER_RESPONSES_URL": f"http://127.0.0.1:{self.provider_port}/v1/responses",
                "AI_GATEWAY_PROVIDER_API_KEY": PROVIDER_KEY,
                "AI_GATEWAY_LOGICAL_MODEL": "gateway-model",
                "AI_GATEWAY_UPSTREAM_MODEL": "provider-model",
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
                    probe.request("GET", "/healthz")
                    probe.getresponse().read()
                    probe.close()
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
        self.assertNotIn(PROVIDER_KEY, logs)
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
        self.assertEqual(int(completion["response_bytes"]), len(stream_body))
        self.assertIn("backpressure_pauses", completion)
        logs = "".join(self.logs)
        self.assertNotIn("stream-prompt-never-log", logs)
        self.assertNotIn("event-body-never-log", logs)

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
    if len(sys.argv) < 2:
        raise SystemExit("usage: test_gateway.py /path/to/AiGateway [unittest filters]")
    unittest.main(argv=[sys.argv[0], *sys.argv[2:]])
