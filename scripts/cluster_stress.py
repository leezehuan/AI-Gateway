#!/usr/bin/env python3
"""Run a repeatable local one-node/three-node AiGateway stress benchmark."""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import dataclasses
import datetime as dt
import gzip
import hashlib
import http.client
import json
import math
import multiprocessing
import os
import pathlib
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Iterable, Optional


COMPARISON_PROFILE = {
    "AI_GATEWAY_DB_POOL_SIZE": "2",
    "AI_GATEWAY_DB_WORKERS": "2",
    "AI_GATEWAY_DB_QUEUE_SIZE": "128",
    "AI_GATEWAY_REDIS_WORKERS": "2",
    "AI_GATEWAY_REDIS_QUEUE_SIZE": "256",
    "AI_GATEWAY_IO_THREADS": "2",
}

PRODUCTION_PROFILE = {
    "AI_GATEWAY_DB_POOL_SIZE": "4",
    "AI_GATEWAY_DB_WORKERS": "4",
    "AI_GATEWAY_DB_QUEUE_SIZE": "1024",
    "AI_GATEWAY_REDIS_WORKERS": "2",
    "AI_GATEWAY_REDIS_QUEUE_SIZE": "4096",
    "AI_GATEWAY_IO_THREADS": "2",
    "AI_GATEWAY_MAX_ACTIVE_REQUESTS": "256",
    "AI_GATEWAY_MAX_ACTIVE_STREAMS": "128",
    "AI_GATEWAY_CURL_MAX_TOTAL_CONNECTIONS": "128",
    "AI_GATEWAY_CURL_MAX_HOST_CONNECTIONS": "64",
}

ERROR_CODES = {
    "gateway_overloaded",
    "authorization_unavailable",
    "governance_unavailable",
    "audit_unavailable",
    "routing_unavailable",
    "gateway_draining",
}


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def percentile(values: Iterable[float], fraction: float) -> Optional[float]:
    ordered = sorted(values)
    if not ordered:
        return None
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1))
    return ordered[index]


def milliseconds(value: Optional[float]) -> Optional[float]:
    return None if value is None else round(value * 1000.0, 2)


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-") or "phase"


def parse_prometheus(text: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([^ ]+)\s+([-+0-9.eE]+)", line)
        if not match:
            continue
        try:
            values[match.group(1)] = float(match.group(2))
        except ValueError:
            continue
    return values


def metric_delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    return {
        key: round(value - before.get(key, 0.0) if value >= before.get(key, 0.0) else value, 6)
        for key, value in after.items()
        if (value - before.get(key, 0.0) if value >= before.get(key, 0.0) else value) != 0
    }


def extract_error_code(payload: bytes) -> str:
    try:
        parsed = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return "invalid_error_body"
    if not isinstance(parsed, dict):
        return "invalid_error_body"
    error = parsed.get("error")
    if isinstance(error, dict):
        code = error.get("code") or error.get("type")
        if isinstance(code, str) and code:
            return code
    code = parsed.get("code")
    if isinstance(code, str) and code:
        return code
    if parsed.get("type") == "error":
        return "upstream_stream_error"
    return "http_error"


def executable(path: pathlib.Path, name: str) -> pathlib.Path:
    resolved = path.resolve()
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise RuntimeError(f"{name} is not executable: {resolved}")
    return resolved


def run_checked(arguments: list[str], *, env: Optional[dict[str, str]] = None,
                timeout: Optional[float] = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        arguments,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"command failed ({' '.join(arguments[:2])}): {detail}")
    return result


@dataclasses.dataclass
class RequestResult:
    phase: str
    request_id: str
    workload: str
    stream: bool
    started_at: float
    finished_at: float
    status: int | str
    success: bool
    error_code: str
    latency_seconds: float
    ttft_seconds: Optional[float]
    response_bytes: int
    upstream: str
    saw_done: bool

    def as_json(self) -> dict[str, object]:
        return dataclasses.asdict(self)


class MockProviderHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_json(200, {"ok": True})
        else:
            self.send_json(404, {"error": {"code": "not_found"}})

    def do_POST(self) -> None:
        size = int(self.headers.get("content-length", "0"))
        raw = self.rfile.read(size)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {}
        stream = bool(payload.get("stream")) if isinstance(payload, dict) else False
        record = {
            "timestamp": time.time(),
            "path": self.path,
            "stream": stream,
            "client_request_id": self.headers.get("x-client-request-id", ""),
        }
        with self.server.log_lock:
            with open(self.server.request_log, "a", encoding="utf-8") as output:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")

        if self.path == "/v1/chat/completions" and stream:
            self.send_chat_stream()
            return
        if self.path == "/v1/responses" and stream:
            self.send_responses_stream()
            return
        if self.path == "/v1/responses":
            self.send_json(
                200,
                {
                    "id": "resp_stress",
                    "object": "response",
                    "model": "provider-responses-model",
                    "output": [],
                    "usage": {"input_tokens": 8, "output_tokens": 2, "total_tokens": 10},
                },
            )
            return
        self.send_json(404, {"error": {"code": "not_found"}})

    def stream_delay(self) -> float:
        with self.server.stream_delay.get_lock():
            return float(self.server.stream_delay.value)

    def send_chat_stream(self) -> None:
        frames = [
            b'data: {"id":"chat_stress","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\n',
            b'data: {"id":"chat_stress","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":null}]}\n\n',
            b'data: {"id":"chat_stress","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":8,"completion_tokens":2,"total_tokens":10}}\n\n',
            b"data: [DONE]\n\n",
        ]
        self.send_sse(frames)

    def send_responses_stream(self) -> None:
        frames = [
            b'event: response.created\ndata: {"type":"response.created","sequence_number":0}\n\n',
            b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","sequence_number":1,"delta":"ok"}\n\n',
            b'event: response.completed\ndata: {"type":"response.completed","sequence_number":2,"response":{"usage":{"input_tokens":8,"output_tokens":2,"total_tokens":10}}}\n\n',
            b"data: [DONE]\n\n",
        ]
        self.send_sse(frames)

    def send_sse(self, frames: list[bytes]) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        delay = self.stream_delay()
        try:
            for frame in frames:
                self.wfile.write(frame)
                self.wfile.flush()
                if delay > 0:
                    time.sleep(delay)
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True

    def send_json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def log_message(self, *_args: object) -> None:
        pass


def serve_mock_provider(port: int, request_log: str,
                        stream_delay: multiprocessing.Value) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", port), MockProviderHandler)
    server.request_log = request_log
    server.stream_delay = stream_delay
    server.log_lock = threading.Lock()
    server.serve_forever(poll_interval=0.1)


@dataclasses.dataclass
class ManagedProcess:
    name: str
    process: subprocess.Popen[bytes]
    log_handle: object

    @property
    def pid(self) -> int:
        return self.process.pid

    def stop(self, sig: int = signal.SIGTERM, timeout: float = 8.0) -> None:
        if self.process.poll() is None:
            self.process.send_signal(sig)
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        with contextlib.suppress(Exception):
            self.log_handle.close()


@dataclasses.dataclass
class GatewayNode:
    index: int
    port: int
    profile_name: str
    managed: ManagedProcess


def read_process(pid: int) -> tuple[int, int, int, int]:
    try:
        status = pathlib.Path(f"/proc/{pid}/status").read_text(encoding="ascii")
        stat = pathlib.Path(f"/proc/{pid}/stat").read_text(encoding="ascii").split()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return 0, 0, 0, 0
    rss = 0
    threads = 0
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            rss = int(line.split()[1])
        elif line.startswith("Threads:"):
            threads = int(line.split()[1])
    sockets = 0
    with contextlib.suppress(FileNotFoundError, PermissionError, ProcessLookupError):
        for descriptor in pathlib.Path(f"/proc/{pid}/fd").iterdir():
            with contextlib.suppress(FileNotFoundError, PermissionError, OSError):
                if os.readlink(descriptor).startswith("socket:["):
                    sockets += 1
    return int(stat[13]) + int(stat[14]), rss, threads, sockets


def descendants(pid: int) -> set[int]:
    parents: dict[int, list[int]] = defaultdict(list)
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            text = (entry / "status").read_text(encoding="ascii")
            ppid_line = next(line for line in text.splitlines() if line.startswith("PPid:"))
            parents[int(ppid_line.split()[1])].append(int(entry.name))
        except (FileNotFoundError, PermissionError, StopIteration, ValueError):
            continue
    found = {pid}
    pending = [pid]
    while pending:
        current = pending.pop()
        for child in parents.get(current, []):
            if child not in found:
                found.add(child)
                pending.append(child)
    return found


def host_cpu_ticks() -> tuple[int, int]:
    fields = pathlib.Path("/proc/stat").read_text(encoding="ascii").splitlines()[0].split()[1:]
    numbers = [int(value) for value in fields]
    idle = numbers[3] + (numbers[4] if len(numbers) > 4 else 0)
    return sum(numbers), idle


class ResourceMonitor:
    def __init__(self, suppliers: dict[str, Callable[[], set[int]]]):
        self.suppliers = suppliers
        self.stop_event = threading.Event()
        self.start_ticks: dict[str, int] = {}
        self.end_ticks: dict[str, int] = {}
        self.peak_rss: Counter[str] = Counter()
        self.peak_threads: Counter[str] = Counter()
        self.peak_socket_fds: Counter[str] = Counter()
        self.host_start = (0, 0)
        self.host_end = (0, 0)
        self.min_available_kib: Optional[int] = None
        self.thread: Optional[threading.Thread] = None

    def _sample(self, first: bool = False) -> None:
        for name, supplier in self.suppliers.items():
            ticks = 0
            rss = 0
            threads = 0
            socket_fds = 0
            for pid in supplier():
                item_ticks, item_rss, item_threads, item_socket_fds = read_process(pid)
                ticks += item_ticks
                rss += item_rss
                threads += item_threads
                socket_fds += item_socket_fds
            if first:
                self.start_ticks[name] = ticks
            self.end_ticks[name] = ticks
            self.peak_rss[name] = max(self.peak_rss[name], rss)
            self.peak_threads[name] = max(self.peak_threads[name], threads)
            self.peak_socket_fds[name] = max(self.peak_socket_fds[name], socket_fds)
        try:
            memory = pathlib.Path("/proc/meminfo").read_text(encoding="ascii")
            available = int(next(
                line.split()[1] for line in memory.splitlines()
                if line.startswith("MemAvailable:")
            ))
            self.min_available_kib = available if self.min_available_kib is None else min(
                self.min_available_kib, available
            )
        except (FileNotFoundError, StopIteration, ValueError):
            pass

    def start(self) -> None:
        self.host_start = host_cpu_ticks()
        self._sample(first=True)

        def loop() -> None:
            while not self.stop_event.wait(0.1):
                self._sample()

        self.thread = threading.Thread(target=loop, daemon=True)
        self.thread.start()

    def stop(self, elapsed: float) -> dict[str, object]:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=1)
        self._sample()
        self.host_end = host_cpu_ticks()
        clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        processes = {}
        for name in self.suppliers:
            processes[name] = {
                "cpu_percent": round(
                    max(0, self.end_ticks.get(name, 0) - self.start_ticks.get(name, 0))
                    / clock_ticks / max(elapsed, 0.001) * 100.0,
                    1,
                ),
                "peak_rss_mib": round(self.peak_rss[name] / 1024.0, 1),
                "peak_threads": int(self.peak_threads[name]),
                "peak_socket_fds": int(self.peak_socket_fds[name]),
            }
        total_delta = self.host_end[0] - self.host_start[0]
        idle_delta = self.host_end[1] - self.host_start[1]
        host_percent = 0.0 if total_delta <= 0 else (total_delta - idle_delta) / total_delta * 100.0
        return {
            "processes": processes,
            "host_cpu_percent": round(host_percent, 1),
            "host_min_available_mib": None if self.min_available_kib is None else round(
                self.min_available_kib / 1024.0, 1
            ),
        }


class ClusterEnvironment:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.repo = pathlib.Path(args.repo).resolve()
        self.gateway_binary = executable(pathlib.Path(args.gateway), "AiGateway")
        self.admin_binary = executable(pathlib.Path(args.admin), "AiGatewayAdmin")
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        self.output = pathlib.Path(args.output_dir or f"/tmp/aigw-three-node-stress-{stamp}").resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.runtime_temp = tempfile.TemporaryDirectory(prefix="aigw-three-node-runtime-")
        self.runtime = pathlib.Path(self.runtime_temp.name)
        self.gateway_ports = [free_port(), free_port(), free_port()]
        self.nginx_port = free_port()
        self.database_port = free_port()
        self.redis_port = free_port()
        self.provider_port = free_port()
        self.redis_prefix = f"aigw-stress-{stamp}"
        self.pepper = "cluster-stress-pepper-at-least-32-bytes"
        self.provider_secret = "local-mock-provider-secret-never-persist"
        self.api_key = ""
        self.mariadb: Optional[ManagedProcess] = None
        self.redis: Optional[ManagedProcess] = None
        self.nginx: Optional[ManagedProcess] = None
        self.provider_process: Optional[multiprocessing.Process] = None
        self.provider_delay = multiprocessing.Value("d", 0.0)
        self.nodes: dict[int, GatewayNode] = {}
        self.active_nginx_nodes: set[int] = set()
        self.profile_name = "production"
        self._cleaned = False

    def require_tools(self) -> None:
        required = [
            "mariadb-install-db", "mariadbd", "mariadb", "redis-server", "redis-cli", "nginx"
        ]
        missing = [name for name in required if shutil.which(name) is None]
        if missing:
            raise RuntimeError("missing required commands: " + ", ".join(missing))

    def setup(self) -> None:
        self.require_tools()
        self.start_provider()
        self.start_mariadb()
        self.start_redis()
        self.initialize_gateway_data()
        self.write_environment()

    def start_provider(self) -> None:
        request_log = self.output / "provider-requests.jsonl"
        request_log.touch()
        self.provider_process = multiprocessing.Process(
            target=serve_mock_provider,
            args=(self.provider_port, str(request_log), self.provider_delay),
            daemon=True,
        )
        self.provider_process.start()
        self.wait_http(self.provider_port, "/health", expected=200, timeout=5)

    def set_stream_delay(self, seconds: float) -> None:
        with self.provider_delay.get_lock():
            self.provider_delay.value = seconds

    def start_mariadb(self) -> None:
        data = self.runtime / "mariadb-data"
        socket_path = self.runtime / "mariadb.sock"
        run_checked([
            "mariadb-install-db", "--no-defaults", f"--datadir={data}",
            "--auth-root-authentication-method=normal", "--skip-test-db",
        ])
        log_path = self.output / "mariadb.log"
        log_handle = open(log_path, "ab", buffering=0)
        process = subprocess.Popen([
            "mariadbd", "--no-defaults", f"--datadir={data}", f"--socket={socket_path}",
            f"--port={self.database_port}", "--bind-address=127.0.0.1", "--skip-name-resolve",
            f"--pid-file={self.runtime / 'mariadb.pid'}", f"--log-error={log_path}",
            "--max-connections=128", "--innodb-flush-log-at-trx-commit=1",
            "--innodb-print-all-deadlocks=ON",
        ], stdout=log_handle, stderr=log_handle)
        self.mariadb = ManagedProcess("mariadb", process, log_handle)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            probe = subprocess.run(
                ["mariadb", "--no-defaults", f"--socket={socket_path}", "-u", "root", "-e", "SELECT 1"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
            )
            if probe.returncode == 0:
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("temporary MariaDB did not start")
        run_checked([
            "mariadb", "--no-defaults", f"--socket={socket_path}", "-u", "root", "-e",
            "CREATE DATABASE ai_gateway; "
            "CREATE USER 'gateway'@'127.0.0.1' IDENTIFIED BY 'test-db-password'; "
            "GRANT ALL ON ai_gateway.* TO 'gateway'@'127.0.0.1'; FLUSH PRIVILEGES;",
        ])

    def start_redis(self) -> None:
        log_handle = open(self.output / "redis.log", "ab", buffering=0)
        process = subprocess.Popen([
            "redis-server", "--bind", "127.0.0.1", "--port", str(self.redis_port),
            "--save", "", "--appendonly", "no", "--dir", str(self.runtime),
        ], stdout=log_handle, stderr=log_handle)
        self.redis = ManagedProcess("redis", process, log_handle)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            probe = subprocess.run(
                ["redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "PING"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False,
            )
            if probe.returncode == 0 and probe.stdout.strip() == "PONG":
                return
            time.sleep(0.05)
        raise RuntimeError("temporary Redis did not start")

    def admin_environment(self) -> dict[str, str]:
        environment = {key: value for key, value in os.environ.items() if not key.startswith("AI_GATEWAY_")}
        environment.update({
            "AI_GATEWAY_DB_HOST": "127.0.0.1",
            "AI_GATEWAY_DB_PORT": str(self.database_port),
            "AI_GATEWAY_DB_USER": "gateway",
            "AI_GATEWAY_DB_PASSWORD": "test-db-password",
            "AI_GATEWAY_DB_NAME": "ai_gateway",
            "AI_GATEWAY_API_KEY_HMAC_PEPPER": self.pepper,
        })
        return environment

    def initialize_gateway_data(self) -> None:
        environment = self.admin_environment()
        run_checked([
            str(self.admin_binary), "migrate", "--dir", str(self.repo / "migrations/gateway")
        ], env=environment)
        config_path = self.runtime / "stress-config.json"
        config_path.write_text(json.dumps(self.gateway_config(), indent=2), encoding="utf-8")
        run_checked([
            str(self.admin_binary), "apply-config", "--file", str(config_path)
        ], env=environment)
        issued = run_checked([
            str(self.admin_binary), "issue-key", "--tenant", "stress", "--policy", "default",
            "--name", "cluster-stress", "--quota", "stress-governance",
        ], env=environment)
        self.api_key = issued.stdout.strip()
        if not self.api_key.startswith("aigw_"):
            raise RuntimeError("AiGatewayAdmin did not issue a Gateway key")

    def gateway_config(self) -> dict[str, object]:
        origin = f"http://127.0.0.1:{self.provider_port}"
        return {
            "tenants": [{"slug": "stress", "name": "Cluster Stress", "status": "active"}],
            "quota_policies": [{
                "tenant": "stress", "slug": "stress-governance", "name": "Stress Governance",
                "concurrency": 100000, "daily_budget_usd": "1000000.00",
                "reservation_per_attempt_usd": "0.000001", "status": "active",
            }],
            "providers": [{
                "tenant": "stress", "slug": "local-mock", "name": "Local Mock",
                "status": "active",
                "endpoints": [
                    {"name": "responses", "protocol": "responses", "url": origin + "/v1/responses", "status": "active"},
                    {"name": "chat", "protocol": "chat_completions", "url": origin + "/v1/chat/completions", "status": "active"},
                ],
                "credentials": [{"name": "default", "secret_ref": "env:MOCK_PROVIDER_API_KEY", "status": "active"}],
            }],
            "logical_models": [
                {"tenant": "stress", "protocol": "responses", "name": "gateway-model", "status": "active"},
                {"tenant": "stress", "protocol": "chat_completions", "name": "gateway-model", "status": "active"},
            ],
            "policies": [{
                "tenant": "stress", "slug": "default", "name": "Default", "status": "active",
                "protocols": ["responses", "chat_completions"],
                "models": ["gateway-model", {"protocol": "chat_completions", "name": "gateway-model"}],
                "providers": ["local-mock"],
            }],
            "mappings": [
                {"tenant": "stress", "protocol": "responses", "logical_model": "gateway-model", "name": "responses-primary", "provider": "local-mock", "endpoint": "responses", "credential": "default", "upstream_model": "provider-responses-model", "status": "active"},
                {"tenant": "stress", "protocol": "chat_completions", "logical_model": "gateway-model", "name": "chat-primary", "provider": "local-mock", "endpoint": "chat", "credential": "default", "upstream_model": "provider-chat-model", "status": "active"},
            ],
        }

    def gateway_environment(self, port: int, profile_name: str) -> dict[str, str]:
        environment = self.admin_environment()
        environment.update({
            "AI_GATEWAY_LISTEN_ADDRESS": "127.0.0.1",
            "AI_GATEWAY_LISTEN_PORT": str(port),
            "AI_GATEWAY_DB_CONNECT_TIMEOUT_SECONDS": "1",
            "AI_GATEWAY_AUTH_CACHE_TTL_SECONDS": "30",
            "AI_GATEWAY_AUTH_CACHE_MAX_ENTRIES": "10000",
            "AI_GATEWAY_CONFIG_POLL_INTERVAL_MS": "200",
            "AI_GATEWAY_REDIS_HOST": "127.0.0.1",
            "AI_GATEWAY_REDIS_PORT": str(self.redis_port),
            "AI_GATEWAY_REDIS_CONNECT_TIMEOUT_MS": "200",
            "AI_GATEWAY_REDIS_COMMAND_TIMEOUT_MS": "200",
            "AI_GATEWAY_REDIS_KEY_PREFIX": self.redis_prefix,
            "AI_GATEWAY_GOVERNANCE_LEASE_TTL_MS": "5000",
            "AI_GATEWAY_GOVERNANCE_LEASE_RENEW_MS": "1000",
            "AI_GATEWAY_DRAIN_TIMEOUT_MS": "15000",
            "AI_GATEWAY_SHUTDOWN_CANCEL_GRACE_MS": "1000",
            "MOCK_PROVIDER_API_KEY": self.provider_secret,
        })
        environment.update(COMPARISON_PROFILE if profile_name == "comparison" else PRODUCTION_PROFILE)
        return environment

    def start_gateways(self, count: int, profile_name: str) -> None:
        self.stop_gateways()
        self.profile_name = profile_name
        for index in range(1, count + 1):
            self.start_node(index, profile_name)
        self.configure_nginx(set(range(1, count + 1)), start=self.nginx is None)

    def start_node(self, index: int, profile_name: Optional[str] = None) -> GatewayNode:
        profile = profile_name or self.profile_name
        existing = self.nodes.get(index)
        if existing and existing.managed.process.poll() is None:
            return existing
        if existing:
            existing.managed.stop()
        log_handle = open(self.output / f"node-{index}.log", "ab", buffering=0)
        process = subprocess.Popen(
            [str(self.gateway_binary)],
            env=self.gateway_environment(self.gateway_ports[index - 1], profile),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        node = GatewayNode(
            index, self.gateway_ports[index - 1], profile,
            ManagedProcess(f"gateway-{index}", process, log_handle),
        )
        self.nodes[index] = node
        self.wait_http(node.port, "/readyz", expected=200, timeout=10)
        return node

    def stop_node(self, index: int, sig: int = signal.SIGTERM, timeout: float = 20.0) -> None:
        node = self.nodes.get(index)
        if node:
            node.managed.stop(sig=sig, timeout=timeout)

    def stop_gateways(self) -> None:
        for index in sorted(self.nodes, reverse=True):
            self.nodes[index].managed.stop()
        self.nodes.clear()

    def configure_nginx(self, active_nodes: set[int], *, start: bool = False) -> None:
        self.active_nginx_nodes = set(active_nodes)
        prefix = self.runtime / "nginx"
        prefix.mkdir(exist_ok=True)
        config = prefix / "nginx.conf"
        config.write_text(self.nginx_config(active_nodes), encoding="utf-8")
        run_checked(["nginx", "-t", "-p", str(prefix), "-c", str(config)])
        if start:
            log_handle = open(self.output / "nginx-process.log", "ab", buffering=0)
            process = subprocess.Popen(
                ["nginx", "-p", str(prefix), "-c", str(config), "-g", "daemon off;"],
                stdout=log_handle, stderr=log_handle,
            )
            self.nginx = ManagedProcess("nginx", process, log_handle)
            self.wait_http(self.nginx_port, "/healthz", expected=200, timeout=5)
        else:
            run_checked(["nginx", "-s", "reload", "-p", str(prefix), "-c", str(config)])
            time.sleep(0.2)

    def nginx_config(self, active_nodes: set[int]) -> str:
        servers = []
        for index, port in enumerate(self.gateway_ports, start=1):
            suffix = "" if index in active_nodes else " down"
            servers.append(f"        server 127.0.0.1:{port} max_fails=1 fail_timeout=2s{suffix};")
        server_text = "\n".join(servers)
        access_log = self.output / "nginx-access.log"
        error_log = self.output / "nginx-error.log"
        return f"""worker_processes auto;
pid {self.runtime / 'nginx.pid'};
error_log {error_log} notice;
events {{ worker_connections 4096; }}
http {{
    log_format stress '$msec\t$upstream_addr\t$status\t$upstream_status\t$request_time\t$upstream_response_time';
    access_log {access_log} stress;
    upstream ai_gateway {{
        zone ai_gateway 64k;
        least_conn;
{server_text}
        keepalive 192;
    }}
    server {{
        listen 127.0.0.1:{self.nginx_port};
        server_name _;
        location = /metrics {{ return 404; }}
        location / {{
            proxy_pass http://ai_gateway;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host $host;
            proxy_buffering off;
            proxy_cache off;
            proxy_request_buffering off;
            proxy_read_timeout 30s;
            proxy_send_timeout 30s;
            proxy_next_upstream off;
            add_header X-Stress-Upstream $upstream_addr always;
        }}
    }}
}}
"""

    def wait_http(self, port: int, path: str, *, expected: int, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        last_status: Optional[int] = None
        while time.monotonic() < deadline:
            try:
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
                connection.request("GET", path)
                response = connection.getresponse()
                last_status = response.status
                response.read()
                connection.close()
                if last_status == expected:
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError(f"HTTP service on port {port} did not return {expected}; last={last_status}")

    @staticmethod
    def probe_http(port: int, path: str, expected: int = 200) -> bool:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            response.read()
            return response.status == expected
        except OSError:
            return False
        finally:
            connection.close()

    def wait_all_ready(self, timeout: float = 15.0) -> None:
        for node in self.nodes.values():
            if node.managed.process.poll() is None:
                self.wait_http(node.port, "/readyz", expected=200, timeout=timeout)

    def scrape_metrics(self, phase: str, position: str) -> dict[int, dict[str, float]]:
        snapshots: dict[int, dict[str, float]] = {}
        for index, node in sorted(self.nodes.items()):
            if node.managed.process.poll() is not None:
                continue
            connection = http.client.HTTPConnection("127.0.0.1", node.port, timeout=2)
            connection.request("GET", "/metrics")
            response = connection.getresponse()
            body = response.read().decode("utf-8", "replace")
            connection.close()
            directory = self.output / f"node-{index}-metrics"
            directory.mkdir(exist_ok=True)
            (directory / f"{safe_name(phase)}-{position}.txt").write_text(body, encoding="utf-8")
            snapshots[index] = parse_prometheus(body)
        return snapshots

    def metrics_delta(self, before: dict[int, dict[str, float]],
                      after: dict[int, dict[str, float]]) -> dict[str, object]:
        nodes = {}
        durable_success = 0.0
        for index in sorted(set(before) | set(after)):
            delta = metric_delta(before.get(index, {}), after.get(index, {}))
            nodes[str(index)] = delta
            durable_success += sum(
                value for key, value in delta.items()
                if key.startswith("ai_gateway_requests_total{")
                and 'status="200"' in key and 'error_class="success"' in key
            )
        return {"nodes": nodes, "durable_successes": int(durable_success)}

    @staticmethod
    def durable_successes(nodes: dict[str, dict[str, float]]) -> int:
        return int(sum(
            value
            for delta in nodes.values()
            for key, value in delta.items()
            if key.startswith("ai_gateway_requests_total{")
            and 'status="200"' in key and 'error_class="success"' in key
        ))

    def database_query(self, sql: str) -> str:
        return run_checked([
            "mariadb", "--no-defaults", "-h", "127.0.0.1", "-P", str(self.database_port),
            "-u", "gateway", "-ptest-db-password", "-N", "ai_gateway", "-e", sql,
        ]).stdout.strip()

    def redis_lease_members(self) -> int:
        keys = run_checked([
            "redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "--raw", "KEYS",
            f"{self.redis_prefix}:governance:v1:lease:*",
        ]).stdout.splitlines()
        redis_time = run_checked([
            "redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "--raw", "TIME",
        ]).stdout.splitlines()
        if len(redis_time) != 2:
            raise RuntimeError("Redis TIME returned an invalid response")
        now_ms = int(redis_time[0]) * 1000 + int(redis_time[1]) // 1000
        total = 0
        for key in keys:
            if key:
                total += int(run_checked([
                    "redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port),
                    "ZCOUNT", key, f"({now_ms}", "+inf",
                ]).stdout.strip() or "0")
        return total

    def dependency_snapshot(self) -> dict[str, Optional[int]]:
        database_connections: Optional[int] = None
        redis_clients: Optional[int] = None
        with contextlib.suppress(Exception):
            value = self.database_query("SHOW STATUS LIKE 'Threads_connected'")
            database_connections = int(value.split()[-1])
        with contextlib.suppress(Exception):
            info = run_checked([
                "redis-cli", "-h", "127.0.0.1", "-p", str(self.redis_port), "INFO", "clients",
            ]).stdout
            redis_clients = int(next(
                line.split(":", 1)[1] for line in info.splitlines()
                if line.startswith("connected_clients:")
            ))
        return {
            "mariadb_threads_connected": database_connections,
            "redis_connected_clients": redis_clients,
        }

    def process_suppliers(self) -> dict[str, Callable[[], set[int]]]:
        suppliers: dict[str, Callable[[], set[int]]] = {"load-generator": lambda: {os.getpid()}}
        for index in self.nodes:
            suppliers[f"gateway-{index}"] = lambda index=index: (
                {self.nodes[index].managed.pid}
                if index in self.nodes and self.nodes[index].managed.process.poll() is None
                else set()
            )
        if self.nginx:
            suppliers["nginx"] = lambda: descendants(self.nginx.pid) if self.nginx else set()
        if self.mariadb:
            suppliers["mariadb"] = lambda: {self.mariadb.pid} if self.mariadb else set()
        if self.redis:
            suppliers["redis"] = lambda: {self.redis.pid} if self.redis else set()
        if self.provider_process:
            suppliers["mock-provider"] = lambda: (
                {self.provider_process.pid} if self.provider_process and self.provider_process.is_alive() else set()
            )
        return suppliers

    def write_environment(self) -> None:
        data = {
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "repo": str(self.repo),
            "gateway": str(self.gateway_binary),
            "admin": str(self.admin_binary),
            "gateway_ports": self.gateway_ports,
            "nginx_port": self.nginx_port,
            "database_port": self.database_port,
            "redis_port": self.redis_port,
            "provider_port": self.provider_port,
            "cpu_count": os.cpu_count(),
            "profiles": {"comparison": COMPARISON_PROFILE, "production": PRODUCTION_PROFILE},
            "governance_lease_ttl_ms": 5000,
        }
        (self.output / "environment.json").write_text(json.dumps(data, indent=2), encoding="utf-8")

    def cleanup(self) -> None:
        if self._cleaned:
            return
        self._cleaned = True
        self.stop_gateways()
        if self.nginx:
            self.nginx.stop()
        if self.redis:
            self.redis.stop()
        if self.mariadb:
            self.mariadb.stop()
        if self.provider_process:
            self.provider_process.terminate()
            self.provider_process.join(timeout=5)
            if self.provider_process.is_alive():
                self.provider_process.kill()
                self.provider_process.join(timeout=2)
        if self.args.keep_runtime:
            print(f"runtime retained at {self.runtime_temp.name}")
            self.runtime_temp.cleanup = lambda: None
        else:
            self.runtime_temp.cleanup()


def make_request(connection: http.client.HTTPConnection, phase: str, workload: str,
                 request_id: str, api_key: str) -> RequestResult:
    started_wall = time.time()
    started = time.perf_counter()
    stream = workload == "stream"
    status: int | str = "client_error"
    error_code = ""
    success = False
    ttft: Optional[float] = None
    response_bytes = 0
    upstream = ""
    saw_done = False
    try:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-Client-Request-Id": request_id,
        }
        if stream:
            body = json.dumps({
                "model": "gateway-model",
                "messages": [{"role": "user", "content": "cluster benchmark request"}],
                "stream": True,
            }, separators=(",", ":"))
            path = "/v1/chat/completions"
        elif workload == "health":
            body = None
            path = "/healthz"
            headers = {"X-Client-Request-Id": request_id}
        else:
            body = json.dumps({
                "model": "gateway-model", "input": "cluster benchmark request", "stream": False,
            }, separators=(",", ":"))
            path = "/v1/responses"
        connection.request("GET" if workload == "health" else "POST", path, body=body, headers=headers)
        response = connection.getresponse()
        status = response.status
        upstream = response.getheader("X-Stress-Upstream", "")
        if stream:
            chunks: list[bytes] = []
            stream_error = ""
            while True:
                line = response.readline()
                if not line:
                    break
                chunks.append(line)
                if ttft is None and line.startswith(b"data:"):
                    ttft = time.perf_counter() - started
                stripped = line.strip()
                if stripped == b"data: [DONE]":
                    saw_done = True
                elif stripped.startswith(b"data:"):
                    candidate = stripped[5:].strip()
                    if candidate and candidate != b"[DONE]":
                        code = extract_error_code(candidate)
                        if code not in {"http_error", "invalid_error_body"}:
                            stream_error = code
            payload = b"".join(chunks)
            response_bytes = len(payload)
            if response.status != 200:
                error_code = extract_error_code(payload)
            elif stream_error:
                error_code = stream_error
            elif not saw_done:
                error_code = "missing_done"
            else:
                success = True
        else:
            payload = response.read()
            response_bytes = len(payload)
            if response.status != 200:
                error_code = extract_error_code(payload)
            elif workload == "health":
                success = b'"status":"ok"' in payload.replace(b" ", b"")
                if not success:
                    error_code = "invalid_health_response"
            else:
                try:
                    success = json.loads(payload).get("object") == "response"
                except (json.JSONDecodeError, AttributeError):
                    error_code = "invalid_json_response"
                if not success and not error_code:
                    error_code = "invalid_response_object"
    except Exception as error:
        status = type(error).__name__
        error_code = f"client_{type(error).__name__}"
    finished = time.perf_counter()
    return RequestResult(
        phase=phase,
        request_id=request_id,
        workload=workload,
        stream=stream,
        started_at=started_wall,
        finished_at=time.time(),
        status=status,
        success=success,
        error_code=error_code or ("success" if success else "unknown_error"),
        latency_seconds=finished - started,
        ttft_seconds=ttft,
        response_bytes=response_bytes,
        upstream=upstream,
        saw_done=saw_done,
    )


class LoadSession:
    def __init__(self, environment: ClusterEnvironment, phase: str,
                 workload: str, concurrency: int):
        self.environment = environment
        self.phase = phase
        self.workload = workload
        self.concurrency = concurrency
        self.stop_event = threading.Event()
        self.start_event = threading.Event()
        self.executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self.futures: list[concurrent.futures.Future[list[RequestResult]]] = []
        self.prefix = hashlib.sha256(f"{phase}-{time.time_ns()}".encode()).hexdigest()[:10]

    def _worker(self, worker_id: int) -> list[RequestResult]:
        results: list[RequestResult] = []
        iteration = 0
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.environment.nginx_port, timeout=10
        )
        self.start_event.wait()
        while not self.stop_event.is_set():
            if self.workload == "mixed":
                request_workload = "non_stream" if (worker_id + iteration) % 2 == 0 else "stream"
            else:
                request_workload = self.workload
            request_id = f"stress-{self.prefix}-{worker_id}-{iteration}"
            result = make_request(
                connection, self.phase, request_workload, request_id, self.environment.api_key
            )
            results.append(result)
            if result.stream or isinstance(result.status, str):
                connection.close()
                connection = http.client.HTTPConnection(
                    "127.0.0.1", self.environment.nginx_port, timeout=10
                )
            iteration += 1
        connection.close()
        return results

    def start(self) -> None:
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency)
        self.futures = [self.executor.submit(self._worker, index) for index in range(self.concurrency)]
        time.sleep(0.05)
        self.start_event.set()

    def stop(self) -> list[RequestResult]:
        self.stop_event.set()
        results = [item for future in self.futures for item in future.result(timeout=20)]
        if self.executor:
            self.executor.shutdown(wait=True)
        return results


class MetricEpochTracker:
    """Accumulate process-local counters across Gateway restarts."""

    def __init__(self, environment: ClusterEnvironment, phase: str):
        self.environment = environment
        self.phase = phase
        self.baselines = environment.scrape_metrics(phase, "before")
        self.totals: dict[int, Counter[str]] = defaultdict(Counter)

    def close_node_epoch(self, index: int, label: str) -> None:
        current = self.environment.scrape_metrics(self.phase, label)
        if index in current:
            self.totals[index].update(metric_delta(self.baselines.get(index, {}), current[index]))
        self.baselines.pop(index, None)

    def start_node_epoch(self, index: int, label: str) -> None:
        current = self.environment.scrape_metrics(self.phase, label)
        if index in current:
            self.baselines[index] = current[index]

    def finish(self, label: str = "after") -> dict[str, object]:
        current = self.environment.scrape_metrics(self.phase, label)
        for index, baseline in list(self.baselines.items()):
            if index in current:
                self.totals[index].update(metric_delta(baseline, current[index]))
        nodes = {
            str(index): {key: round(value, 6) for key, value in values.items() if value != 0}
            for index, values in sorted(self.totals.items())
        }
        return {
            "nodes": nodes,
            "durable_successes": self.environment.durable_successes(nodes),
        }


def summarize_samples(samples: list[RequestResult], elapsed: float) -> dict[str, object]:
    successes = [sample for sample in samples if sample.success]
    streams = [sample for sample in samples if sample.stream]
    successful_streams = [sample for sample in streams if sample.success]
    errors = Counter(sample.error_code for sample in samples if not sample.success)
    statuses = Counter(str(sample.status) for sample in samples)
    upstreams = Counter(sample.upstream for sample in samples if sample.upstream)
    all_latencies = [sample.latency_seconds for sample in samples]
    success_latencies = [sample.latency_seconds for sample in successes]
    ttfts = [sample.ttft_seconds for sample in successful_streams if sample.ttft_seconds is not None]
    stream_latencies = [sample.latency_seconds for sample in successful_streams]
    upstream_outcomes: dict[str, Counter[str]] = defaultdict(Counter)
    for sample in samples:
        if sample.upstream:
            upstream_outcomes[sample.upstream]["success" if sample.success else "error"] += 1
    http_503 = sum(1 for sample in samples if sample.status == 503)
    known_5xx_codes = ERROR_CODES | {"upstream_stream_error"}
    other_5xx = sum(
        1 for sample in samples
        if isinstance(sample.status, int) and 500 <= sample.status < 600
        and sample.error_code not in known_5xx_codes
    )
    return {
        "elapsed_seconds": round(elapsed, 3),
        "requests": len(samples),
        "successes": len(successes),
        "errors": len(samples) - len(successes),
        "success_rate_percent": round(len(successes) / max(1, len(samples)) * 100.0, 4),
        "all_response_rps": round(len(samples) / max(elapsed, 0.001), 2),
        "success_rps": round(len(successes) / max(elapsed, 0.001), 2),
        "rejection_rps": round((len(samples) - len(successes)) / max(elapsed, 0.001), 2),
        "http_503_rps": round(http_503 / max(elapsed, 0.001), 2),
        "other_5xx": other_5xx,
        "status_breakdown": dict(statuses),
        "error_breakdown": dict(errors),
        "upstream_distribution": dict(upstreams),
        "upstream_outcomes": {
            upstream: dict(outcomes) for upstream, outcomes in upstream_outcomes.items()
        },
        "latency_all_p50_ms": milliseconds(percentile(all_latencies, 0.50)),
        "latency_all_p95_ms": milliseconds(percentile(all_latencies, 0.95)),
        "latency_all_p99_ms": milliseconds(percentile(all_latencies, 0.99)),
        "latency_success_p50_ms": milliseconds(percentile(success_latencies, 0.50)),
        "latency_success_p95_ms": milliseconds(percentile(success_latencies, 0.95)),
        "latency_success_p99_ms": milliseconds(percentile(success_latencies, 0.99)),
        "sse_requests": len(streams),
        "sse_successes": len(successful_streams),
        "sse_missing_done": sum(1 for sample in streams if not sample.saw_done),
        "sse_ttft_p50_ms": milliseconds(percentile(ttfts, 0.50)),
        "sse_ttft_p95_ms": milliseconds(percentile(ttfts, 0.95)),
        "sse_ttft_p99_ms": milliseconds(percentile(ttfts, 0.99)),
        "sse_completion_p50_ms": milliseconds(percentile(stream_latencies, 0.50)),
        "sse_completion_p95_ms": milliseconds(percentile(stream_latencies, 0.95)),
        "sse_completion_p99_ms": milliseconds(percentile(stream_latencies, 0.99)),
    }


def failure_window(samples: list[RequestResult], started_at: float) -> dict[str, object]:
    failures = [
        sample for sample in samples
        if sample.finished_at >= started_at
        and isinstance(sample.status, int) and 500 <= sample.status < 600
    ]
    buckets = Counter(int(sample.finished_at - started_at) for sample in failures)
    return {
        "http_5xx": len(failures),
        "http_5xx_peak_per_second": max(buckets.values(), default=0),
        "http_5xx_duration_seconds": None if not failures else round(
            max(sample.finished_at for sample in failures)
            - min(sample.finished_at for sample in failures),
            3,
        ),
    }


def count_metric_error(nodes: dict[str, dict[str, float]], error_code: str) -> int:
    return int(sum(
        value
        for metrics in nodes.values()
        for key, value in metrics.items()
        if key.startswith("ai_gateway_requests_total{")
        and f'error_class="{error_code}"' in key
    ))


def rolling_down_routing_violations(samples: list[RequestResult],
                                    steps: list[dict[str, object]],
                                    gateway_ports: list[int]) -> list[str]:
    violations = []
    for sample in samples:
        for step in steps:
            target = f"127.0.0.1:{gateway_ports[int(step['node']) - 1]}"
            if float(step["signalled_at"]) <= sample.started_at < float(step["ready_at"]):
                if sample.upstream == target:
                    violations.append(sample.request_id)
    return violations


class BenchmarkRunner:
    def __init__(self, environment: ClusterEnvironment, args: argparse.Namespace):
        self.environment = environment
        self.args = args
        self.results: list[dict[str, object]] = []
        self.raw_path = environment.output / "raw-samples.jsonl.gz"

    def write_raw(self, samples: list[RequestResult]) -> None:
        with gzip.open(self.raw_path, "at", encoding="utf-8") as output:
            for sample in samples:
                output.write(json.dumps(sample.as_json(), separators=(",", ":")) + "\n")

    def run_load(self, phase: str, stage: str, profile: str, topology: str,
                 workload: str, concurrency: int) -> dict[str, object]:
        self.environment.wait_all_ready()
        if self.args.warmup_seconds > 0:
            warmup = LoadSession(self.environment, phase + "-warmup", workload, concurrency)
            warmup.start()
            time.sleep(self.args.warmup_seconds)
            warmup.stop()
            time.sleep(min(0.5, self.args.between_seconds))
            self.environment.wait_all_ready()
        before = self.environment.scrape_metrics(phase, "before")
        monitor = ResourceMonitor(self.environment.process_suppliers())
        session = LoadSession(self.environment, phase, workload, concurrency)
        monitor.start()
        started = time.monotonic()
        session.start()
        time.sleep(self.args.sample_seconds)
        samples = session.stop()
        elapsed = time.monotonic() - started
        resources = monitor.stop(elapsed)
        time.sleep(min(1.0, self.args.between_seconds))
        after = self.environment.scrape_metrics(phase, "after")
        metrics = self.environment.metrics_delta(before, after)
        summary = summarize_samples(samples, elapsed)
        summary.update({
            "phase": phase,
            "stage": stage,
            "profile": profile,
            "topology": topology,
            "workload": workload,
            "concurrency": concurrency,
            "durable_successes": metrics["durable_successes"],
            "durable_success_rps": round(metrics["durable_successes"] / max(elapsed, 0.001), 2),
            "metric_deltas": metrics["nodes"],
            "resources": resources,
            "dependencies": self.environment.dependency_snapshot(),
        })
        self.write_raw(samples)
        self.results.append(summary)
        print(json.dumps({
            "phase": phase,
            "success_rps": summary["success_rps"],
            "durable_success_rps": summary["durable_success_rps"],
            "success_rate_percent": summary["success_rate_percent"],
            "p95_ms": summary["latency_success_p95_ms"],
        }), flush=True)
        if self.args.between_seconds > 0:
            time.sleep(self.args.between_seconds)
        return summary

    def matrix(self, values: list[int]) -> list[int]:
        filtered = [value for value in values if value <= self.args.max_concurrency]
        return filtered or [self.args.max_concurrency]

    def run_capacity(self) -> None:
        self.environment.start_gateways(3, "production")
        for concurrency in self.matrix([1, 32, 128, 256, 512]):
            self.run_load(
                f"A-health-c{concurrency}", "A", "production", "three-node",
                "health", concurrency,
            )

        self.environment.start_gateways(1, "comparison")
        for workload in ("non_stream", "mixed"):
            for concurrency in self.matrix([8, 32, 64, 128, 256]):
                self.run_load(
                    f"B-single-{workload}-c{concurrency}", "B", "comparison",
                    "single-node", workload, concurrency,
                )

        self.environment.start_gateways(3, "comparison")
        for workload in ("non_stream", "mixed"):
            for concurrency in self.matrix([8, 32, 64, 128, 256, 384, 512]):
                self.run_load(
                    f"C-cluster-{workload}-c{concurrency}", "C", "comparison",
                    "three-node", workload, concurrency,
                )

        self.environment.start_gateways(3, "production")
        for workload in ("non_stream", "mixed"):
            for concurrency in self.matrix([32, 64, 128, 256, 384, 512]):
                self.run_load(
                    f"D-production-{workload}-c{concurrency}", "D", "production",
                    "three-node", workload, concurrency,
                )

    def run_hard_failure(self) -> None:
        environment = self.environment
        environment.start_gateways(3, "production")
        concurrency = min(256, self.args.max_concurrency)
        phase = "E-hard-failure"
        if self.args.quick:
            warmup_seconds, baseline_seconds, failed_seconds, recovery_seconds = 0.5, 1.0, 2.5, 1.0
        else:
            warmup_seconds, baseline_seconds, failed_seconds, recovery_seconds = 20.0, 30.0, 60.0, 30.0
        session = LoadSession(environment, phase, "mixed", concurrency)
        monitor = ResourceMonitor(environment.process_suppliers())
        session.start()
        time.sleep(warmup_seconds)
        measurement_start = time.time()
        metric_tracker = MetricEpochTracker(environment, phase)
        abandoned_before = int(environment.database_query(
            "SELECT COUNT(*) FROM usage_records WHERE state='abandoned'"
        ) or "0")
        monitor.start()
        time.sleep(baseline_seconds)
        metric_tracker.close_node_epoch(2, "node-2-pre-kill")
        killed_at = time.time()
        environment.stop_node(2, sig=signal.SIGKILL, timeout=3)
        leases_after_kill = environment.redis_lease_members()
        started_after_kill = int(environment.database_query(
            "SELECT COUNT(*) FROM usage_records WHERE state='started'"
        ) or "0")
        time.sleep(failed_seconds)
        restarted_at = time.time()
        environment.start_node(2, "production")
        ready_at = time.time()
        metric_tracker.start_node_epoch(2, "node-2-post-restart")
        time.sleep(recovery_seconds)
        samples = session.stop()
        measurement_end = time.time()
        elapsed = measurement_end - measurement_start
        resources = monitor.stop(elapsed)
        time.sleep(min(1.0, self.args.between_seconds))
        metrics = metric_tracker.finish()
        lease_deadline = time.monotonic() + (2 if self.args.quick else 12)
        remaining_leases = environment.redis_lease_members()
        while remaining_leases and time.monotonic() < lease_deadline:
            time.sleep(0.25)
            remaining_leases = environment.redis_lease_members()
        lease_recovered_at = time.time() if remaining_leases == 0 else None
        stranded_before_reconciliation = int(environment.database_query(
            "SELECT COUNT(*) FROM usage_records WHERE state='started'"
        ) or "0")
        accelerated_reservations = int(environment.database_query(
            "UPDATE budget_reservations br "
            "JOIN usage_records ur ON ur.id=br.usage_record_id "
            "SET br.lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6), INTERVAL 1 SECOND) "
            "WHERE br.state='reserved' AND ur.state='started'; "
            "SELECT ROW_COUNT()"
        ) or "0")
        reconciliation_deadline = time.monotonic() + (2 if self.args.quick else 5)
        stranded_after_reconciliation = int(environment.database_query(
            "SELECT COUNT(*) FROM usage_records WHERE state='started'"
        ) or "0")
        while stranded_after_reconciliation and time.monotonic() < reconciliation_deadline:
            time.sleep(0.25)
            stranded_after_reconciliation = int(environment.database_query(
                "SELECT COUNT(*) FROM usage_records WHERE state='started'"
            ) or "0")
        abandoned_after = int(environment.database_query(
            "SELECT COUNT(*) FROM usage_records WHERE state='abandoned'"
        ) or "0")
        measured = [sample for sample in samples if sample.started_at >= measurement_start]
        interrupted = [
            sample for sample in measured
            if sample.stream and not sample.success
            and sample.started_at < killed_at < sample.finished_at
        ]
        interrupted_ids = {sample.request_id for sample in interrupted}
        availability_samples = [
            sample for sample in measured if sample.request_id not in interrupted_ids
        ]
        post_kill = [sample for sample in measured if sample.started_at >= killed_at]
        first_success = min(
            (sample.started_at for sample in post_kill if sample.success), default=None
        )
        failures_after_kill = [sample.finished_at for sample in post_kill if not sample.success]
        dead_target = f"127.0.0.1:{environment.gateway_ports[1]}"
        routed_to_dead = [
            sample for sample in post_kill
            if sample.started_at < ready_at and sample.upstream == dead_target
        ]
        dead_target_failures = [
            sample for sample in measured
            if sample.finished_at >= killed_at and sample.started_at < ready_at
            and sample.upstream == dead_target and not sample.success
        ]
        detected = min(
            (sample.finished_at for sample in dead_target_failures), default=None
        )
        segments = {
            "baseline": summarize_samples(
                [sample for sample in measured if sample.started_at < killed_at], baseline_seconds
            ),
            "node_down": summarize_samples(
                [sample for sample in measured if killed_at <= sample.started_at < restarted_at],
                max(0.001, restarted_at - killed_at),
            ),
            "recovered": summarize_samples(
                [sample for sample in measured if sample.started_at >= ready_at], recovery_seconds
            ),
        }
        duplicates = provider_duplicates(environment.output / "provider-requests.jsonl", session.prefix)
        summary = summarize_samples(measured, elapsed)
        summary.update({
            "phase": phase, "stage": "E", "profile": "production",
            "topology": "three-node", "workload": "mixed", "concurrency": concurrency,
            "segments": segments,
            "fault": {
                "killed_node": 2,
                "killed_at": killed_at,
                "restarted_at": restarted_at,
                "ready_at": ready_at,
                "ready_recovery_seconds": round(ready_at - restarted_at, 3),
                "leases_after_kill": leases_after_kill,
                "leases_after_recovery": remaining_leases,
                "lease_recovery_seconds": None if lease_recovered_at is None else round(
                    lease_recovered_at - killed_at, 3
                ),
                "started_usage_after_kill": started_after_kill,
                "mysql_lease_time_accelerated": True,
                "accelerated_mysql_reservations": accelerated_reservations,
                "stranded_usage_before_reconciliation": stranded_before_reconciliation,
                "stranded_usage_after_reconciliation": stranded_after_reconciliation,
                "abandoned_usage_delta": abandoned_after - abandoned_before,
                "interrupted_inflight_sse": [sample.request_id for sample in interrupted],
                "first_success_after_kill_seconds": None if first_success is None else round(
                    first_success - killed_at, 3
                ),
                "last_failure_after_kill_seconds": None if not failures_after_kill else round(
                    max(failures_after_kill) - killed_at, 3
                ),
                "passive_failure_detected_seconds": None if detected is None else round(
                    detected - killed_at, 3
                ),
                "dead_node_routed_requests": len(routed_to_dead),
                "last_dead_node_routing_seconds": None if not routed_to_dead else round(
                    max(sample.started_at for sample in routed_to_dead) - killed_at, 3
                ),
                **failure_window(post_kill, killed_at),
                "duplicate_provider_request_ids": duplicates,
            },
            "cluster_availability": summarize_samples(availability_samples, elapsed),
            "durable_successes": metrics["durable_successes"],
            "durable_success_rps": round(metrics["durable_successes"] / max(elapsed, 0.001), 2),
            "metric_deltas": metrics["nodes"],
            "resources": resources,
            "dependencies": environment.dependency_snapshot(),
        })
        self.write_raw(measured)
        self.results.append(summary)

    def run_rolling_drain(self) -> None:
        environment = self.environment
        environment.start_gateways(3, "production")
        environment.set_stream_delay(0.05 if self.args.quick else 0.25)
        concurrency = min(128, self.args.max_concurrency)
        phase = "F-rolling-drain"
        session = LoadSession(environment, phase, "mixed", concurrency)
        monitor = ResourceMonitor(environment.process_suppliers())
        metric_tracker = MetricEpochTracker(environment, phase)
        started_at = time.time()
        monitor.start()
        session.start()
        time.sleep(0.5 if self.args.quick else 5.0)
        steps = []
        for index in (1, 2, 3):
            active = {1, 2, 3} - {index}
            reload_started = time.time()
            environment.configure_nginx(active)
            metric_tracker.close_node_epoch(index, f"node-{index}-pre-drain")
            signalled_at = time.time()
            node = environment.nodes[index]
            node.managed.process.send_signal(signal.SIGTERM)
            direct_code = "connection_closed"
            with contextlib.suppress(Exception):
                direct_code = direct_proxy_error(node.port, environment.api_key)
            exit_code = node.managed.process.wait(timeout=20)
            exited_at = time.time()
            with contextlib.suppress(Exception):
                node.managed.log_handle.close()
            environment.start_node(index, "production")
            ready_at = time.time()
            metric_tracker.start_node_epoch(index, f"node-{index}-post-restart")
            environment.configure_nginx({1, 2, 3})
            rejoined_at = time.time()
            steps.append({
                "node": index,
                "reload_started_at": reload_started,
                "signalled_at": signalled_at,
                "exited_at": exited_at,
                "exit_code": exit_code,
                "drain_seconds": round(exited_at - signalled_at, 3),
                "direct_drain_result": direct_code,
                "ready_at": ready_at,
                "rejoined_at": rejoined_at,
            })
            time.sleep(0.2 if self.args.quick else 2.0)
        samples = session.stop()
        ended_at = time.time()
        elapsed = ended_at - started_at
        resources = monitor.stop(elapsed)
        environment.set_stream_delay(0.0)
        time.sleep(min(1.0, self.args.between_seconds))
        metrics = metric_tracker.finish()
        interrupted = []
        for sample in samples:
            if not sample.stream or sample.success:
                continue
            if any(sample.started_at < step["signalled_at"] < sample.finished_at for step in steps):
                interrupted.append(sample.request_id)
        duplicates = provider_duplicates(environment.output / "provider-requests.jsonl", session.prefix)
        down_routing_violations = rolling_down_routing_violations(
            samples, steps, environment.gateway_ports
        )
        summary = summarize_samples(samples, elapsed)
        summary.update({
            "phase": phase, "stage": "F", "profile": "production",
            "topology": "three-node", "workload": "mixed", "concurrency": concurrency,
            "rolling_steps": steps,
            "interrupted_preexisting_sse": interrupted,
            "requests_routed_to_down_nodes_after_grace": down_routing_violations,
            "duplicate_provider_request_ids": duplicates,
            "all_nodes_ready": all(
                node.managed.process.poll() is None
                and environment.probe_http(node.port, "/readyz")
                for node in environment.nodes.values()
            ),
            "durable_successes": metrics["durable_successes"],
            "durable_success_rps": round(metrics["durable_successes"] / max(elapsed, 0.001), 2),
            "metric_deltas": metrics["nodes"],
            "gateway_draining_probe_responses": sum(
                1 for step in steps if step["direct_drain_result"] == "gateway_draining"
            ),
            "gateway_draining_metric_delta": count_metric_error(
                metrics["nodes"], "gateway_draining"
            ),
            "resources": resources,
            "dependencies": environment.dependency_snapshot(),
        })
        self.write_raw(samples)
        self.results.append(summary)

    def finalize(self) -> None:
        scaling = scaling_summary(self.results)
        capacity = production_capacity_summary(self.results)
        error_onsets = error_onset_summary(self.results)
        payload = {
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "results": self.results,
            "scaling": scaling,
            "production_capacity": capacity,
            "error_onsets": error_onsets,
        }
        (self.environment.output / "results.json").write_text(
            json.dumps(payload, indent=2), encoding="utf-8"
        )
        (self.environment.output / "SUMMARY.md").write_text(
            render_summary(self.results, scaling, capacity, error_onsets), encoding="utf-8"
        )


def direct_proxy_error(port: int, api_key: str) -> str:
    deadline = time.monotonic() + 1.0
    last = "connection_closed"
    while time.monotonic() < deadline:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
        try:
            body = json.dumps({"model": "gateway-model", "input": "drain probe", "stream": False})
            connection.request("POST", "/v1/responses", body=body, headers={
                "Authorization": f"Bearer {api_key}", "Content-Type": "application/json",
            })
            response = connection.getresponse()
            payload = response.read()
            last = extract_error_code(payload) if response.status != 200 else "success"
            if last == "gateway_draining":
                return last
        except OSError:
            last = "connection_closed"
        finally:
            connection.close()
        time.sleep(0.03)
    return last


def provider_duplicates(path: pathlib.Path, phase_prefix: str) -> list[str]:
    counts: Counter[str] = Counter()
    if not path.exists():
        return []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            request_id = json.loads(line).get("client_request_id", "")
        except json.JSONDecodeError:
            continue
        if phase_prefix in request_id:
            counts[request_id] += 1
    return sorted(request_id for request_id, count in counts.items() if count > 1)


def scaling_summary(results: list[dict[str, object]]) -> dict[str, object]:
    output = {}
    for workload in ("non_stream", "mixed"):
        single = [
            item for item in results
            if item.get("stage") == "B" and item.get("workload") == workload
        ]
        cluster = [
            item for item in results
            if item.get("stage") == "C" and item.get("workload") == workload
        ]
        if not single or not cluster:
            continue
        best_single = max(single, key=lambda item: float(item["durable_success_rps"]))
        best_cluster = max(cluster, key=lambda item: float(item["durable_success_rps"]))
        baseline = float(best_single["durable_success_rps"])
        cluster_rps = float(best_cluster["durable_success_rps"])
        output[workload] = {
            "single_peak_durable_rps": baseline,
            "single_peak_concurrency": best_single["concurrency"],
            "single_peak_client_success_rps": best_single.get("success_rps"),
            "single_peak_success_p95_ms": best_single.get("latency_success_p95_ms"),
            "single_peak_success_p99_ms": best_single.get("latency_success_p99_ms"),
            "cluster_peak_durable_rps": cluster_rps,
            "cluster_peak_concurrency": best_cluster["concurrency"],
            "cluster_peak_client_success_rps": best_cluster.get("success_rps"),
            "cluster_peak_success_p95_ms": best_cluster.get("latency_success_p95_ms"),
            "cluster_peak_success_p99_ms": best_cluster.get("latency_success_p99_ms"),
            "gain": None if baseline == 0 else round(cluster_rps / baseline, 3),
            "efficiency": None if baseline == 0 else round(cluster_rps / (baseline * 3), 3),
        }
    return output


def production_capacity_summary(results: list[dict[str, object]]) -> dict[str, object]:
    output: dict[str, object] = {}
    for workload in ("non_stream", "mixed"):
        rows = sorted(
            (
                item for item in results
                if item.get("stage") == "D" and item.get("workload") == workload
            ),
            key=lambda item: int(item["concurrency"]),
        )
        previous_rps: Optional[float] = None
        for item in rows:
            signals = []
            durable_rps = float(item["durable_success_rps"])
            if float(item.get("success_rate_percent", 100.0)) < 99.0:
                signals.append("success_rate_below_99_percent")
            if float(item.get("latency_success_p95_ms") or 0.0) > 1000.0:
                signals.append("success_p95_above_1000_ms")
            if any(int(item.get("error_breakdown", {}).get(code, 0)) > 0 for code in ERROR_CODES):
                signals.append("gateway_or_dependency_rejections")
            if previous_rps is not None and durable_rps <= previous_rps * 1.01:
                signals.append("durable_success_rps_not_growing")
            if signals:
                output[workload] = {
                    "phase": item["phase"],
                    "concurrency": item["concurrency"],
                    "durable_success_rps": durable_rps,
                    "success_p95_ms": item.get("latency_success_p95_ms"),
                    "signals": signals,
                }
                break
            previous_rps = durable_rps
        if workload not in output:
            output[workload] = None
    return output


def error_onset_summary(results: list[dict[str, object]]) -> dict[str, object]:
    output: dict[str, object] = {}
    for stage in ("B", "C", "D"):
        for workload in ("non_stream", "mixed"):
            rows = sorted(
                (
                    item for item in results
                    if item.get("stage") == stage and item.get("workload") == workload
                ),
                key=lambda item: int(item["concurrency"]),
            )
            for code in sorted(ERROR_CODES):
                first = next(
                    (item for item in rows if int(item.get("error_breakdown", {}).get(code, 0)) > 0),
                    None,
                )
                if first is not None:
                    output[f"{stage}:{workload}:{code}"] = {
                        "phase": first["phase"],
                        "concurrency": first["concurrency"],
                        "count": first["error_breakdown"][code],
                    }
    return output


def render_summary(results: list[dict[str, object]], scaling: dict[str, object],
                   capacity: Optional[dict[str, object]] = None,
                   error_onsets: Optional[dict[str, object]] = None) -> str:
    lines = [
        "# AiGateway local three-node stress test",
        "",
        "One host, three AiGateway processes behind Nginx, shared temporary MariaDB/Redis and a deterministic mock Provider.",
        "",
        "| Stage | Phase | Topology | Profile | Workload | Concurrency | Durable RPS | Client success RPS | Success % | Success P95 ms |",
        "|---|---|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for item in results:
        lines.append(
            f"| {item.get('stage', '')} | {item.get('phase', '')} | {item.get('topology', '')} | "
            f"{item.get('profile', '')} | {item.get('workload', '')} | {item.get('concurrency', '')} | "
            f"{item.get('durable_success_rps', 0)} | {item.get('success_rps', 0)} | "
            f"{item.get('success_rate_percent', 0)} | {item.get('latency_success_p95_ms', '-')} |"
        )
    lines.extend([
        "", "## Scaling", "", "```json", json.dumps(scaling, indent=2), "```", "",
        "## Production capacity knee", "", "```json",
        json.dumps(capacity or {}, indent=2), "```", "",
        "## Gateway/dependency error onsets", "", "```json",
        json.dumps(error_onsets or {}, indent=2), "```", "",
    ])
    lines.extend([
        "## Interpretation",
        "",
        "`all_response_rps` includes fast failures. Use `durable_success_rps` as the headline throughput.",
        "A local three-process test does not reproduce cross-host latency or independent failure domains.",
        "",
    ])
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    repo_default = pathlib.Path(__file__).resolve().parents[1]
    parser.add_argument("--repo", default=str(repo_default))
    parser.add_argument("--gateway", default=str(repo_default / "build/gateway/bin/AiGateway"))
    parser.add_argument("--admin", default=str(repo_default / "build/gateway/bin/AiGatewayAdmin"))
    parser.add_argument("--output-dir")
    parser.add_argument("--warmup-seconds", type=float, default=10.0)
    parser.add_argument("--sample-seconds", type=float, default=30.0)
    parser.add_argument("--between-seconds", type=float, default=5.0)
    parser.add_argument("--max-concurrency", type=int, default=512)
    parser.add_argument("--skip-capacity", action="store_true")
    parser.add_argument("--skip-faults", action="store_true")
    parser.add_argument("--quick", action="store_true", help="Run a short smoke matrix")
    parser.add_argument("--keep-runtime", action="store_true")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    for name in ("warmup_seconds", "sample_seconds", "between_seconds"):
        if getattr(args, name) < 0:
            raise RuntimeError(f"--{name.replace('_', '-')} must be non-negative")
    if args.sample_seconds <= 0:
        raise RuntimeError("--sample-seconds must be positive")
    if args.max_concurrency < 1 or args.max_concurrency > 4096:
        raise RuntimeError("--max-concurrency must be between 1 and 4096")
    if args.quick:
        args.warmup_seconds = min(args.warmup_seconds, 0.25)
        args.sample_seconds = min(args.sample_seconds, 0.5)
        args.between_seconds = min(args.between_seconds, 0.1)
        args.max_concurrency = min(args.max_concurrency, 8)


def main() -> int:
    args = build_parser().parse_args()
    environment: Optional[ClusterEnvironment] = None
    try:
        validate_args(args)
        environment = ClusterEnvironment(args)
        environment.setup()
        runner = BenchmarkRunner(environment, args)
        if not args.skip_capacity:
            runner.run_capacity()
        if not args.skip_faults:
            runner.run_hard_failure()
            runner.run_rolling_drain()
        runner.finalize()
        print(f"results: {environment.output}")
        return 0
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"cluster stress failed: {error}", file=sys.stderr)
        return 1
    finally:
        if environment is not None:
            environment.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
