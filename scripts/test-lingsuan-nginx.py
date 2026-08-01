#!/usr/bin/env python3
"""Run explicit, billable Lingsuan checks through an already running Nginx and AiGateway."""

import http.client
import json
import os
import sys
import time
import urllib.parse


DEFAULT_MODEL = "gpt-5.4-mini"


def fail(message):
    raise RuntimeError(message)


def connection_for(base_url, timeout=90):
    parsed = urllib.parse.urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        fail("AI_GATEWAY_NGINX_URL must be an HTTP(S) origin")
    connection_type = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    return connection_type(parsed.hostname, parsed.port, timeout=timeout), parsed.path.rstrip("/")


def request(base_url, method, path, api_key=None, payload=None):
    connection, prefix = connection_for(base_url)
    headers = {}
    body = None
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload)
    connection.request(method, f"{prefix}{path}", body=body, headers=headers)
    response = connection.getresponse()
    response_body = response.read()
    result = response.status, dict(response.getheaders()), response_body
    connection.close()
    return result


def require_json_success(name, status, body):
    if status != 200:
        fail(f"{name} returned HTTP {status}")
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as error:
        fail(f"{name} returned invalid JSON: {error}")
    if not isinstance(parsed, dict):
        fail(f"{name} returned a non-object JSON response")
    return parsed


def check_stream(base_url, api_key, model):
    connection, prefix = connection_for(base_url)
    payload = json.dumps(
        {
            "model": model,
            "input": "Reply with exactly: nginx-sse-ok",
            "stream": True,
        }
    )
    connection.request(
        "POST",
        f"{prefix}/v1/responses",
        body=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    response = connection.getresponse()
    if response.status != 200:
        response.read()
        connection.close()
        fail(f"streaming response returned HTTP {response.status}")
    content_type = response.getheader("Content-Type", "")
    if not content_type.startswith("text/event-stream"):
        response.read()
        connection.close()
        fail(f"streaming response used unexpected Content-Type {content_type!r}")

    started = time.monotonic()
    first_event_seconds = None
    event_names = []
    saw_done = False
    while True:
        line = response.readline()
        if not line:
            break
        if first_event_seconds is None and (line.startswith(b"event:") or line.startswith(b"data:")):
            first_event_seconds = time.monotonic() - started
        if line.startswith(b"event:"):
            event_names.append(line[6:].decode("utf-8", "replace").strip())
        if line.strip() == b"data: [DONE]":
            saw_done = True
    total_seconds = time.monotonic() - started
    connection.close()
    if first_event_seconds is None:
        fail("streaming response contained no SSE event")
    if "response.completed" not in event_names or not saw_done:
        fail("streaming response did not contain response.completed and [DONE]")
    if first_event_seconds >= total_seconds:
        fail("Nginx buffered the complete SSE response before exposing the first event")
    return event_names, first_event_seconds, total_seconds


def main():
    base_url = os.environ.get("AI_GATEWAY_NGINX_URL", "http://127.0.0.1:8081")
    api_key = os.environ.get("AI_GATEWAY_API_KEY", "")
    model = os.environ.get("AI_GATEWAY_LIVE_TEST_MODEL", DEFAULT_MODEL)
    if not api_key:
        fail("AI_GATEWAY_API_KEY is required")

    health_status, _, health_body = request(base_url, "GET", "/healthz")
    health = require_json_success("healthz", health_status, health_body)
    ready_status, _, ready_body = request(base_url, "GET", "/readyz")
    ready = require_json_success("readyz", ready_status, ready_body)
    models_status, _, models_body = request(base_url, "GET", "/v1/models", api_key=api_key)
    models = require_json_success("models", models_status, models_body)
    available = {entry.get("id") for entry in models.get("data", [])}
    if model not in available:
        fail(f"default test model {model!r} is not exposed by the Gateway")

    response_status, _, response_body = request(
        base_url,
        "POST",
        "/v1/responses",
        api_key=api_key,
        payload={
            "model": model,
            "input": "Reply with exactly: nginx-live-ok",
            "stream": False,
        },
    )
    require_json_success("non-streaming response", response_status, response_body)
    events, first_event_seconds, total_seconds = check_stream(base_url, api_key, model)
    print(
        json.dumps(
            {
                "health": health.get("status"),
                "ready": ready.get("status"),
                "model": model,
                "model_count": len(available),
                "non_streaming_status": response_status,
                "streaming_status": 200,
                "first_event_ms": round(first_event_seconds * 1000),
                "stream_total_ms": round(total_seconds * 1000),
                "events": events,
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as error:
        print(f"Lingsuan Nginx test failed: {error}", file=sys.stderr)
        sys.exit(1)
