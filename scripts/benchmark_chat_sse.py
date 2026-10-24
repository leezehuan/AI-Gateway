#!/usr/bin/env python3
"""Benchmark OpenAI-compatible streaming Chat Completions endpoints."""

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * p
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def one_request(index, row, args):
    source = row["final_llm_request"]
    payload = {
        "model": args.model,
        "messages": source["messages"],
        "temperature": source.get("temperature", 0.0),
        "max_tokens": source.get("max_tokens", 512),
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    if args.seed is not None:
        payload["seed"] = args.seed

    started = time.perf_counter()
    first = None
    finished = None
    usage = {}
    output_chars = 0
    events = 0
    error = None
    try:
        with requests.post(
            args.url,
            headers={
                "Authorization": "Bearer " + args.api_key,
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
            },
            json=payload,
            stream=True,
            timeout=(args.connect_timeout, args.read_timeout),
        ) as response:
            if response.status_code != 200:
                error = f"http_{response.status_code}: {response.text[:300]}"
            else:
                for line in response.iter_lines(decode_unicode=True, chunk_size=1):
                    if not line or not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        finished = time.perf_counter()
                        break
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    if first is None:
                        first = time.perf_counter()
                    events += 1
                    if isinstance(event.get("usage"), dict):
                        usage = event["usage"]
                    for choice in event.get("choices", []):
                        delta = choice.get("delta") or {}
                        output_chars += len(delta.get("content") or "")
                if finished is None:
                    finished = time.perf_counter()
    except Exception as exc:  # keep one failed row from aborting a 1000-row run
        error = f"{type(exc).__name__}: {exc}"
        finished = time.perf_counter()

    result = {
        "index": index,
        "row_index": row.get("row_index", index),
        "input": row.get("input", ""),
        "ttft_ms": None if first is None else (first - started) * 1000,
        "e2e_ms": None if finished is None else (finished - started) * 1000,
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "total_tokens": usage.get("total_tokens"),
        "output_chars": output_chars,
        "sse_events": events,
        "error": error,
    }
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--connect-timeout", type=float, default=10)
    parser.add_argument("--read-timeout", type=float, default=1800)
    args = parser.parse_args()

    rows = []
    with open(args.input, encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    if args.limit > 0:
        rows = rows[: args.limit]

    if args.workers <= 1:
        results = [one_request(i, row, args) for i, row in enumerate(rows)]
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            results = list(pool.map(lambda item: one_request(item[0], item[1], args), enumerate(rows)))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")

    ok = [result for result in results if not result["error"] and result["ttft_ms"] is not None]
    ttft = [result["ttft_ms"] for result in ok]
    e2e = [result["e2e_ms"] for result in ok]
    completion = [result["completion_tokens"] for result in ok if result["completion_tokens"] is not None]
    total_seconds = sum(e2e) / 1000 if e2e else 0
    summary = {
        "url": args.url,
        "model": args.model,
        "requests": len(results),
        "success": len(ok),
        "errors": len(results) - len(ok),
        "ttft_ms": {"p50": percentile(ttft, 0.50), "p95": percentile(ttft, 0.95), "mean": statistics.mean(ttft) if ttft else None},
        "e2e_ms": {"p50": percentile(e2e, 0.50), "p95": percentile(e2e, 0.95), "mean": statistics.mean(e2e) if e2e else None},
        "completion_tokens_sum": sum(completion) if completion else None,
        "completion_tokens_per_second": (sum(completion) / total_seconds) if completion and total_seconds else None,
        "result_file": str(output_path),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
