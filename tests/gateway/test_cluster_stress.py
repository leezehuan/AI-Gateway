#!/usr/bin/env python3
import importlib.util
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


REPO = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "cluster_stress", REPO / "scripts/cluster_stress.py"
)
cluster_stress = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cluster_stress
SPEC.loader.exec_module(cluster_stress)


class ClusterStressUnitTest(unittest.TestCase):
    def test_percentiles_use_nearest_rank(self):
        values = [4.0, 1.0, 3.0, 2.0]
        self.assertEqual(cluster_stress.percentile(values, 0.50), 2.0)
        self.assertEqual(cluster_stress.percentile(values, 0.95), 4.0)
        self.assertIsNone(cluster_stress.percentile([], 0.95))

    def test_prometheus_delta_handles_process_restart(self):
        before = {"counter": 10.0, "gauge": 2.0}
        after = {"counter": 3.0, "gauge": 2.0, "new": 4.0}
        self.assertEqual(
            cluster_stress.metric_delta(before, after),
            {"counter": 3.0, "new": 4.0},
        )
        parsed = cluster_stress.parse_prometheus(
            '# TYPE counter counter\ncounter{status="200"} 12\ngauge 1\n'
        )
        self.assertEqual(parsed, {'counter{status="200"}': 12.0, "gauge": 1.0})

    def test_error_codes_are_protocol_aware(self):
        self.assertEqual(
            cluster_stress.extract_error_code(
                b'{"error":{"type":"server_error","code":"audit_unavailable"}}'
            ),
            "audit_unavailable",
        )
        self.assertEqual(
            cluster_stress.extract_error_code(b'{"type":"error","code":"stream_failed"}'),
            "stream_failed",
        )

    def test_sample_summary_separates_success_and_fast_rejection(self):
        started = time.time()
        samples = [
            cluster_stress.RequestResult(
                "phase", "ok", "non_stream", False, started, started + 0.1,
                200, True, "success", 0.1, None, 100, "127.0.0.1:1", False,
            ),
            cluster_stress.RequestResult(
                "phase", "rejected", "non_stream", False, started, started + 0.01,
                503, False, "gateway_overloaded", 0.01, None, 80, "127.0.0.1:2", False,
            ),
        ]
        summary = cluster_stress.summarize_samples(samples, 1.0)
        self.assertEqual(summary["all_response_rps"], 2.0)
        self.assertEqual(summary["success_rps"], 1.0)
        self.assertEqual(summary["rejection_rps"], 1.0)
        self.assertEqual(summary["http_503_rps"], 1.0)
        self.assertEqual(summary["error_breakdown"], {"gateway_overloaded": 1})
        self.assertEqual(
            summary["upstream_outcomes"],
            {
                "127.0.0.1:1": {"success": 1},
                "127.0.0.1:2": {"error": 1},
            },
        )

    def test_nginx_config_has_three_nodes_and_disables_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            environment = object.__new__(cluster_stress.ClusterEnvironment)
            environment.gateway_ports = [18080, 18082, 18084]
            environment.nginx_port = 18081
            environment.runtime = pathlib.Path(temporary)
            environment.output = pathlib.Path(temporary)
            config = environment.nginx_config({1, 3})
            if shutil.which("nginx"):
                config_path = pathlib.Path(temporary) / "nginx.conf"
                config_path.write_text(config, encoding="utf-8")
                result = subprocess.run(
                    ["nginx", "-t", "-p", temporary, "-c", str(config_path)],
                    text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
                )
                if result.returncode != 0 and "Operation not permitted" in result.stdout:
                    self.skipTest("sandbox does not permit nginx socket validation")
                self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("least_conn;", config)
        self.assertIn("server 127.0.0.1:18080", config)
        self.assertIn("server 127.0.0.1:18082 max_fails=1 fail_timeout=2s down;", config)
        self.assertIn("server 127.0.0.1:18084", config)
        self.assertIn("proxy_next_upstream off;", config)
        self.assertIn("proxy_buffering off;", config)

    def test_scaling_uses_durable_success_rps(self):
        results = [
            {"stage": "B", "workload": "mixed", "durable_success_rps": 100.0,
             "concurrency": 32},
            {"stage": "C", "workload": "mixed", "durable_success_rps": 240.0,
             "concurrency": 128},
        ]
        scaling = cluster_stress.scaling_summary(results)["mixed"]
        self.assertEqual(scaling["gain"], 2.4)
        self.assertEqual(scaling["efficiency"], 0.8)

    def test_duplicate_provider_request_detection_is_phase_scoped(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "provider.jsonl"
            path.write_text(
                "\n".join([
                    json.dumps({"client_request_id": "stress-target-1"}),
                    json.dumps({"client_request_id": "stress-target-1"}),
                    json.dumps({"client_request_id": "stress-other-1"}),
                ]),
                encoding="utf-8",
            )
            duplicates = cluster_stress.provider_duplicates(path, "target")
        self.assertEqual(duplicates, ["stress-target-1"])

    def test_capacity_knee_includes_throughput_plateau(self):
        results = [
            {
                "stage": "D", "phase": "D-production-mixed-c32", "workload": "mixed",
                "concurrency": 32, "durable_success_rps": 100.0,
                "success_rate_percent": 100.0, "latency_success_p95_ms": 20.0,
                "error_breakdown": {},
            },
            {
                "stage": "D", "phase": "D-production-mixed-c64", "workload": "mixed",
                "concurrency": 64, "durable_success_rps": 100.5,
                "success_rate_percent": 100.0, "latency_success_p95_ms": 30.0,
                "error_breakdown": {},
            },
        ]
        capacity = cluster_stress.production_capacity_summary(results)["mixed"]
        self.assertEqual(capacity["concurrency"], 64)
        self.assertIn("durable_success_rps_not_growing", capacity["signals"])

    def test_failure_window_reports_peak_and_duration(self):
        started = time.time()
        samples = [
            cluster_stress.RequestResult(
                "phase", str(index), "non_stream", False,
                started, started + offset, 502, False, "upstream_unavailable", offset,
                None, 10, "127.0.0.1:2", False,
            )
            for index, offset in enumerate((0.1, 0.2, 1.2))
        ]
        window = cluster_stress.failure_window(samples, started)
        self.assertEqual(window["http_5xx"], 3)
        self.assertEqual(window["http_5xx_peak_per_second"], 2)
        self.assertAlmostEqual(window["http_5xx_duration_seconds"], 1.1)

    def test_redis_lease_count_excludes_expired_scores(self):
        environment = object.__new__(cluster_stress.ClusterEnvironment)
        environment.redis_port = 16379
        environment.redis_prefix = "test"
        responses = [
            subprocess.CompletedProcess([], 0, "test:governance:v1:lease:tenant:1\n", ""),
            subprocess.CompletedProcess([], 0, "100\n500000\n", ""),
            subprocess.CompletedProcess([], 0, "2\n", ""),
        ]
        with mock.patch.object(cluster_stress, "run_checked", side_effect=responses) as checked:
            self.assertEqual(environment.redis_lease_members(), 2)
        zcount_command = checked.call_args_list[-1].args[0]
        self.assertEqual(zcount_command[-2:], ["(100500", "+inf"])
        self.assertEqual(zcount_command[-4], "ZCOUNT")

    def test_rolling_violation_window_ends_when_node_is_ready(self):
        sample = lambda request_id, started: cluster_stress.RequestResult(
            "phase", request_id, "non_stream", False, started, started + 0.1,
            200, True, "success", 0.1, None, 10, "127.0.0.1:18082", False,
        )
        violations = cluster_stress.rolling_down_routing_violations(
            [sample("during", 2.0), sample("recovered", 3.1)],
            [{"node": 2, "signalled_at": 1.0, "ready_at": 3.0}],
            [18080, 18082, 18084],
        )
        self.assertEqual(violations, ["during"])


if __name__ == "__main__":
    unittest.main()
