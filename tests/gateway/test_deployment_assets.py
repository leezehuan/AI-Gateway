#!/usr/bin/env python3
import importlib.util
import json
import pathlib
import sys
import unittest


DISCOVERED_LINGSUAN_MODELS = {
    "codex-auto-review",
    "gpt-4o-audio-preview",
    "gpt-4o-realtime-preview",
    "gpt-5.2",
    "gpt-5.2-2025-12-11",
    "gpt-5.2-chat-latest",
    "gpt-5.2-pro",
    "gpt-5.2-pro-2025-12-11",
    "gpt-5.3-codex-spark",
    "gpt-5.4",
    "gpt-5.4-2026-03-05",
    "gpt-5.4-mini",
    "gpt-5.5",
    "gpt-5.6",
    "gpt-5.6-luna",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-image-1",
    "gpt-image-1.5",
    "gpt-image-2",
}


class DeploymentAssetsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = pathlib.Path(sys.argv[1])
        path = cls.repo / "config" / "ai-gateway.lingsuan.json"
        cls.raw_config = path.read_text(encoding="utf-8")
        cls.config = json.loads(cls.raw_config)
        cls.live_test = (
            cls.repo / "scripts" / "test-lingsuan-nginx.py"
        ).read_text(encoding="utf-8")
        spec = importlib.util.spec_from_file_location(
            "lingsuan_live_test",
            cls.repo / "scripts" / "test-lingsuan-nginx.py",
        )
        cls.live_test_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.live_test_module)
        cls.nginx_config = (
            cls.repo / "deploy" / "nginx" / "ai-gateway.conf.example"
        ).read_text(encoding="utf-8")

    def test_lingsuan_provider_uses_responses_endpoint_and_environment_secret(self):
        provider = self.config["providers"][0]
        self.assertEqual(provider["slug"], "lingsuan")
        self.assertEqual(
            provider["endpoints"][0]["url"],
            "https://lingsuan.top/v1/responses",
        )
        self.assertEqual(
            provider["credentials"][0]["secret_ref"],
            "env:LINGSUAN_API_KEY",
        )
        self.assertNotIn("sk-", self.raw_config)

    def test_lingsuan_models_match_upstream_discovery(self):
        logical_models = {item["name"] for item in self.config["logical_models"]}
        policy_models = set(self.config["policies"][0]["models"])
        mapped_models = {item["logical_model"] for item in self.config["mappings"]}
        upstream_models = {item["upstream_model"] for item in self.config["mappings"]}
        self.assertEqual(logical_models, DISCOVERED_LINGSUAN_MODELS)
        self.assertEqual(policy_models, DISCOVERED_LINGSUAN_MODELS)
        self.assertEqual(mapped_models, DISCOVERED_LINGSUAN_MODELS)
        self.assertEqual(upstream_models, DISCOVERED_LINGSUAN_MODELS)
        self.assertIn("gpt-5.4-mini", logical_models)

    def test_live_nginx_check_defaults_to_configured_responses_model(self):
        self.assertIn('DEFAULT_MODEL = "gpt-5.6-terra"', self.live_test)

    def test_live_nginx_check_recognizes_data_only_completed_event(self):
        self.assertEqual(
            self.live_test_module.classify_sse_line(
                b'data: {"type":"response.completed","sequence_number":5}\n'
            ),
            ("response.completed", True, False),
        )

    def test_public_nginx_proxy_does_not_expose_metrics(self):
        self.assertIn("location = /metrics", self.nginx_config)
        self.assertIn("return 404", self.nginx_config)


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
