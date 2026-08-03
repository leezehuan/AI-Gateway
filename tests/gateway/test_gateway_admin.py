#!/usr/bin/env python3
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class GatewayAdminIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = sys.argv[1]
        cls.repo = sys.argv[2]
        cls.temp = tempfile.TemporaryDirectory(prefix="aigw-admin-")
        cls.datadir = os.path.join(cls.temp.name, "data")
        cls.socket = os.path.join(cls.temp.name, "mariadb.sock")
        cls.port = free_port()
        subprocess.run(
            ["mariadb-install-db", "--no-defaults", f"--datadir={cls.datadir}",
             "--auth-root-authentication-method=normal", "--skip-test-db"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        cls.database = subprocess.Popen(
            ["mariadbd", "--no-defaults", f"--datadir={cls.datadir}",
             f"--socket={cls.socket}", f"--port={cls.port}", "--bind-address=127.0.0.1",
             "--skip-name-resolve", f"--pid-file={cls.temp.name}/mariadb.pid",
             f"--log-error={cls.temp.name}/mariadb.log"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            probe = subprocess.run(
                ["mariadb", "--no-defaults", f"--socket={cls.socket}", "-u", "root", "-e", "SELECT 1"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if probe.returncode == 0:
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("temporary MariaDB did not start")
        subprocess.run(
            ["mariadb", "--no-defaults", f"--socket={cls.socket}", "-u", "root", "-e",
             "CREATE DATABASE ai_gateway; CREATE USER 'gateway'@'127.0.0.1' IDENTIFIED BY 'test-db-password'; "
             "GRANT ALL ON ai_gateway.* TO 'gateway'@'127.0.0.1'; FLUSH PRIVILEGES;"],
            check=True,
        )
        cls.env = os.environ.copy()
        cls.env.update({
            "AI_GATEWAY_DB_HOST": "127.0.0.1",
            "AI_GATEWAY_DB_PORT": str(cls.port),
            "AI_GATEWAY_DB_USER": "gateway",
            "AI_GATEWAY_DB_PASSWORD": "test-db-password",
            "AI_GATEWAY_DB_NAME": "ai_gateway",
            "AI_GATEWAY_API_KEY_HMAC_PEPPER": "phase3-test-pepper-that-is-at-least-32-bytes",
            "TEST_PROVIDER_SECRET": "provider-secret-never-store",
        })

    @classmethod
    def tearDownClass(cls):
        cls.database.terminate()
        cls.database.wait(timeout=5)
        cls.temp.cleanup()

    def run_admin(self, *arguments, check=True):
        result = subprocess.run(
            [self.admin, *arguments], env=self.env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if check and result.returncode != 0:
            self.fail(f"AiGatewayAdmin {' '.join(arguments)} failed: {result.stderr}")
        return result

    def query(self, sql):
        return subprocess.run(
            ["mariadb", "--no-defaults", "-h", "127.0.0.1", "-P", str(self.port),
             "-u", "gateway", "-ptest-db-password", "-N", "ai_gateway", "-e", sql],
            check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ).stdout.strip()

    def test_migrate_apply_config_and_issue_hmac_key(self):
        migration_dir = os.path.join(self.repo, "migrations", "gateway")
        self.run_admin("migrate", "--dir", migration_dir)
        self.run_admin("migrate", "--dir", migration_dir)

        config = {
            "tenants": [{"slug": "tenant-a", "name": "Tenant A", "status": "active"}],
            "providers": [{
                "tenant": "tenant-a", "slug": "provider-a", "name": "Provider A",
                "status": "active",
                "endpoints": [{"name": "responses", "protocol": "responses",
                               "url": "http://127.0.0.1:9999/v1/responses", "status": "active"}],
                "credentials": [{"name": "default", "secret_ref": "env:TEST_PROVIDER_SECRET",
                                 "status": "active"}],
            }],
            "logical_models": [{"tenant": "tenant-a", "protocol": "responses",
                                "name": "shared-model", "status": "active",
                                "routing": {"mode": "load_balance", "max_attempts": 5}}],
            "policies": [{"tenant": "tenant-a", "slug": "default", "name": "Default",
                          "status": "active", "protocols": ["responses"],
                          "models": ["shared-model"], "providers": ["provider-a"]}],
            "mappings": [{"tenant": "tenant-a", "logical_model": "shared-model",
                          "protocol": "responses", "name": "primary", "provider": "provider-a",
                          "endpoint": "responses", "credential": "default",
                          "upstream_model": "provider-model-a", "priority": 7,
                          "status": "active"}],
        }
        path = os.path.join(self.temp.name, "config.json")
        with open(path, "w", encoding="utf-8") as output:
            json.dump(config, output)
        self.run_admin("apply-config", "--file", path)
        self.assertEqual(
            self.query(
                "SELECT rp.scheduling_mode, rp.max_attempts, mm.priority "
                "FROM route_policies rp JOIN logical_models lm ON lm.id=rp.logical_model_id "
                "JOIN model_mappings mm ON mm.logical_model_id=lm.id "
                "WHERE lm.name='shared-model'"
            ),
            "load_balance\t5\t7",
        )
        self.assertEqual(
            self.query(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema='ai_gateway' AND table_name='request_attempts'"
            ),
            "1",
        )
        applied_version = int(self.query(
            "SELECT version FROM gateway_config_versions WHERE singleton_id=1"
        ))
        self.run_admin("apply-config", "--file", path)
        self.assertEqual(
            int(self.query("SELECT version FROM gateway_config_versions WHERE singleton_id=1")),
            applied_version,
        )
        self.run_admin("bump-version")
        self.assertEqual(
            int(self.query("SELECT version FROM gateway_config_versions WHERE singleton_id=1")),
            applied_version + 1,
        )
        issued = self.run_admin("issue-key", "--tenant", "tenant-a", "--policy", "default",
                                "--name", "test key").stdout.strip()
        self.assertRegex(issued, r"^aigw_[A-Za-z0-9_-]{12}_[A-Za-z0-9_-]{43}$")

        dump = subprocess.run(
            ["mariadb", "--no-defaults", "-h", "127.0.0.1", "-P", str(self.port),
             "-u", "gateway", "-ptest-db-password", "-N", "ai_gateway", "-e",
             "SELECT CONCAT(key_id, ':', display_prefix, ':', HEX(key_hmac)) FROM api_keys"],
            check=True, text=True, stdout=subprocess.PIPE,
        ).stdout
        self.assertNotIn(issued, dump)
        self.assertNotIn("provider-secret-never-store", dump)
        self.assertTrue(re.search(r"key_[0-9a-f]{24}:[A-Za-z0-9_-]{12}:[0-9A-F]{64}", dump))

        conflict_dir = os.path.join(self.temp.name, "conflicting-migrations")
        shutil.copytree(migration_dir, conflict_dir)
        migration = os.path.join(conflict_dir, "0001_phase3_identity_policy.sql")
        with open(migration, "a", encoding="utf-8") as output:
            output.write("\n-- checksum conflict fixture\n")
        conflict = self.run_admin("migrate", "--dir", conflict_dir, check=False)
        self.assertNotEqual(conflict.returncode, 0)
        self.assertIn("checksum mismatch", conflict.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: test_gateway_admin.py /path/to/AiGatewayAdmin /path/to/repo")
    unittest.main(argv=[sys.argv[0]])
