"""Tests for tenant retention policies and retention runs (store, CLI, HTTP)."""

import contextlib
import io
import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from obsd import AccessControl, AlertEngine, SeriesStore, create_server
from obsd.cli import main as cli_main
from obsd.tsdb import ObsError


class RetentionCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-retention-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))


class TestRetentionPolicy(RetentionCase):
    def test_unconfigured_tenant_has_null_policy_and_real_usage(self):
        store = self.store()
        self.assertEqual(store.get_retention("acme"),
                         {"tenant": "acme", "retention_ms": None,
                          "series": 0, "points": 0})
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        self.assertEqual(store.get_retention("acme"),
                         {"tenant": "acme", "retention_ms": None,
                          "series": 1, "points": 2})

    def test_set_returns_normalised_policy_and_usage_without_series(self):
        store = self.store()
        out = store.set_retention("acme", 5000)
        self.assertEqual(out, {"tenant": "acme", "retention_ms": 5000,
                               "series": 0, "points": 0})
        self.assertEqual(store.stats()["series"], 0)
        self.assertTrue(os.path.exists(os.path.join(store.root, "retention.json")))
        store.write("acme", "m", {}, [[1, 1.0]])
        self.assertEqual(store.get_retention("acme")["retention_ms"], 5000)
        # Null means no cleanup and is a real configured policy.
        out = store.set_retention("acme", None)
        self.assertEqual(out["retention_ms"], None)
        self.assertEqual(store.get_retention("acme"),
                         {"tenant": "acme", "retention_ms": None,
                          "series": 1, "points": 1})

    def test_invalid_policy_raises_and_leaves_config_untouched(self):
        store = self.store()
        store.set_retention("acme", 1000)
        for bad_tenant in ("", None, 7):
            with self.assertRaises(ObsError, msg=repr(bad_tenant)):
                store.set_retention(bad_tenant, 1000)
        for bad_ms in (True, False, "1000", 1.5, -1, -0.5, []):
            with self.assertRaises(ObsError, msg=repr(bad_ms)):
                store.set_retention("acme", bad_ms)
        with self.assertRaises(ObsError):
            store.get_retention("")
        self.assertEqual(store.get_retention("acme")["retention_ms"], 1000)
        self.assertEqual(store.get_retention("globex")["retention_ms"], None)

    def test_policy_survives_reopen_and_legacy_dir_has_none(self):
        first = self.store("restart")
        first.set_retention("acme", 60_000)
        first.set_retention("globex", None)
        first.write("acme", "m", {}, [[1, 1.0]])
        second = self.store("restart")
        self.assertEqual(second.get_retention("acme"),
                         {"tenant": "acme", "retention_ms": 60_000,
                          "series": 1, "points": 1})
        self.assertIsNone(second.get_retention("globex")["retention_ms"])
        legacy = self.store("legacy")
        legacy.write("acme", "m", {}, [[1, 1.0]])
        self.assertFalse(os.path.exists(os.path.join(legacy.root, "retention.json")))
        self.assertIsNone(self.store("legacy").get_retention("acme")["retention_ms"])


class TestRetentionRun(RetentionCase):
    def seed(self, store, tenant="acme"):
        store.write(tenant, "m", {"k": "a"}, [[1000, 1.0], [2000, 2.0], [3000, 3.0]])
        store.write(tenant, "m", {"k": "b"}, [[500, 5.0], [2500, 6.0]])

    def test_cutoff_is_exclusive_and_boundary_point_is_kept(self):
        store = self.store()
        self.seed(store)
        store.set_retention("acme", 500)
        result = store.run_retention(2500, tenant="acme")
        # cutoff = 2500 - 500 = 2000; only timestamps < 2000 are dropped.
        self.assertEqual(result, {"dry_run": False, "tenants": [
            {"tenant": "acme", "cutoff_ms": 2000, "dropped": 2,
             "affected_series": 2, "remaining_points": 3,
             "compacted_series": 2}]})
        rows = sorted(row["points"] for row in store.query("acme", "m"))
        self.assertEqual(rows, [[[2000, 2.0], [3000, 3.0]], [[2500, 6.0]]])
        # The run is idempotent: a second identical run drops nothing.
        again = store.run_retention(2500, tenant="acme")
        self.assertEqual(again["tenants"][0]["dropped"], 0)
        self.assertEqual(again["tenants"][0]["remaining_points"], 3)

    def test_run_requires_integer_now_and_boolean_dry_run(self):
        store = self.store()
        store.set_retention("acme", 100)
        for bad_now in (None, True, 1.5, "1000"):
            with self.assertRaises(ObsError, msg=repr(bad_now)):
                store.run_retention(bad_now, tenant="acme")
        for bad_dry in (1, "true", None):
            with self.assertRaises(ObsError, msg=repr(bad_dry)):
                store.run_retention(1000, tenant="acme", dry_run=bad_dry)
        with self.assertRaises(ObsError):
            store.run_retention(1000, tenant="")
        self.assertEqual(store.stats()["writes"], 0)

    def test_null_policy_reports_null_cutoff_and_zero_deletions(self):
        store = self.store()
        self.seed(store)
        store.set_retention("acme", None)
        result = store.run_retention(10_000, tenant="acme")
        self.assertEqual(result["tenants"], [
            {"tenant": "acme", "cutoff_ms": None, "dropped": 0,
             "affected_series": 0, "remaining_points": 5, "compacted_series": 0}])
        self.assertEqual(store.get_retention("acme")["points"], 5)
        # An explicit run for an unconfigured tenant behaves the same way.
        result = store.run_retention(10_000, tenant="globex")
        self.assertEqual(result["tenants"][0]["cutoff_ms"], None)
        self.assertEqual(result["tenants"][0]["dropped"], 0)

    def test_all_tenants_run_in_lexicographic_order(self):
        store = self.store()
        self.seed(store, "beta")
        self.seed(store, "alpha")
        self.seed(store, "unconfigured")
        store.set_retention("beta", 1000)
        store.set_retention("alpha", None)
        result = store.run_retention(3000)
        self.assertEqual([row["tenant"] for row in result["tenants"]],
                         ["alpha", "beta"])
        self.assertEqual(result["tenants"][0]["cutoff_ms"], None)
        self.assertEqual(result["tenants"][1]["cutoff_ms"], 2000)
        self.assertEqual(result["tenants"][1]["dropped"], 2)
        # A tenant without a configured policy is not part of the sweep.
        self.assertEqual(store.get_retention("unconfigured")["points"], 5)

    def test_dry_run_changes_nothing_but_reports_everything(self):
        store = self.store()
        self.seed(store)
        store.set_retention("acme", 500)

        def point_files():
            out = {}
            for name in os.listdir(store.points_dir):
                with open(os.path.join(store.points_dir, name),
                          encoding="utf-8") as handle:
                    out[name] = handle.read()
            return out

        before_files = point_files()
        result = store.run_retention(2500, tenant="acme", dry_run=True)
        self.assertEqual(result, {"dry_run": True, "tenants": [
            {"tenant": "acme", "cutoff_ms": 2000, "dropped": 2,
             "affected_series": 2, "remaining_points": 3,
             "compacted_series": 2}]})
        # Memory, files, quota usage and the write counter are untouched.
        self.assertEqual(store.get_retention("acme")["points"], 5)
        self.assertEqual(store.stats()["writes"], 2)
        self.assertEqual(before_files, point_files())

    def test_empty_series_stay_registered_and_free_quota_for_reuse(self):
        store = self.store()
        self.seed(store)
        # Limits may be lowered below current usage; the sweep frees them.
        store.set_quota("acme", 2, 2)
        store.set_retention("acme", 0)
        result = store.run_retention(10_000, tenant="acme")
        self.assertEqual(result["tenants"][0]["dropped"], 5)
        self.assertEqual(result["tenants"][0]["remaining_points"], 0)
        # Both series are still registered with zero points.
        usage = store.get_quota("acme")
        self.assertEqual((usage["series"], usage["points"]), (2, 0))
        rows = store.query("acme", "m")
        self.assertEqual(len(rows), 2)
        self.assertEqual([row["points"] for row in rows], [[], []])
        # The freed occupancy is immediately available for new writes.
        self.assertEqual(store.write("acme", "m", {"k": "a"},
                                     [[20_000, 1.0], [21_000, 2.0]])["written"], 2)
        with self.assertRaises(ObsError):
            store.write("acme", "m", {"k": "a"}, [[22_000, 3.0]])

    def test_run_compacts_physical_duplicate_timestamps(self):
        store = self.store("compact")
        sid = store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])["series_id"]
        # Simulate a legacy point file with duplicate timestamp lines.
        with open(os.path.join(store.points_dir, sid + ".jsonl"), "a",
                  encoding="utf-8") as handle:
            handle.write('{"t":2000,"v":9.0}\n{"t":3000,"v":3.0}\n')
        reopened = self.store("compact")
        reopened.set_retention("acme", 0)  # cutoff = now: drops everything < now
        result = reopened.run_retention(4000, tenant="acme")
        self.assertEqual(result["tenants"][0]["compacted_series"], 1)
        # The series stays registered; every point is below the cutoff.
        self.assertEqual([row["points"] for row in reopened.query("acme", "m")],
                         [[]])
        reopened2 = self.store("compact")
        self.assertEqual(reopened2.get_retention("acme")["points"], 0)
        # A duplicate-only file with nothing below the cutoff is compacted too.
        store2 = self.store("compact2")
        sid = store2.write("acme", "m", {}, [[1000, 1.0]])["series_id"]
        with open(os.path.join(store2.points_dir, sid + ".jsonl"), "a",
                  encoding="utf-8") as handle:
            handle.write('{"t":1000,"v":7.0}\n')
        reopened = self.store("compact2")
        reopened.set_retention("acme", 100)
        result = reopened.run_retention(500, tenant="acme")  # cutoff 400: no drops
        self.assertEqual(result["tenants"][0]["dropped"], 0)
        self.assertEqual(result["tenants"][0]["compacted_series"], 1)
        with open(os.path.join(reopened.points_dir, sid + ".jsonl"),
                  encoding="utf-8") as handle:
            lines = handle.read().splitlines()
        self.assertEqual(lines, ['{"t":1000,"v":7.0}'])
        self.assertEqual(reopened.query("acme", "m")[0]["points"], [[1000, 7.0]])

    def test_run_results_survive_reopen(self):
        first = self.store("restart")
        self.seed(first)
        first.set_retention("acme", 1500)
        first.run_retention(3000, tenant="acme")
        second = self.store("restart")
        self.assertEqual(second.get_retention("acme")["points"], 3)
        rows = sorted(row["points"] for row in second.query("acme", "m"))
        self.assertEqual(rows, [[[2000, 2.0], [3000, 3.0]], [[2500, 6.0]]])
        self.assertEqual(second.run_retention(3000, tenant="acme")
                         ["tenants"][0]["dropped"], 0)

    def test_concurrent_run_and_reads_show_whole_states_only(self):
        store = self.store()
        for index in range(6):
            store.write("acme", "m", {"k": str(index)},
                        [[t, float(t)] for t in range(0, 11_000, 1000)])
        store.set_retention("acme", 100)
        errors = []

        def reader():
            try:
                for _ in range(50):
                    rows = store.query("acme", "m")
                    counts = {len(row["points"]) for row in rows}
                    # Every series is seen either fully before or fully after
                    # the sweep, never with a partial mix of old timestamps.
                    for row in rows:
                        stamps = [p[0] for p in row["points"]]
                        self.assertEqual(stamps, sorted(set(stamps)))
                        self.assertTrue(all(t >= 9900 for t in stamps)
                                        or len(stamps) == 11)
                    self.assertTrue(counts <= {0, 1, 11})
                    usage = store.get_quota("acme")
                    self.assertIn(usage["points"], (66, 6))
            except (ObsError, AssertionError) as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for thread in threads:
            thread.start()
        store.run_retention(10_000, tenant="acme")
        for thread in threads:
            thread.join(timeout=15)
        self.assertEqual(errors, [])
        self.assertEqual(store.get_quota("acme")["points"], 6)


class TestRetentionCli(RetentionCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", os.path.join(self.tmp, "cli")] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_set_get_and_run_roundtrip(self):
        code, out, _ = self.run_cli("retention-set", "--tenant", "acme",
                                    "--retention-ms", "500")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), {"tenant": "acme", "retention_ms": 500,
                                           "series": 0, "points": 0})
        code, out, _ = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                    "--sample", "1000:1.0", "--sample", "2000:2.0")
        self.assertEqual(code, 0)
        code, out, _ = self.run_cli("retention-get", "--tenant", "acme")
        self.assertEqual(json.loads(out), {"tenant": "acme", "retention_ms": 500,
                                           "series": 1, "points": 2})
        code, out, _ = self.run_cli("retention-run", "--tenant", "acme",
                                    "--now-ms", "2500", "--dry-run")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), {"dry_run": True, "tenants": [
            {"tenant": "acme", "cutoff_ms": 2000, "dropped": 1,
             "affected_series": 1, "remaining_points": 1, "compacted_series": 1}]})
        code, out, _ = self.run_cli("retention-run", "--tenant", "acme",
                                    "--now-ms", "2500")
        self.assertEqual(json.loads(out)["tenants"][0]["dropped"], 1)
        code, out, _ = self.run_cli("retention-get", "--tenant", "acme")
        self.assertEqual(json.loads(out)["points"], 1)
        # Omitting --retention-ms configures a null (no-cleanup) policy.
        code, out, _ = self.run_cli("retention-set", "--tenant", "acme")
        self.assertEqual(json.loads(out)["retention_ms"], None)

    def test_invalid_arguments_are_json_errors(self):
        code, _, err = self.run_cli("retention-set", "--tenant", "acme",
                                    "--retention-ms", "-1")
        self.assertEqual(code, 1)
        self.assertIn("retention_ms", json.loads(err)["error"])
        code, _, err = self.run_cli("retention-run", "--tenant", "acme")
        self.assertEqual(code, 1)
        self.assertIn("error", json.loads(err.splitlines()[-1]))
        code, _, err = self.run_cli("retention-run", "--now-ms", "abc")
        self.assertEqual(code, 1)
        self.assertIn("error", json.loads(err.splitlines()[-1]))


class TestRetentionHttp(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-retention-http-")
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.access = AccessControl(root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0,
                                    access=self.access)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.tmp, ignore_errors=True)
    def request(self, method, path, payload=None, token=None):
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=body, method=method)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        if token is not None:
            req.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_policy_and_run_over_http(self):
        code, body = self.request("GET", "/v1/retention/policies?tenant=acme")
        self.assertEqual((code, body), (200, {"tenant": "acme", "retention_ms": None,
                                              "series": 0, "points": 0}))
        code, body = self.request("POST", "/v1/retention/policies",
                                  {"tenant": "acme", "retention_ms": 500})
        self.assertEqual((code, body["retention_ms"]), (200, 500))
        code, _ = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {},
            "samples": [[1000, 1.0], [2000, 2.0], [3000, 3.0]]})
        code, body = self.request("POST", "/v1/retention/run",
                                  {"now_ms": 2500, "tenant": "acme"})
        self.assertEqual(code, 200)
        self.assertEqual(body, {"dry_run": False, "tenants": [
            {"tenant": "acme", "cutoff_ms": 2000, "dropped": 1,
             "affected_series": 1, "remaining_points": 2, "compacted_series": 1}]})
        code, body = self.request("GET", "/v1/retention/policies?tenant=acme")
        self.assertEqual((body["retention_ms"], body["points"]), (500, 2))
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m")
        self.assertEqual(body["series"][0]["points"], [[2000, 2.0], [3000, 3.0]])

    def test_invalid_requests_are_400_and_leave_no_partial_change(self):
        self.request("POST", "/v1/retention/policies",
                     {"tenant": "acme", "retention_ms": 1000})
        for payload in ({"tenant": "acme", "retention_ms": -1},
                        {"tenant": "acme", "retention_ms": True},
                        {"tenant": "acme", "retention_ms": "1000"},
                        {"tenant": "acme", "retention_ms": 1.5},
                        {"tenant": "", "retention_ms": 1000},
                        {"retention_ms": 1000},
                        {"tenant": "acme", "retention_ms": 1000, "bogus": 1}):
            code, body = self.request("POST", "/v1/retention/policies", payload)
            self.assertEqual(code, 400, repr(payload))
            self.assertIn("error", body)
        code, body = self.request("GET", "/v1/retention/policies?tenant=acme")
        self.assertEqual(body["retention_ms"], 1000)
        for payload in ({}, {"tenant": "acme"}, {"now_ms": True},
                        {"now_ms": "2500"}, {"now_ms": 1.5},
                        {"now_ms": 2500, "dry_run": 1},
                        {"now_ms": 2500, "dry_run": "true"},
                        {"now_ms": 2500, "tenant": ""},
                        {"now_ms": 2500, "extra": 1}):
            code, body = self.request("POST", "/v1/retention/run", payload)
            self.assertEqual(code, 400, repr(payload))
            self.assertIn("error", body)
        code, body = self.request("GET", "/v1/retention/policies")
        self.assertEqual(code, 400)
        # The failed runs changed nothing.
        code, body = self.request("POST", "/v1/retention/run",
                                  {"now_ms": 2500, "dry_run": True})
        self.assertEqual((code, body["dry_run"]), (200, True))
        self.assertEqual(self.store.stats()["writes"], 0)

    def test_access_control_scopes_and_audit(self):
        self.access.create_principal("admin", "admin-tok", "admin", ["acme"])
        self.access.create_principal("writer", "write-tok", "writer", ["acme"])
        self.access.create_principal("viewer", "view-tok", "viewer", ["acme"])
        # A writer manages policies and runs retention inside its own tenant.
        code, body = self.request("POST", "/v1/retention/policies",
                                  {"tenant": "acme", "retention_ms": 100},
                                  token="write-tok")
        self.assertEqual(code, 200)
        code, _ = self.request("POST", "/v1/retention/run",
                               {"now_ms": 1000, "tenant": "acme"},
                               token="write-tok")
        self.assertEqual(code, 200)
        # A viewer may read the policy but not write it or run a sweep.
        code, _ = self.request("GET", "/v1/retention/policies?tenant=acme",
                               token="view-tok")
        self.assertEqual(code, 200)
        for method, path, payload in (
                ("POST", "/v1/retention/policies",
                 {"tenant": "acme", "retention_ms": 1}),
                ("POST", "/v1/retention/run", {"now_ms": 1, "tenant": "acme"})):
            code, body = self.request(method, path, payload, token="view-tok")
            self.assertEqual((code, body), (403, {"error": "forbidden"}))
        # Tenant scope is enforced for writes and reads alike.
        for method, path, payload in (
                ("POST", "/v1/retention/policies",
                 {"tenant": "globex", "retention_ms": 1}),
                ("POST", "/v1/retention/run", {"now_ms": 1, "tenant": "globex"}),
                ("GET", "/v1/retention/policies?tenant=globex", None)):
            code, _ = self.request(method, path, payload, token="write-tok")
            self.assertEqual(code, 403)
        # A sweep across every configured tenant is admin-only.
        code, _ = self.request("POST", "/v1/retention/run", {"now_ms": 1000},
                               token="write-tok")
        self.assertEqual(code, 403)
        code, body = self.request("POST", "/v1/retention/run", {"now_ms": 1000},
                                  token="admin-tok")
        self.assertEqual(code, 200)
        self.assertEqual([row["tenant"] for row in body["tenants"]], ["acme"])
        # Every request above was audited with its tenant where there is one.
        entries = self.access.query_audit()
        self.assertTrue(entries)
        run_entries = [row for row in entries
                       if row["path"] == "/v1/retention/run"]
        self.assertTrue(any(row["tenant"] == "acme" for row in run_entries))
        self.assertIsNone(run_entries[-1]["tenant"])
        self.assertTrue(all(row["outcome"] in ("allowed", "denied")
                            for row in entries))


if __name__ == "__main__":
    unittest.main()
