"""Tests for tenant retention policies and retention runs (store, CLI, HTTP)."""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

from obsd import AccessControl, AlertEngine, ObsError, SeriesStore, create_server
from obsd.cli import main as cli_main


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
        store.write("acme", "m", {"k": "v"}, [[1000, 1.0], [2000, 2.0]])
        self.assertEqual(store.get_retention("acme"),
                         {"tenant": "acme", "retention_ms": None,
                          "series": 1, "points": 2})

    def test_set_returns_normalized_policy_and_usage_without_series(self):
        store = self.store()
        out = store.set_retention("acme", 5000)
        self.assertEqual(out, {"tenant": "acme", "retention_ms": 5000,
                               "series": 0, "points": 0})
        self.assertEqual(store.stats()["series"], 0)
        self.assertTrue(os.path.exists(store.retention_path))
        # Zero is a real policy (drop everything older than now_ms).
        self.assertEqual(store.set_retention("acme", 0)["retention_ms"], 0)
        # Null means configured but never cleaned.
        self.assertEqual(store.set_retention("acme", None)["retention_ms"], None)

    def test_invalid_policy_raises_and_leaves_config_untouched(self):
        store = self.store()
        store.set_retention("acme", 1000)
        for bad_tenant in ("", None, 7):
            with self.assertRaises(ObsError, msg=repr(bad_tenant)):
                store.set_retention(bad_tenant, 1000)
        for bad_retention in (True, False, "1000", 1.5, -1, -0.1, []):
            with self.assertRaises(ObsError, msg=repr(bad_retention)):
                store.set_retention("acme", bad_retention)
        with self.assertRaises(ObsError):
            store.get_retention("")
        self.assertEqual(store.get_retention("acme")["retention_ms"], 1000)
        reopened = self.store()
        self.assertEqual(reopened.get_retention("acme")["retention_ms"], 1000)

    def test_policy_and_cleanup_survive_reopen(self):
        first = self.store("restart")
        first.set_retention("acme", 2000)
        first.write("acme", "m", {}, [[1000, 1.0], [3000, 3.0]])
        result = first.run_retention(4000)
        self.assertEqual(result["tenants"][0]["dropped"], 1)
        second = self.store("restart")
        self.assertEqual(second.get_retention("acme"),
                         {"tenant": "acme", "retention_ms": 2000,
                          "series": 1, "points": 1})
        self.assertEqual(second.query("acme", "m")[0]["points"], [[3000, 3.0]])

    def test_legacy_directory_without_retention_file_has_no_policies(self):
        first = self.store("legacy")
        first.write("acme", "m", {}, [[1, 1.0]])
        self.assertFalse(os.path.exists(first.retention_path))
        reopened = self.store("legacy")
        self.assertIsNone(reopened.get_retention("acme")["retention_ms"])
        self.assertEqual(reopened.run_retention(10_000),
                         {"dry_run": False, "tenants": []})


class TestRetentionRun(RetentionCase):
    def test_cutoff_is_strict_and_boundary_point_is_kept(self):
        store = self.store()
        store.set_retention("acme", 2000)
        store.write("acme", "m", {}, [[999, 1.0], [1000, 2.0], [1001, 3.0]])
        result = store.run_retention(3000)  # cutoff = 1000
        self.assertEqual(result, {"dry_run": False, "tenants": [
            {"tenant": "acme", "cutoff_ms": 1000, "dropped": 1,
             "affected_series": 1, "remaining_points": 2,
             "compacted_series": 1}]})
        self.assertEqual(store.query("acme", "m")[0]["points"],
                         [[1000, 2.0], [1001, 3.0]])

    def test_all_configured_tenants_run_in_lexicographic_order(self):
        store = self.store()
        store.set_retention("globex", 100)
        store.set_retention("acme", 100)
        store.set_retention("mid", None)
        for tenant in ("acme", "globex", "mid"):
            store.write(tenant, "m", {}, [[1, 1.0], [1000, 2.0]])
        result = store.run_retention(500)
        self.assertEqual([row["tenant"] for row in result["tenants"]],
                         ["acme", "globex", "mid"])
        self.assertEqual(result["tenants"][0]["cutoff_ms"], 400)
        self.assertEqual(result["tenants"][0]["dropped"], 1)
        self.assertEqual(result["tenants"][1]["dropped"], 1)
        # Null policy: null cutoff, zero deletions, real remaining points.
        self.assertEqual(result["tenants"][2],
                         {"tenant": "mid", "cutoff_ms": None, "dropped": 0,
                          "affected_series": 0, "remaining_points": 2,
                          "compacted_series": 0})
        self.assertEqual(len(store.query("mid", "m")[0]["points"]), 2)

    def test_explicit_tenant_limits_the_run(self):
        store = self.store()
        store.set_retention("acme", 100)
        store.set_retention("globex", 100)
        for tenant in ("acme", "globex"):
            store.write(tenant, "m", {}, [[1, 1.0]])
        result = store.run_retention(500, tenant="globex")
        self.assertEqual([row["tenant"] for row in result["tenants"]], ["globex"])
        self.assertEqual(len(store.query("acme", "m")[0]["points"]), 1)
        self.assertEqual(store.query("globex", "m")[0]["points"], [])
        # An explicit tenant without a policy is a null-policy run.
        result = store.run_retention(500, tenant="acme")
        self.assertEqual(result["tenants"][0]["dropped"], 1)
        store.set_retention("other", None)
        result = store.run_retention(500, tenant="unconfigured")
        self.assertEqual(result["tenants"][0],
                         {"tenant": "unconfigured", "cutoff_ms": None,
                          "dropped": 0, "affected_series": 0,
                          "remaining_points": 0, "compacted_series": 0})

    def test_empty_series_stays_registered_and_quota_is_freed(self):
        store = self.store()
        store.set_quota("acme", 1, 2)
        store.set_retention("acme", 1000)
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        with self.assertRaises(ObsError):
            store.write("acme", "m", {}, [[3000, 3.0]])
        result = store.run_retention(4000)  # cutoff 3000 drops both points
        self.assertEqual(result["tenants"][0]["remaining_points"], 0)
        usage = store.get_quota("acme")
        self.assertEqual((usage["series"], usage["points"]), (1, 0))
        # The freed occupancy is usable immediately.
        self.assertEqual(store.write("acme", "m", {}, [[5000, 5.0]])["written"], 1)
        # The series quota still counts the empty-then-refilled series.
        with self.assertRaises(ObsError):
            store.write("acme", "m", {"k": "other"}, [[1, 1.0]])

    def test_run_does_not_move_write_counter_or_stats_shape(self):
        store = self.store()
        store.set_retention("acme", 100)
        store.write("acme", "m", {}, [[1, 1.0], [1000, 2.0]])
        before = store.stats()["writes"]
        store.run_retention(500)
        self.assertEqual(store.stats()["writes"], before)
        self.assertEqual(store.stats()["points"], 1)

    def test_dry_run_changes_nothing_but_reports_the_plan(self):
        store = self.store()
        store.set_quota("acme", None, 10)
        store.set_retention("acme", 100)
        store.write("acme", "m", {}, [[1, 1.0], [1000, 2.0]])
        sid = store.query("acme", "m")[0]["series_id"]
        path = os.path.join(store.points_dir, sid + ".jsonl")
        with open(path, "r", encoding="utf-8") as handle:
            physical_before = handle.read()
        result = store.run_retention(500, dry_run=True)
        self.assertEqual(result, {"dry_run": True, "tenants": [
            {"tenant": "acme", "cutoff_ms": 400, "dropped": 1,
             "affected_series": 1, "remaining_points": 1,
             "compacted_series": 1}]})
        # Memory, files, quota usage and the write counter are untouched.
        self.assertEqual(len(store.query("acme", "m")[0]["points"]), 2)
        self.assertEqual(store.get_quota("acme")["points"], 2)
        self.assertEqual(store.stats()["writes"], 1)
        with open(path, "r", encoding="utf-8") as handle:
            self.assertEqual(handle.read(), physical_before)
        # A real run afterwards applies exactly the previewed plan.
        real = store.run_retention(500)
        self.assertEqual(real["tenants"], result["tenants"])
        self.assertFalse(real["dry_run"])
        self.assertEqual(store.get_quota("acme")["points"], 1)

    def test_compaction_rewrites_physical_records_with_final_values(self):
        store = self.store("compact")
        store.set_retention("acme", 100)
        sid = store.write("acme", "m", {}, [[1, 1.0], [500, 5.0]])["series_id"]
        path = os.path.join(store.points_dir, sid + ".jsonl")
        # Legacy duplicate timestamp lines: the last value wins on reload.
        with open(path, "a", encoding="utf-8") as handle:
            handle.write('{"t":500,"v":7.0}\n{"t":500,"v":9.0}\n')
        store = self.store("compact")
        self.assertEqual(store.query("acme", "m")[0]["points"],
                         [[1, 1.0], [500, 9.0]])
        result = store.run_retention(400, tenant="acme")
        self.assertEqual(result["tenants"][0]["compacted_series"], 1)
        with open(path, "r", encoding="utf-8") as handle:
            lines = [json.loads(line) for line in handle if line.strip()]
        self.assertEqual(lines, [{"t": 500, "v": 9.0}])
        # A run that drops nothing rewrites nothing.
        with open(path, "r", encoding="utf-8") as handle:
            before = handle.read()
        result = store.run_retention(400, tenant="acme")
        self.assertEqual(result["tenants"][0]["compacted_series"], 0)
        with open(path, "r", encoding="utf-8") as handle:
            self.assertEqual(handle.read(), before)

    def test_invalid_run_arguments_raise_and_change_nothing(self):
        store = self.store()
        store.set_retention("acme", 100)
        store.write("acme", "m", {}, [[1, 1.0]])
        for bad_now in (None, True, False, "100", 1.5, []):
            with self.assertRaises(ObsError, msg=repr(bad_now)):
                store.run_retention(bad_now)
        for bad_dry in (None, 0, 1, "true"):
            with self.assertRaises(ObsError, msg=repr(bad_dry)):
                store.run_retention(500, dry_run=bad_dry)
        for bad_tenant in ("", 7, []):
            with self.assertRaises(ObsError, msg=repr(bad_tenant)):
                store.run_retention(500, tenant=bad_tenant)
        self.assertEqual(len(store.query("acme", "m")[0]["points"]), 1)
        self.assertEqual(store.stats()["writes"], 1)

    def test_concurrent_run_query_and_stats_stay_consistent(self):
        store = self.store()
        store.set_retention("acme", 5000)
        for worker in range(8):
            store.write("acme", "m", {"w": str(worker)},
                        [[seq * 1000, float(seq)] for seq in range(40)])
        errors, snapshots = [], []

        def prune():
            try:
                store.run_retention(20_000)  # cutoff 15000 keeps 25 of 40
            except ObsError as exc:  # pragma: no cover - defensive
                errors.append(exc)

        def read():
            try:
                for _ in range(20):
                    rows = store.query("acme", "m")
                    snapshots.append(sum(len(row["points"]) for row in rows))
                    store.get_quota("acme")
                    store.stats()
            except ObsError as exc:  # pragma: no cover - defensive
                errors.append(exc)

        threads = [threading.Thread(target=prune)]
        threads += [threading.Thread(target=read) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        self.assertEqual(errors, [])
        # Every concurrent read saw a coherent state: never a point count
        # between the pre-run and post-run totals other than the two ends.
        self.assertTrue(set(snapshots) <= {8 * 40, 8 * 25})
        self.assertEqual(store.stats()["points"], 8 * 25)
        recomputed = sum(
            len(store._samples[sid]) for sid, row in store._series.items()
            if row["tenant"] == "acme")
        self.assertEqual(store.get_quota("acme")["points"], recomputed)


class TestRetentionCli(RetentionCase):
    def run_cli(self, *argv):
        out, err = StringIO(), StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = cli_main(["--data-dir", os.path.join(self.tmp, "cli")] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_set_get_and_run_roundtrip(self):
        code, out, err = self.run_cli("retention-set", "--tenant", "acme",
                                      "--retention-ms", "1000")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), {"tenant": "acme", "retention_ms": 1000,
                                           "series": 0, "points": 0})
        code, out, err = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                      "--sample", "1000:1.0", "--sample", "5000:2.0")
        self.assertEqual(code, 0)
        code, out, err = self.run_cli("retention-get", "--tenant", "acme")
        self.assertEqual(json.loads(out), {"tenant": "acme", "retention_ms": 1000,
                                           "series": 1, "points": 2})
        code, out, err = self.run_cli("retention-run", "--now-ms", "4000",
                                      "--dry-run")
        self.assertEqual(code, 0)
        planned = json.loads(out)
        self.assertTrue(planned["dry_run"])
        self.assertEqual(planned["tenants"][0]["dropped"], 1)
        code, out, err = self.run_cli("retention-get", "--tenant", "acme")
        self.assertEqual(json.loads(out)["points"], 2)  # dry run changed nothing
        code, out, err = self.run_cli("retention-run", "--now-ms", "4000")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["tenants"][0]["cutoff_ms"], 3000)
        code, out, err = self.run_cli("retention-get", "--tenant", "acme")
        self.assertEqual(json.loads(out)["points"], 1)
        code, out, err = self.run_cli("query", "--tenant", "acme", "--metric", "m")
        self.assertEqual(json.loads(out)["series"][0]["points"], [[5000, 2.0]])

    def test_omitted_retention_ms_means_no_cleanup(self):
        code, out, err = self.run_cli("retention-set", "--tenant", "acme")
        self.assertEqual(code, 0)
        self.assertIsNone(json.loads(out)["retention_ms"])
        code, out, err = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                      "--sample", "1:1.0")
        self.assertEqual(code, 0)
        code, out, err = self.run_cli("retention-run", "--now-ms", "10_000"
                                      .replace("_", ""))
        self.assertEqual(code, 0)
        row = json.loads(out)["tenants"][0]
        self.assertEqual((row["cutoff_ms"], row["dropped"]), (None, 0))

    def test_invalid_arguments_are_json_errors(self):
        code, out, err = self.run_cli("retention-set", "--tenant", "acme",
                                      "--retention-ms", "-1")
        self.assertNotEqual(code, 0)
        self.assertIn("retention_ms", json.loads(err)["error"])
        code, out, err = self.run_cli("retention-run", "--now-ms", "100",
                                      "--tenant", "")
        self.assertNotEqual(code, 0)
        self.assertIn("error", json.loads(err))
        code, out, err = self.run_cli("retention-run")
        self.assertNotEqual(code, 0)
        # argparse prints its usage first; the final stderr line is the JSON error.
        self.assertIn("error", json.loads(err.strip().splitlines()[-1]))


class TestRetentionHttp(RetentionCase):
    def setUp(self):
        super().setUp()
        root = os.path.join(self.tmp, "http")
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
        super().tearDown()
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
    def write(self, samples, tenant="acme"):
        code, body = self.request("POST", "/v1/series", {
            "tenant": tenant, "metric": "m", "labels": {}, "samples": samples})
        self.assertEqual(code, 202)
        return body

    def test_policy_set_get_and_run_over_http(self):
        code, body = self.request("GET", "/v1/retention/policies?tenant=acme")
        self.assertEqual((code, body), (200, {"tenant": "acme",
                                              "retention_ms": None,
                                              "series": 0, "points": 0}))
        code, body = self.request("POST", "/v1/retention/policies",
                                  {"tenant": "acme", "retention_ms": 1000})
        self.assertEqual((code, body), (200, {"tenant": "acme",
                                              "retention_ms": 1000,
                                              "series": 0, "points": 0}))
        self.write([[1000, 1.0], [5000, 2.0]])
        code, body = self.request("POST", "/v1/retention/run",
                                  {"now_ms": 4000, "tenant": "acme",
                                   "dry_run": True})
        self.assertEqual(code, 200)
        self.assertEqual(body, {"dry_run": True, "tenants": [
            {"tenant": "acme", "cutoff_ms": 3000, "dropped": 1,
             "affected_series": 1, "remaining_points": 1,
             "compacted_series": 1}]})
        code, body = self.request("GET", "/v1/retention/policies?tenant=acme")
        self.assertEqual(body["points"], 2)  # dry run changed nothing
        code, body = self.request("POST", "/v1/retention/run", {"now_ms": 4000})
        self.assertEqual(code, 200)
        self.assertFalse(body["dry_run"])
        self.assertEqual(body["tenants"][0]["dropped"], 1)
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m")
        self.assertEqual(body["series"][0]["points"], [[5000, 2.0]])

    def test_invalid_requests_are_400_and_leave_no_partial_change(self):
        self.request("POST", "/v1/retention/policies",
                     {"tenant": "acme", "retention_ms": 1000})
        self.write([[1000, 1.0]])
        for payload in ({"tenant": "acme", "retention_ms": -1},
                        {"tenant": "acme", "retention_ms": True},
                        {"tenant": "acme", "retention_ms": "1000"},
                        {"tenant": "acme", "retention_ms": 1.5},
                        {"tenant": "", "retention_ms": 1000},
                        {"retention_ms": 1000},
                        {"tenant": "acme", "retention_ms": 1000, "extra": 1}):
            code, body = self.request("POST", "/v1/retention/policies", payload)
            self.assertEqual(code, 400, repr(payload))
            self.assertIn("error", body)
        for payload in ({}, {"now_ms": None}, {"now_ms": True},
                        {"now_ms": "1000"}, {"now_ms": 1.5},
                        {"now_ms": 4000, "dry_run": 1},
                        {"now_ms": 4000, "dry_run": "true"},
                        {"now_ms": 4000, "tenant": ""},
                        {"now_ms": 4000, "bogus": 1}):
            code, body = self.request("POST", "/v1/retention/run", payload)
            self.assertEqual(code, 400, repr(payload))
            self.assertIn("error", body)
        code, body = self.request("GET", "/v1/retention/policies")
        self.assertEqual(code, 400)
        # No partial modification: policy and points are exactly as before.
        code, body = self.request("GET", "/v1/retention/policies?tenant=acme")
        self.assertEqual(body, {"tenant": "acme", "retention_ms": 1000,
                                "series": 1, "points": 1})

    def test_access_control_scopes_and_audit(self):
        self.access.create_principal("root", "admin-tok", "admin", ["acme"])
        self.access.create_principal("writer", "write-tok", "writer", ["acme"])
        self.access.create_principal("reader", "view-tok", "viewer", ["acme"])
        self.access.create_principal("outsider", "out-tok", "writer", ["globex"])
        # Policy writes are tenant-scoped writes.
        code, _ = self.request("POST", "/v1/retention/policies",
                               {"tenant": "acme", "retention_ms": 1000},
                               token="write-tok")
        self.assertEqual(code, 200)
        code, _ = self.request("POST", "/v1/retention/policies",
                               {"tenant": "acme", "retention_ms": 1000},
                               token="view-tok")
        self.assertEqual(code, 403)
        code, _ = self.request("POST", "/v1/retention/policies",
                               {"tenant": "acme", "retention_ms": 1000},
                               token="out-tok")
        self.assertEqual(code, 403)
        # Policy reads are tenant-scoped reads.
        code, _ = self.request("GET", "/v1/retention/policies?tenant=acme",
                               token="view-tok")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/retention/policies?tenant=acme",
                               token="out-tok")
        self.assertEqual(code, 403)
        # A tenant-scoped run is a write; a cross-tenant run is admin-only.
        code, _ = self.request("POST", "/v1/retention/run",
                               {"now_ms": 1000, "tenant": "acme"},
                               token="write-tok")
        self.assertEqual(code, 200)
        code, _ = self.request("POST", "/v1/retention/run",
                               {"now_ms": 1000, "tenant": "acme"},
                               token="out-tok")
        self.assertEqual(code, 403)
        code, _ = self.request("POST", "/v1/retention/run", {"now_ms": 1000},
                               token="write-tok")
        self.assertEqual(code, 403)
        code, body = self.request("POST", "/v1/retention/run", {"now_ms": 1000},
                                  token="admin-tok")
        self.assertEqual(code, 200)
        self.assertEqual([row["tenant"] for row in body["tenants"]], ["acme"])
        # Every request above was audited.
        entries = self.access.query_audit()
        self.assertEqual(len(entries), 9)
        self.assertEqual([row["outcome"] for row in entries],
                         ["allowed", "denied", "denied", "allowed", "denied",
                          "allowed", "denied", "denied", "allowed"])
        scoped = [row for row in entries if row["path"] == "/v1/retention/run"]
        self.assertEqual([row["tenant"] for row in scoped],
                         ["acme", "acme", None, None])


if __name__ == "__main__":
    unittest.main()
