"""Tests for persistent downsampling: policies, runs, downsampled queries."""

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


class DownsamplingCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-downsampling-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))


class TestPolicy(DownsamplingCase):
    def test_unconfigured_policy_reads_back_null_fields(self):
        store = self.store()
        self.assertEqual(store.get_downsampling_policy("acme", "m"),
                         {"tenant": "acme", "metric": "m", "step_ms": None,
                          "aggregations": None})

    def test_set_and_get_roundtrip(self):
        store = self.store()
        out = store.set_downsampling_policy("acme", "m", 1000, ["sum", "avg"])
        self.assertEqual(out, {"tenant": "acme", "metric": "m", "step_ms": 1000,
                               "aggregations": ["sum", "avg"]})
        self.assertEqual(store.get_downsampling_policy("acme", "m"), out)
        # Setting a policy never creates a series nor moves the revision.
        self.assertEqual(store.stats()["series"], 0)
        self.assertEqual(store.consistency_token("acme")["revision"], 0)

    def test_invalid_policy_raises_and_leaves_config_untouched(self):
        store = self.store()
        store.set_downsampling_policy("acme", "m", 1000, ["sum"])
        bad = [("acme", "m", 0, ["sum"]), ("acme", "m", -5, ["sum"]),
               ("acme", "m", True, ["sum"]), ("acme", "m", 1.5, ["sum"]),
               ("acme", "m", "1000", ["sum"]), ("acme", "m", None, ["sum"]),
               ("acme", "m", 1000, []), ("acme", "m", 1000, None),
               ("acme", "m", 1000, "sum"), ("acme", "m", 1000, ["bogus"]),
               ("acme", "m", 1000, ["sum", "sum"]),
               ("acme", "m", 1000, ["sum", None]),
               ("", "m", 1000, ["sum"]), (None, "m", 1000, ["sum"]),
               ("acme", "", 1000, ["sum"]), ("acme", None, 1000, ["sum"])]
        for tenant, metric, step, aggs in bad:
            with self.assertRaises(ObsError, msg=repr((tenant, metric, step, aggs))) as ctx:
                store.set_downsampling_policy(tenant, metric, step, aggs)
            self.assertEqual(str(ctx.exception), "downsampling policy invalid")
        self.assertEqual(store.get_downsampling_policy("acme", "m")["step_ms"], 1000)

    def test_policy_survives_reopen(self):
        first = self.store("restart")
        first.set_downsampling_policy("acme", "m", 500, ["min", "max", "count"])
        second = self.store("restart")
        self.assertEqual(second.get_downsampling_policy("acme", "m"),
                         {"tenant": "acme", "metric": "m", "step_ms": 500,
                          "aggregations": ["min", "max", "count"]})

    def test_replace_keeps_raw_samples_but_clears_results(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0], [1000, 3.0]])
        store.set_downsampling_policy("acme", "m", 1000, ["sum"])
        store.run_downsampling(2000)
        self.assertEqual(store.query_downsampled("acme", "m", agg="sum")[0]["points"],
                         [[0, 1.0], [1000, 3.0]])
        store.set_downsampling_policy("acme", "m", 1000, ["sum"])
        # Raw samples are untouched; the computed results are gone.
        self.assertEqual(store.query("acme", "m")[0]["points"],
                         [[0, 1.0], [1000, 3.0]])
        self.assertEqual(store.query_downsampled("acme", "m", agg="sum")[0]["points"],
                         [])
        self.assertFalse(os.path.exists(
            os.path.join(store.downsampled_dir, os.listdir(store.points_dir)[0]
                         .replace(".jsonl", ".json"))))


class TestRun(DownsamplingCase):
    def test_invalid_arguments_raise_run_invalid(self):
        store = self.store()
        store.set_downsampling_policy("acme", "m", 1000, ["sum"])
        for bad_now in (None, True, False, "1000", 1.5, []):
            with self.assertRaises(ObsError, msg=repr(bad_now)) as ctx:
                store.run_downsampling(bad_now)
            self.assertEqual(str(ctx.exception), "downsampling run invalid")
        for kwargs in ({"tenant": ""}, {"tenant": 7}, {"metric": ""},
                       {"metric": 7}, {"dry_run": None}, {"dry_run": 1},
                       {"return_revision": None}, {"return_revision": "x"}):
            with self.assertRaises(ObsError, msg=repr(kwargs)) as ctx:
                store.run_downsampling(1000, **kwargs)
            self.assertEqual(str(ctx.exception), "downsampling run invalid")

    def test_run_computes_epoch_buckets_for_declared_aggs(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"},
                    [[0, 1.0], [100, 2.0], [1000, 4.0], [2500, 8.0]])
        store.set_downsampling_policy("acme", "m", 1000,
                                      ["sum", "avg", "min", "max", "count"])
        out = store.run_downsampling(2000)
        self.assertEqual(out["dry_run"], False)
        self.assertEqual(out["policies"],
                         [{"tenant": "acme", "metric": "m", "buckets": 2,
                           "changed": 2}])
        rows = {row["labels"]["host"]: row["points"]
                for row in [store.query_downsampled("acme", "m", agg=agg)[0]
                            for agg in ("sum", "avg", "min", "max", "count")]}
        self.assertEqual(rows["a"], [[0, 2], [1000, 1]])
        # The sample at 2500 is after now_ms and not processed yet.
        self.assertEqual(store.query_downsampled("acme", "m", agg="max")[0]["points"],
                         [[0, 2.0], [1000, 4.0]])
        later = store.run_downsampling(3000)
        self.assertEqual(later["policies"][0]["buckets"], 3)
        self.assertEqual(later["policies"][0]["changed"], 1)
        self.assertEqual(store.query_downsampled("acme", "m", agg="max")[0]["points"],
                         [[0, 2.0], [1000, 4.0], [2000, 8.0]])

    def test_rerun_recomputes_affected_buckets_after_overwrite(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0]])
        store.set_downsampling_policy("acme", "m", 1000, ["sum"])
        store.run_downsampling(1000)
        store.write("acme", "m", {}, [[0, 5.0]], overwrite=True)
        out = store.run_downsampling(1000)
        self.assertEqual(out["policies"][0]["changed"], 1)
        self.assertEqual(store.query_downsampled("acme", "m", agg="sum")[0]["points"],
                         [[0, 5.0]])

    def test_repeat_run_changes_nothing_and_keeps_revision(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0], [1000, 2.0]])
        store.set_downsampling_policy("acme", "m", 1000, ["sum"])
        first = store.run_downsampling(2000, return_revision=True)
        second = store.run_downsampling(2000, return_revision=True)
        self.assertEqual(second["policies"][0]["buckets"], 2)
        self.assertEqual(second["policies"][0]["changed"], 0)
        self.assertEqual(second["revision"], first["revision"])

    def test_real_run_bumps_revision_once_per_commit(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0]])
        store.write("acme", "n", {}, [[0, 2.0]])
        store.set_downsampling_policy("acme", "m", 1000, ["sum"])
        store.set_downsampling_policy("acme", "n", 1000, ["sum"])
        before = store.consistency_token("acme")["revision"]
        out = store.run_downsampling(1000, return_revision=True)
        self.assertEqual(out["revision"], before + 1)
        self.assertEqual(len(out["policies"]), 2)

    def test_dry_run_reports_without_storing_or_bumping(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0]])
        store.set_downsampling_policy("acme", "m", 1000, ["sum"])
        before = store.consistency_token("acme")["revision"]
        out = store.run_downsampling(1000, dry_run=True, return_revision=True)
        self.assertEqual(out["dry_run"], True)
        self.assertEqual(out["policies"][0]["changed"], 1)
        self.assertEqual(out["revision"], before)
        self.assertEqual(store.query_downsampled("acme", "m", agg="sum")[0]["points"],
                         [])
        self.assertEqual(os.listdir(store.downsampled_dir), [])

    def test_tenant_and_metric_filters_select_policies(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0]])
        store.write("acme", "n", {}, [[0, 2.0]])
        store.write("globex", "m", {}, [[0, 3.0]])
        for metric in ("m", "n"):
            store.set_downsampling_policy("acme", metric, 1000, ["sum"])
        store.set_downsampling_policy("globex", "m", 1000, ["sum"])
        out = store.run_downsampling(1000, tenant="acme")
        self.assertEqual([(p["tenant"], p["metric"]) for p in out["policies"]],
                         [("acme", "m"), ("acme", "n")])
        out = store.run_downsampling(1000, tenant="globex", metric="m")
        self.assertEqual([(p["tenant"], p["metric"]) for p in out["policies"]],
                         [("globex", "m")])
        out = store.run_downsampling(1000, tenant="unknown")
        self.assertEqual(out["policies"], [])

    def test_results_survive_restart_and_retention(self):
        first = self.store("keep")
        first.write("acme", "m", {}, [[0, 1.0], [1000, 2.0]])
        first.set_downsampling_policy("acme", "m", 1000, ["sum", "count"])
        first.run_downsampling(2000)
        first.set_retention("acme", 1500)
        first.run_retention(2000)
        # Retention dropped the raw point at 0 but the downsampled bucket stays.
        second = self.store("keep")
        self.assertEqual(second.query("acme", "m")[0]["points"], [[1000, 2.0]])
        self.assertEqual(
            second.query_downsampled("acme", "m", agg="sum")[0]["points"],
            [[0, 1.0], [1000, 2.0]])
        # A re-run after the raw cleanup does not resurrect or drop buckets.
        out = second.run_downsampling(2000)
        self.assertEqual(out["policies"][0]["buckets"], 1)
        self.assertEqual(out["policies"][0]["changed"], 0)
        self.assertEqual(
            second.query_downsampled("acme", "m", agg="sum")[0]["points"],
            [[0, 1.0], [1000, 2.0]])


class TestDownsampledQuery(DownsamplingCase):
    def seeded(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"}, [[0, 1.0], [100, 2.0], [2000, 5.0]])
        store.write("acme", "m", {"host": "b"}, [[1000, 4.0]])
        store.set_downsampling_policy("acme", "m", 1000, ["sum", "avg", "count"])
        store.run_downsampling(5000)
        return store

    def test_bucket_starts_null_fill_and_sorting(self):
        store = self.seeded()
        rows = {row["labels"]["host"]: row["points"]
                for row in store.query_downsampled("acme", "m", agg="sum")}
        self.assertEqual(rows["a"], [[0, 3.0], [1000, None], [2000, 5.0]])
        self.assertEqual(rows["b"], [[1000, 4.0]])

    def test_interval_clips_the_bucket_range(self):
        store = self.seeded()
        rows = store.query_downsampled("acme", "m", labels={"host": "a"},
                                       start_ms=1000, end_ms=2500, agg="sum")
        self.assertEqual(rows[0]["points"], [[2000, 5.0]])
        rows = store.query_downsampled("acme", "m", labels={"host": "a"},
                                       start_ms=500, end_ms=1999, agg="sum")
        self.assertEqual(rows[0]["points"], [[0, 3.0]])
        rows = store.query_downsampled("acme", "m", labels={"host": "a"},
                                       start_ms=0, end_ms=2000, agg="sum")
        self.assertEqual(rows[0]["points"], [[0, 3.0], [1000, None], [2000, 5.0]])
        rows = store.query_downsampled("acme", "m", labels={"host": "a"},
                                       start_ms=3000, end_ms=4000, agg="sum")
        self.assertEqual(rows[0]["points"], [])

    def test_labels_and_matchers_filter_series(self):
        store = self.seeded()
        rows = store.query_downsampled(
            "acme", "m", matchers=[{"key": "host", "op": "=~", "value": "[ab]"}],
            agg="avg")
        self.assertEqual(len(rows), 2)
        rows = store.query_downsampled(
            "acme", "m", matchers=[{"key": "host", "op": "!=", "value": "a"}],
            agg="avg")
        self.assertEqual([row["labels"] for row in rows], [{"host": "b"}])
        with self.assertRaises(ObsError):
            store.query_downsampled(
                "acme", "m", matchers=[{"key": "host", "op": "=~", "value": "("}],
                agg="avg")

    def test_missing_policy_is_unavailable(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0]])
        with self.assertRaises(ObsError) as ctx:
            store.query_downsampled("acme", "m", agg="sum")
        self.assertEqual(str(ctx.exception), "downsampling policy unavailable")

    def test_undeclared_aggregation_is_unavailable(self):
        store = self.seeded()
        with self.assertRaises(ObsError) as ctx:
            store.query_downsampled("acme", "m", agg="min")
        self.assertEqual(str(ctx.exception), "downsampling aggregation unavailable")
        with self.assertRaises(ObsError) as ctx:
            store.query_downsampled("acme", "m")
        self.assertEqual(str(ctx.exception), "downsampling aggregation unavailable")

    def test_windows_groups_and_counter_aggs_are_unsupported(self):
        store = self.seeded()
        for kwargs in ({"window_ms": 1000}, {"group_by": ["host"]},
                       {"agg": "increase"}, {"agg": "rate"}):
            call = {"agg": "sum"}
            call.update(kwargs)
            with self.assertRaises(ObsError, msg=repr(kwargs)) as ctx:
                store.query_downsampled("acme", "m", **call)
            self.assertEqual(str(ctx.exception), "downsampled query unsupported")

    def test_read_token_reuses_consistency_errors(self):
        store = self.seeded()
        token = store.consistency_token("acme")["token"]
        rows = store.query_downsampled("acme", "m", agg="sum", read_token=token)
        self.assertEqual(len(rows), 2)
        with self.assertRaises(ObsError) as ctx:
            store.query_downsampled("acme", "m", agg="sum", read_token="bogus")
        self.assertEqual(str(ctx.exception), "invalid read token")
        other = store.consistency_token("globex")["token"]
        with self.assertRaises(ObsError) as ctx:
            store.query_downsampled("acme", "m", agg="sum", read_token=other)
        self.assertEqual(str(ctx.exception), "invalid read token")


class TestCli(DownsamplingCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", os.path.join(self.tmp, "cli")] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_policy_run_query_roundtrip(self):
        code, _, _ = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                  "--sample", "0:1.5", "--sample", "1000:2.5")
        self.assertEqual(code, 0)
        code, out, _ = self.run_cli("downsampling-set", "--tenant", "acme",
                                    "--metric", "m", "--step-ms", "1000",
                                    "--agg", "sum", "--agg", "count")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["step_ms"], 1000)
        code, out, _ = self.run_cli("downsampling-get", "--tenant", "acme",
                                    "--metric", "m")
        self.assertEqual(json.loads(out)["aggregations"], ["sum", "count"])
        code, out, _ = self.run_cli("downsampling-run", "--now-ms", "2000")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["policies"][0]["changed"], 2)
        code, out, _ = self.run_cli("downsampled-query", "--tenant", "acme",
                                    "--metric", "m", "--agg", "sum")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["series"][0]["points"],
                         [[0, 1.5], [1000, 2.5]])

    def test_cli_errors(self):
        code, _, err = self.run_cli("downsampling-set", "--tenant", "acme",
                                    "--metric", "m", "--step-ms", "0",
                                    "--agg", "sum")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(err)["error"], "downsampling policy invalid")
        code, _, err = self.run_cli("downsampling-run", "--now-ms", "1000",
                                    "--tenant", "")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(err)["error"], "downsampling run invalid")
        code, _, err = self.run_cli("downsampled-query", "--tenant", "acme",
                                    "--metric", "m", "--agg", "sum")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(err)["error"], "downsampling policy unavailable")


class HttpCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-ds-http-")
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.access = None
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0)
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


class TestHttpApi(HttpCase):
    def test_policy_run_query_flow(self):
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {"host": "a"},
            "samples": [[0, 1.0], [100, 2.0], [1000, 4.0]]})
        self.assertEqual(code, 202)
        code, body = self.request("POST", "/v1/downsampling/policies", {
            "tenant": "acme", "metric": "m", "step_ms": 1000,
            "aggregations": ["sum", "count"]})
        self.assertEqual(code, 200)
        self.assertEqual(body["step_ms"], 1000)
        code, body = self.request("GET", "/v1/downsampling/policies?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        self.assertEqual(body["aggregations"], ["sum", "count"])
        code, body = self.request("GET", "/v1/downsampling/policies?tenant=acme&metric=nope")
        self.assertEqual(code, 200)
        self.assertEqual(body["step_ms"], None)
        code, body = self.request("POST", "/v1/downsampling/run", {"now_ms": 2000})
        self.assertEqual(code, 200)
        self.assertEqual(body["dry_run"], False)
        self.assertEqual(body["policies"],
                         [{"tenant": "acme", "metric": "m", "buckets": 2,
                           "changed": 2}])
        code, body = self.request(
            "GET", "/v1/query/downsampled?tenant=acme&metric=m&agg=sum")
        self.assertEqual(code, 200)
        self.assertEqual(body["series"][0]["labels"], {"host": "a"})
        self.assertEqual(body["series"][0]["points"], [[0, 3.0], [1000, 4.0]])

    def test_error_statuses(self):
        code, body = self.request("POST", "/v1/downsampling/policies", {
            "tenant": "acme", "metric": "m", "step_ms": 0, "aggregations": ["sum"]})
        self.assertEqual((code, body), (400, {"error": "downsampling policy invalid"}))
        code, body = self.request("POST", "/v1/downsampling/policies", {
            "tenant": "acme", "metric": "m", "step_ms": 1000,
            "aggregations": ["sum"], "bogus": 1})
        self.assertEqual((code, body), (400, {"error": "downsampling policy invalid"}))
        code, body = self.request("POST", "/v1/downsampling/run", {"now_ms": True})
        self.assertEqual((code, body), (400, {"error": "downsampling run invalid"}))
        code, body = self.request("POST", "/v1/downsampling/run",
                                  {"now_ms": 1000, "extra": 1})
        self.assertEqual((code, body), (400, {"error": "downsampling run invalid"}))
        code, body = self.request(
            "GET", "/v1/query/downsampled?tenant=acme&metric=m&agg=sum")
        self.assertEqual((code, body),
                         (404, {"error": "downsampling policy unavailable"}))
        self.request("POST", "/v1/downsampling/policies", {
            "tenant": "acme", "metric": "m", "step_ms": 1000,
            "aggregations": ["sum"]})
        code, body = self.request(
            "GET", "/v1/query/downsampled?tenant=acme&metric=m&agg=avg")
        self.assertEqual((code, body),
                         (400, {"error": "downsampling aggregation unavailable"}))
        code, body = self.request(
            "GET", "/v1/query/downsampled?tenant=acme&metric=m&agg=rate")
        self.assertEqual((code, body), (400, {"error": "downsampled query unsupported"}))
        code, body = self.request(
            "GET", "/v1/query/downsampled?tenant=acme&metric=m&agg=sum&window=1000")
        self.assertEqual((code, body), (400, {"error": "downsampled query unsupported"}))
        code, body = self.request(
            "GET", "/v1/query/downsampled?tenant=acme&metric=m&agg=sum&read_token=x")
        self.assertEqual((code, body), (400, {"error": "invalid read token"}))


class TestHttpAccess(HttpCase):
    def setUp(self):
        super().setUp()
        root = os.path.join(self.tmp, "data")
        self.access = AccessControl(root)
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0,
                                    access=self.access)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.access.create_principal("admin", "admintok", "admin", ["acme", "globex"])
        self.access.create_principal("writer", "writertok", "writer", ["acme"])
        self.access.create_principal("viewer", "viewertok", "viewer", ["acme"])

    def test_roles_and_audit(self):
        # Policy modification is a tenant-scoped write.
        code, _ = self.request("POST", "/v1/downsampling/policies", {
            "tenant": "acme", "metric": "m", "step_ms": 1000,
            "aggregations": ["sum"]}, token="viewertok")
        self.assertEqual(code, 403)
        code, _ = self.request("POST", "/v1/downsampling/policies", {
            "tenant": "acme", "metric": "m", "step_ms": 1000,
            "aggregations": ["sum"]}, token="writertok")
        self.assertEqual(code, 200)
        # Reading the policy and querying are tenant-scoped reads.
        code, _ = self.request("GET", "/v1/downsampling/policies?tenant=acme&metric=m",
                               token="viewertok")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/query/downsampled?tenant=acme&metric=m&agg=sum",
                               token="viewertok")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/query/downsampled?tenant=globex&metric=m&agg=sum",
                               token="viewertok")
        self.assertEqual(code, 403)
        # A single-tenant run is a write; a full run is admin-only.
        code, _ = self.request("POST", "/v1/downsampling/run",
                               {"now_ms": 1000, "tenant": "acme"}, token="writertok")
        self.assertEqual(code, 200)
        code, _ = self.request("POST", "/v1/downsampling/run",
                               {"now_ms": 1000}, token="writertok")
        self.assertEqual(code, 403)
        code, _ = self.request("POST", "/v1/downsampling/run",
                               {"now_ms": 1000}, token="admintok")
        self.assertEqual(code, 200)
        # Every request above was audited.
        code, body = self.request("GET", "/v1/audit", token="admintok")
        self.assertEqual(code, 200)
        paths = [entry["path"] for entry in body["entries"]]
        self.assertIn("/v1/downsampling/policies", paths)
        self.assertIn("/v1/downsampling/run", paths)
        self.assertIn("/v1/query/downsampled", paths)


if __name__ == "__main__":
    unittest.main()
