"""Tests for persistent downsampling: policies, runs and downsampled queries."""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from obsd import AccessControl, AlertEngine, SeriesStore, create_server
from obsd.tsdb import ObsError


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="obsd-downsampling-")
        self.addCleanup(shutil.rmtree, self.dir, True)

    def store(self):
        return SeriesStore(self.dir)

    def policy(self, store, tenant="acme", metric="latency_ms", step=1000,
               aggs=("sum", "avg", "min", "max", "count")):
        return store.set_downsampling_policy(tenant, metric, step, list(aggs))


class TestPolicy(StoreCase):
    def test_set_and_get_roundtrip(self):
        store = self.store()
        created = self.policy(store, step=500, aggs=["max", "sum"])
        self.assertEqual(created, {"tenant": "acme", "metric": "latency_ms",
                                   "step_ms": 500,
                                   "aggregations": ["sum", "max"]})
        self.assertEqual(store.get_downsampling_policy("acme", "latency_ms"),
                         created)

    def test_policy_survives_restart(self):
        self.policy(self.store())
        reopened = self.store()
        self.assertEqual(reopened.get_downsampling_policy("acme", "latency_ms")["step_ms"],
                         1000)
        self.assertTrue(os.path.exists(
            os.path.join(self.dir, "downsampling.json")))

    def test_invalid_policies_are_rejected(self):
        store = self.store()
        bad = [
            ("", "m", 1000, ["sum"]),
            ("acme", "", 1000, ["sum"]),
            ("acme", "m", 0, ["sum"]),
            ("acme", "m", -5, ["sum"]),
            ("acme", "m", True, ["sum"]),
            ("acme", "m", 1.5, ["sum"]),
            ("acme", "m", 1000, []),
            ("acme", "m", 1000, None),
            ("acme", "m", 1000, ["median"]),
            ("acme", "m", 1000, ["sum", "sum"]),
            ("acme", "m", 1000, "sum"),
        ]
        for tenant, metric, step, aggs in bad:
            with self.assertRaises(ObsError) as ctx:
                store.set_downsampling_policy(tenant, metric, step, aggs)
            self.assertEqual(str(ctx.exception), "downsampling policy invalid")
        with self.assertRaises(ObsError):
            store.get_downsampling_policy("acme", "latency_ms")

    def test_get_unknown_policy_is_unavailable(self):
        store = self.store()
        with self.assertRaises(ObsError) as ctx:
            store.get_downsampling_policy("acme", "nope")
        self.assertEqual(str(ctx.exception), "downsampling policy unavailable")

    def test_replace_keeps_raw_samples_and_clears_results(self):
        store = self.store()
        store.write("acme", "latency_ms", {}, [[0, 1.0], [1000, 2.0]])
        self.policy(store, aggs=["sum"])
        store.run_downsampling(2000)
        self.assertEqual(
            store.query_downsampled("acme", "latency_ms", agg="sum")[0]["points"],
            [[0, 1.0], [1000, 2.0]])
        replaced = self.policy(store, step=2000, aggs=["sum"])
        self.assertEqual(replaced["step_ms"], 2000)
        # Old results are gone; the raw samples are not.
        self.assertEqual(store.query_downsampled("acme", "latency_ms", agg="sum"), [])
        rows = store.query("acme", "latency_ms")
        self.assertEqual(rows[0]["points"], [[0, 1.0], [1000, 2.0]])

    def test_identical_restate_keeps_results(self):
        store = self.store()
        store.write("acme", "latency_ms", {}, [[0, 1.0]])
        self.policy(store, aggs=["sum"])
        store.run_downsampling(1000)
        self.policy(store, aggs=["sum"])
        self.assertEqual(
            store.query_downsampled("acme", "latency_ms", agg="sum")[0]["points"],
            [[0, 1.0]])


class TestRun(StoreCase):
    def fill(self, store):
        store.write("acme", "latency_ms", {"host": "a"},
                    [[0, 1.0], [100, 3.0], [1000, 5.0], [3000, 7.0]])
        store.write("acme", "latency_ms", {"host": "b"}, [[500, 2.0], [2500, 4.0]])
        return store

    def test_run_computes_query_bucket_semantics(self):
        store = self.fill(self.store())
        self.policy(store)
        result = store.run_downsampling(4000)
        self.assertEqual(result["dry_run"], False)
        self.assertEqual(len(result["policies"]), 1)
        report = result["policies"][0]
        self.assertEqual((report["tenant"], report["metric"]),
                         ("acme", "latency_ms"))
        self.assertEqual(report["buckets"], 5)
        self.assertEqual(report["changed"], 5)
        rows = store.query_downsampled("acme", "latency_ms", agg="avg")
        by_host = {row["labels"]["host"]: row["points"] for row in rows}
        self.assertEqual(by_host["a"], [[0, 2.0], [1000, 5.0], [2000, None], [3000, 7.0]])
        self.assertEqual(by_host["b"], [[0, 2.0], [1000, None], [2000, 4.0]])

    def test_run_ignores_samples_newer_than_now(self):
        store = self.fill(self.store())
        self.policy(store)
        report = store.run_downsampling(1500)["policies"][0]
        self.assertEqual(report["buckets"], 3)
        rows = store.query_downsampled("acme", "latency_ms", agg="max")
        by_host = {row["labels"]["host"]: row["points"] for row in rows}
        self.assertEqual(by_host["a"], [[0, 3.0], [1000, 5.0]])
        self.assertEqual(by_host["b"], [[0, 2.0]])

    def test_rerun_is_idempotent_and_counts_changes(self):
        store = self.fill(self.store())
        self.policy(store)
        first = store.run_downsampling(4000)["policies"][0]
        self.assertEqual(first["changed"], 5)
        revision = store.consistency_token("acme")["revision"]
        second = store.run_downsampling(4000)["policies"][0]
        self.assertEqual((second["buckets"], second["changed"]), (5, 0))
        self.assertEqual(store.consistency_token("acme")["revision"], revision)
        # New raw data recomputes only the affected buckets (the write itself
        # is one commit, the downsampling run another).
        store.write("acme", "latency_ms", {"host": "a"}, [[2000, 9.0], [3100, 1.0]])
        third = store.run_downsampling(4000)["policies"][0]
        self.assertEqual((third["buckets"], third["changed"]), (6, 2))
        self.assertEqual(store.consistency_token("acme")["revision"], revision + 2)
        rows = store.query_downsampled("acme", "latency_ms",
                                       labels={"host": "a"}, agg="sum")
        self.assertEqual(rows[0]["points"],
                         [[0, 4.0], [1000, 5.0], [2000, 9.0], [3000, 8.0]])

    def test_dry_run_changes_nothing(self):
        store = self.fill(self.store())
        self.policy(store)
        revision = store.consistency_token("acme")["revision"]
        result = store.run_downsampling(4000, dry_run=True)
        self.assertEqual(result["dry_run"], True)
        self.assertEqual(result["policies"][0]["changed"], 5)
        self.assertEqual(store.query_downsampled("acme", "latency_ms", agg="sum"), [])
        self.assertEqual(store.consistency_token("acme")["revision"], revision)
        with self.assertRaises(ObsError) as ctx:
            store.run_downsampling(4000, dry_run=1)
        self.assertEqual(str(ctx.exception), "downsampling run invalid")

    def test_run_filters_and_ordering(self):
        store = self.store()
        store.write("acme", "m1", {}, [[0, 1.0]])
        store.write("acme", "m2", {}, [[0, 2.0]])
        store.write("globex", "m1", {}, [[0, 3.0]])
        self.policy(store, tenant="acme", metric="m1")
        self.policy(store, tenant="acme", metric="m2")
        self.policy(store, tenant="globex", metric="m1")
        result = store.run_downsampling(1000)
        self.assertEqual([(p["tenant"], p["metric"]) for p in result["policies"]],
                         [("acme", "m1"), ("acme", "m2"), ("globex", "m1")])
        result = store.run_downsampling(1000, tenant="acme")
        self.assertEqual([p["metric"] for p in result["policies"]], ["m1", "m2"])
        result = store.run_downsampling(1000, metric="m1")
        self.assertEqual([p["tenant"] for p in result["policies"]], ["acme", "globex"])
        result = store.run_downsampling(1000, tenant="acme", metric="m2")
        self.assertEqual(len(result["policies"]), 1)
        # No matching policy is an empty run, not an error.
        self.assertEqual(store.run_downsampling(1000, tenant="nope"),
                         {"dry_run": False, "policies": []})

    def test_run_argument_validation(self):
        store = self.store()
        for now_ms in (None, True, 1.5, "1000"):
            with self.assertRaises(ObsError) as ctx:
                store.run_downsampling(now_ms)
            self.assertEqual(str(ctx.exception), "downsampling run invalid")
        with self.assertRaises(ObsError):
            store.run_downsampling(1000, tenant="")
        with self.assertRaises(ObsError):
            store.run_downsampling(1000, metric=5)

    def test_results_survive_restart_and_retention(self):
        store = self.fill(self.store())
        self.policy(store)
        store.run_downsampling(4000)
        reopened = self.store()
        rows = reopened.query_downsampled("acme", "latency_ms", agg="count")
        by_host = {row["labels"]["host"]: row["points"] for row in rows}
        self.assertEqual(by_host["a"], [[0, 2], [1000, 1], [2000, None], [3000, 1]])
        # Retention deletes the raw points only; the downsampled results stay.
        reopened.set_retention("acme", 1)
        reopened.run_retention(10000)
        self.assertEqual(reopened.stats()["points"], 0)
        rows = reopened.query_downsampled("acme", "latency_ms", agg="count")
        self.assertEqual({row["labels"]["host"]: row["points"] for row in rows},
                         by_host)
        # A run after the cleanup recomputes nothing and deletes nothing.
        report = reopened.run_downsampling(4000)["policies"][0]
        self.assertEqual((report["buckets"], report["changed"]), (0, 0))
        rows = reopened.query_downsampled("acme", "latency_ms", agg="count")
        self.assertEqual({row["labels"]["host"]: row["points"] for row in rows},
                         by_host)


class TestDownsampledQuery(StoreCase):
    def fill(self, store):
        store.write("acme", "latency_ms", {"host": "a", "region": "x"},
                    [[0, 1.0], [100, 3.0], [3000, 5.0]])
        store.write("acme", "latency_ms", {"host": "b"}, [[1000, 2.0], [2000, 4.0]])
        self.policy(store)
        store.run_downsampling(10000)
        return store

    def test_interval_null_fill_and_order(self):
        store = self.fill(self.store())
        rows = store.query_downsampled("acme", "latency_ms", agg="sum",
                                       start_ms=0, end_ms=4000)
        self.assertEqual([row["labels"] for row in rows],
                         [{"host": "a", "region": "x"}, {"host": "b"}])
        self.assertEqual(rows[0]["points"], [[0, 4.0], [1000, None],
                                             [2000, None], [3000, 5.0]])
        self.assertEqual(rows[1]["points"], [[1000, 2.0], [2000, 4.0]])
        # The interval clamps the emitted buckets.
        rows = store.query_downsampled("acme", "latency_ms", agg="sum",
                                       start_ms=1500, end_ms=2500)
        self.assertEqual([row["points"] for row in rows], [[[2000, 4.0]]])

    def test_labels_and_matchers_filter(self):
        store = self.fill(self.store())
        rows = store.query_downsampled("acme", "latency_ms", agg="avg",
                                       labels={"host": "a"})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["labels"], {"host": "a", "region": "x"})
        matchers = [{"key": "region", "op": "!~", "value": "x"}]
        rows = store.query_downsampled("acme", "latency_ms", agg="avg",
                                       matchers=matchers)
        self.assertEqual([row["labels"] for row in rows], [{"host": "b"}])
        with self.assertRaises(ObsError):
            store.query_downsampled("acme", "latency_ms", agg="avg",
                                    matchers=[{"key": "h", "op": "??", "value": "x"}])

    def test_each_declared_aggregation(self):
        store = self.fill(self.store())
        rows = store.query_downsampled("acme", "latency_ms", labels={"host": "a"},
                                       agg="min")
        self.assertEqual(rows[0]["points"][0], [0, 1.0])
        rows = store.query_downsampled("acme", "latency_ms", labels={"host": "a"},
                                       agg="max")
        self.assertEqual(rows[0]["points"][0], [0, 3.0])

    def test_errors(self):
        store = self.store()
        with self.assertRaises(ObsError) as ctx:
            store.query_downsampled("acme", "latency_ms", agg="sum")
        self.assertEqual(str(ctx.exception), "downsampling policy unavailable")
        self.policy(store, aggs=["sum"])
        with self.assertRaises(ObsError) as ctx:
            store.query_downsampled("acme", "latency_ms", agg="avg")
        self.assertEqual(str(ctx.exception), "downsampling aggregation unavailable")
        for kwargs in ({"agg": "rate"}, {"agg": "increase"},
                       {"agg": "sum", "window_ms": 1000},
                       {"agg": "sum", "group_by": ["host"]}):
            with self.assertRaises(ObsError) as ctx:
                store.query_downsampled("acme", "latency_ms", **kwargs)
            self.assertEqual(str(ctx.exception), "downsampled query unsupported")

    def test_read_token_reused(self):
        store = self.fill(self.store())
        token = store.consistency_token("acme")["token"]
        rows = store.query_downsampled("acme", "latency_ms", agg="sum",
                                       read_token=token)
        self.assertEqual(len(rows), 2)
        with self.assertRaises(ObsError) as ctx:
            store.query_downsampled("acme", "latency_ms", agg="sum",
                                    read_token=token + "x")
        self.assertEqual(str(ctx.exception), "invalid read token")
        other = store.consistency_token("globex")["token"]
        with self.assertRaises(ObsError):
            store.query_downsampled("acme", "latency_ms", agg="sum",
                                    read_token=other)


class HttpCase(unittest.TestCase):
    guarded = False

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-downsampling-http-")
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.access = AccessControl(root) if self.guarded else None
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


class TestDownsamplingHttp(HttpCase):
    def fill(self):
        code, body = self.request("POST", "/v1/downsampling/policies", {
            "tenant": "acme", "metric": "m", "step_ms": 1000,
            "aggregations": ["sum", "count"]})
        self.assertEqual(code, 200)
        self.assertEqual(body, {"tenant": "acme", "metric": "m", "step_ms": 1000,
                                "aggregations": ["sum", "count"]})
        code, _ = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {"host": "a"},
            "samples": [[0, 1.0], [500, 3.0], [1000, 5.0], [3000, 7.0]]})
        self.assertEqual(code, 202)

    def test_policy_set_get_and_validation(self):
        self.fill()
        code, body = self.request("GET", "/v1/downsampling/policies"
                                         "?tenant=acme&metric=m")
        self.assertEqual((code, body["step_ms"]), (200, 1000))
        for payload in ({"tenant": "acme", "metric": "m", "step_ms": 0,
                         "aggregations": ["sum"]},
                        {"tenant": "acme", "metric": "m", "step_ms": 1000,
                         "aggregations": []},
                        {"tenant": "acme", "metric": "m", "step_ms": 1000},
                        {"tenant": "acme", "metric": "m", "step_ms": 1000,
                         "aggregations": ["sum"], "extra": 1}):
            code, body = self.request("POST", "/v1/downsampling/policies", payload)
            self.assertEqual(code, 400, repr(payload))
            self.assertEqual(body["error"], "downsampling policy invalid")
        code, body = self.request("GET", "/v1/downsampling/policies"
                                         "?tenant=acme&metric=nope")
        self.assertEqual((code, body["error"]),
                         (404, "downsampling policy unavailable"))

    def test_run_and_query_flow(self):
        self.fill()
        code, body = self.request("POST", "/v1/downsampling/run",
                                  {"now_ms": 4000, "dry_run": True})
        self.assertEqual(code, 200)
        self.assertEqual(body["dry_run"], True)
        self.assertEqual(body["policies"][0]["changed"], 3)
        code, body = self.request("GET", "/v1/query/downsampled"
                                         "?tenant=acme&metric=m&agg=sum")
        self.assertEqual((code, body["series"]), (200, []))
        code, body = self.request("POST", "/v1/downsampling/run", {"now_ms": 4000})
        self.assertEqual(body["policies"][0]["buckets"], 3)
        code, body = self.request("GET", "/v1/query/downsampled"
                                         "?tenant=acme&metric=m&agg=sum"
                                         "&start=0&end=4000")
        self.assertEqual(body["series"][0]["points"],
                         [[0, 4.0], [1000, 5.0], [2000, None], [3000, 7.0]])
        code, body = self.request("GET", "/v1/query/downsampled"
                                         "?tenant=acme&metric=m&agg=count")
        self.assertEqual(body["series"][0]["points"][0], [0, 2])

    def test_run_validation(self):
        self.fill()
        for payload in ({}, {"now_ms": True}, {"now_ms": 1.5},
                        {"now_ms": 4000, "dry_run": 1},
                        {"now_ms": 4000, "tenant": ""},
                        {"now_ms": 4000, "speed": "fast"}):
            code, body = self.request("POST", "/v1/downsampling/run", payload)
            self.assertEqual(code, 400, repr(payload))
            self.assertEqual(body["error"], "downsampling run invalid")

    def test_query_errors(self):
        self.fill()
        code, body = self.request("GET", "/v1/query/downsampled"
                                         "?tenant=acme&metric=nope&agg=sum")
        self.assertEqual((code, body["error"]),
                         (404, "downsampling policy unavailable"))
        code, body = self.request("GET", "/v1/query/downsampled"
                                         "?tenant=acme&metric=m&agg=avg")
        self.assertEqual((code, body["error"]),
                         (400, "downsampling aggregation unavailable"))
        for suffix in ("&agg=rate", "&agg=increase", "&agg=sum&window=1000",
                       "&agg=sum&group_by=%5B%22host%22%5D"):
            code, body = self.request(
                "GET", "/v1/query/downsampled?tenant=acme&metric=m" + suffix)
            self.assertEqual(code, 400, suffix)
            self.assertEqual(body["error"], "downsampled query unsupported")


class TestDownsamplingAccess(HttpCase):
    guarded = True

    def setUp(self):
        super().setUp()
        self.access.create_principal("viewer", "tok-view", "viewer", ["acme"])
        self.access.create_principal("writer", "tok-write", "writer", ["acme"])
        self.access.create_principal("admin", "tok-admin", "admin", ["acme"])

    def test_roles_and_audit(self):
        policy = {"tenant": "acme", "metric": "m", "step_ms": 1000,
                  "aggregations": ["sum"]}
        code, _ = self.request("GET", "/v1/downsampling/policies"
                                      "?tenant=acme&metric=m")
        self.assertEqual(code, 401)
        code, _ = self.request("POST", "/v1/downsampling/policies", policy,
                               token="tok-view")
        self.assertEqual(code, 403)
        code, _ = self.request("POST", "/v1/downsampling/policies", policy,
                               token="tok-write")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/downsampling/policies"
                                      "?tenant=acme&metric=m", token="tok-view")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/query/downsampled"
                                      "?tenant=acme&metric=m&agg=sum",
                               token="tok-view")
        self.assertEqual(code, 200)
        code, _ = self.request("POST", "/v1/downsampling/run",
                               {"now_ms": 1000, "tenant": "acme"},
                               token="tok-view")
        self.assertEqual(code, 403)
        code, _ = self.request("POST", "/v1/downsampling/run",
                               {"now_ms": 1000, "tenant": "acme"},
                               token="tok-write")
        self.assertEqual(code, 200)
        # A run across every policy is admin-only.
        code, _ = self.request("POST", "/v1/downsampling/run", {"now_ms": 1000},
                               token="tok-write")
        self.assertEqual(code, 403)
        code, _ = self.request("POST", "/v1/downsampling/run", {"now_ms": 1000},
                               token="tok-admin")
        self.assertEqual(code, 200)
        code, body = self.request("GET", "/v1/audit", token="tok-admin")
        self.assertEqual(code, 200)
        paths = [entry["path"] for entry in body["entries"]]
        self.assertIn("/v1/downsampling/policies", paths)
        self.assertIn("/v1/downsampling/run", paths)
        self.assertIn("/v1/query/downsampled", paths)


if __name__ == "__main__":
    unittest.main()
