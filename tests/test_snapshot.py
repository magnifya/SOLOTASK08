"""Tests for verifiable history snapshots: SeriesStore.export_snapshot /
replay_snapshot, GET /v1/export, POST /v1/replay and the export/replay CLI."""

import hashlib
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
from obsd.tsdb import series_id_for, snapshot_digest


def digest_of(entries):
    canonical = json.dumps({"version": 1, "entries": entries},
                           sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class SnapshotCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-snapshot-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))


class TestExport(SnapshotCase):
    def test_shape_order_and_digest(self):
        store = self.store()
        store.write("acme", "m", {"h": "b"}, [[2000, 2.0], [1000, 1.0]])
        store.write("acme", "m", {"h": "a"}, [[1000, 3.0]])
        snap = store.export_snapshot("acme", "m")
        self.assertEqual(set(snap), {"version", "snapshot_id", "entries"})
        self.assertEqual(snap["version"], 1)
        self.assertEqual(len(snap["entries"]), 2)
        ids = [series_id_for("acme", "m", e["labels"]) for e in snap["entries"]]
        self.assertEqual(ids, sorted(ids))
        for entry in snap["entries"]:
            self.assertEqual(set(entry), {"tenant", "metric", "labels", "samples"})
            self.assertEqual(entry["tenant"], "acme")
            self.assertEqual(entry["metric"], "m")
            stamps = [s[0] for s in entry["samples"]]
            self.assertEqual(stamps, sorted(stamps))
        self.assertEqual(snap["snapshot_id"], digest_of(snap["entries"]))
        self.assertEqual(snap["snapshot_id"], snapshot_digest(snap["entries"]))

    def test_export_is_deterministic_and_pure(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        before = store.stats()
        first = store.export_snapshot("acme", "m")
        second = store.export_snapshot("acme", "m")
        self.assertEqual(first, second)
        self.assertEqual(json.dumps(first, sort_keys=True),
                         json.dumps(second, sort_keys=True))
        self.assertEqual(store.stats(), before)

    def test_filters_range_labels_matchers(self):
        store = self.store()
        store.write("acme", "m", {"h": "a", "r": "eu"},
                    [[1000, 1.0], [2000, 2.0], [3000, 3.0]])
        store.write("acme", "m", {"h": "b", "r": "us"}, [[1000, 4.0]])
        # Closed interval on both ends.
        snap = store.export_snapshot("acme", "m", start_ms=1000, end_ms=2000)
        by_host = {e["labels"]["h"]: e["samples"] for e in snap["entries"]}
        self.assertEqual(by_host["a"], [[1000, 1.0], [2000, 2.0]])
        self.assertEqual(by_host["b"], [[1000, 4.0]])
        # Exact label filter.
        snap = store.export_snapshot("acme", "m", labels={"r": "us"})
        self.assertEqual([e["labels"] for e in snap["entries"]],
                         [{"h": "b", "r": "us"}])
        # Matcher filter.
        snap = store.export_snapshot(
            "acme", "m", matchers=[{"key": "h", "op": "=~", "value": "a"}])
        self.assertEqual(len(snap["entries"]), 1)
        self.assertEqual(snap["entries"][0]["labels"]["h"], "a")
        # Range that excludes every sample of a series drops that series.
        snap = store.export_snapshot("acme", "m", labels={"h": "b"},
                                     start_ms=5000, end_ms=6000)
        self.assertEqual(snap["entries"], [])
        self.assertEqual(snap["snapshot_id"], digest_of([]))
        with self.assertRaises(ObsError):
            store.export_snapshot("acme", "m", start_ms=10, end_ms=5)

    def test_tenant_and_metric_isolation(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        store.write("beta", "m", {}, [[1000, 2.0]])
        store.write("acme", "other", {}, [[1000, 3.0]])
        snap = store.export_snapshot("acme", "m")
        self.assertEqual(len(snap["entries"]), 1)
        self.assertEqual(snap["entries"][0]["samples"], [[1000, 1.0]])

    def test_empty_series_are_not_exported(self):
        store = self.store()
        store.write("acme", "m", {"h": "a"}, [[1000, 1.0]])
        store.write("acme", "m", {"h": "b"}, [[1000, 2.0]])
        store.enforce_retention("acme", 2000)  # empties both series
        self.assertEqual(store.stats()["series"], 2)  # still registered
        snap = store.export_snapshot("acme", "m")
        self.assertEqual(snap["entries"], [])


class TestReplay(SnapshotCase):
    def test_replay_into_empty_store_migrates_history(self):
        source = self.store("src")
        source.write("acme", "m", {"h": "a"}, [[1000, 1.5], [2000, 2.5]])
        source.write("acme", "m", {}, [[1500, 3.5]])
        snap = source.export_snapshot("acme", "m")
        target = self.store("dst")
        out = target.replay_snapshot(snap)
        self.assertTrue(out["applied"])
        self.assertEqual(out["written"], 3)
        self.assertEqual(out["duplicates"], 0)
        self.assertEqual(len(out["results"]), 2)
        self.assertEqual([r["series_id"] for r in out["results"]],
                         [series_id_for("acme", "m", e["labels"])
                          for e in snap["entries"]])
        self.assertEqual(target.query("acme", "m"), source.query("acme", "m"))
        self.assertEqual(target.stats()["writes"], 1)

    def test_replay_onto_same_data_is_all_duplicates(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        writes_before = store.stats()["writes"]
        snap = store.export_snapshot("acme", "m")
        out = store.replay_snapshot(snap)
        self.assertTrue(out["applied"])
        self.assertEqual(out["written"], 0)
        self.assertEqual(out["duplicates"], 2)
        self.assertEqual(store.stats()["writes"], writes_before)

    def test_replay_fills_only_missing_points(self):
        source = self.store("src")
        source.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0], [3000, 3.0]])
        snap = source.export_snapshot("acme", "m")
        target = self.store("dst")
        target.write("acme", "m", {}, [[2000, 2.0]])
        out = target.replay_snapshot(snap)
        self.assertEqual((out["written"], out["duplicates"]), (2, 1))
        self.assertEqual(target.query("acme", "m")[0]["points"],
                         [[1000, 1.0], [2000, 2.0], [3000, 3.0]])

    def test_replay_survives_restart(self):
        source = self.store("src")
        source.write("acme", "m", {}, [[1000, 1.0]])
        snap = source.export_snapshot("acme", "m")
        target = self.store("dst")
        first = target.replay_snapshot(snap)
        reopened = self.store("dst")
        second = reopened.replay_snapshot(snap)
        self.assertEqual((second["written"], second["duplicates"]), (0, 1))
        self.assertEqual(reopened.query("acme", "m")[0]["points"], [[1000, 1.0]])
        # The write counter is in-memory; the duplicate replay adds nothing.
        self.assertEqual(reopened.stats()["writes"], 0)

    def test_dry_run_validates_without_applying(self):
        source = self.store("src")
        source.write("acme", "m", {}, [[1000, 1.0]])
        snap = source.export_snapshot("acme", "m")
        target = self.store("dst")
        out = target.replay_snapshot(snap, dry_run=True)
        self.assertFalse(out["applied"])
        self.assertEqual((out["written"], out["duplicates"]), (1, 0))
        self.assertEqual(target.stats(), {"series": 0, "points": 0, "writes": 0,
                                          "tenants": {}})
        self.assertEqual(target.query("acme", "m"), [])
        # A real replay afterwards applies the same counts.
        out = target.replay_snapshot(snap)
        self.assertTrue(out["applied"])
        self.assertEqual((out["written"], out["duplicates"]), (1, 0))

    def test_dry_run_still_reports_conflicts(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        snap = store.export_snapshot("acme", "m")
        snap["entries"][0]["samples"] = [[1000, 9.0]]
        snap["snapshot_id"] = snapshot_digest(snap["entries"])
        with self.assertRaisesRegex(ObsError, "conflict"):
            store.replay_snapshot(snap, dry_run=True)

    def test_invalid_version_digest_and_shape(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        snap = store.export_snapshot("acme", "m")
        for bad in ({"version": 2, "snapshot_id": snap["snapshot_id"],
                     "entries": snap["entries"]},
                    {"version": "1", "snapshot_id": snap["snapshot_id"],
                     "entries": snap["entries"]},
                    {"version": 1, "snapshot_id": "0" * 64,
                     "entries": snap["entries"]},
                    {"version": 1, "entries": snap["entries"]},
                    {"version": 1, "snapshot_id": snap["snapshot_id"]},
                    {"version": 1, "snapshot_id": snap["snapshot_id"],
                     "entries": "nope"},
                    "not-an-object"):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.replay_snapshot(bad)
        self.assertEqual(store.stats()["points"], 1)

    def test_tampered_entries_fail_digest(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        snap = store.export_snapshot("acme", "m")
        snap["entries"][0]["samples"][0][1] = 99.0
        with self.assertRaisesRegex(ObsError, "digest"):
            store.replay_snapshot(snap)

    def test_future_samples_and_bad_entries_rejected(self):
        source = self.store("src")
        source.write("acme", "m", {}, [[1000, 1.0], [9000, 2.0]])
        snap = source.export_snapshot("acme", "m")
        target = self.store("dst")
        with self.assertRaisesRegex(ObsError, "future"):
            target.replay_snapshot(snap, now=5000)
        self.assertEqual(target.stats()["points"], 0)
        # Malformed entry (unknown key) is rejected with a valid digest.
        snap["entries"][0]["bogus"] = 1
        snap["snapshot_id"] = snapshot_digest(snap["entries"])
        with self.assertRaises(ObsError):
            target.replay_snapshot(snap)
        self.assertEqual(target.stats()["points"], 0)

    def test_conflict_and_overwrite(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        snap = store.export_snapshot("acme", "m")
        snap["entries"][0]["samples"] = [[1000, 7.0], [2000, 2.0]]
        snap["snapshot_id"] = snapshot_digest(snap["entries"])
        with self.assertRaisesRegex(ObsError, "conflict"):
            store.replay_snapshot(snap)
        # Nothing from the failed replay was applied.
        self.assertEqual(store.query("acme", "m")[0]["points"], [[1000, 1.0]])
        out = store.replay_snapshot(snap, overwrite=True)
        self.assertEqual((out["written"], out["duplicates"]), (2, 0))
        self.assertEqual(store.query("acme", "m")[0]["points"],
                         [[1000, 7.0], [2000, 2.0]])

    def test_quota_enforced_and_atomic(self):
        source = self.store("src")
        source.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        snap = source.export_snapshot("acme", "m")
        target = self.store("dst")
        target.set_quota("acme", None, 1)
        with self.assertRaisesRegex(ObsError, "quota exceeded"):
            target.replay_snapshot(snap)
        self.assertEqual(target.stats()["points"], 0)
        self.assertEqual(target.query("acme", "m"), [])

    def test_empty_snapshot_replays_as_noop(self):
        store = self.store()
        snap = {"version": 1, "snapshot_id": digest_of([]), "entries": []}
        out = store.replay_snapshot(snap)
        self.assertEqual(out, {"written": 0, "duplicates": 0, "results": [],
                               "applied": True})
        out = store.replay_snapshot(snap, dry_run=True)
        self.assertFalse(out["applied"])
        self.assertEqual(store.stats()["writes"], 0)

    def test_results_follow_entry_order(self):
        source = self.store("src")
        source.write("acme", "m", {"h": "a"}, [[1000, 1.0]])
        source.write("acme", "m", {"h": "b"}, [[1000, 2.0]])
        snap = source.export_snapshot("acme", "m")
        target = self.store("dst")
        out = target.replay_snapshot(snap)
        self.assertEqual([r["series_id"] for r in out["results"]],
                         [series_id_for("acme", "m", e["labels"])
                          for e in snap["entries"]])


class HttpSnapshotCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-http-snapshot-")
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
    def write(self, labels, samples, metric="m", tenant="acme"):
        code, body = self.request("POST", "/v1/series", {
            "tenant": tenant, "metric": metric, "labels": labels,
            "samples": samples})
        self.assertEqual(code, 202)
        return body


class TestHttpSnapshot(HttpSnapshotCase):
    def test_export_http(self):
        self.write({"h": "a"}, [[1000, 1.0], [2000, 2.0]])
        self.write({"h": "b"}, [[1000, 3.0]])
        code, body = self.request("GET", "/v1/export?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        self.assertEqual(body["version"], 1)
        self.assertEqual(len(body["entries"]), 2)
        self.assertEqual(body["snapshot_id"], digest_of(body["entries"]))
        # label.k filter and closed range reuse the query semantics.
        code, body = self.request(
            "GET", "/v1/export?tenant=acme&metric=m&label.h=a&start=1500&end=2000")
        self.assertEqual(code, 200)
        self.assertEqual(body["entries"],
                         [{"tenant": "acme", "metric": "m",
                           "labels": {"h": "a"}, "samples": [[2000, 2.0]]}])
        code, body = self.request("GET", "/v1/export?metric=m")
        self.assertEqual(code, 400)
        code, body = self.request(
            "GET", "/v1/export?tenant=acme&metric=m&matchers=not-json")
        self.assertEqual(code, 400)

    def test_replay_http_roundtrip(self):
        self.write({}, [[1000, 1.0], [2000, 2.0]])
        code, snap = self.request("GET", "/v1/export?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        code, out = self.request("POST", "/v1/replay", snap)
        self.assertEqual(code, 202)
        self.assertEqual((out["written"], out["duplicates"]), (0, 2))
        self.assertTrue(out["applied"])
        self.assertEqual(len(out["results"]), 1)
        # dry_run previews without applying.
        other = dict(snap)
        other["entries"] = [dict(e) for e in snap["entries"]]
        other["entries"][0] = dict(other["entries"][0])
        other["entries"][0]["samples"] = [[3000, 3.0]]
        other["snapshot_id"] = digest_of(other["entries"])
        other["dry_run"] = True
        code, out = self.request("POST", "/v1/replay", other)
        self.assertEqual(code, 202)
        self.assertFalse(out["applied"])
        self.assertEqual((out["written"], out["duplicates"]), (1, 0))
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m")
        self.assertEqual(body["series"][0]["points"], [[1000, 1.0], [2000, 2.0]])

    def test_replay_http_errors(self):
        self.write({}, [[1000, 1.0]])
        code, snap = self.request("GET", "/v1/export?tenant=acme&metric=m")
        bad = dict(snap, snapshot_id="0" * 64)
        code, body = self.request("POST", "/v1/replay", bad)
        self.assertEqual(code, 400)
        bad = dict(snap, version=2)
        code, body = self.request("POST", "/v1/replay", bad)
        self.assertEqual(code, 400)
        bad = dict(snap, unexpected=True)
        code, body = self.request("POST", "/v1/replay", bad)
        self.assertEqual(code, 400)
        # Conflict without overwrite -> 409 and nothing applied.
        conflict = dict(snap)
        conflict["entries"] = [dict(snap["entries"][0], samples=[[1000, 9.0]])]
        conflict["snapshot_id"] = digest_of(conflict["entries"])
        code, body = self.request("POST", "/v1/replay", conflict)
        self.assertEqual(code, 409)
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m")
        self.assertEqual(body["series"][0]["points"], [[1000, 1.0]])
        # Future sample with now_ms -> 400.
        future = dict(snap)
        future["entries"] = [dict(snap["entries"][0], samples=[[5000, 5.0]])]
        future["snapshot_id"] = digest_of(future["entries"])
        future["now_ms"] = 2000
        code, body = self.request("POST", "/v1/replay", future)
        self.assertEqual(code, 400)


class TestHttpSnapshotAccess(HttpSnapshotCase):
    def setUp(self):
        super().setUp()
        self.access.create_principal("root", "admin-tok", "admin", ["acme"])
        self.access.create_principal("reader", "view-tok", "viewer", ["acme"])
        self.access.create_principal("writer", "write-tok", "writer", ["acme"])
        self.access.create_principal("other", "other-tok", "writer", ["beta"])

    def test_export_requires_read_scope(self):
        self.store.write("acme", "m", {}, [[1000, 1.0]])
        code, _ = self.request("GET", "/v1/export?tenant=acme&metric=m")
        self.assertEqual(code, 401)
        code, body = self.request("GET", "/v1/export?tenant=acme&metric=m",
                                  token="view-tok")
        self.assertEqual(code, 200)
        self.assertEqual(len(body["entries"]), 1)
        code, _ = self.request("GET", "/v1/export?tenant=acme&metric=m",
                               token="other-tok")
        self.assertEqual(code, 403)

    def test_replay_checks_write_scope_per_entry(self):
        self.store.write("acme", "m", {}, [[1000, 1.0]])
        code, snap = self.request("GET", "/v1/export?tenant=acme&metric=m",
                                  token="view-tok")
        self.assertEqual(code, 200)
        # A viewer cannot replay.
        code, _ = self.request("POST", "/v1/replay", snap, token="view-tok")
        self.assertEqual(code, 403)
        # A writer scoped to another tenant is rejected for the whole batch.
        code, _ = self.request("POST", "/v1/replay", snap, token="other-tok")
        self.assertEqual(code, 403)
        # In-scope writer replays fine.
        code, out = self.request("POST", "/v1/replay", snap, token="write-tok")
        self.assertEqual(code, 202)
        self.assertTrue(out["applied"])
        # Every request above was audited.
        entries = self.access.query_audit()
        paths = [(e["path"], e["outcome"]) for e in entries]
        self.assertIn(("/v1/export", "allowed"), paths)
        self.assertIn(("/v1/replay", "denied"), paths)
        self.assertIn(("/v1/replay", "allowed"), paths)


class TestSnapshotCli(SnapshotCase):
    def run_cli(self, *argv, stdin=None):
        out, err = StringIO(), StringIO()
        if stdin is not None:
            import sys
            old = sys.stdin
            sys.stdin = StringIO(stdin)
            try:
                with redirect_stdout(out), redirect_stderr(err):
                    code = cli_main(list(argv))
            finally:
                sys.stdin = old
        else:
            with redirect_stdout(out), redirect_stderr(err):
                code = cli_main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_export_replay_cli_roundtrip(self):
        src = os.path.join(self.tmp, "src")
        dst = os.path.join(self.tmp, "dst")
        code, _, _ = self.run_cli(
            "--data-dir", src, "write", "--tenant", "acme", "--metric", "m",
            "--label", "h=a", "--sample", "1000:1.5", "--sample", "2000:2.5")
        self.assertEqual(code, 0)
        code, out, err = self.run_cli(
            "--data-dir", src, "export", "--tenant", "acme", "--metric", "m")
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertEqual(len(out.strip().splitlines()), 1)
        snap = json.loads(out)
        self.assertEqual(snap["snapshot_id"], digest_of(snap["entries"]))
        # Replay into the empty destination through stdin.
        code, out, err = self.run_cli(
            "--data-dir", dst, "replay", "--snapshot", "-", stdin=json.dumps(snap))
        self.assertEqual(code, 0)
        result = json.loads(out)
        self.assertTrue(result["applied"])
        self.assertEqual((result["written"], result["duplicates"]), (2, 0))
        # Replaying again is all duplicates.
        code, out, _ = self.run_cli(
            "--data-dir", dst, "replay", "--snapshot", json.dumps(snap))
        self.assertEqual(code, 0)
        result = json.loads(out)
        self.assertEqual((result["written"], result["duplicates"]), (0, 2))
        # The migrated history queries identically.
        code, out_src, _ = self.run_cli(
            "--data-dir", src, "query", "--tenant", "acme", "--metric", "m")
        code, out_dst, _ = self.run_cli(
            "--data-dir", dst, "query", "--tenant", "acme", "--metric", "m")
        self.assertEqual(json.loads(out_src), json.loads(out_dst))

    def test_cli_errors_and_dry_run(self):
        src = os.path.join(self.tmp, "src")
        code, _, _ = self.run_cli(
            "--data-dir", src, "write", "--tenant", "acme", "--metric", "m",
            "--sample", "1000:1.0")
        self.assertEqual(code, 0)
        code, out, _ = self.run_cli(
            "--data-dir", src, "export", "--tenant", "acme", "--metric", "m")
        snap = json.loads(out)
        dst = os.path.join(self.tmp, "dst")
        code, out, err = self.run_cli(
            "--data-dir", dst, "replay", "--snapshot", json.dumps(snap),
            "--dry-run")
        self.assertEqual(code, 0)
        self.assertFalse(json.loads(out)["applied"])
        code, out, _ = self.run_cli(
            "--data-dir", dst, "query", "--tenant", "acme", "--metric", "m")
        self.assertEqual(json.loads(out)["series"], [])
        # Bad digest -> non-zero exit, one JSON error line on stderr.
        bad = dict(snap, snapshot_id="0" * 64)
        code, out, err = self.run_cli(
            "--data-dir", dst, "replay", "--snapshot", json.dumps(bad))
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("error", json.loads(err))
        code, out, err = self.run_cli(
            "--data-dir", dst, "replay", "--snapshot", "not json")
        self.assertEqual(code, 1)
        self.assertIn("error", json.loads(err))


if __name__ == "__main__":
    unittest.main()
