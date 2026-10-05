"""Tests for verifiable snapshot export and replay: SeriesStore.export_snapshot
and replay_snapshot, GET /v1/export, POST /v1/replay and the export/replay CLI
commands."""

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
from obsd.tsdb import series_id_for


def digest_of(entries):
    document = {"version": 1, "entries": entries}
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":"))
                          .encode("utf-8")).hexdigest()


class SnapshotCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-snapshot-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))


class TestExport(SnapshotCase):
    def seed(self, store):
        store.write("acme", "latency_ms", {"host": "a"},
                    [[2000, 3.0], [1000, 1.0], [3000, 5.0]])
        store.write("acme", "latency_ms", {"host": "b"}, [[1000, 9.0]])
        store.write("acme", "errors", {"host": "a"}, [[1000, 7.0]])
        store.write("globex", "latency_ms", {"host": "a"}, [[1000, 4.0]])

    def test_shape_ordering_and_digest(self):
        store = self.store()
        self.seed(store)
        snap = store.export_snapshot("acme", "latency_ms")
        self.assertEqual(set(snap), {"version", "snapshot_id", "entries"})
        self.assertEqual(snap["version"], 1)
        self.assertEqual(len(snap["entries"]), 2)
        ids = [series_id_for("acme", "latency_ms", {"host": "a"}),
               series_id_for("acme", "latency_ms", {"host": "b"})]
        # Entries carry no series_id, only the four documented fields, and are
        # ordered by series_id; samples are ascending by timestamp.
        for entry in snap["entries"]:
            self.assertEqual(set(entry), {"tenant", "metric", "labels", "samples"})
            self.assertEqual(entry["tenant"], "acme")
            self.assertEqual(entry["metric"], "latency_ms")
        by_labels = {tuple(sorted(e["labels"].items())): e for e in snap["entries"]}
        self.assertEqual(by_labels[(("host", "a"),)]["samples"],
                         [[1000, 1.0], [2000, 3.0], [3000, 5.0]])
        self.assertEqual(by_labels[(("host", "b"),)]["samples"], [[1000, 9.0]])
        first_labels = ({"host": "a"} if ids[0] < ids[1] else {"host": "b"})
        self.assertEqual(snap["entries"][0]["labels"], first_labels)
        self.assertEqual(snap["snapshot_id"], digest_of(snap["entries"]))

    def test_filters_window_labels_and_matchers(self):
        store = self.store()
        self.seed(store)
        snap = store.export_snapshot("acme", "latency_ms", labels={"host": "a"})
        self.assertEqual(len(snap["entries"]), 1)
        self.assertEqual(snap["entries"][0]["labels"], {"host": "a"})
        # Closed interval on both ends.
        snap = store.export_snapshot("acme", "latency_ms",
                                     start_ms=1000, end_ms=2000)
        self.assertEqual(len(snap["entries"]), 2)
        for entry in snap["entries"]:
            for stamp, _ in entry["samples"]:
                self.assertTrue(1000 <= stamp <= 2000)
        snap = store.export_snapshot(
            "acme", "latency_ms",
            matchers=[{"key": "host", "op": "=~", "value": "a|b"}])
        self.assertEqual(len(snap["entries"]), 2)
        snap = store.export_snapshot(
            "acme", "latency_ms",
            matchers=[{"key": "host", "op": "!=", "value": "a"}])
        self.assertEqual([e["labels"] for e in snap["entries"]], [{"host": "b"}])
        with self.assertRaises(ObsError):
            store.export_snapshot("acme", "latency_ms", start_ms=5, end_ms=4)
        with self.assertRaises(ObsError):
            store.export_snapshot("acme", "latency_ms",
                                  matchers=[{"key": "h", "op": "~", "value": "x"}])

    def test_series_without_samples_in_range_are_omitted(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"}, [[1000, 1.0]])
        store.write("acme", "m", {"host": "b"}, [[5000, 2.0]])
        snap = store.export_snapshot("acme", "m", start_ms=0, end_ms=2000)
        self.assertEqual([e["labels"] for e in snap["entries"]], [{"host": "a"}])
        # Nothing in range at all: a valid empty snapshot.
        snap = store.export_snapshot("acme", "m", start_ms=9000, end_ms=9999)
        self.assertEqual(snap["entries"], [])
        self.assertEqual(snap["snapshot_id"], digest_of([]))

    def test_export_is_repeatable_and_side_effect_free(self):
        store = self.store()
        self.seed(store)
        before_stats = store.stats()
        before_query = store.query("acme", "latency_ms")
        first = store.export_snapshot("acme", "latency_ms")
        second = store.export_snapshot("acme", "latency_ms")
        self.assertEqual(first, second)
        self.assertEqual(json.dumps(first, sort_keys=True),
                         json.dumps(second, sort_keys=True))
        self.assertEqual(store.stats(), before_stats)
        self.assertEqual(store.query("acme", "latency_ms"), before_query)
        # A reopen exports byte-identical content.
        reopened = self.store()
        self.assertEqual(reopened.export_snapshot("acme", "latency_ms"), first)


class TestReplay(SnapshotCase):
    def exported(self, source=None):
        store = source or self.store("source")
        store.write("acme", "latency_ms", {"host": "a"},
                    [[1000, 1.0], [2000, 2.0], [3000, 3.0]])
        store.write("acme", "latency_ms", {"host": "b"}, [[1500, 9.0]])
        return store.export_snapshot("acme", "latency_ms")

    def test_roundtrip_into_an_empty_store(self):
        snap = self.exported()
        target = self.store("target")
        out = target.replay_snapshot(snap)
        self.assertTrue(out["applied"])
        self.assertEqual((out["written"], out["duplicates"]), (4, 0))
        self.assertEqual(len(out["results"]), 2)
        self.assertEqual([row["series_id"] for row in out["results"]],
                         [series_id_for("acme", "latency_ms", e["labels"])
                          for e in snap["entries"]])
        self.assertEqual(target.query("acme", "latency_ms"),
                         self.store("source").query("acme", "latency_ms"))
        # Re-exporting the replica yields the same snapshot id.
        self.assertEqual(target.export_snapshot("acme", "latency_ms")["snapshot_id"],
                         snap["snapshot_id"])

    def test_replay_onto_itself_is_all_duplicates(self):
        source = self.store("source")
        snap = self.exported(source)
        writes = source.stats()["writes"]
        out = source.replay_snapshot(snap)
        self.assertTrue(out["applied"])
        self.assertEqual((out["written"], out["duplicates"]), (0, 4))
        self.assertEqual(source.stats()["writes"], writes)

    def test_replay_fills_only_missing_points(self):
        snap = self.exported()
        target = self.store("target")
        target.write("acme", "latency_ms", {"host": "a"}, [[1000, 1.0]])
        out = target.replay_snapshot(snap)
        self.assertEqual((out["written"], out["duplicates"]), (3, 1))
        points = {tuple(e["labels"].items()): e["samples"]
                  for e in target.export_snapshot("acme", "latency_ms")["entries"]}
        self.assertEqual(points[(("host", "a"),)],
                         [[1000, 1.0], [2000, 2.0], [3000, 3.0]])

    def test_replay_is_stable_across_restarts(self):
        snap = self.exported()
        target = self.store("target")
        first = target.replay_snapshot(snap)
        reopened = self.store("target")
        again = reopened.replay_snapshot(snap)
        self.assertEqual((again["written"], again["duplicates"]), (0, 4))
        # Same per-entry re-judgement as an in-process second replay: every
        # sample is a duplicate, in entry order.
        self.assertEqual([row["series_id"] for row in again["results"]],
                         [row["series_id"] for row in first["results"]])
        self.assertEqual([row["written"] for row in again["results"]], [0, 0])
        self.assertEqual([row["duplicates"] for row in again["results"]],
                         [len(e["samples"]) for e in snap["entries"]])
        self.assertEqual(reopened.export_snapshot("acme", "latency_ms")["snapshot_id"],
                         snap["snapshot_id"])

    def test_empty_snapshot_is_a_noop(self):
        store = self.store()
        snap = {"version": 1, "snapshot_id": digest_of([]), "entries": []}
        out = store.replay_snapshot(snap)
        self.assertEqual(out, {"written": 0, "duplicates": 0, "results": [],
                               "applied": True})
        self.assertEqual(store.stats()["writes"], 0)

    def test_validation_errors_leave_no_trace(self):
        store = self.store()
        snap = self.exported()
        valid = json.loads(json.dumps(snap))
        bad_inputs = [
            None, [], "x",
            dict(valid, version=2),
            dict(valid, version="1"),
            dict(valid, version=True),
            {k: v for k, v in valid.items() if k != "snapshot_id"},
            dict(valid, snapshot_id=""),
            dict(valid, snapshot_id=None),
            dict(valid, snapshot_id="0" * 64),
            dict(valid, entries={}),
            dict(valid, entries=None),
            # Tampered entries no longer match the recorded digest.
            dict(valid, entries=valid["entries"][:1]),
        ]
        for bad in bad_inputs:
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.replay_snapshot(bad)
        self.assertEqual(store.stats(),
                         {"series": 0, "points": 0, "writes": 0, "tenants": {}})
        self.assertEqual(os.listdir(os.path.join(self.tmp, "data", "points")), [])

    def test_entry_validation_follows_batch_rules(self):
        store = self.store()
        base = {"tenant": "acme", "metric": "m", "labels": {},
                "samples": [[1000, 1.0]]}
        for entry in (dict(base, samples=[]),
                      dict(base, samples=[[1000]]),
                      dict(base, extra=1),
                      {k: v for k, v in base.items() if k != "metric"},
                      dict(base, labels={"a": None})):
            snap = {"version": 1, "snapshot_id": digest_of([entry]),
                    "entries": [entry]}
            with self.assertRaises(ObsError, msg=repr(entry)):
                store.replay_snapshot(snap)
        self.assertEqual(store.stats()["series"], 0)

    def test_future_samples_are_rejected(self):
        store = self.store()
        snap = self.exported()
        with self.assertRaisesRegex(ObsError, "future"):
            store.replay_snapshot(snap, now_ms=500)
        self.assertEqual(store.stats()["points"], 0)
        out = store.replay_snapshot(snap, now_ms=3000)
        self.assertEqual(out["written"], 4)

    def test_conflict_and_overwrite(self):
        snap = self.exported()
        target = self.store("target")
        target.write("acme", "latency_ms", {"host": "a"}, [[1000, 99.0]])
        with self.assertRaisesRegex(ObsError, "conflict"):
            target.replay_snapshot(snap)
        # The failed replay applied nothing.
        self.assertEqual(target.query("acme", "latency_ms")[0]["points"],
                         [[1000, 99.0]])
        out = target.replay_snapshot(snap, overwrite=True)
        self.assertEqual((out["written"], out["duplicates"]), (4, 0))
        self.assertEqual(target.export_snapshot("acme", "latency_ms")["snapshot_id"],
                         snap["snapshot_id"])

    def test_quota_breach_rejects_the_whole_replay(self):
        snap = self.exported()
        target = self.store("target")
        target.set_quota("acme", None, 2)
        with self.assertRaisesRegex(ObsError, "quota exceeded"):
            target.replay_snapshot(snap)
        self.assertEqual(target.stats()["points"], 0)
        self.assertEqual(target.get_quota("acme")["points"], 0)

    def test_dry_run_validates_and_counts_without_applying(self):
        snap = self.exported()
        target = self.store("target")
        out = target.replay_snapshot(snap, dry_run=True)
        self.assertFalse(out["applied"])
        self.assertEqual((out["written"], out["duplicates"]), (4, 0))
        self.assertEqual(len(out["results"]), 2)
        self.assertEqual(target.stats(),
                         {"series": 0, "points": 0, "writes": 0, "tenants": {}})
        # A dry run still surfaces conflicts and quota breaches.
        target.write("acme", "latency_ms", {"host": "a"}, [[1000, 99.0]])
        with self.assertRaisesRegex(ObsError, "conflict"):
            target.replay_snapshot(snap, dry_run=True)
        target.set_quota("acme", None, 2)
        with self.assertRaisesRegex(ObsError, "quota exceeded"):
            target.replay_snapshot(snap, dry_run=True, overwrite=True)
        # After dry runs the real replay still applies cleanly.
        target.set_quota("acme", None, None)
        real = target.replay_snapshot(snap, overwrite=True)
        self.assertTrue(real["applied"])
        self.assertEqual(real["written"], 4)

    def test_option_validation(self):
        store = self.store()
        snap = self.exported()
        for kwargs in ({"now_ms": "5"}, {"now_ms": 1.5}, {"now_ms": True},
                       {"overwrite": 1}, {"dry_run": 1}):
            with self.assertRaises(ObsError, msg=repr(kwargs)):
                store.replay_snapshot(snap, **kwargs)


class TestHttpSnapshot(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-snapshot-http-")
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.access = AccessControl(root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0,
                                    access=self.access)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
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

    def seed(self):
        code, _ = self.request("POST", "/v1/series/batch", {"entries": [
            {"tenant": "acme", "metric": "m", "labels": {"host": "a"},
             "samples": [[1000, 1.0], [2000, 2.0]]},
            {"tenant": "acme", "metric": "m", "labels": {"host": "b"},
             "samples": [[1000, 3.0]]}]})
        self.assertEqual(code, 202)

    def test_export_endpoint(self):
        self.seed()
        code, body = self.request("GET", "/v1/export?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        self.assertEqual(body["version"], 1)
        self.assertEqual(len(body["entries"]), 2)
        self.assertEqual(body["snapshot_id"], digest_of(body["entries"]))
        code, filtered = self.request(
            "GET", "/v1/export?tenant=acme&metric=m&label.host=a&start=2000&end=2000")
        self.assertEqual(code, 200)
        self.assertEqual(filtered["entries"],
                         [{"tenant": "acme", "metric": "m",
                           "labels": {"host": "a"}, "samples": [[2000, 2.0]]}])
        code, body = self.request("GET", "/v1/export?metric=m")
        self.assertEqual(code, 400)
        code, body = self.request("GET", "/v1/export?tenant=acme&metric=m&matchers=")
        self.assertEqual(code, 400)

    def test_replay_endpoint(self):
        self.seed()
        code, snap = self.request("GET", "/v1/export?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        # Replaying onto the same state is a pure duplicate replay.
        code, out = self.request("POST", "/v1/replay", snap)
        self.assertEqual(code, 202)
        self.assertTrue(out["applied"])
        self.assertEqual((out["written"], out["duplicates"]), (0, 3))
        # Dry run: 200, applied=false, nothing changes.
        code, dry = self.request("POST", "/v1/replay", dict(snap, dry_run=True))
        self.assertEqual(code, 200)
        self.assertFalse(dry["applied"])
        self.assertEqual((dry["written"], dry["duplicates"]), (0, 3))
        # A tampered digest is a 400; a conflict is a 409.
        code, body = self.request("POST", "/v1/replay",
                                  dict(snap, snapshot_id="0" * 64))
        self.assertEqual(code, 400)
        code, body = self.request("POST", "/v1/replay", dict(snap, version=2))
        self.assertEqual(code, 400)
        self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {"host": "a"},
            "samples": [[1000, 42.0]], "overwrite": True})
        code, body = self.request("POST", "/v1/replay", snap)
        self.assertEqual(code, 409)
        self.assertIn("conflict", body["error"])
        code, out = self.request("POST", "/v1/replay", dict(snap, overwrite=True))
        self.assertEqual(code, 202)
        self.assertEqual((out["written"], out["duplicates"]), (1, 2))

    def test_access_control_and_audit(self):
        self.seed()
        self.access.create_principal("admin", "admin-tok", "admin", ["acme"])
        self.access.create_principal("reader", "view-tok", "viewer", ["acme"])
        self.access.create_principal("writer", "write-tok", "writer", ["acme"])
        self.access.create_principal("outsider", "out-tok", "writer", ["globex"])
        # Export needs read scope on the tenant.
        code, _ = self.request("GET", "/v1/export?tenant=acme&metric=m")
        self.assertEqual(code, 401)
        code, snap = self.request("GET", "/v1/export?tenant=acme&metric=m",
                                  token="view-tok")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/export?tenant=globex&metric=m",
                               token="view-tok")
        self.assertEqual(code, 403)
        # Replay needs write scope covering every entry's tenant.
        code, _ = self.request("POST", "/v1/replay", snap, token="view-tok")
        self.assertEqual(code, 403)
        code, _ = self.request("POST", "/v1/replay", snap, token="out-tok")
        self.assertEqual(code, 403)
        code, out = self.request("POST", "/v1/replay", snap, token="write-tok")
        self.assertEqual(code, 202)
        self.assertTrue(out["applied"])
        # A snapshot mixing tenants is rejected whole when one entry is out of
        # scope, even for an otherwise valid writer.
        mixed = dict(snap, entries=snap["entries"] + [
            {"tenant": "globex", "metric": "m", "labels": {},
             "samples": [[1000, 1.0]]}])
        mixed["snapshot_id"] = digest_of(mixed["entries"])
        code, _ = self.request("POST", "/v1/replay", mixed, token="write-tok")
        self.assertEqual(code, 403)
        self.assertEqual(self.store.get_quota("globex")["points"], 0)
        # Every guarded request above was audited.
        outcomes = [row["outcome"] for row in self.access.query_audit()]
        self.assertIn("denied", outcomes)
        self.assertIn("allowed", outcomes)
        paths = [row["path"] for row in self.access.query_audit()]
        self.assertIn("/v1/export", paths)
        self.assertIn("/v1/replay", paths)


class TestCliSnapshot(SnapshotCase):
    def run_cli(self, *argv, sub="cli"):
        out, err = StringIO(), StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = cli_main(["--data-dir", os.path.join(self.tmp, sub)] + list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_export_replay_roundtrip_between_dirs(self):
        code, out, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "m", "--label", "host=a",
            "--sample", "1000:1.5", "--sample", "2000:2.5")
        self.assertEqual((code, err), (0, ""))
        code, out, err = self.run_cli(
            "export", "--tenant", "acme", "--metric", "m")
        self.assertEqual((code, err), (0, ""))
        # Exactly one JSON line on stdout.
        lines = out.splitlines()
        self.assertEqual(len(lines), 1)
        snap = json.loads(lines[0])
        self.assertEqual(snap["snapshot_id"], digest_of(snap["entries"]))
        self.assertEqual(snap["entries"][0]["samples"],
                         [[1000, 1.5], [2000, 2.5]])
        # Replay into a fresh data dir.
        code, out, err = self.run_cli("replay", "--snapshot", lines[0], sub="cli2")
        self.assertEqual((code, err), (0, ""))
        result = json.loads(out)
        self.assertTrue(result["applied"])
        self.assertEqual((result["written"], result["duplicates"]), (2, 0))
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m", sub="cli2")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["series"][0]["points"],
                         [[1000, 1.5], [2000, 2.5]])
        # Dry run previews without applying; replaying again is all duplicates.
        code, out, err = self.run_cli("replay", "--snapshot", lines[0],
                                      "--dry-run", sub="cli2")
        self.assertEqual(code, 0)
        dry = json.loads(out)
        self.assertFalse(dry["applied"])
        self.assertEqual((dry["written"], dry["duplicates"]), (0, 2))
        code, out, err = self.run_cli("replay", "--snapshot", lines[0], sub="cli2")
        self.assertEqual(code, 0)
        self.assertEqual((json.loads(out)["written"],
                          json.loads(out)["duplicates"]), (0, 2))

    def test_cli_errors_go_to_stderr(self):
        code, out, err = self.run_cli("replay", "--snapshot", "not json")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("error", json.loads(err))
        snap = {"version": 1, "snapshot_id": "0" * 64, "entries": []}
        code, out, err = self.run_cli("replay", "--snapshot", json.dumps(snap))
        self.assertEqual(code, 1)
        self.assertIn("error", json.loads(err))


if __name__ == "__main__":
    unittest.main()
