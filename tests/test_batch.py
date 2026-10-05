"""Tests for multi-series atomic batch writes: SeriesStore.write_batch,
POST /v1/series/batch and the write-batch CLI command."""

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

from obsd import AlertEngine, ObsError, SeriesStore, create_server
from obsd.cli import main as cli_main
from obsd.tsdb import series_id_for


class BatchCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-batch-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))


class TestBatchBasics(BatchCase):
    def test_cross_tenant_cross_metric_batch_and_result_order(self):
        store = self.store()
        out = store.write_batch([
            {"tenant": "acme", "metric": "m1", "labels": {"h": "a"},
             "samples": [[1000, 1.0], [2000, 2.0]]},
            {"tenant": "beta", "metric": "m2", "samples": [[1000, 3.0]]},
            {"tenant": "acme", "metric": "m2", "samples": [[3000, 4.0]]},
        ])
        self.assertEqual(out["written"], 4)
        self.assertEqual(out["duplicates"], 0)
        self.assertEqual([row["series_id"] for row in out["results"]],
                         [series_id_for("acme", "m1", {"h": "a"}),
                          series_id_for("beta", "m2", {}),
                          series_id_for("acme", "m2", {})])
        self.assertEqual([(row["written"], row["duplicates"])
                          for row in out["results"]], [(2, 0), (1, 0), (1, 0)])
        # Omitted labels mean the empty-labels identity.
        self.assertEqual(store.query("beta", "m2")[0]["labels"], {})
        self.assertEqual(store.query("beta", "m2")[0]["points"], [[1000, 3.0]])
        self.assertEqual(store.stats()["writes"], 1)

    def test_repeated_series_inside_batch_sees_earlier_entries(self):
        store = self.store()
        entry = {"tenant": "acme", "metric": "m",
                 "samples": [[1000, 1.0], [2000, 2.0]]}
        out = store.write_batch([dict(entry), dict(entry)])
        self.assertEqual((out["written"], out["duplicates"]), (2, 2))
        self.assertEqual([(row["written"], row["duplicates"])
                          for row in out["results"]], [(2, 0), (0, 2)])
        self.assertEqual(store.query("acme", "m")[0]["points"],
                         [[1000, 1.0], [2000, 2.0]])
        self.assertEqual(store.stats()["writes"], 1)

    def test_pure_duplicate_batch_counts_no_write(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        self.assertEqual(store.stats()["writes"], 1)
        out = store.write_batch([{"tenant": "acme", "metric": "m",
                                  "samples": [[1000, 1.0]]}])
        self.assertEqual((out["written"], out["duplicates"]), (0, 1))
        self.assertEqual(store.stats()["writes"], 1)

    def test_replay_of_same_batch_is_all_duplicates(self):
        store = self.store()
        entries = [{"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]},
                   {"tenant": "beta", "metric": "n", "samples": [[1000, 2.0]]}]
        store.write_batch(entries)
        out = store.write_batch(entries)
        self.assertEqual((out["written"], out["duplicates"]), (0, 2))
        self.assertEqual(store.stats()["writes"], 1)

    def test_overwrite_replaces_stored_and_batch_values(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        out = store.write_batch(
            [{"tenant": "acme", "metric": "m",
              "samples": [[1000, 9.0], [3000, 3.0]]},
             {"tenant": "acme", "metric": "m", "samples": [[3000, 8.0]]}],
            overwrite=True)
        # Entry 2 overwrites the value entry 1 wrote at 3000 inside the batch.
        self.assertEqual((out["written"], out["duplicates"]), (3, 0))
        self.assertEqual(store.query("acme", "m")[0]["points"],
                         [[1000, 9.0], [2000, 2.0], [3000, 8.0]])
        # The overwrite rewrote the file: no duplicate timestamp lines remain.
        reopened = self.store()
        self.assertEqual(reopened.query("acme", "m")[0]["points"],
                         [[1000, 9.0], [2000, 2.0], [3000, 8.0]])
        self.assertEqual(reopened.stats()["points"], 3)

    def test_samples_survive_reopen(self):
        store = self.store()
        store.write_batch([
            {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]},
            {"tenant": "beta", "metric": "n", "labels": {"k": "v"},
             "samples": [[2000, 2.0], [3000, 3.0]]}])
        reopened = self.store()
        self.assertEqual(reopened.query("acme", "m")[0]["points"], [[1000, 1.0]])
        self.assertEqual(reopened.query("beta", "n")[0]["points"],
                         [[2000, 2.0], [3000, 3.0]])
        self.assertEqual(reopened.stats()["writes"], 0)  # writes are in-memory only


class TestBatchValidation(BatchCase):
    def test_entries_must_be_a_non_empty_list(self):
        store = self.store()
        for bad in (None, {}, "x", 7, []):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.write_batch(bad)

    def test_entry_structure_and_required_fields(self):
        store = self.store()
        for bad in ("x", 7, [], None):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.write_batch([bad])
        for entry in ({"metric": "m", "samples": [[1, 1.0]]},
                      {"tenant": "acme", "samples": [[1, 1.0]]},
                      {"tenant": "acme", "metric": "m"},
                      {"tenant": "acme", "metric": "m", "samples": None},
                      {"tenant": "acme", "metric": "m", "samples": [[1, 1.0]],
                       "extra": 1}):
            with self.assertRaises(ObsError, msg=repr(entry)):
                store.write_batch([entry])
        self.assertEqual(store.stats()["series"], 0)

    def test_sample_and_identity_validation_matches_single_write(self):
        store = self.store()
        for entry in ({"tenant": "", "metric": "m", "samples": [[1, 1.0]]},
                      {"tenant": "acme", "metric": "", "samples": [[1, 1.0]]},
                      {"tenant": "acme", "metric": "m", "samples": []},
                      {"tenant": "acme", "metric": "m", "samples": [[1]]},
                      {"tenant": "acme", "metric": "m",
                       "samples": [[True, 1.0]]},
                      {"tenant": "acme", "metric": "m",
                       "samples": [[1, "x"]]},
                      {"tenant": "acme", "metric": "m", "labels": {"k": None},
                       "samples": [[1, 1.0]]}):
            with self.assertRaises(ObsError, msg=repr(entry)):
                store.write_batch([entry])

    def test_now_must_be_none_or_non_bool_int(self):
        store = self.store()
        entry = {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]}
        for bad in (True, False, 1.5, "1000", [1000]):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.write_batch([entry], now=bad)
        self.assertEqual(store.write_batch([entry], now=2000)["written"], 1)
        # None (and an omitted clock) skips the future check entirely.
        self.assertEqual(store.write_batch(
            [{"tenant": "acme", "metric": "m", "samples": [[10**15, 1.0]]}],
            now=None)["written"], 1)

    def test_future_sample_rejected_when_now_given(self):
        store = self.store()
        with self.assertRaises(ObsError) as caught:
            store.write_batch(
                [{"tenant": "acme", "metric": "m", "samples": [[3000, 1.0]]}],
                now=2000)
        self.assertIn("future", str(caught.exception))
        self.assertEqual(store.stats()["series"], 0)

    def test_overwrite_must_be_bool(self):
        store = self.store()
        entry = {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]}
        for bad in (1, 0, "true", None, []):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.write_batch([entry], overwrite=bad)

    def test_whole_batch_validated_before_conflicts(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        # Entry 0 would conflict; entry 1 is malformed. The validation error
        # of entry 1 must surface, not the conflict of entry 0.
        with self.assertRaises(ObsError) as caught:
            store.write_batch([
                {"tenant": "acme", "metric": "m", "samples": [[1000, 2.0]]},
                {"tenant": "acme", "metric": "m", "samples": "nope"}])
        self.assertNotIn("conflict", str(caught.exception))
        self.assertEqual(store.query("acme", "m")[0]["points"], [[1000, 1.0]])


class TestBatchAtomicity(BatchCase):
    def test_conflict_rejects_whole_batch_leaving_nothing(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        with self.assertRaises(ObsError) as caught:
            store.write_batch([
                {"tenant": "beta", "metric": "n", "samples": [[1000, 5.0]]},
                {"tenant": "acme", "metric": "m", "samples": [[1000, 2.0]]}])
        self.assertIn("conflict", str(caught.exception))
        self.assertEqual(store.stats()["series"], 1)
        self.assertEqual(store.stats()["writes"], 1)
        self.assertEqual(store.query("beta", "n"), [])
        self.assertEqual(store.query("acme", "m")[0]["points"], [[1000, 1.0]])
        # Nothing on disk either: a reopen sees the same state.
        reopened = self.store()
        self.assertEqual(reopened.stats()["series"], 1)
        self.assertEqual(reopened.query("beta", "n"), [])
        self.assertEqual(reopened.query("acme", "m")[0]["points"], [[1000, 1.0]])

    def test_batch_internal_conflict_rejects_everything(self):
        store = self.store()
        with self.assertRaises(ObsError):
            store.write_batch([
                {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]},
                {"tenant": "acme", "metric": "m", "samples": [[1000, 2.0]]}])
        self.assertEqual(store.stats()["series"], 0)
        self.assertEqual(store.stats()["writes"], 0)
        self.assertEqual(self.store().stats()["series"], 0)

    def test_quota_rejection_leaves_nothing(self):
        store = self.store()
        store.set_quota("acme", 1, None)
        with self.assertRaises(ObsError) as caught:
            store.write_batch([
                {"tenant": "acme", "metric": "m1", "samples": [[1000, 1.0]]},
                {"tenant": "acme", "metric": "m2", "samples": [[1000, 2.0]]},
                {"tenant": "beta", "metric": "m3", "samples": [[1000, 3.0]]}])
        self.assertIn("quota exceeded", str(caught.exception))
        self.assertEqual(store.stats()["series"], 0)
        self.assertEqual(store.stats()["writes"], 0)
        self.assertEqual(self.store().stats()["series"], 0)

    def test_conflicts_checked_before_quotas(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        store.set_quota("acme", 1, None)  # any new series would exceed it
        with self.assertRaises(ObsError) as caught:
            store.write_batch([
                {"tenant": "acme", "metric": "m2", "samples": [[1000, 1.0]]},
                {"tenant": "acme", "metric": "m", "samples": [[1000, 2.0]]}])
        self.assertIn("conflict", str(caught.exception))


class TestBatchQuotas(BatchCase):
    def test_net_new_series_and_points_per_tenant(self):
        store = self.store()
        store.set_quota("acme", 2, 3)
        out = store.write_batch([
            {"tenant": "acme", "metric": "m1", "samples": [[1000, 1.0], [2000, 2.0]]},
            {"tenant": "acme", "metric": "m2", "samples": [[1000, 3.0]]},
            {"tenant": "beta", "metric": "m1", "samples": [[1000, 4.0]]}])
        self.assertEqual(out["written"], 4)
        usage = store.get_quota("acme")
        self.assertEqual((usage["series"], usage["points"]), (2, 3))

    def test_repeated_timestamp_across_entries_counts_once(self):
        store = self.store()
        store.set_quota("acme", None, 2)
        # Both entries write timestamp 1000 to the same series: one new point.
        out = store.write_batch([
            {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0], [2000, 2.0]]},
            {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]}])
        self.assertEqual((out["written"], out["duplicates"]), (2, 1))
        self.assertEqual(store.get_quota("acme")["points"], 2)

    def test_duplicates_and_overwrites_add_no_occupancy(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        store.set_quota("acme", 1, 2)  # exactly at the limit on both dimensions
        out = store.write_batch(
            [{"tenant": "acme", "metric": "m",
              "samples": [[1000, 1.0], [2000, 9.0]]}], overwrite=True)
        self.assertEqual((out["written"], out["duplicates"]), (1, 1))
        self.assertEqual(store.get_quota("acme")["points"], 2)

    def test_lowered_quota_checks_only_increased_dimensions(self):
        store = self.store()
        store.write("acme", "m1", {}, [[1000, 1.0], [2000, 2.0]])
        store.write("acme", "m2", {}, [[1000, 3.0]])
        store.set_quota("acme", 1, 1)  # below current usage on both dimensions
        # A batch adding only points to an existing series is rejected on
        # points alone; a pure duplicate/overwrite batch still passes.
        with self.assertRaises(ObsError) as caught:
            store.write_batch([{"tenant": "acme", "metric": "m1",
                                "samples": [[3000, 3.0]]}])
        self.assertIn("max_points", str(caught.exception))
        out = store.write_batch(
            [{"tenant": "acme", "metric": "m1", "samples": [[1000, 1.0]]},
             {"tenant": "acme", "metric": "m2", "samples": [[1000, 7.0]]}],
            overwrite=True)
        self.assertEqual((out["written"], out["duplicates"]), (1, 1))

    def test_points_quota_counts_distinct_timestamps_per_tenant(self):
        store = self.store()
        store.set_quota("acme", None, 2)
        # m1 gets 1000+2000, m2 gets 1000 again: per-series occupancy is 2+1.
        with self.assertRaises(ObsError):
            store.write_batch([
                {"tenant": "acme", "metric": "m1",
                 "samples": [[1000, 1.0], [2000, 2.0]]},
                {"tenant": "acme", "metric": "m2", "samples": [[1000, 3.0]]}])
        self.assertEqual(store.stats()["series"], 0)


class TestBatchHttp(BatchCase):
    def setUp(self):
        super().setUp()
        root = os.path.join(self.tmp, "http")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        super().tearDown()
    def post(self, payload, path="/v1/series/batch"):
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_batch_write_success_shape(self):
        code, body = self.post({"entries": [
            {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]},
            {"tenant": "beta", "metric": "n", "labels": {"h": "a"},
             "samples": [[1000, 2.0], [2000, 3.0]]}]})
        self.assertEqual(code, 202)
        self.assertEqual((body["written"], body["duplicates"]), (3, 0))
        self.assertEqual(len(body["results"]), 2)
        self.assertEqual(body["results"][0]["series_id"],
                         series_id_for("acme", "m", {}))
        code, body = self.post({"entries": [
            {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]}]})
        self.assertEqual(code, 202)
        self.assertEqual((body["written"], body["duplicates"]), (0, 1))

    def test_now_ms_and_overwrite_accepted(self):
        code, body = self.post({"entries": [
            {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]}],
            "now_ms": 2000})
        self.assertEqual(code, 202)
        code, body = self.post({"entries": [
            {"tenant": "acme", "metric": "m", "samples": [[1000, 9.0]]}],
            "now_ms": 2000, "overwrite": True})
        self.assertEqual(code, 202)
        self.assertEqual(body["written"], 1)

    def test_validation_errors_are_400(self):
        for payload in ({},
                        {"entries": []},
                        {"entries": [{"tenant": "acme", "metric": "m"}]},
                        {"entries": [{"tenant": "acme", "metric": "m",
                                      "samples": [[1000, 1.0]]}], "now_ms": True},
                        {"entries": [{"tenant": "acme", "metric": "m",
                                      "samples": [[1000, 1.0]]}], "overwrite": 1},
                        {"entries": [{"tenant": "acme", "metric": "m",
                                      "samples": [[9000, 1.0]]}], "now_ms": 2000}):
            code, body = self.post(payload)
            self.assertEqual(code, 400, msg=repr(payload))
            self.assertIn("error", body)
        self.assertEqual(self.store.stats()["series"], 0)

    def test_conflict_and_quota_are_409(self):
        code, _ = self.post({"entries": [
            {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]}]})
        self.assertEqual(code, 202)
        code, body = self.post({"entries": [
            {"tenant": "acme", "metric": "m", "samples": [[1000, 2.0]]}]})
        self.assertEqual(code, 409)
        self.assertIn("conflict", body["error"])
        code, _ = self.post({"tenant": "acme", "max_series": 1,
                             "max_points": None}, path="/v1/quotas")
        self.assertEqual(code, 200)
        code, body = self.post({"entries": [
            {"tenant": "acme", "metric": "other", "samples": [[1000, 1.0]]}]})
        self.assertEqual(code, 409)
        self.assertIn("quota exceeded", body["error"])


class TestBatchCli(BatchCase):
    def run_cli(self, *argv):
        out, err = StringIO(), StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = cli_main(["--data-dir", os.path.join(self.tmp, "cli")] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_write_batch_success_line(self):
        entries = json.dumps([
            {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]},
            {"tenant": "beta", "metric": "n", "labels": {"h": "a"},
             "samples": [[1000, 2.0], [2000, 3.0]]}])
        code, out, err = self.run_cli("write-batch", "--entries", entries,
                                      "--now-ms", "5000")
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        body = json.loads(out)
        self.assertEqual((body["written"], body["duplicates"]), (3, 0))
        self.assertEqual(len(body["results"]), 2)
        # Replay with an overwrite of one value.
        entries = json.dumps([
            {"tenant": "acme", "metric": "m", "samples": [[1000, 7.0]]}])
        code, out, err = self.run_cli("write-batch", "--entries", entries,
                                      "--overwrite")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["written"], 1)

    def test_write_batch_failure_is_one_json_error_line(self):
        entries = json.dumps([
            {"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]},
            {"tenant": "acme", "metric": "m", "samples": [[1000, 2.0]]}])
        code, out, err = self.run_cli("write-batch", "--entries", entries)
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")
        self.assertIn("error", json.loads(err))
        self.assertEqual(len(err.splitlines()), 1)
        # The rejected batch left nothing behind.
        code, out, err = self.run_cli("query", "--tenant", "acme", "--metric", "m")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), {"series": []})

    def test_write_batch_bad_entries_json(self):
        code, out, err = self.run_cli("write-batch", "--entries", "not json")
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")
        self.assertIn("error", json.loads(err))


if __name__ == "__main__":
    unittest.main()
