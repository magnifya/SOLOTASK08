"""Tests for atomic multi-series writes: SeriesStore.write_batch, HTTP and CLI."""

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

from obsd import AlertEngine, SeriesStore, create_server, ObsError
from obsd.cli import main as cli_main
from obsd.tsdb import series_id_for


class BatchCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-batch-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))


def entry(tenant="acme", metric="m", labels=None, samples=None):
    row = {"tenant": tenant, "metric": metric,
           "samples": [[1, 1.0]] if samples is None else samples}
    if labels is not None:
        row["labels"] = labels
    return row


class TestWriteBatchShape(BatchCase):
    def test_cross_tenant_metric_entries_and_results_order(self):
        store = self.store()
        result = store.write_batch([
            entry("acme", "m", {"k": "a"}, [[1000, 1.0], [2000, 2.0]]),
            entry("acme", "n", None, [[3000, 3.0]]),
            entry("globex", "m", {}, [[4000, 4.0]])])
        self.assertEqual(result["written"], 4)
        self.assertEqual(result["duplicates"], 0)
        self.assertEqual([row["written"] for row in result["results"]], [2, 1, 1])
        self.assertEqual([row["duplicates"] for row in result["results"]], [0, 0, 0])
        sids = [series_id_for("acme", "m", {"k": "a"}),
                series_id_for("acme", "n", {}),
                series_id_for("globex", "m", {})]
        self.assertEqual([row["series_id"] for row in result["results"]], sids)
        stats = store.stats()
        self.assertEqual((stats["series"], stats["points"], stats["writes"]), (3, 4, 1))
        self.assertEqual(store.query("globex", "m")[0]["points"], [[4000, 4.0]])

    def test_omitted_labels_mean_empty_object(self):
        store = self.store()
        result = store.write_batch([entry(tenant="acme", metric="m")])
        self.assertEqual(result["results"][0]["series_id"], series_id_for("acme", "m", {}))

    def test_totals_are_sum_of_per_entry_counts(self):
        store = self.store()
        store.write_batch([entry(samples=[[1, 1.0]])])
        result = store.write_batch([
            entry(samples=[[1, 1.0], [2, 2.0]]),
            entry(samples=[[1, 1.0], [2, 2.0], [3, 3.0]])])
        per = [(row["written"], row["duplicates"]) for row in result["results"]]
        self.assertEqual(per, [(1, 1), (1, 2)])
        self.assertEqual(result["written"], sum(w for w, _ in per))
        self.assertEqual(result["duplicates"], sum(d for _, d in per))


class TestWriteBatchOrdering(BatchCase):
    def test_later_entries_see_earlier_entries_in_same_batch(self):
        store = self.store()
        result = store.write_batch([
            entry(samples=[[1000, 1.0]]),
            entry(samples=[[1000, 1.0]]),       # same value -> duplicate
            entry(labels={"k": "v"}, samples=[[1000, 5.0], [2000, 6.0]])])
        self.assertEqual([(r["written"], r["duplicates"]) for r in result["results"]],
                         [(1, 0), (0, 1), (2, 0)])
        rows = {tuple(sorted(r["labels"].items())): r["points"]
                for r in store.query("acme", "m")}
        self.assertEqual(rows[()], [[1000, 1.0]])
        self.assertEqual(rows[(("k", "v"),)], [[1000, 5.0], [2000, 6.0]])

    def test_conflict_seen_from_earlier_entry_without_overwrite(self):
        store = self.store()
        with self.assertRaises(ObsError) as caught:
            store.write_batch([
                entry(samples=[[1000, 1.0]]),
                entry(samples=[[1000, 2.0]])])
        self.assertTrue(str(caught.exception).startswith("conflict"))
        # Nothing landed: the first entry is rolled back too.
        self.assertEqual(store.stats()["series"], 0)
        self.assertEqual(store.stats()["writes"], 0)

    def test_overwrite_replaces_values_seen_earlier_in_batch(self):
        store = self.store()
        result = store.write_batch([
            entry(samples=[[1000, 1.0], [2000, 2.0]]),
            entry(samples=[[1000, 9.0]])], overwrite=True)
        self.assertEqual([(r["written"], r["duplicates"]) for r in result["results"]],
                         [(2, 0), (1, 0)])
        self.assertEqual(store.query("acme", "m")[0]["points"],
                         [[1000, 9.0], [2000, 2.0]])

    def test_repeated_same_value_within_and_across_entries_counts_duplicate(self):
        store = self.store()
        result = store.write_batch([
            entry(samples=[[1000, 1.0], [1000, 1.0], [2000, 2.0]]),
            entry(samples=[[2000, 2.0]])])
        self.assertEqual([(r["written"], r["duplicates"]) for r in result["results"]],
                         [(2, 1), (0, 1)])

    def test_same_batch_replayed_in_same_order_is_all_duplicates(self):
        store = self.store()
        payload = [entry(samples=[[1000, 1.0]]),
                   entry(labels={"k": "v"}, samples=[[1000, 1.0], [2000, 2.0]])]
        first = store.write_batch(payload)
        self.assertEqual((first["written"], first["duplicates"]), (3, 0))
        again = store.write_batch(payload)
        self.assertEqual((again["written"], again["duplicates"]), (0, 3))
        self.assertEqual([r["series_id"] for r in again["results"]],
                         [r["series_id"] for r in first["results"]])
        # An overwrite replay of identical values is also pure duplicates.
        over = store.write_batch(payload, overwrite=True)
        self.assertEqual((over["written"], over["duplicates"]), (0, 3))


class TestWriteBatchValidation(BatchCase):
    def test_entries_must_be_a_non_empty_array(self):
        store = self.store()
        for bad in (None, [], {}, "x", 1):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.write_batch(bad)

    def test_entry_structure_and_required_fields(self):
        store = self.store()
        valid = entry()
        cases = [
            [42],
            [{"tenant": "acme", "metric": "m"}],                       # no samples
            [{"tenant": "acme", "metric": "m", "samples": []}],        # empty samples
            [{"tenant": "", "metric": "m", "samples": [[1, 1.0]]}],
            [{"tenant": 7, "metric": "m", "samples": [[1, 1.0]]}],
            [{"tenant": "acme", "metric": "", "samples": [[1, 1.0]]}],
            [{"tenant": "acme", "metric": 9, "samples": [[1, 1.0]]}],
            [{"tenant": "acme", "metric": "m", "samples": "x"}],
            [{"tenant": "acme", "metric": "m", "samples": [[1]]}],
            [{"tenant": "acme", "metric": "m", "samples": [[1, "x"]]}],
            [{"tenant": "acme", "metric": "m", "samples": [[True, 1.0]]}],
            [{"tenant": "acme", "metric": "m", "labels": "x",
              "samples": [[1, 1.0]]}],
            [{"tenant": "acme", "metric": "m", "labels": [],
              "samples": [[1, 1.0]]}],
            [dict(valid, bogus=1)],
        ]
        for bad in cases:
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.write_batch(bad)

    def test_now_and_overwrite_types(self):
        store = self.store()
        for bad_now in (True, 1.5, "1000"):
            with self.assertRaises(ObsError, msg=repr(bad_now)):
                store.write_batch([entry()], now=bad_now)
        with self.assertRaises(ObsError):
            store.write_batch([entry()], overwrite=1)

    def test_null_now_skips_future_check_integer_now_enforces_it(self):
        store = self.store()
        self.assertEqual(
            store.write_batch([entry(samples=[[10_000, 1.0]])], now=None)["written"], 1)
        self.assertEqual(
            store.write_batch([entry(metric="n", samples=[[10_000, 1.0]])])["written"], 1)
        with self.assertRaises(ObsError):
            store.write_batch([entry(metric="p", samples=[[10_001, 1.0]])], now=10_000)

    def test_full_validation_runs_before_any_conflict(self):
        store = self.store()
        store.write_batch([entry(samples=[[1000, 1.0]])])
        # First entry conflicts, but a future sample anywhere must win because
        # every entry is validated before a single conflict is examined.
        with self.assertRaises(ObsError) as caught:
            store.write_batch([
                entry(samples=[[1000, 2.0]]),
                entry(metric="n", samples=[[10_000, 1.0]])], now=2000)
        self.assertIn("future", str(caught.exception))
        self.assertEqual(store.stats()["series"], 1)


class TestWriteBatchQuota(BatchCase):
    def test_quota_uses_net_new_series_and_distinct_timestamps(self):
        store = self.store()
        store.set_quota("acme", 2, 4)
        result = store.write_batch([
            entry(labels={"k": "a"}, samples=[[1, 1.0], [2, 2.0]]),
            entry(labels={"k": "b"}, samples=[[3, 3.0], [4, 4.0]])])
        self.assertEqual(result["written"], 4)
        usage = store.get_quota("acme")
        self.assertEqual((usage["series"], usage["points"]), (2, 4))
        # One new point anywhere exceeds max_points; the whole batch is dropped.
        with self.assertRaises(ObsError) as caught:
            store.write_batch([
                entry(labels={"k": "a"}, samples=[[1, 1.0], [5, 5.0]]),
                entry(labels={"k": "c"}, samples=[[6, 6.0]])])
        self.assertIn("quota exceeded", str(caught.exception))
        usage = store.get_quota("acme")
        self.assertEqual((usage["series"], usage["points"]), (2, 4))
        self.assertEqual(store.stats()["writes"], 1)

    def test_duplicates_and_overwrites_add_no_occupancy(self):
        store = self.store()
        store.set_quota("acme", 1, 2)
        store.write_batch([entry(samples=[[1, 1.0], [2, 2.0]])])
        result = store.write_batch([
            entry(samples=[[1, 1.0]]),
            entry(samples=[[2, 9.0]])], overwrite=True)
        self.assertEqual((result["written"], result["duplicates"]), (1, 1))
        self.assertEqual(store.get_quota("acme")["points"], 2)
        # A third series and a new point both at the limit conflict with quota;
        # conflict rules still take precedence over quota.
        with self.assertRaises(ObsError) as caught:
            store.write_batch([
                entry(samples=[[2, 8.0]]),
                entry(labels={"k": "z"}, samples=[[9, 9.0]])])
        self.assertTrue(str(caught.exception).startswith("conflict"))

    def test_lowered_limits_only_check_actually_increasing_dimensions(self):
        store = self.store()
        store.write_batch([entry(samples=[[1, 1.0], [2, 2.0]])])
        store.set_quota("acme", 0, 1)  # below current 1 series / 2 points
        # Pure duplicate: no dimension increases.
        self.assertEqual(
            store.write_batch([entry(samples=[[1, 1.0]])])["duplicates"], 1)
        # Overwrite: also no increase.
        self.assertEqual(
            store.write_batch([entry(samples=[[1, 9.0]])], overwrite=True)["written"], 1)
        # A new point is blocked by max_points, not max_series.
        with self.assertRaises(ObsError) as caught:
            store.write_batch([entry(samples=[[3, 3.0]])])
        self.assertIn("max_points", str(caught.exception))
        # A new series is blocked by max_series even with points headroom.
        store.set_quota("acme", 0, 10)
        with self.assertRaises(ObsError) as caught:
            store.write_batch([entry(labels={"k": "v"}, samples=[[3, 3.0]])])
        self.assertIn("max_series", str(caught.exception))

    def test_quotas_are_per_tenant(self):
        store = self.store()
        store.set_quota("acme", 1, 1)
        result = store.write_batch([
            entry("acme", "m", None, [[1, 1.0]]),
            entry("globex", "m", None, [[1, 1.0], [2, 2.0]])])
        self.assertEqual(result["written"], 3)
        with self.assertRaises(ObsError):
            store.write_batch([entry("acme", "m", None, [[2, 2.0]])])
        self.assertEqual(store.get_quota("acme")["points"], 1)
        self.assertEqual(store.get_quota("globex")["points"], 2)


class TestWriteBatchAtomicity(BatchCase):
    def test_rejected_batch_leaves_nothing_on_disk(self):
        first = self.store("atom")
        first.write_batch([entry(samples=[[1000, 1.0]])])
        with self.assertRaises(ObsError):
            first.write_batch([
                entry(samples=[[2000, 2.0]]),
                entry(samples=[[2000, 9.0]])])
        reopened = self.store("atom")
        self.assertEqual(reopened.stats()["series"], 1)
        self.assertEqual(reopened.query("acme", "m")[0]["points"], [[1000, 1.0]])

    def test_successful_batch_reads_back_fully_after_reopen(self):
        first = self.store("atom")
        first.write_batch([
            entry("acme", "m", {"k": "a"}, [[1000, 1.0]]),
            entry("acme", "m", {"k": "b"}, [[2000, 2.0]]),
            entry("globex", "m", None, [[3000, 3.0]])])
        first.write_batch([
            entry("acme", "m", {"k": "a"}, samples=[[1000, 9.0]]),
            entry("acme", "m", {"k": "b"}, samples=[[4000, 4.0]])], overwrite=True)
        reopened = self.store("atom")
        rows = {tuple(sorted(r["labels"].items())): r["points"]
                for r in reopened.query("acme", "m")}
        self.assertEqual(rows[(("k", "a"),)], [[1000, 9.0]])
        self.assertEqual(rows[(("k", "b"),)], [[2000, 2.0], [4000, 4.0]])
        self.assertEqual(reopened.query("globex", "m")[0]["points"], [[3000, 3.0]])
        self.assertEqual(first.stats()["writes"], 2)

    def test_pure_duplicate_batch_does_not_increment_writes(self):
        store = self.store()
        store.write_batch([entry(samples=[[1, 1.0]])])
        again = store.write_batch([entry(samples=[[1, 1.0]])])
        self.assertEqual((again["written"], again["duplicates"]), (0, 1))
        self.assertEqual(store.stats()["writes"], 1)

    def test_concurrent_readers_see_only_committed_states(self):
        store = self.store()
        store.set_quota("acme", None, 1_000_000)
        stop = threading.Event()
        violations = []

        def reader():
            while not stop.is_set():
                rows = store.query("acme", "m")
                counts = {tuple(sorted(r["labels"].items())): len(r["points"])
                          for r in rows}
                # Every writer series either does not exist or holds exactly 10
                # points; a partial commit would show intermediate counts.
                for count in counts.values():
                    if count not in (0, 10):
                        violations.append(count)

        def writer(worker):
            items = [entry("acme", "m", {"w": str(worker)},
                           [[worker * 1000 + seq, float(seq)] for seq in range(10)])]
            for _ in range(5):
                try:
                    store.write_batch(items)
                except ObsError:
                    pass  # the replay is all duplicates, which is fine
                items = list(reversed(items))

        threads = [threading.Thread(target=reader)]
        threads += [threading.Thread(target=writer, args=(w,)) for w in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads[1:]:
            thread.join(timeout=15)
        stop.set()
        threads[0].join(timeout=5)
        self.assertEqual(violations, [])


class TestBatchHttp(BatchCase):
    def setUp(self):
        super().setUp()
        root = os.path.join(self.tmp, "data")
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
    def request(self, method, path, payload=None):
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=body, method=method)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))
    def batch(self, payload):
        return self.request("POST", "/v1/series/batch", payload)

    def test_batch_success_and_shape(self):
        code, body = self.batch({"entries": [
            entry("acme", "m", {"k": "a"}, [[1000, 1.0]]),
            entry("globex", "n", None, [[2000, 2.0]])]})
        self.assertEqual(code, 202)
        self.assertEqual(body["written"], 2)
        self.assertEqual(len(body["results"]), 2)
        self.assertEqual(body["results"][0]["series_id"],
                         series_id_for("acme", "m", {"k": "a"}))

    def test_null_now_ms_and_overwrite(self):
        code, body = self.batch({"entries": [entry(samples=[[1000, 1.0]])]})
        self.assertEqual(code, 202)
        code, body = self.batch({"entries": [entry(samples=[[1000, 1.0]])],
                                 "now_ms": None, "overwrite": False})
        self.assertEqual((code, body["written"], body["duplicates"]), (202, 0, 1))
        code, body = self.batch({"entries": [entry(samples=[[1000, 9.0]])],
                                 "overwrite": True})
        self.assertEqual(code, 202)
        self.assertEqual(body["written"], 1)

    def test_structural_errors_return_400(self):
        self.batch({"entries": [entry(samples=[[1000, 1.0]])]})
        for payload in (
                {}, {"entries": []}, {"entries": "x"},
                {"entries": [{"tenant": "acme", "metric": "m"}]},
                {"entries": [entry()], "now_ms": True},
                {"entries": [entry()], "now_ms": 1.5},
                {"entries": [entry()], "now_ms": "1000"},
                {"entries": [entry()], "overwrite": 1},
                {"entries": [entry(metric="q", samples=[[9, 1.0]])], "now_ms": 5}):
            code, body = self.batch(payload)
            self.assertEqual(code, 400, repr(payload))
            self.assertIn("error", body)

    def test_conflict_and_quota_return_409_and_change_nothing(self):
        self.batch({"entries": [entry(samples=[[1000, 1.0]])]})
        code, body = self.batch({"entries": [entry(samples=[[1000, 2.0]])]})
        self.assertEqual(code, 409)
        self.assertIn("conflict", body["error"])
        code, _ = self.request("POST", "/v1/quotas",
                               {"tenant": "acme", "max_series": 0, "max_points": 0})
        self.assertEqual(code, 200)
        code, body = self.batch({"entries": [entry(metric="r", samples=[[5000, 5.0]])]})
        self.assertEqual(code, 409)
        self.assertIn("quota exceeded", body["error"])
        code, quota = self.request("GET", "/v1/quotas?tenant=acme")
        self.assertEqual((code, quota["series"], quota["points"]), (200, 1, 1))


class TestBatchCli(BatchCase):
    def run_cli(self, *argv):
        out, err = StringIO(), StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = cli_main(["--data-dir", os.path.join(self.tmp, "cli")] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_write_batch_success(self):
        payload = json.dumps([
            entry("acme", "m", {"k": "a"}, [[1000, 1.0]]),
            entry("globex", "n", None, [[2000, 2.0], [3000, 3.0]])])
        code, out, err = self.run_cli("write-batch", "--entries", payload)
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        body = json.loads(out)
        self.assertEqual((body["written"], body["duplicates"]), (3, 0))
        self.assertEqual(len(body["results"]), 2)

    def test_write_batch_now_and_overwrite_flags(self):
        payload = json.dumps([entry(samples=[[1000, 1.0], [1000, 1.0]])])
        code, _, err = self.run_cli("write-batch", "--entries", payload,
                                    "--now-ms", "1000")
        self.assertEqual(code, 0)
        future = json.dumps([entry(samples=[[1001, 1.0]])])
        code, out, err = self.run_cli("write-batch", "--entries", future,
                                      "--now-ms", "1000")
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")
        self.assertEqual(len(err.splitlines()), 1)
        self.assertIn("error", json.loads(err))
        differing = json.dumps([entry(samples=[[1000, 9.0]])])
        code, out, err = self.run_cli("write-batch", "--entries", differing)
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")
        code, out, err = self.run_cli("write-batch", "--entries", differing,
                                      "--overwrite")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["written"], 1)

    def test_malformed_entries_is_one_line_json_error(self):
        code, out, err = self.run_cli("write-batch", "--entries", "[not json")
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")
        self.assertEqual(len(err.splitlines()), 1)
        self.assertIn("error", json.loads(err))
        code, out, err = self.run_cli("write-batch", "--entries", json.dumps([]))
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")
        self.assertIn("error", json.loads(err))


if __name__ == "__main__":
    unittest.main()
