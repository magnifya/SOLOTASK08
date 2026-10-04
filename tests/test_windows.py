"""Tests for sliding-window queries: SeriesStore.query, HTTP and CLI."""

import contextlib
import io
import json
import os
import shutil
import tempfile
import threading
import unittest

from obsd import AlertEngine, ObsError, SeriesStore, create_server
from obsd.cli import main as cli_main


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-win-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))
    def wquery(self, store, **over):
        args = dict(start_ms=1000, end_ms=3000, step_ms=1000,
                    window_ms=1000, agg="sum")
        args.update(over)
        return store.query("acme", "m", **args)


class TestWindowSemantics(StoreCase):
    def fill(self, store, labels=None):
        samples = [[-500, 1.0], [0, 2.0], [500, 4.0], [1000, 8.0],
                   [1500, 16.0], [2000, 32.0]]
        store.write("acme", "m", labels or {}, samples)
        return samples

    def test_window_is_left_open_right_closed_and_reads_history(self):
        store = self.store()
        self.fill(store)
        rows = self.wquery(store)
        self.assertEqual(len(rows), 1)
        # t=1000: (0,1000] holds 500 and 1000 (0 excluded; pre-start history
        # read); t=2000: (1000,2000] holds 1500 and 2000 (1000 excluded);
        # t=3000: empty -> null.
        self.assertEqual(rows[0]["points"],
                         [[1000, 12.0], [2000, 48.0], [3000, None]])

    def test_grid_is_start_plus_k_step_not_past_end(self):
        store = self.store()
        self.fill(store)
        # end need not line up: 1000 and 2000 are kept, 3000 > 2500 is not.
        rows = self.wquery(store, end_ms=2500)
        self.assertEqual([p[0] for p in rows[0]["points"]], [1000, 2000])
        # start == end yields exactly one evaluation time.
        rows = self.wquery(store, start_ms=2000, end_ms=2000)
        self.assertEqual(rows[0]["points"], [[2000, 48.0]])

    def test_step_need_not_divide_window_or_align_to_buckets(self):
        store = self.store()
        self.fill(store)
        rows = self.wquery(store, start_ms=1000, end_ms=2000,
                           step_ms=600, window_ms=700)
        # t=1000: (300,1000] -> 500,1000; t=1600: (900,1600] -> 1000,1500.
        self.assertEqual([p[0] for p in rows[0]["points"]], [1000, 1600])
        self.assertEqual([p[1] for p in rows[0]["points"]], [12.0, 24.0])

    def test_all_aggregations_and_empty_window_null_even_for_count(self):
        store = self.store()
        self.fill(store)
        expected = {"sum": 48.0, "avg": 24.0, "min": 16.0,
                    "max": 32.0, "count": 2}
        for agg, value in expected.items():
            rows = self.wquery(store, agg=agg)
            self.assertEqual(rows[0]["points"][1], [2000, value], agg)
            self.assertIsNone(rows[0]["points"][2][1], agg)

    def test_boundary_samples_are_excluded_left_included_right(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 5.0], [1000, 7.0]])
        rows = self.wquery(store, start_ms=1000, end_ms=1000,
                           step_ms=1000, window_ms=1000, agg="sum")
        # Exactly one window length earlier is excluded; the evaluation time is in.
        self.assertEqual(rows[0]["points"], [[1000, 7.0]])

    def test_windows_never_read_past_their_evaluation_time(self):
        store = self.store()
        store.write("acme", "m", {}, [[1500, 100.0]])
        rows = self.wquery(store, start_ms=1000, end_ms=2000,
                           step_ms=1000, window_ms=10000, agg="sum")
        self.assertEqual(rows[0]["points"], [[1000, None], [2000, 100.0]])

    def test_every_matching_series_gets_the_full_grid_even_when_all_empty(self):
        store = self.store()
        self.fill(store, {"host": "a"})
        store.write("acme", "m", {"host": "b"}, [[9000, 1.0]])
        rows = self.wquery(store)
        self.assertEqual(len(rows), 2)
        self.assertIn("series_id", rows[0])
        self.assertEqual(len(rows[0]["series_id"]), 64)
        by_host = {row["labels"]["host"]: row["points"] for row in rows}
        self.assertEqual(by_host["a"], [[1000, 12.0], [2000, 48.0], [3000, None]])
        self.assertEqual(by_host["b"], [[1000, None], [2000, None], [3000, None]])
        # No matching series at all -> empty array, not null grids.
        self.assertEqual(store.query("acme", "nope", start_ms=1000, end_ms=3000,
                                     step_ms=1000, window_ms=1000, agg="sum"), [])

    def test_rows_are_ordered_by_series_id(self):
        store = self.store()
        self.fill(store, {"host": "a"})
        self.fill(store, {"host": "b"})
        rows = self.wquery(store)
        ids = [row["series_id"] for row in rows]
        self.assertEqual(ids, sorted(ids))


class TestWindowGroupBy(StoreCase):
    def fill(self, store):
        store.write("acme", "m", {"host": "a"}, [[500, 1.0], [1000, 3.0]])
        store.write("acme", "m", {"host": "b"}, [[1000, 10.0]])
        store.write("acme", "m", {"host": "c", "region": "x"}, [[9000, 1.0]])
        store.write("acme", "m", {"host": "d", "region": ""}, [[1000, 2.0]])
        return store

    def test_group_pools_raw_samples_with_same_timestamp_counted_twice(self):
        store = self.fill(self.store())
        # group_by=[] merges every match: hosts a (500 and 1000), b (1000) and
        # d (1000) pool into four raw samples for the first window.
        rows = store.query("acme", "m", group_by=[], agg="count",
                           start_ms=1000, end_ms=2000, step_ms=1000, window_ms=1000)
        self.assertEqual(rows, [{"labels": {}, "points": [[1000, 4], [2000, None]]}])
        rows = store.query("acme", "m", group_by=[], agg="avg",
                           start_ms=1000, end_ms=1000, step_ms=1000, window_ms=1000)
        # (1 + 3 + 10 + 2) / 4 across three series sharing timestamp 1000.
        self.assertEqual(rows[0]["points"], [[1000, 16.0 / 4.0]])
        self.assertNotIn("series_id", rows[0])

    def test_groups_keep_full_grid_and_missing_differs_from_empty_string(self):
        store = self.fill(self.store())
        rows = store.query("acme", "m", group_by=["region"], agg="sum",
                           start_ms=1000, end_ms=2000, step_ms=1000, window_ms=1000)
        labels = [row["labels"] for row in rows]
        # Missing key, empty-string value and "x" are three groups in token order.
        self.assertEqual(labels, [{}, {"region": ""}, {"region": "x"}])
        # The "x" series has no sample anywhere near the grid: full null grid.
        self.assertEqual(rows[2]["points"], [[1000, None], [2000, None]])
        # Missing-region group pools hosts a and b; empty-string group is host d.
        self.assertEqual(rows[0]["points"], [[1000, 14.0], [2000, None]])
        self.assertEqual(rows[1]["points"], [[1000, 2.0], [2000, None]])

    def test_first_group_window_reads_pre_start_history(self):
        store = self.fill(self.store())
        rows = store.query("acme", "m", labels={"host": "a"}, group_by=[],
                           agg="sum", start_ms=1000, end_ms=1000,
                           step_ms=1000, window_ms=1000)
        # (0,1000]: the 500 history sample and the 1000 sample.
        self.assertEqual(rows[0]["points"], [[1000, 4.0]])
        rows = store.query("acme", "m", labels={"host": "a"}, group_by=[],
                           agg="sum", start_ms=1000, end_ms=1000,
                           step_ms=1000, window_ms=400)
        # (600,1000]: the 500 sample has left the window.
        self.assertEqual(rows[0]["points"], [[1000, 3.0]])
        rows = store.query("acme", "m", labels={"host": "a"}, group_by=[],
                           agg="sum", start_ms=1000, end_ms=1000,
                           step_ms=1000, window_ms=500)
        # Left edge excluded: (500,1000] keeps only the 1000 sample.
        self.assertEqual(rows[0]["points"], [[1000, 3.0]])

    def test_no_matching_series_yields_empty_array(self):
        store = self.fill(self.store())
        rows = store.query("acme", "nope", group_by=[], agg="sum",
                           start_ms=0, end_ms=1000, step_ms=1000, window_ms=1000)
        self.assertEqual(rows, [])


class TestWindowValidation(StoreCase):
    def test_invalid_arguments_raise_obs_error(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        base = dict(start_ms=0, end_ms=1000, step_ms=1000, window_ms=1000, agg="sum")

        def expect(**over):
            args = dict(base)
            args.update(over)
            with self.assertRaises(ObsError, msg=repr(over)):
                store.query("acme", "m", **args)

        # Window length: bools, floats, zero, negative, missing required peers.
        expect(window_ms=True)
        expect(window_ms=0)
        expect(window_ms=-1)
        expect(window_ms=1.5)
        expect(window_ms=1.0)
        expect(start_ms=None)
        expect(end_ms=None)
        expect(step_ms=None)
        expect(agg=None)
        # Bounds and types.
        expect(start_ms=True)
        expect(end_ms=1.5)
        expect(start_ms=1.5)
        expect(step_ms=True)
        expect(step_ms=1.5)
        expect(step_ms=0)
        expect(step_ms=-10)
        expect(start_ms=2000, end_ms=1000)
        expect(agg="median")
        # Illegal group settings.
        expect(group_by="host")
        expect(group_by=["host", "host"])
        expect(group_by=[""])
        expect(group_by=[1])

    def test_unknown_agg_is_rejected_even_with_window(self):
        store = self.store()
        with self.assertRaises(ObsError):
            store.query("acme", "m", start_ms=0, end_ms=1000, step_ms=1000,
                        window_ms=1000, agg="median")

    def test_legacy_query_is_unchanged_without_window(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0], [999, 2.0], [1000, 3.0]])
        self.assertEqual(store.query("acme", "m", step_ms=1000, agg="sum")[0]["points"],
                         [[0, 3.0], [1000, 3.0]])
        # Floats remain accepted by the legacy path.
        self.assertEqual(store.query("acme", "m", step_ms=1000.0, agg="sum")[0]["points"],
                         [[0, 3.0], [1000, 3.0]])


class TestWindowSnapshotAndPersistence(StoreCase):
    def test_query_does_not_mutate_store(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        before = store.stats()
        for _ in range(5):
            self.wquery(store)
        self.assertEqual(store.stats(), before)

    def test_results_are_stable_after_reopen(self):
        store = self.store("keep")
        store.write("acme", "m", {"host": "a"},
                    [[500, 1.0], [1000, 3.0], [1500, 9.0]])
        expected = store.query("acme", "m", group_by=["host"], agg="avg",
                               start_ms=1000, end_ms=3000, step_ms=1000,
                               window_ms=1000)
        reopened = self.store("keep")
        self.assertEqual(reopened.query("acme", "m", group_by=["host"], agg="avg",
                                        start_ms=1000, end_ms=3000, step_ms=1000,
                                        window_ms=1000), expected)

    def test_concurrent_writes_and_retention_never_tear_a_snapshot(self):
        store = self.store()
        stamps = list(range(1000, 2000, 100))
        store.write("acme", "m", {}, [[t, 1.0] for t in stamps])
        stop = threading.Event()

        def flip(value):
            while not stop.is_set():
                store.write("acme", "m", {}, [[t, float(value)] for t in stamps],
                            overwrite=True)
                value = 3 - value  # 1 <-> 2

        def prune():
            while not stop.is_set():
                store.enforce_retention("acme", 1500)

        threads = [threading.Thread(target=flip, args=(2,)),
                   threading.Thread(target=prune)]
        for thread in threads:
            thread.start()
        try:
            # One giant window over a single grid point: every value must come
            # from one coherent snapshot. Ten 1s -> 10, ten 2s -> 20, the five
            # survivors after pruning are 1s -> 5 or 2s -> 10; any torn read
            # lands outside this set.
            for _ in range(400):
                rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                                   step_ms=1000, window_ms=100000, agg="sum")
                self.assertIn(rows[0]["points"][0][1], {5, 10, 20})
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)


class TestWindowHttp(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-win-http-")
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0)
        import urllib.request
        self.urlopen = urllib.request.urlopen
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.store.write("acme", "m", {"host": "a"},
                         [[500, 1.0], [1000, 3.0], [2000, 9.0]])

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def request(self, path):
        import json as _json
        import urllib.error
        try:
            with self.urlopen(self.base + path, timeout=10) as response:
                return response.status, _json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, _json.loads(exc.read().decode("utf-8"))

    def test_window_query_uses_existing_result_shape(self):
        code, body = self.request(
            "/v1/query?tenant=acme&metric=m&start=1000&end=3000"
            "&step=1000&window=1000&agg=sum")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"series": [{"labels": {"host": "a"},
                             "points": [[1000, 4.0], [2000, 9.0], [3000, None]]}]})

    def test_window_errors_are_400_json(self):
        for path in (
                "/v1/query?tenant=acme&metric=m&end=1000&step=1000&window=1000&agg=sum",
                "/v1/query?tenant=acme&metric=m&start=0&end=1000&step=1000&window=1.5&agg=sum",
                "/v1/query?tenant=acme&metric=m&start=0&end=1000&step=1000&window=-5&agg=sum",
                "/v1/query?tenant=acme&metric=m&start=2000&end=1000&step=1000&window=1000&agg=sum",
                "/v1/query?tenant=acme&metric=m&start=0&end=1000&step=1000&window=1000&agg=median",
                "/v1/query?tenant=acme&metric=m&start=0&end=1000&step=1000"
                "&window=1000&agg=sum&group_by=bad"):
            code, body = self.request(path)
            self.assertEqual(code, 400, path)
            self.assertIn("error", body)


class TestWindowCli(StoreCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        data_dir = os.path.join(self.tmp, "cli")
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", data_dir] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_window_query_success_and_failure(self):
        code, _, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "m",
            "--sample", "1000:3", "--sample", "2000:9")
        self.assertEqual(code, 0, err)
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--start", "1000", "--end", "2000", "--step", "1000",
            "--window-ms", "1000", "--agg", "sum")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out),
                         {"series": [{"labels": {},
                                      "points": [[1000, 3.0], [2000, 9.0]]}]})
        # Missing required settings: one JSON error line on stderr, non-zero,
        # and nothing at all on stdout.
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--start", "0", "--end", "1000", "--step", "1000",
            "--window-ms", "1000")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("agg", json.loads(err)["error"])
        # Non-integer / non-numeric window fails argument parsing; the final
        # stderr line is still the one-line JSON error.
        for bad in ("1.5", "true", "abc", "0"):
            code, out, err = self.run_cli(
                "query", "--tenant", "acme", "--metric", "m",
                "--start", "0", "--end", "1000", "--step", "1000",
                "--window-ms", bad, "--agg", "sum")
            self.assertEqual(code, 1, bad)
            self.assertEqual(out, "")
            self.assertIn("error", json.loads(err.strip().splitlines()[-1]))


if __name__ == "__main__":
    unittest.main()
