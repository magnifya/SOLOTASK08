"""Tests for sliding-window increase/rate queries: store, HTTP and CLI."""

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
        self.tmp = tempfile.mkdtemp(prefix="obsd-counter-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))

    def cquery(self, store, agg, **over):
        args = dict(start_ms=1000, end_ms=3000, step_ms=1000,
                    window_ms=10000, agg=agg)
        args.update(over)
        return store.query("acme", "m", **args)


class TestIncreaseRateSemantics(StoreCase):
    def test_spec_example_deltas_sum_to_twelve(self):
        store = self.store()
        store.write("acme", "m", {},
                    [[0, 10.0], [1000, 15.0], [2000, 3.0], [3000, 7.0]])
        rows = self.cquery(store, "increase")
        self.assertEqual(len(rows), 1)
        # 10->15 adds 5, 15->3 is a reset adding 3, 3->7 adds 4: total 12.
        self.assertEqual(rows[0]["points"],
                         [[1000, 5.0], [2000, 8.0], [3000, 12.0]])

    def test_rate_divides_by_first_last_sample_spacing_not_window(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 0.0], [2000, 10.0]])
        # The window is 10s long but the two readings are only 1s apart: no
        # extrapolation to the window length.
        rows = self.cquery(store, "rate", start_ms=2000, end_ms=2000,
                           window_ms=10000)
        self.assertEqual(rows[0]["points"], [[2000, 10.0]])
        # 2.5s spacing, increase 5 -> 2.0 per second.
        store.write("acme", "m2", {}, [[500, 0.0], [3000, 5.0]])
        rows = store.query("acme", "m2", start_ms=3000, end_ms=3000,
                           step_ms=1000, window_ms=10000, agg="rate")
        self.assertEqual(rows[0]["points"], [[3000, 2.0]])

    def test_first_reading_contributes_nothing(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 99.0], [2000, 100.0]])
        rows = self.cquery(store, "increase")
        self.assertEqual([p[1] for p in rows[0]["points"]], [None, 1.0, 1.0])

    def test_multiple_resets_each_counted_separately(self):
        store = self.store()
        store.write("acme", "m", {},
                    [[0, 10.0], [1000, 2.0], [2000, 8.0], [3000, 1.0]])
        rows = self.cquery(store, "increase")
        # t=1000: [10,2] -> reset adds 2
        # t=2000: [10,2,8] -> 2 + 6 = 8
        # t=3000: [10,2,8,1] -> 2 + 6 + 1 = 9
        self.assertEqual([p[1] for p in rows[0]["points"]], [2.0, 8.0, 9.0])

    def test_equal_readings_add_zero_and_constant_series_yields_zero(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 5.0], [2000, 5.0], [3000, 5.0]])
        rows = self.cquery(store, "increase")
        self.assertEqual([p[1] for p in rows[0]["points"]], [None, 0.0, 0.0])
        rows = self.cquery(store, "rate")
        self.assertEqual([p[1] for p in rows[0]["points"]], [None, 0.0, 0.0])

    def test_fewer_than_two_distinct_timestamps_is_null(self):
        store = self.store()
        store.write("acme", "m", {}, [[1500, 7.0]])
        for agg in ("increase", "rate"):
            rows = self.cquery(store, agg)
            self.assertEqual(rows[0]["points"],
                             [[1000, None], [2000, None], [3000, None]], agg)
        # Two readings need two distinct timestamps; an empty window is null too.
        store.write("acme", "m2", {}, [[4000, 1.0], [5000, 2.0]])
        rows = store.query("acme", "m2", start_ms=1000, end_ms=3000,
                           step_ms=1000, window_ms=10000, agg="increase")
        self.assertEqual(rows[0]["points"],
                         [[1000, None], [2000, None], [3000, None]])

    def test_only_inside_window_samples_are_used(self):
        store = self.store()
        store.write("acme", "m", {},
                    [[500, 100.0], [1000, 100.0], [2000, 1.0]])
        # Window (1000,2000]: the 1000 reading is on the excluded left edge and
        # must not be borrowed as the baseline; only the 2000 reading is in,
        # which alone cannot define an increase.
        rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                           step_ms=1000, window_ms=1000, agg="increase")
        self.assertEqual(rows[0]["points"], [[2000, None]])
        # Right edge included: (1500,2500] keeps just the 2000 reading.
        rows = store.query("acme", "m", start_ms=2500, end_ms=2500,
                           step_ms=1000, window_ms=1000, agg="increase")
        self.assertEqual(rows[0]["points"], [[2500, None]])
        # A wide window includes both 1000 and 2000: reset adds 1.
        rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                           step_ms=1000, window_ms=1001, agg="increase")
        self.assertEqual(rows[0]["points"], [[2000, 1.0]])

    def test_history_before_start_is_read_but_future_is_not(self):
        store = self.store()
        store.write("acme", "m", {}, [[-500, 1.0], [1000, 4.0], [2500, 9.0]])
        rows = self.cquery(store, "increase", window_ms=4000)
        # t=1000: history -500 and 1000 -> +3; t=2000: same pair, 2500 is
        # future -> +3; t=3000: all three -> 3 + 5 = 8.
        self.assertEqual([p[1] for p in rows[0]["points"]], [3.0, 3.0, 8.0])

    def test_full_grid_for_every_matching_series_and_empty_result(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"}, [[1000, 1.0], [2000, 2.0]])
        store.write("acme", "m", {"host": "b"}, [[9000, 1.0]])
        rows = self.cquery(store, "increase")
        by_host = {row["labels"]["host"]: row["points"] for row in rows}
        self.assertEqual(by_host["a"],
                         [[1000, None], [2000, 1.0], [3000, 1.0]])
        self.assertEqual(by_host["b"],
                         [[1000, None], [2000, None], [3000, None]])
        self.assertEqual(store.query("acme", "nope", start_ms=1000, end_ms=3000,
                                     step_ms=1000, window_ms=1000,
                                     agg="increase"), [])


class TestIncreaseRateErrors(StoreCase):
    def write_bad_file(self, store, metric, rows):
        """Persist raw JSON lines (allows NaN) and reopen to load them."""
        sid = store.write("acme", metric, {}, [[0, 0.0]])["series_id"]
        path = os.path.join(store.points_dir, sid + ".jsonl")
        with open(path, "w", encoding="utf-8") as handle:
            for stamp, value in rows:
                handle.write('{"t":%d,"v":%s}\n' % (stamp, value))

    def test_negative_sample_in_window_fails_whole_query(self):
        store = self.store()
        store.write("acme", "m", {},
                    [[1000, 1.0], [2000, 2.0], [3000, -1.0]])
        for agg in ("increase", "rate"):
            with self.assertRaises(ObsError):
                self.cquery(store, agg)

    def test_negative_sample_outside_window_does_not_trigger(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, -5.0], [2000, 1.0], [3000, 2.0]])
        # (1000,3000] excludes the negative reading on the left edge, so only
        # the two good readings are used: increase 2 - 1 = 1.
        rows = store.query("acme", "m", start_ms=3000, end_ms=3000,
                           step_ms=1000, window_ms=2000, agg="increase")
        self.assertEqual(rows[0]["points"], [[3000, 1.0]])

    def test_negative_sample_filtered_out_by_labels_is_ignored(self):
        store = self.store()
        store.write("acme", "m", {"host": "ok"}, [[1000, 1.0], [2000, 2.0]])
        store.write("acme", "m", {"host": "bad"}, [[1000, -9.0], [2000, -8.0]])
        rows = store.query("acme", "m", labels={"host": "ok"},
                           start_ms=1000, end_ms=2000, step_ms=1000,
                           window_ms=10000, agg="increase")
        self.assertEqual(rows[0]["points"], [[1000, None], [2000, 1.0]])

    def test_non_finite_samples_fail(self):
        for name, literal in (("nan", "NaN"), ("inf", "Infinity"),
                              ("ninf", "-Infinity")):
            sub = "d-" + name
            store = self.store(sub)
            self.write_bad_file(store, "m", [[1000, 1.0], [2000, literal]])
            reopened = SeriesStore(os.path.join(self.tmp, sub))
            with self.assertRaises(ObsError, msg=literal):
                reopened.query("acme", "m", start_ms=2000, end_ms=2000,
                               step_ms=1000, window_ms=10000, agg="rate")

    def test_missing_window_params_and_bad_times_fail_even_without_series(self):
        store = self.store()
        base = dict(start_ms=0, end_ms=1000, step_ms=1000,
                    window_ms=1000, agg="increase")

        def expect(**over):
            args = dict(base)
            args.update(over)
            with self.assertRaises(ObsError, msg=repr(over)):
                store.query("acme", "nope", **args)

        expect(window_ms=None)
        expect(agg=None)
        expect(start_ms=None)
        expect(end_ms=None)
        expect(step_ms=None)
        expect(window_ms=0)
        expect(window_ms=1.5)
        expect(start_ms=2000, end_ms=1000)

    def test_counter_aggs_need_a_window_and_stay_out_of_rollup(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        with self.assertRaises(ObsError):
            store.query("acme", "m", agg="increase")
        with self.assertRaises(ObsError):
            store.query("acme", "m", start_ms=0, end_ms=1000,
                        step_ms=1000, agg="rate")
        with self.assertRaises(ObsError):
            store.rollup("acme", "m", {}, 1000, "increase")


class TestIncreaseRateGroupBy(StoreCase):
    def test_series_reduced_independently_then_non_null_values_sum(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"},
                    [[1000, 1.0], [2000, 3.0]])           # increase 2
        store.write("acme", "m", {"host": "b"},
                    [[1000, 10.0], [2000, 7.0]])          # reset -> 7
        store.write("acme", "m", {"host": "c"},
                    [[2000, 42.0]])                       # one point -> null
        rows = store.query("acme", "m", group_by=[], agg="increase",
                           start_ms=1000, end_ms=2000, step_ms=1000,
                           window_ms=10000)
        # At t=1000 every series has at most one in-window reading -> null.
        # At t=2000 the two computable series sum 2 + 7 = 9; c adds nothing.
        self.assertEqual(rows, [{"labels": {},
                                 "points": [[1000, None], [2000, 9.0]]}])
        rows = store.query("acme", "m", group_by=[], agg="rate",
                           start_ms=2000, end_ms=2000, step_ms=1000,
                           window_ms=10000)
        self.assertEqual(rows[0]["points"], [[2000, 9.0]])

    def test_no_differences_across_series_and_groups_keep_full_grid(self):
        store = self.store()
        store.write("acme", "m", {"region": "x"}, [[1000, 100.0], [2000, 1.0]])
        store.write("acme", "m", {"region": ""}, [[2000, 5.0]])
        rows = store.query("acme", "m", group_by=["region"], agg="increase",
                           start_ms=1000, end_ms=2000, step_ms=1000,
                           window_ms=10000)
        labels = [row["labels"] for row in rows]
        self.assertEqual(labels, [{"region": ""}, {"region": "x"}])
        # Missing... no missing-key group here; empty-string group has a single
        # point -> full null grid; "x" is a within-series reset adding 1.
        self.assertEqual(rows[0]["points"], [[1000, None], [2000, None]])
        self.assertEqual(rows[1]["points"], [[1000, None], [2000, 1.0]])

    def test_negative_in_one_series_fails_the_group_query(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"}, [[1000, 1.0], [2000, 2.0]])
        store.write("acme", "m", {"host": "b"}, [[1000, -3.0], [2000, 4.0]])
        with self.assertRaises(ObsError):
            store.query("acme", "m", group_by=[], agg="increase",
                        start_ms=2000, end_ms=2000, step_ms=1000,
                        window_ms=10000)


class TestCounterSnapshotAndPersistence(StoreCase):
    def test_query_does_not_mutate_store(self):
        store = self.store()
        store.write("acme", "m", {},
                    [[1000, 10.0], [2000, 3.0], [3000, 7.0]])
        before = store.stats()
        for _ in range(5):
            store.query("acme", "m", start_ms=1000, end_ms=3000,
                        step_ms=1000, window_ms=10000, agg="rate")
        self.assertEqual(store.stats(), before)

    def test_results_are_stable_after_reopen(self):
        store = self.store("keep")
        store.write("acme", "m", {"host": "a"},
                    [[500, 10.0], [1000, 4.0], [1500, 6.0]])
        kwargs = dict(group_by=["host"], agg="increase", start_ms=1000,
                      end_ms=3000, step_ms=1000, window_ms=1000)
        expected = store.query("acme", "m", **kwargs)
        reopened = self.store("keep")
        self.assertEqual(reopened.query("acme", "m", **kwargs), expected)

    def test_concurrent_writes_and_retention_never_tear_a_snapshot(self):
        store = self.store()
        stamps = list(range(1000, 2000, 100))
        store.write("acme", "m", {}, [[t, 1.0] for t in stamps])
        stop = threading.Event()

        def flip(value):
            while not stop.is_set():
                store.write("acme", "m", {}, [[t, float(value)] for t in stamps],
                            overwrite=True)
                value = 3 - value

        def prune():
            while not stop.is_set():
                store.enforce_retention("acme", 1500)

        threads = [threading.Thread(target=flip, args=(2,)),
                   threading.Thread(target=prune)]
        for thread in threads:
            thread.start()
        try:
            for _ in range(400):
                rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                                   step_ms=1000, window_ms=100000,
                                   agg="increase")
                # A coherent snapshot of equal values has a zero increase,
                # whether all ten points or the five post-retention survive.
                self.assertEqual(rows[0]["points"][0][1], 0.0)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)


class TestCounterHttp(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-counter-http-")
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.store.write("acme", "m", {"host": "a"},
                         [[1000, 10.0], [2000, 15.0], [3000, 3.0], [4000, 7.0]])
        self.store.write("acme", "bad", {}, [[1000, 1.0], [2000, -1.0]])

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def request(self, path):
        import json as _json
        import urllib.error
        import urllib.request
        try:
            with urllib.request.urlopen(self.base + path, timeout=10) as response:
                return response.status, _json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, _json.loads(exc.read().decode("utf-8"))

    def test_increase_rate_keep_existing_shape(self):
        code, body = self.request(
            "/v1/query?tenant=acme&metric=m&start=1000&end=4000"
            "&step=1000&window=10000&agg=increase")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"series": [{"labels": {"host": "a"},
                             "points": [[1000, None], [2000, 5.0],
                                        [3000, 8.0], [4000, 12.0]]}]})
        code, body = self.request(
            "/v1/query?tenant=acme&metric=m&start=4000&end=4000"
            "&step=1000&window=10000&agg=rate")
        self.assertEqual(code, 200)
        # 12 over the 3s first/last spacing.
        self.assertEqual(body["series"][0]["points"], [[4000, 4.0]])

    def test_errors_are_400_json(self):
        for path in (
                # Negative sample actually inside the window.
                "/v1/query?tenant=acme&metric=bad&start=2000&end=2000"
                "&step=1000&window=10000&agg=increase",
                # Missing window for a window-only aggregation.
                "/v1/query?tenant=acme&metric=m&start=0&end=1000"
                "&step=1000&agg=rate",
                # Bad window parameters even with no matching series.
                "/v1/query?tenant=acme&metric=nope&start=0&end=1000"
                "&step=1000&window=0&agg=increase",
                "/v1/query?tenant=acme&metric=nope&start=0&end=1000"
                "&step=1000&window=1000&agg=nope"):
            code, body = self.request(path)
            self.assertEqual(code, 400, path)
            self.assertIn("error", body)

    def test_negative_outside_window_is_not_an_error(self):
        code, body = self.request(
            "/v1/query?tenant=acme&metric=bad&start=9000&end=9000"
            "&step=1000&window=1000&agg=increase")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"series": [{"labels": {},
                             "points": [[9000, None]]}]})


class TestCounterCli(StoreCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        data_dir = os.path.join(self.tmp, "cli")
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", data_dir] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_cli_success_and_failure(self):
        code, _, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "m",
            "--sample", "1000:10", "--sample", "2000:15",
            "--sample", "3000:3", "--sample", "4000:7")
        self.assertEqual(code, 0, err)
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--start", "4000", "--end", "4000", "--step", "1000",
            "--window-ms", "10000", "--agg", "increase")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out),
                         {"series": [{"labels": {}, "points": [[4000, 12.0]]}]})
        # A negative in-window sample: one single-line JSON error, non-zero,
        # nothing on stdout.
        code, _, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "bad",
            "--sample", "1000:1", "--sample", "2000:-1")
        self.assertEqual(code, 0, err)
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "bad",
            "--start", "2000", "--end", "2000", "--step", "1000",
            "--window-ms", "10000", "--agg", "rate")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        lines = err.splitlines()
        self.assertEqual(len(lines), 1)
        self.assertIn("error", json.loads(lines[0]))
        # increase/rate without a window is rejected by the store as well.
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--start", "0", "--end", "1000", "--step", "1000", "--agg", "rate")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("error", json.loads(err.splitlines()[-1]))
        # Alert rules keep the original five aggregations.
        code, out, err = self.run_cli(
            "rule-add", "--tenant", "acme", "--metric", "m",
            "--comparator", "<", "--threshold", "1",
            "--window-ms", "1000", "--agg", "increase", "--severity", "warning")
        self.assertEqual(code, 1)
        self.assertIn("error", json.loads(err.splitlines()[-1]))


if __name__ == "__main__":
    unittest.main()
