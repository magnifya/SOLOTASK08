"""Tests for sliding-window counter queries (increase/rate)."""

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


class TestCounterSemantics(StoreCase):
    def test_increase_counts_resets_and_ignores_first_reading(self):
        store = self.store()
        # Deltas: +5, reset -> +3, +4 = 12.
        store.write("acme", "m", {}, [[1000, 10.0], [2000, 15.0],
                                      [3000, 3.0], [4000, 7.0]])
        rows = store.query("acme", "m", start_ms=4000, end_ms=4000,
                           step_ms=1000, window_ms=10000, agg="increase")
        self.assertEqual(rows[0]["points"], [[4000, 12.0]])

    def test_rate_divides_by_first_last_sample_span_seconds(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 10.0], [2000, 15.0],
                                      [3000, 3.0], [4000, 7.0]])
        rows = store.query("acme", "m", start_ms=4000, end_ms=4000,
                           step_ms=1000, window_ms=10000, agg="rate")
        # 12 over (4000-1000)ms = 3s -> 4.0, not divided by the window length.
        self.assertEqual(rows[0]["points"], [[4000, 4.0]])

    def test_multiple_resets_are_handled_separately(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 10.0], [1, 2.0], [2, 1.0], [3, 5.0]])
        rows = store.query("acme", "m", start_ms=3, end_ms=3,
                           step_ms=1, window_ms=100, agg="increase")
        # reset -> 2, reset -> 1, +4 = 7.
        self.assertEqual(rows[0]["points"], [[3, 7.0]])

    def test_window_boundaries_match_sliding_window_rules(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 5.0], [1000, 7.0], [2000, 9.0]])
        # t=2000, (1000,2000]: left edge excluded -> one sample -> null.
        rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                           step_ms=1000, window_ms=1000, agg="increase")
        self.assertEqual(rows[0]["points"], [[2000, None]])
        # t=2000, (0,2000]: the t=0 sample is on the excluded left edge,
        # so only 1000->2000 counts -> +2.
        rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                           step_ms=1000, window_ms=2000, agg="increase")
        self.assertEqual(rows[0]["points"], [[2000, 2.0]])
        # A slightly wider window pulls in the pre-start history: +2 +2 = 4.
        rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                           step_ms=1000, window_ms=2500, agg="increase")
        self.assertEqual(rows[0]["points"], [[2000, 4.0]])
        # Never reads past the evaluation time.
        rows = store.query("acme", "m", start_ms=1000, end_ms=1000,
                           step_ms=1000, window_ms=10000, agg="increase")
        self.assertEqual(rows[0]["points"], [[1000, 2.0]])

    def test_fewer_than_two_timestamps_is_null_and_unchanged_is_zero(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 5.0], [2000, 5.0]])
        rows = store.query("acme", "m", start_ms=1000, end_ms=2000,
                           step_ms=1000, window_ms=1500, agg="increase")
        # t=1000: one sample -> null; t=2000: two equal readings -> zero.
        self.assertEqual(rows[0]["points"], [[1000, None], [2000, 0.0]])
        rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                           step_ms=1000, window_ms=1500, agg="rate")
        self.assertEqual(rows[0]["points"], [[2000, 0.0]])

    def test_every_matching_series_gets_the_full_grid(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"}, [[1000, 1.0], [2000, 4.0]])
        store.write("acme", "m", {"host": "b"}, [[9000, 1.0]])
        rows = store.query("acme", "m", start_ms=2000, end_ms=3000,
                           step_ms=1000, window_ms=1500, agg="increase")
        self.assertEqual(len(rows), 2)
        by_host = {row["labels"]["host"]: row["points"] for row in rows}
        self.assertEqual(by_host["a"], [[2000, 3.0], [3000, None]])
        self.assertEqual(by_host["b"], [[2000, None], [3000, None]])
        self.assertEqual(store.query("acme", "nope", start_ms=0, end_ms=1000,
                                     step_ms=1000, window_ms=1000,
                                     agg="increase"), [])


class TestCounterGroupBy(StoreCase):
    def test_group_sums_per_series_results_without_cross_series_diffs(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"}, [[1000, 10.0], [2000, 15.0]])
        store.write("acme", "m", {"host": "b"}, [[1000, 100.0], [2000, 90.0]])
        rows = store.query("acme", "m", group_by=[], agg="increase",
                           start_ms=2000, end_ms=2000, step_ms=1000,
                           window_ms=1500)
        # host a: +5; host b: reset -> 90; pooled raw diff would be wrong.
        self.assertEqual(rows, [{"labels": {}, "points": [[2000, 95.0]]}])

    def test_all_null_group_is_null_and_groups_keep_full_grid(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"}, [[1000, 1.0], [2000, 4.0]])
        store.write("acme", "m", {"host": "b"}, [[500, 1.0]])
        rows = store.query("acme", "m", group_by=["host"], agg="increase",
                           start_ms=2000, end_ms=3000, step_ms=1000,
                           window_ms=1500)
        self.assertEqual([row["labels"] for row in rows],
                         [{"host": "a"}, {"host": "b"}])
        self.assertEqual(rows[0]["points"], [[2000, 3.0], [3000, None]])
        # Single-sample series: null at every grid point, grid kept.
        self.assertEqual(rows[1]["points"], [[2000, None], [3000, None]])

    def test_grouped_rate_and_missing_vs_empty_string(self):
        store = self.store()
        store.write("acme", "m", {"region": "x"}, [[1000, 0.0], [3000, 6.0]])
        store.write("acme", "m", {"region": ""}, [[1000, 0.0], [3000, 2.0]])
        store.write("acme", "m", {}, [[1000, 0.0], [3000, 4.0]])
        rows = store.query("acme", "m", group_by=["region"], agg="rate",
                           start_ms=3000, end_ms=3000, step_ms=1000,
                           window_ms=2500)
        self.assertEqual([row["labels"] for row in rows],
                         [{}, {"region": ""}, {"region": "x"}])
        self.assertEqual(rows[0]["points"], [[3000, 2.0]])   # 4 / 2s
        self.assertEqual(rows[1]["points"], [[3000, 1.0]])   # 2 / 2s
        self.assertEqual(rows[2]["points"], [[3000, 3.0]])   # 6 / 2s


class TestCounterValidation(StoreCase):
    def test_counter_aggs_require_window_and_valid_window_params(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        for agg in ("increase", "rate"):
            with self.assertRaises(ObsError, msg=agg):
                store.query("acme", "m", start_ms=0, end_ms=1000,
                            step_ms=1000, agg=agg)
            with self.assertRaises(ObsError, msg=agg):
                store.query("acme", "m", agg=agg)
            # The sliding-window constraints apply unchanged.
            with self.assertRaises(ObsError, msg=agg):
                store.query("acme", "m", start_ms=0, end_ms=1000,
                            step_ms=1000, window_ms=0, agg=agg)
            with self.assertRaises(ObsError, msg=agg):
                store.query("acme", "m", start_ms=2000, end_ms=1000,
                            step_ms=1000, window_ms=1000, agg=agg)

    def test_params_are_validated_even_without_matching_series(self):
        store = self.store()
        with self.assertRaises(ObsError):
            store.query("acme", "nope", agg="increase")
        with self.assertRaises(ObsError):
            store.query("acme", "nope", start_ms=0, end_ms=1000, step_ms=1000,
                        window_ms=-1, agg="rate")

    def test_negative_or_non_finite_sample_in_window_fails_whole_query(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, -1.0], [2000, 2.0]])
        store.write("acme", "ok", {}, [[1000, 1.0], [2000, 3.0]])
        for agg in ("increase", "rate"):
            with self.assertRaises(ObsError, msg=agg):
                store.query("acme", "m", start_ms=2000, end_ms=2000,
                            step_ms=1000, window_ms=1500, agg=agg)
        store.write("acme", "inf", {}, [[1000, 1.0], [2000, float("inf")]])
        store.write("acme", "nan", {}, [[1000, 1.0], [2000, float("nan")]])
        for metric in ("inf", "nan"):
            with self.assertRaises(ObsError, msg=metric):
                store.query("acme", metric, start_ms=2000, end_ms=2000,
                            step_ms=1000, window_ms=1500, agg="increase")
        # No partial result: the good series is not returned either.
        with self.assertRaises(ObsError):
            store.query("acme", "m", labels={}, start_ms=2000, end_ms=2000,
                        step_ms=1000, window_ms=1500, agg="increase")

    def test_samples_outside_window_or_filtered_out_never_fail(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"}, [[-500, -1.0], [2000, 2.0]])
        store.write("acme", "m", {"host": "b"}, [[1500, -3.0], [2000, 4.0]])
        # The negative samples sit outside (1500,2000] / are label-filtered.
        rows = store.query("acme", "m", labels={"host": "a"},
                           start_ms=2000, end_ms=2000, step_ms=1000,
                           window_ms=1000, agg="increase")
        self.assertEqual(rows[0]["points"], [[2000, None]])
        # Matchers filter the bad series out as well.
        rows = store.query("acme", "m",
                           matchers=[{"key": "host", "op": "=", "value": "a"}],
                           start_ms=2000, end_ms=2000, step_ms=1000,
                           window_ms=1000, agg="increase")
        self.assertEqual(len(rows), 1)


class TestCounterStability(StoreCase):
    def test_query_does_not_mutate_and_is_stable_after_reopen(self):
        store = self.store("keep")
        store.write("acme", "m", {"host": "a"},
                    [[500, 1.0], [1000, 3.0], [1500, 2.0]])
        args = dict(start_ms=1000, end_ms=3000, step_ms=1000,
                    window_ms=1000, agg="increase")
        before = store.stats()
        expected = store.query("acme", "m", **args)
        for _ in range(5):
            store.query("acme", "m", **args)
        self.assertEqual(store.stats(), before)
        reopened = self.store("keep")
        self.assertEqual(reopened.query("acme", "m", **args), expected)

    def test_rollup_and_legacy_aggs_still_reject_counter_aggs(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        for agg in ("increase", "rate"):
            with self.assertRaises(ObsError, msg=agg):
                store.rollup("acme", "m", {}, 1000, agg)
        # Legacy sliding-window aggregations are untouched.
        rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                           step_ms=1000, window_ms=1500, agg="sum")
        self.assertEqual(rows[0]["points"], [[2000, 3.0]])


class TestCounterHttp(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-counter-http-")
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.store.write("acme", "m", {}, [[1000, 10.0], [2000, 15.0],
                                           [3000, 3.0], [4000, 7.0]])

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def request(self, path):
        import urllib.error
        import urllib.request
        try:
            with urllib.request.urlopen(self.base + path, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_increase_and_rate_over_http(self):
        code, body = self.request(
            "/v1/query?tenant=acme&metric=m&start=4000&end=4000"
            "&step=1000&window=10000&agg=increase")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"series": [{"labels": {},
                                            "points": [[4000, 12.0]]}]})
        code, body = self.request(
            "/v1/query?tenant=acme&metric=m&start=4000&end=4000"
            "&step=1000&window=10000&agg=rate")
        self.assertEqual(code, 200)
        self.assertEqual(body["series"][0]["points"], [[4000, 4.0]])

    def test_counter_errors_are_400_json(self):
        self.store.write("acme", "neg", {}, [[1000, -1.0], [2000, 2.0]])
        for path in (
                "/v1/query?tenant=acme&metric=m&start=0&end=1000&step=1000&agg=rate",
                "/v1/query?tenant=acme&metric=m&start=0&end=1000&step=1000"
                "&window=0&agg=increase",
                "/v1/query?tenant=acme&metric=neg&start=2000&end=2000"
                "&step=1000&window=1500&agg=increase"):
            code, body = self.request(path)
            self.assertEqual(code, 400, path)
            self.assertIn("error", body)


class TestCounterCli(StoreCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        data_dir = os.path.join(self.tmp, "cli")
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", data_dir] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_counter_query_success_and_failure(self):
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
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--start", "4000", "--end", "4000", "--step", "1000",
            "--window-ms", "10000", "--agg", "rate")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["series"][0]["points"], [[4000, 4.0]])
        # Missing window: one JSON error line on stderr, non-zero, no stdout.
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--start", "0", "--end", "1000", "--step", "1000", "--agg", "rate")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("window", json.loads(err)["error"])
        # Negative sample inside the window fails the whole query.
        code, _, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "neg",
            "--sample", "1000:-1", "--sample", "2000:2")
        self.assertEqual(code, 0, err)
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "neg",
            "--start", "2000", "--end", "2000", "--step", "1000",
            "--window-ms", "1500", "--agg", "increase")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("error", json.loads(err.strip().splitlines()[-1]))


if __name__ == "__main__":
    unittest.main()
