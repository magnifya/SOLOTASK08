"""Tests for obsd.tsdb: identity, idempotency, query, aggregation, retention."""

import contextlib
import io
import json
import os
import shutil
import tempfile
import unittest

from obsd import ObsError, SeriesStore
from obsd.cli import main as cli_main
from obsd.tsdb import canonical_identity, series_id_for


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-tsdb-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))
class TestIdentity(StoreCase):
    def test_series_id_is_sha256_of_canonical_identity(self):
        ident = canonical_identity("acme", "http_requests", {"b": "2", "a": "1"})
        self.assertEqual(ident, "acme\nhttp_requests\na=1\nb=2")
        sid = series_id_for("acme", "http_requests", {"a": "1", "b": "2"})
        self.assertEqual(len(sid), 64)
        self.assertEqual(sid, series_id_for("acme", "http_requests", [["b", "2"], ["a", "1"]]))
    def test_identity_separates_tenant_metric_and_labels(self):
        ids = {series_id_for("acme", "m", {"k": "v"}), series_id_for("acme", "m", {"k": "w"}),
               series_id_for("acme", "other", {"k": "v"}), series_id_for("other", "m", {"k": "v"}),
               series_id_for("acme", "m", {})}
        self.assertEqual(len(ids), 5)
    def test_write_deduplicates_series_rows(self):
        store = self.store()
        first = store.write("acme", "m", {"k": "v"}, [[1000, 1.0]])
        second = store.write("acme", "m", [["k", "v"]], [[2000, 2.0]])
        self.assertEqual(first["series_id"], second["series_id"])
        self.assertEqual((store.stats()["series"], store.stats()["points"]), (1, 2))
class TestWrite(StoreCase):
    def test_idempotent_rewrite_and_conflict(self):
        store = self.store()
        store.write("acme", "m", {"k": "v"}, [[1000, 1.0], [2000, 2.0]])
        again = store.write("acme", "m", {"k": "v"}, [[1000, 1.0], [2000, 2.0], [3000, 3.0]])
        self.assertEqual((again["written"], again["duplicates"]), (1, 2))
        self.assertEqual(store.stats()["points"], 3)
        with self.assertRaises(ObsError):
            store.write("acme", "m", {"k": "v"}, [[1000, 9.0]])
        self.assertEqual(store.stats()["points"], 3)
    def test_conflict_accepted_with_overwrite(self):
        store = self.store()
        store.write("acme", "m", {"k": "v"}, [[1000, 1.0]])
        result = store.write("acme", "m", {"k": "v"}, [[1000, 9.0]], overwrite=True)
        self.assertEqual(result["written"], 1)
        self.assertEqual(store.query("acme", "m")[0]["points"], [[1000, 9.0]])
    def test_rejects_malformed_input(self):
        store = self.store()
        for bad in ([], [[1000]], [[1000, "x"]]):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.write("acme", "m", {"k": "v"}, bad)
        with self.assertRaises(ObsError):
            store.write("", "m", {}, [[1000, 1.0]])
        with self.assertRaises(ObsError):
            store.write("acme", "m", {"k": "v"}, [[1000, 1.0]], now=500)
class TestQuery(StoreCase):
    def test_raw_range_query_is_sorted_and_filtered(self):
        store = self.store()
        store.write("acme", "m", {"k": "v"}, [[3000, 3.0], [1000, 1.0], [2000, 2.0]])
        store.write("acme", "m", {"k": "w"}, [[1000, 9.0]])
        rows = store.query("acme", "m", start_ms=1500, end_ms=3000)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["labels"], {"k": "v"})
        self.assertEqual(rows[0]["points"], [[2000, 2.0], [3000, 3.0]])
        # A series with no sample in the window is still listed, with no points.
        self.assertEqual(rows[1]["points"], [])
    def test_aggregation_skips_series_without_samples_in_window(self):
        store = self.store()
        store.write("acme", "m", {"k": "v"}, [[0, 1.0]])
        store.write("acme", "m", {"k": "w"}, [[99000, 1.0]])
        rows = store.query("acme", "m", start_ms=0, end_ms=1000, step_ms=1000, agg="avg")
        self.assertEqual([row["labels"] for row in rows], [{"k": "v"}])
    def test_label_matcher_selects_series(self):
        store = self.store()
        store.write("acme", "m", {"k": "v", "h": "1"}, [[1000, 1.0]])
        store.write("acme", "m", {"k": "w", "h": "1"}, [[1000, 2.0]])
        rows = store.query("acme", "m", labels={"k": "v"})
        self.assertEqual([row["labels"] for row in rows], [{"h": "1", "k": "v"}])
    def test_step_buckets_are_epoch_aligned_and_left_closed(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0], [999, 2.0], [1000, 3.0], [2999, 4.0]])
        rows = store.query("acme", "m", step_ms=1000, agg="sum")
        self.assertEqual(rows[0]["points"], [[0, 3.0], [1000, 3.0], [2000, 4.0]])
    def test_empty_buckets_are_null(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0], [3000, 4.0]])
        rows = store.query("acme", "m", step_ms=1000, agg="max")
        self.assertEqual(rows[0]["points"], [[0, 1.0], [1000, None], [2000, None], [3000, 4.0]])
    def test_all_aggregations_and_single_bucket(self):
        store = self.store()
        store.write("acme", "m", {}, [[0, 1.0], [100, 3.0], [1000, 5.0]])
        for agg, value in {"sum": 4.0, "avg": 2.0, "min": 1.0, "max": 3.0, "count": 2}.items():
            rows = store.query("acme", "m", start_ms=0, end_ms=999, step_ms=1000, agg=agg)
            self.assertEqual(rows[0]["points"], [[0, value]], agg)
        rows = store.query("acme", "m", start_ms=0, end_ms=1000, agg="avg")
        self.assertEqual(rows[0]["points"], [[0, 3.0]])
    def test_invalid_aggregation_rejected(self):
        store = self.store()
        with self.assertRaises(ObsError):
            store.query("acme", "m", agg="median")
        with self.assertRaises(ObsError):
            store.query("acme", "m", start_ms=10, end_ms=5)
    def test_rollup_is_deterministic_and_drops_empty_buckets(self):
        store = self.store()
        store.write("acme", "m", {"k": "b"}, [[0, 1.0], [5000, 2.0]])
        store.write("acme", "m", {"k": "a"}, [[0, 4.0], [10000, 5.0]])
        one = store.rollup("acme", "m", None, 10000, "sum")
        self.assertEqual(one, store.rollup("acme", "m", None, 10000, "sum"))
        by_labels = {row["labels"]["k"]: row["points"] for row in one}
        self.assertEqual(sorted(by_labels), ["a", "b"])
        self.assertEqual(by_labels["a"], [[0, 4.0], [10000, 5.0]])
        self.assertEqual(by_labels["b"], [[0, 3.0]])
class TestGroupBy(StoreCase):
    def fill(self, store):
        store.write("acme", "m", {"host": "a", "region": "x"}, [[0, 1.0], [1000, 3.0]])
        store.write("acme", "m", {"host": "b", "region": "x"}, [[1000, 9.0]])
        store.write("acme", "m", {"host": "a"}, [[500, 5.0]])
        store.write("acme", "m", {"host": "a", "region": ""}, [[700, 7.0]])
        store.write("acme", "m", {"host": "c", "region": "x"}, [[9000, 1.0]])
        return store
    def test_group_aggregates_pooled_raw_samples(self):
        store = self.fill(self.store())
        rows = store.query("acme", "m", group_by=["region"], agg="avg",
                           start_ms=0, end_ms=2000)
        # Groups ordered by their sorted label pairs; a missing key and an
        # empty-string value are different groups.
        self.assertEqual([row["labels"] for row in rows],
                         [{}, {"region": ""}, {"region": "x"}])
        # (1+3+9)/3: samples at the same timestamp count individually.
        self.assertEqual(rows[2]["points"], [[0, 13.0 / 3.0]])
        self.assertEqual(rows[1]["points"], [[700, 7.0]])
        self.assertEqual(rows[0]["points"], [[500, 5.0]])
        self.assertNotIn("series_id", rows[0])
    def test_empty_group_by_merges_all_series(self):
        store = self.fill(self.store())
        rows = store.query("acme", "m", group_by=[], agg="sum")
        self.assertEqual(rows, [{"labels": {}, "points": [[0, 26.0]]}])
    def test_grouped_step_buckets_with_null_gaps(self):
        store = self.fill(self.store())
        rows = store.query("acme", "m", group_by=["region"], agg="sum",
                           step_ms=1000, start_ms=0, end_ms=2000)
        region_x = [row for row in rows if row["labels"] == {"region": "x"}][0]
        self.assertEqual(region_x["points"], [[0, 1.0], [1000, 12.0]])
        store.write("acme", "m", {"host": "d"}, [[0, 2.0], [9000, 4.0]])
        rows = store.query("acme", "m", labels={"host": "d"}, group_by=[],
                           agg="sum", step_ms=3000)
        self.assertEqual(rows[0]["points"],
                         [[0, 2.0], [3000, None], [6000, None], [9000, 4.0]])
    def test_group_without_samples_in_range_is_listed_empty(self):
        store = self.fill(self.store())
        rows = store.query("acme", "m", group_by=["host"], agg="sum",
                           start_ms=0, end_ms=2000)
        host_c = [row for row in rows if row["labels"] == {"host": "c"}][0]
        self.assertEqual(host_c["points"], [])
        rows = store.query("acme", "m", group_by=["host"], agg="sum",
                           step_ms=1000, start_ms=0, end_ms=2000)
        host_c = [row for row in rows if row["labels"] == {"host": "c"}][0]
        self.assertEqual(host_c["points"], [])
        self.assertEqual(store.query("acme", "nope", group_by=["host"], agg="sum"), [])
    def test_group_by_key_order_is_irrelevant(self):
        store = self.fill(self.store())
        one = store.query("acme", "m", group_by=["host", "region"], agg="count")
        two = store.query("acme", "m", group_by=["region", "host"], agg="count")
        self.assertEqual(one, two)
        self.assertEqual([row["labels"] for row in one], [
            {"host": "a"}, {"host": "a", "region": ""}, {"host": "a", "region": "x"},
            {"host": "b", "region": "x"}, {"host": "c", "region": "x"}])
    def test_group_by_validation(self):
        store = self.fill(self.store())
        for bad in ("host", ["host", "host"], [""], [1], {"host": 1}):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.query("acme", "m", group_by=bad, agg="sum")
        with self.assertRaises(ObsError):
            store.query("acme", "m", group_by=["host"])
        with self.assertRaises(ObsError):
            store.query("acme", "m", group_by=["host"], agg="median")
    def test_grouped_query_survives_reopen(self):
        store = self.fill(self.store())
        expected = store.query("acme", "m", group_by=["region"], agg="avg")
        reopened = self.store()
        self.assertEqual(reopened.query("acme", "m", group_by=["region"], agg="avg"),
                         expected)
class TestWindowQuery(StoreCase):
    def fill(self, store):
        store.write("acme", "m", {"k": "v"},
                    [[500, 1.0], [1000, 2.0], [1500, 4.0], [2500, 8.0]])
        return store
    def test_sliding_window_reads_history_before_start(self):
        store = self.fill(self.store())
        rows = store.query("acme", "m", start_ms=1000, end_ms=3000,
                           step_ms=500, agg="sum", window_ms=1000)
        self.assertEqual(len(rows), 1)
        self.assertIn("series_id", rows[0])
        # t=1000 sees (0,1000]: the sample at 500 predates start but counts.
        self.assertEqual(rows[0]["points"], [[1000, 3.0], [1500, 6.0], [2000, 4.0],
                                             [2500, 8.0], [3000, 8.0]])
    def test_window_is_left_open_right_closed(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0], [2001, 4.0]])
        rows = store.query("acme", "m", start_ms=2000, end_ms=2000,
                           step_ms=1000, agg="sum", window_ms=1000)
        # (1000, 2000]: the sample at exactly t-window is excluded.
        self.assertEqual(rows[0]["points"], [[2000, 2.0]])
        rows = store.query("acme", "m", start_ms=2001, end_ms=2001,
                           step_ms=1000, agg="sum", window_ms=1000)
        self.assertEqual(rows[0]["points"], [[2001, 6.0]])
    def test_step_need_not_divide_window_or_align_to_buckets(self):
        store = self.fill(self.store())
        rows = store.query("acme", "m", start_ms=333, end_ms=2433,
                           step_ms=700, agg="count", window_ms=1000)
        self.assertEqual([point[0] for point in rows[0]["points"]],
                         [333, 1033, 1733, 2433])
        # t=333 sees (-667,333]: empty windows are null even for count.
        self.assertEqual([point[1] for point in rows[0]["points"]],
                         [None, 2, 2, 1])
    def test_empty_windows_are_null_for_every_aggregation(self):
        store = self.fill(self.store())
        for agg in ("sum", "avg", "min", "max", "count"):
            rows = store.query("acme", "m", start_ms=3000, end_ms=4000,
                               step_ms=1000, agg=agg, window_ms=400)
            self.assertEqual(rows[0]["points"],
                             [[3000, None], [4000, None]], agg)
    def test_full_grid_for_series_with_only_empty_windows(self):
        store = self.fill(self.store())
        store.write("acme", "m", {"k": "w"}, [[100, 9.0]])
        rows = store.query("acme", "m", start_ms=5000, end_ms=7000,
                           step_ms=1000, agg="avg", window_ms=500)
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual(row["points"],
                             [[5000, None], [6000, None], [7000, None]])
        self.assertEqual(store.query("acme", "nope", start_ms=0, end_ms=1000,
                                     step_ms=100, agg="sum", window_ms=100), [])
    def test_window_group_by_pools_raw_samples(self):
        store = self.store()
        store.write("acme", "m", {"host": "a", "region": "x"}, [[1000, 1.0]])
        store.write("acme", "m", {"host": "b", "region": "x"}, [[1000, 3.0]])
        store.write("acme", "m", {"host": "c"}, [[1000, 100.0]])
        store.write("acme", "m", {"host": "d", "region": ""}, [[9000, 5.0]])
        rows = store.query("acme", "m", group_by=["region"], agg="avg",
                           start_ms=1000, end_ms=2000, step_ms=1000, window_ms=1000)
        self.assertEqual([row["labels"] for row in rows],
                         [{}, {"region": ""}, {"region": "x"}])
        # Same-timestamp samples from different series count individually.
        self.assertEqual(rows[2]["points"], [[1000, 2.0], [2000, None]])
        self.assertEqual(rows[0]["points"], [[1000, 100.0], [2000, None]])
        # An all-empty group keeps the full time grid.
        self.assertEqual(rows[1]["points"], [[1000, None], [2000, None]])
        self.assertNotIn("series_id", rows[0])
        counts = store.query("acme", "m", group_by=["region"], agg="count",
                             start_ms=1000, end_ms=1000, step_ms=1000, window_ms=1000)
        region_x = [row for row in counts if row["labels"] == {"region": "x"}][0]
        self.assertEqual(region_x["points"], [[1000, 2]])
    def test_window_query_validation(self):
        store = self.fill(self.store())
        base = {"start_ms": 0, "end_ms": 1000, "step_ms": 100,
                "agg": "sum", "window_ms": 500}
        # Missing required settings.
        for field in ("start_ms", "end_ms", "step_ms", "agg"):
            kwargs = dict(base, **{field: None})
            with self.assertRaises(ObsError, msg=field):
                store.query("acme", "m", **kwargs)
        # Booleans and floats are not integer milliseconds.
        for field in ("start_ms", "end_ms", "step_ms", "window_ms"):
            for bad in (True, 1.5):
                kwargs = dict(base, **{field: bad})
                with self.assertRaises(ObsError, msg="%s=%r" % (field, bad)):
                    store.query("acme", "m", **kwargs)
        # Non-positive step/window, end before start, unknown aggregation.
        for kwargs in (dict(base, step_ms=0), dict(base, step_ms=-5),
                       dict(base, window_ms=0), dict(base, window_ms=-1),
                       dict(base, start_ms=1000, end_ms=999),
                       dict(base, agg="median")):
            with self.assertRaises(ObsError, msg=repr(kwargs)):
                store.query("acme", "m", **kwargs)
        # Invalid group_by settings.
        for bad in ("host", ["host", "host"], [""], [1], {"host": 1}):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.query("acme", "m", group_by=bad, **base)
    def test_window_query_is_read_only_and_survives_reopen(self):
        store = self.fill(self.store())
        before = store.stats()
        expected = store.query("acme", "m", start_ms=0, end_ms=3000,
                               step_ms=500, agg="avg", window_ms=1000)
        self.assertEqual(store.stats(), before)
        reopened = self.store()
        self.assertEqual(reopened.query("acme", "m", start_ms=0, end_ms=3000,
                                        step_ms=500, agg="avg", window_ms=1000),
                         expected)
class TestWindowCli(StoreCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", os.path.join(self.tmp, "data")] + list(argv))
        return code, out.getvalue(), err.getvalue()
    def test_cli_window_query_and_error(self):
        store = self.store()
        store.write("acme", "m", {"k": "v"}, [[500, 1.0], [1000, 2.0]])
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--start", "1000", "--end", "2000", "--step", "1000",
            "--agg", "sum", "--window-ms", "1000")
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertEqual(json.loads(out), {"series": [{
            "labels": {"k": "v"}, "points": [[1000, 3.0], [2000, None]]}]})
        # A missing required setting is one JSON error on stderr, non-zero exit.
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--start", "0", "--end", "2000", "--agg", "sum", "--window-ms", "1000")
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")
        self.assertIn("error", json.loads(err))
class TestRetentionAndPersistence(StoreCase):
    def test_enforce_retention_drops_old_points(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0], [3000, 3.0]])
        store.write("other", "m", {}, [[1000, 1.0]])
        self.assertEqual(store.enforce_retention("acme", 2000)["dropped"], 1)
        self.assertEqual(store.query("acme", "m")[0]["points"], [[2000, 2.0], [3000, 3.0]])
        self.assertEqual(store.stats()["points"], 3)
    def test_state_survives_restart_and_files_exist(self):
        first = self.store("restart")
        first.write("acme", "m", {"k": "v"}, [[1000, 1.0], [2000, 2.0]])
        self.assertTrue(os.path.exists(os.path.join(first.root, "series.json")))
        self.assertTrue(os.path.isdir(os.path.join(first.root, "points")))
        second = self.store("restart")
        self.assertEqual(second.stats()["series"], 1)
        self.assertEqual(second.query("acme", "m")[0]["points"], [[1000, 1.0], [2000, 2.0]])
        self.assertEqual(second.write("acme", "m", {"k": "v"}, [[1000, 1.0]])["duplicates"], 1)
if __name__ == "__main__":
    unittest.main()
