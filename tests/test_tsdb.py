"""Tests for obsd.tsdb: identity, idempotency, query, aggregation, retention."""

import os
import shutil
import tempfile
import unittest

from obsd import ObsError, SeriesStore
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
