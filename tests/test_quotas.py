"""Tests for per-tenant write quotas: limits, net-increase accounting, persistence."""

import json
import os
import shutil
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

from obsd import ObsError, SeriesStore
from obsd.cli import main as cli_main


class QuotaCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-quota-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))


class TestQuotaConfig(QuotaCase):
    def test_unconfigured_tenant_is_unlimited_with_zero_usage(self):
        store = self.store()
        self.assertEqual(store.get_quota("acme"),
                         {"tenant": "acme", "max_series": None, "max_points": None,
                          "series": 0, "points": 0})
        store.write("acme", "m", {"k": "v"}, [[1000, 1.0], [2000, 2.0]])
        self.assertEqual(store.get_quota("acme"),
                         {"tenant": "acme", "max_series": None, "max_points": None,
                          "series": 1, "points": 2})

    def test_set_quota_returns_limits_and_usage_and_creates_no_series(self):
        store = self.store()
        out = store.set_quota("acme", 2, 5)
        self.assertEqual(out, {"tenant": "acme", "max_series": 2, "max_points": 5,
                               "series": 0, "points": 0})
        self.assertEqual(store.stats()["series"], 0)
        self.assertTrue(os.path.exists(os.path.join(store.root, "quotas.json")))

    def test_omitted_limits_mean_unlimited(self):
        store = self.store()
        store.set_quota("acme", None, None)
        self.assertEqual(store.write("acme", "m", {}, [[1, 1.0]])["written"], 1)

    def test_invalid_tenant_or_limits_raise_and_leave_config_untouched(self):
        store = self.store()
        store.set_quota("acme", 3, 9)
        for bad_tenant in ("", None, 7):
            with self.assertRaises(ObsError, msg=repr(bad_tenant)):
                store.set_quota(bad_tenant, 1, 1)
        for bad_limit in (True, False, "1", 1.5, -1, -0.1, []):
            with self.assertRaises(ObsError, msg=repr(bad_limit)):
                store.set_quota("acme", bad_limit, 9)
            with self.assertRaises(ObsError, msg=repr(bad_limit)):
                store.set_quota("acme", 3, bad_limit)
        with self.assertRaises(ObsError):
            store.get_quota("")
        self.assertEqual(store.get_quota("acme")["max_series"], 3)
        self.assertEqual(store.get_quota("acme")["max_points"], 9)


class TestQuotaEnforcement(QuotaCase):
    def test_points_quota_rejects_whole_batch(self):
        store = self.store()
        store.set_quota("acme", None, 2)
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        with self.assertRaises(ObsError) as caught:
            store.write("acme", "m", {}, [[3000, 3.0], [4000, 4.0]])
        self.assertIn("max_points", str(caught.exception))
        # Nothing from the rejected batch remains: no new points, no write count.
        self.assertEqual(store.get_quota("acme")["points"], 2)
        self.assertEqual(store.stats()["writes"], 1)
        self.assertEqual(store.query("acme", "m")[0]["points"], [[1000, 1.0], [2000, 2.0]])

    def test_series_quota_rejects_new_series(self):
        store = self.store()
        store.set_quota("acme", 1, None)
        store.write("acme", "m", {"k": "a"}, [[1000, 1.0]])
        with self.assertRaises(ObsError) as caught:
            store.write("acme", "m", {"k": "b"}, [[1000, 1.0]])
        self.assertIn("max_series", str(caught.exception))
        self.assertEqual(store.get_quota("acme")["series"], 1)
        self.assertEqual(store.stats()["writes"], 1)

    def test_zero_limits_forbid_new_but_allow_duplicates_and_overwrites(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        store.set_quota("acme", 0, 0)
        # Pure replay and overwrite of existing timestamps add no occupancy.
        self.assertEqual(store.write("acme", "m", {}, [[1000, 1.0]])["duplicates"], 1)
        result = store.write("acme", "m", {}, [[1000, 9.0]], overwrite=True)
        self.assertEqual(result["written"], 1)
        with self.assertRaises(ObsError):
            store.write("acme", "m", {}, [[2000, 2.0]])
        with self.assertRaises(ObsError):
            store.write("acme", "m", {"k": "v"}, [[1000, 1.0]])

    def test_replay_overwrite_and_intra_batch_dups_do_not_count(self):
        store = self.store()
        store.set_quota("acme", None, 3)
        store.write("acme", "m", {"k": "a"}, [[1000, 1.0], [2000, 2.0]])
        # Same-value replay mixed with an intra-batch repeated timestamp: net +1 point.
        result = store.write("acme", "m", {"k": "a"},
                             [[2000, 2.0], [3000, 3.0], [3000, 3.0]])
        self.assertEqual((result["written"], result["duplicates"]), (1, 2))
        self.assertEqual(store.get_quota("acme")["points"], 3)
        # Existing series + an overwrite adds no series and no points.
        result = store.write("acme", "m", {"k": "a"}, [[1000, 5.0]], overwrite=True)
        self.assertEqual(result["written"], 1)
        self.assertEqual(store.get_quota("acme"),
                         {"tenant": "acme", "max_series": None, "max_points": 3,
                          "series": 1, "points": 3})
        # One more distinct point would exceed the limit.
        with self.assertRaises(ObsError):
            store.write("acme", "m", {"k": "a"}, [[4000, 4.0]])
        # But a batch whose only new timestamp duplicates itself once is still +1: rejected.
        with self.assertRaises(ObsError):
            store.write("acme", "m", {"k": "a"}, [[4000, 4.0], [4000, 4.0]])

    def test_label_order_and_series_quota_boundary(self):
        store = self.store()
        store.set_quota("acme", 2, None)
        store.write("acme", "m", [["b", "2"], ["a", "1"]], [[1, 1.0]])
        store.write("acme", "m", {"a": "1", "b": "2"}, [[2, 2.0]])  # same series
        store.write("acme", "m", {"a": "9"}, [[1, 1.0]])            # second series
        self.assertEqual(store.get_quota("acme")["series"], 2)
        with self.assertRaises(ObsError):
            store.write("acme", "other", {}, [[1, 1.0]])

    def test_limit_can_drop_below_usage_and_only_increases_are_checked(self):
        store = self.store()
        store.set_quota("acme", 1, 5)
        store.write("acme", "m", {}, [[t, float(t)] for t in range(1, 6)])
        store.set_quota("acme", 0, 2)  # below current 1 series / 5 points
        # Points already over quota but no new points: overwrite stays allowed.
        result = store.write("acme", "m", {}, [[1, 9.0]], overwrite=True)
        self.assertEqual(result["written"], 1)
        # Series already over quota but a new point in the existing series is the
        # only increasing dimension; points limit is what blocks it, not series.
        with self.assertRaises(ObsError) as caught:
            store.write("acme", "m", {}, [[6, 6.0]])
        self.assertIn("max_points", str(caught.exception))
        # Raising the points limit while series remains at 0 allows the point.
        store.set_quota("acme", 0, 6)
        self.assertEqual(store.write("acme", "m", {}, [[6, 6.0]])["written"], 1)
        # ...but a new series is still blocked by max_series even with room on points.
        with self.assertRaises(ObsError) as caught:
            store.write("acme", "m", {"k": "v"}, [[1, 1.0]])
        self.assertIn("max_series", str(caught.exception))

    def test_tenants_are_isolated(self):
        store = self.store()
        store.set_quota("acme", 1, 1)
        store.write("acme", "m", {}, [[1, 1.0]])
        # Other tenant has no quota configured at all.
        self.assertEqual(store.write("globex", "m", {}, [[1, 1.0], [2, 2.0]])["written"], 2)
        with self.assertRaises(ObsError):
            store.write("acme", "m", {}, [[2, 2.0]])
        self.assertEqual(store.get_quota("globex")["points"], 2)
        self.assertEqual(store.get_quota("acme")["points"], 1)

    def test_validation_and_conflicts_take_precedence_over_quota(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        store.set_quota("acme", 0, 0)
        # A conflicting value is reported as a conflict even though both are at zero.
        with self.assertRaises(ObsError) as caught:
            store.write("acme", "m", {}, [[1000, 2.0]])
        self.assertTrue(str(caught.exception).startswith("conflict"))
        # Malformed input raises before any quota check.
        with self.assertRaises(ObsError):
            store.write("acme", "m", {}, [["x", 1.0]])


class TestQuotaRetentionAndPersistence(QuotaCase):
    def test_retention_frees_points_but_keeps_series(self):
        store = self.store()
        store.set_quota("acme", 1, 2)
        store.write("acme", "m", {}, [[1000, 1.0], [2000, 2.0]])
        with self.assertRaises(ObsError):
            store.write("acme", "m", {}, [[3000, 3.0]])
        store.enforce_retention("acme", 2000)
        usage = store.get_quota("acme")
        self.assertEqual((usage["series"], usage["points"]), (1, 1))
        # Freed point occupancy can be used again.
        self.assertEqual(store.write("acme", "m", {}, [[3000, 3.0]])["written"], 1)
        # Clearing all points leaves the series registered.
        store.enforce_retention("acme", 10_000)
        usage = store.get_quota("acme")
        self.assertEqual((usage["series"], usage["points"]), (1, 0))
        with self.assertRaises(ObsError):
            store.write("acme", "m", {"k": "other"}, [[1, 1.0]])
        self.assertEqual((store.get_quota("acme")["series"],
                          store.get_quota("acme")["points"]), (1, 0))

    def test_quota_and_usage_survive_reopen(self):
        first = self.store("restart")
        first.set_quota("acme", 1, 3)
        first.write("acme", "m", {}, [[1, 1.0], [2, 2.0]])
        second = self.store("restart")
        self.assertEqual(second.get_quota("acme"),
                         {"tenant": "acme", "max_series": 1, "max_points": 3,
                          "series": 1, "points": 2})
        with self.assertRaises(ObsError):
            second.write("acme", "m", {}, [[3, 3.0], [4, 4.0]])
        self.assertEqual(second.write("acme", "m", {}, [[3, 3.0]])["written"], 1)

    def test_repeated_overwrites_count_once_after_reopen(self):
        first = self.store("overwrite")
        first.write("acme", "m", {}, [[1000, 1.0]])
        for value in (2.0, 3.0, 4.0):
            first.write("acme", "m", {}, [[1000, value]], overwrite=True)
        reopened = self.store("overwrite")
        self.assertEqual(reopened.get_quota("acme")["points"], 1)
        self.assertEqual(reopened.query("acme", "m")[0]["points"], [[1000, 4.0]])
        # A seed-time point file with duplicate timestamp lines (legacy layout)
        # likewise collapses to one occupied timestamp.
        sid = first.write("acme", "n", {}, [[1, 1.0]])["series_id"]
        with open(os.path.join(first.points_dir, sid + ".jsonl"), "a",
                  encoding="utf-8") as handle:
            handle.write('{"t":1,"v":9.0}\n{"t":2,"v":1.0}\n')
        legacy = self.store("overwrite")
        self.assertEqual(legacy.get_quota("acme")["points"], 3)

    def test_legacy_directory_without_quota_file_is_unlimited(self):
        first = self.store("legacy")
        first.write("acme", "m", {}, [[1, 1.0], [2, 2.0]])
        self.assertFalse(os.path.exists(os.path.join(first.root, "quotas.json")))
        reopened = self.store("legacy")
        self.assertIsNone(reopened.get_quota("acme")["max_points"])
        self.assertEqual(reopened.write("acme", "m", {}, [[3, 3.0]])["written"], 1)

    def test_concurrent_writes_config_and_retention_stay_consistent(self):
        store = self.store()
        store.set_quota("acme", 50, 100_000)
        errors = []

        def writer(worker):
            try:
                for seq in range(20):
                    store.write("acme", "m", {"w": str(worker)},
                                [[worker * 1000 + seq, float(seq)]])
            except ObsError as exc:
                errors.append(exc)

        def reconfigure():
            try:
                for limit in (50, 40, 60, 50):
                    store.set_quota("acme", limit, 100_000)
            except ObsError as exc:  # pragma: no cover - defensive
                errors.append(exc)

        def prune():
            try:
                store.enforce_retention("acme", 5_000)
            except ObsError as exc:  # pragma: no cover - defensive
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(w,)) for w in range(8)]
        threads += [threading.Thread(target=reconfigure), threading.Thread(target=prune)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        self.assertEqual(errors, [])
        usage = store.get_quota("acme")
        # Snapshot consistency: the reported usage equals a fresh recomputation.
        recomputed = sum(
            len(store._samples[sid]) for sid, row in store._series.items()
            if row["tenant"] == "acme")
        self.assertEqual(usage["points"], recomputed)
        self.assertEqual(usage["series"], 8)
        # Each worker series holds at most 20 distinct timestamps.
        self.assertLessEqual(usage["points"], 8 * 20)
        # No physical point file keeps duplicate timestamp lines either.
        for sid, row in store._series.items():
            if row["tenant"] == "acme":
                stamps = [item[0] for item in store._samples[sid]]
                self.assertEqual(len(stamps), len(set(stamps)))


class TestQuotaCli(QuotaCase):
    def run_cli(self, *argv):
        out, err = StringIO(), StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = cli_main(["--data-dir", os.path.join(self.tmp, "cli")] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_quota_set_get_and_enforced_write(self):
        code, out, err = self.run_cli("quota-set", "--tenant", "acme",
                                      "--max-series", "1", "--max-points", "1")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), {"tenant": "acme", "max_series": 1,
                                           "max_points": 1, "series": 0, "points": 0})
        code, out, err = self.run_cli("quota-get", "--tenant", "acme")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["max_points"], 1)
        code, out, err = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                      "--sample", "1000:1.0")
        self.assertEqual(code, 0)
        code, out, err = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                      "--sample", "2000:2.0")
        self.assertNotEqual(code, 0)
        self.assertIn("error", json.loads(err))
        code, out, err = self.run_cli("quota-get", "--tenant", "acme")
        self.assertEqual((json.loads(out)["series"], json.loads(out)["points"]), (1, 1))

    def test_invalid_limit_is_json_error(self):
        code, out, err = self.run_cli("quota-set", "--tenant", "acme",
                                      "--max-series", "-1")
        self.assertNotEqual(code, 0)
        self.assertIn("max_series", json.loads(err)["error"])


if __name__ == "__main__":
    unittest.main()
