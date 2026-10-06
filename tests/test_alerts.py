"""Tests for obsd.alerts: rules, firing, silence, inhibition, dedup, SLO math."""

import json
import os
import shutil
import tempfile
import unittest

from obsd import AlertEngine, ObsError, SeriesStore

TENANT = "acme"
METRIC = "latency_ms"


def rule(**overrides):
    row = {
        "id": "r1",
        "tenant": TENANT,
        "metric": METRIC,
        "labels": {},
        "comparator": "<",
        "threshold": 10.0,
        "for_ms": 0,
        "window_ms": 60000,
        "agg": "avg",
        "severity": "warning",
        "annotations": {"summary": "latency low"},
    }
    row.update(overrides)
    return row
class EngineCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-alerts-")
        self.root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(self.root)
        self.engine = AlertEngine(self.store, self.root)
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def values(self, points, labels=None):
        self.store.write(TENANT, METRIC, labels or {}, points)
class TestRuleValidation(EngineCase):
    def test_valid_rule_gets_defaults(self):
        clean = self.engine.add_rule(rule())
        self.assertEqual(clean["id"], "r1")
        self.assertEqual(clean["labels"], [])
        self.assertEqual(clean["threshold"], 10.0)
    def test_generated_id_is_stable_across_rules(self):
        first = self.engine.add_rule(rule(id=None))
        second = self.engine.add_rule(rule(id=None))
        self.assertNotEqual(first["id"], second["id"])
    def test_malformed_rules_rejected(self):
        for bad in (
            rule(comparator="=<"),
            rule(threshold="high"),
            rule(agg="median"),
            rule(severity="urgent"),
            rule(window_ms=0),
            rule(for_ms=-1),
            rule(tenant=""),
            rule(metric=""),
            rule(annotations=["nope"]),
        ):
            with self.assertRaises(ObsError, msg=repr(bad)):
                self.engine.add_rule(bad)
    def test_rules_persist(self):
        self.engine.add_rule(rule())
        reloaded = AlertEngine(SeriesStore(self.root), self.root)
        self.assertEqual([r["id"] for r in reloaded.list_rules()], ["r1"])
class TestFiringAndDedup(EngineCase):
    def test_fires_only_after_condition_holds_for_for_ms(self):
        self.engine.add_rule(rule(for_ms=30000, window_ms=30000, agg="avg"))
        self.values([[30000, 5.0]])
        self.assertEqual(self.engine.evaluate(63000)["firing"], [])
        self.values([[60000, 5.0]])
        later = self.engine.evaluate(68000)
        self.assertEqual(len(later["firing"]), 1)
        self.assertEqual(later["firing"][0]["severity"], "warning")
        self.assertEqual(later["firing"][0]["labels"], {})
        self.assertEqual(later["firing"][0]["held_ms"], 30000)
    def test_repeated_evaluation_dedups_and_counts_occurrences(self):
        self.engine.add_rule(rule(for_ms=0, window_ms=30000))
        self.values([[60000, 5.0], [65000, 5.0]])
        first = self.engine.evaluate(68000)["firing"][0]
        second = self.engine.evaluate(68000)["firing"][0]
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["occurrences"], 1)
        self.assertEqual(second["occurrences"], 2)
    def test_single_bad_bucket_stops_the_alert(self):
        self.engine.add_rule(rule(for_ms=0, window_ms=30000, comparator="<", threshold=10.0))
        self.values([[60000, 5.0], [65000, 5.0]])
        self.assertEqual(len(self.engine.evaluate(68000)["firing"]), 1)
        self.values([[68000, 99.0]])
        result = self.engine.evaluate(69000)
        self.assertEqual(result["firing"], [])
        self.assertEqual(len(result["resolved"]), 1)
        self.assertEqual(result["resolved"][0]["state"], "resolved")
    def test_alert_resolves_then_can_fire_again_with_same_identity(self):
        self.engine.add_rule(rule(for_ms=0, window_ms=30000))
        self.values([[60000, 5.0], [61000, 5.0]])
        alert_id = self.engine.evaluate(64000)["firing"][0]["id"]
        self.values([[91000, 99.0]])
        self.assertEqual(len(self.engine.evaluate(94000)["resolved"]), 1)
        # The bad bucket must leave the window before the condition holds again:
        # at 124000 the window still covers the bad bucket 90000.
        self.values([[120000, 5.0], [121000, 5.0]])
        self.assertEqual(self.engine.evaluate(124000)["firing"], [])
        again = self.engine.evaluate(154000)["firing"][0]
        self.assertEqual(again["id"], alert_id)
        self.assertEqual(again["occurrences"], 2)
    def test_boundary_comparators(self):
        for comparator, threshold, value, fires in (
            (">", 5.0, 5.0, False), (">=", 5.0, 5.0, True),
            ("<", 5.0, 5.0, False), ("<=", 5.0, 5.0, True),
            ("==", 5.0, 5.0, True), ("!=", 5.0, 5.0, False),
        ):
            engine = AlertEngine(SeriesStore(self.root + "-cmp"), self.root + "-cmp")
            engine.add_rule(rule(id="cmp", comparator=comparator, threshold=threshold,
                                 for_ms=0, window_ms=1000))
            engine.store.write(TENANT, METRIC, {}, [[1000, value]])
            result = engine.evaluate(1000)
            self.assertEqual(bool(result["firing"]), fires, comparator)
    def test_labels_isolate_series(self):
        self.engine.add_rule(rule(labels={"host": "a"}, for_ms=0, window_ms=1000))
        self.values([[1000, 5.0]], labels={"host": "b"})
        self.assertEqual(self.engine.evaluate(1000)["firing"], [])
        self.values([[1000, 5.0]], labels={"host": "a"})
        self.assertEqual(len(self.engine.evaluate(1000)["firing"]), 1)
class TestSilenceAndInhibition(EngineCase):
    def test_active_silence_yields_silenced_state(self):
        self.engine.add_rule(rule(for_ms=0, window_ms=1000))
        self.engine.add_silence(TENANT, {}, 0, 9500, "maintenance")
        self.values([[1000, 5.0]])
        result = self.engine.evaluate(1000)
        self.assertEqual(result["firing"], [])
        self.assertEqual(len(result["silenced"]), 1)
        self.assertEqual(result["silenced"][0]["state"], "silenced")
        self.assertEqual(result["silenced"][0]["silence_reason"], "maintenance")
        self.assertEqual(len(self.engine.list_alerts(state="silenced")), 1)
    def test_expired_silence_lets_the_alert_fire(self):
        self.engine.add_rule(rule(for_ms=0, window_ms=1000))
        self.engine.add_silence(TENANT, {}, 0, 1500, "maintenance")
        self.values([[1000, 5.0]])
        self.assertEqual(len(self.engine.evaluate(1000)["silenced"]), 1)
        self.values([[2000, 5.0]])
        self.assertEqual(len(self.engine.evaluate(2000)["firing"]), 1)
    def test_silence_matchers_scope_by_labels(self):
        self.engine.add_rule(rule(for_ms=0, window_ms=1000))
        self.engine.add_silence(TENANT, {"host": "b"}, 0, 9500, "host b")
        self.values([[1000, 5.0]], labels={"host": "a"})
        self.assertEqual(len(self.engine.evaluate(1000)["firing"]), 1)
    def test_inhibition_suppresses_lower_severity_same_labels(self):
        self.engine.add_rule(rule(id="crit", severity="critical", comparator="<", threshold=10.0,
                                  for_ms=0, window_ms=1000))
        self.engine.add_rule(rule(id="warn", severity="warning", comparator="<", threshold=20.0,
                                  for_ms=0, window_ms=1000))
        self.engine.add_inhibition("critical", "warning", True)
        self.values([[1000, 5.0]])
        result = self.engine.evaluate(1000)
        self.assertEqual([row["rule_id"] for row in result["firing"]], ["crit"])
        self.assertEqual([row["rule_id"] for row in result["inhibited"]], ["warn"])
        self.assertEqual(result["inhibited"][0]["state"], "inhibited")
    def test_inhibition_requires_exact_label_set(self):
        self.engine.add_rule(rule(id="crit", severity="critical", labels={"host": "a"},
                                  for_ms=0, window_ms=1000))
        self.engine.add_rule(rule(id="warn", severity="warning", labels={"host": "b"},
                                  comparator="<", threshold=20.0, for_ms=0, window_ms=1000))
        self.engine.add_inhibition("critical", "warning", True)
        self.values([[1000, 5.0]], labels={"host": "a"})
        self.values([[1000, 5.0]], labels={"host": "b"})
        result = self.engine.evaluate(1000)
        self.assertEqual(sorted(row["rule_id"] for row in result["firing"]), ["crit", "warn"])
        self.assertEqual(result["inhibited"], [])
    def test_inhibition_requires_higher_source_severity(self):
        with self.assertRaises(ObsError):
            self.engine.add_inhibition("warning", "critical", True)
    def test_inhibition_same_labels_must_be_bool(self):
        self.engine.add_inhibition("critical", "warning", True)
        self.engine.add_inhibition("critical", "info", False)
        for bad in (None, 1, 0, "true", []):
            with self.assertRaises(ObsError, msg=repr(bad)) as caught:
                self.engine.add_inhibition("critical", "warning", bad)
            self.assertEqual(str(caught.exception), "same_labels must be a boolean")
        # No inhibition id was assigned, no counter incremented, no file written.
        rows = self.engine.list_inhibitions()
        self.assertEqual([row["id"] for row in rows],
                         ["inhibition-0001", "inhibition-0002"])
        self.assertEqual([row["same_labels"] for row in rows], [True, False])
        with open(os.path.join(self.engine.root, "inhibitions.json"),
                  encoding="utf-8") as handle:
            self.assertEqual(len(json.load(handle)), 2)
    def test_alerts_persist_across_restart(self):
        self.engine.add_rule(rule(for_ms=0, window_ms=1000))
        self.values([[1000, 5.0]])
        self.engine.evaluate(1000)
        reloaded = AlertEngine(SeriesStore(self.root), self.root)
        alerts = reloaded.list_alerts(state="firing")
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["rule_id"], "r1")
class TestSlo(EngineCase):
    END = 60000

    def fill(self, good, bad, end=END, step=1000, window=60000):
        """``good`` 1.0 samples then ``bad`` 0.0 samples, all inside [end-window, end]."""
        total = good + bad
        start = end - (total - 1) * step
        points = [[start + index * step, 1.0] for index in range(good)]
        points += [[start + (good + index) * step, 0.0] for index in range(bad)]
        self.values(points)
    def test_error_budget_and_burn_rate_math(self):
        self.engine.set_slo(TENANT, "availability", METRIC, {}, ">=", 1.0, 0.9, 60000)
        self.fill(9, 1)
        status = self.engine.slo_status("availability", self.END)
        self.assertEqual((status["total"], status["good"], status["bad"]), (10, 9, 1))
        self.assertAlmostEqual(status["ratio"], 0.9)
        self.assertAlmostEqual(status["target_ratio"], 0.9)
        self.assertAlmostEqual(status["error_budget"], 0.0)
        self.assertAlmostEqual(status["burn_rate"], 1.0)
        self.assertTrue(status["met"])
    def test_budget_remaining_and_burn_above_one(self):
        # 100 samples need 100s, so widen the SLO window to keep every sample inside it.
        self.engine.set_slo(TENANT, "slo95", METRIC, {}, ">=", 1.0, 0.95, 200000)
        self.fill(98, 2, end=200000)
        status = self.engine.slo_status("slo95", 200000)
        self.assertEqual((status["total"], status["good"], status["bad"]), (100, 98, 2))
        self.assertAlmostEqual(status["ratio"], 0.98)
        self.assertAlmostEqual(status["error_budget"], 0.6)
        self.assertAlmostEqual(status["burn_rate"], 0.02 / 0.05)
        self.assertTrue(status["met"])
    def test_unmet_and_exhausted_budget(self):
        self.engine.set_slo(TENANT, "slo99", METRIC, {}, ">=", 1.0, 0.99, 200000)
        self.fill(90, 10, end=200000)
        status = self.engine.slo_status("slo99", 200000)
        self.assertAlmostEqual(status["ratio"], 0.9)
        self.assertEqual(status["error_budget"], 0.0)
        self.assertAlmostEqual(status["burn_rate"], 0.1 / 0.01)
        self.assertFalse(status["met"])
    def test_empty_window_is_zeroed(self):
        self.engine.set_slo(TENANT, "empty", METRIC, {}, ">=", 1.0, 0.9, 60000)
        status = self.engine.slo_status("empty", self.END)
        self.assertEqual((status["total"], status["good"], status["bad"]), (0, 0, 0))
        self.assertEqual(status["ratio"], 0.0)
        self.assertEqual(status["burn_rate"], 0.0)
        # No traffic at all: nothing is proven, so the budget counts as exhausted.
        self.assertEqual(status["error_budget"], 0.0)
        self.assertFalse(status["met"])
    def test_window_excludes_old_samples(self):
        self.engine.set_slo(TENANT, "win", METRIC, {}, ">=", 1.0, 0.9, 60000)
        self.values([[0, 1.0], [1000, 1.0]])
        self.values([[120000, 1.0], [121000, 1.0], [122000, 1.0]])
        status = self.engine.slo_status("win", 122000)
        self.assertEqual(status["total"], 3)
        self.assertEqual(status["good"], 3)
    def test_bad_samples_are_counted_as_bad(self):
        self.engine.set_slo(TENANT, "count", METRIC, {}, ">=", 1.0, 0.5, 60000)
        self.values([[0, 1.0], [1000, 0.0], [2000, 0.0]])
        status = self.engine.slo_status("count", 2000)
        self.assertEqual((status["total"], status["good"], status["bad"]), (3, 1, 2))
        self.assertFalse(status["met"])
    def test_invalid_slo_rejected(self):
        with self.assertRaises(ObsError):
            self.engine.set_slo(TENANT, "bad", METRIC, {}, ">=", 1.0, 1.5, 60000)
        with self.assertRaises(ObsError):
            self.engine.set_slo(TENANT, "bad", METRIC, {}, "=>", 1.0, 0.9, 60000)
        with self.assertRaises(ObsError):
            self.engine.set_slo(TENANT, "bad", METRIC, {}, ">=", 1.0, 0.9, 0)
        with self.assertRaises(ObsError):
            self.engine.slo_status("missing", 0)
    def test_slos_persist(self):
        self.engine.set_slo(TENANT, "keep", METRIC, {}, ">=", 1.0, 0.9, 60000)
        reloaded = AlertEngine(SeriesStore(self.root), self.root)
        self.assertEqual([row["name"] for row in reloaded.list_slos()], ["keep"])
if __name__ == "__main__":
    unittest.main()
