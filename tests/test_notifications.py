"""Tests for obsd notification routing and the notification queue."""

import os
import shutil
import tempfile
import unittest

from obsd import AlertEngine, ObsError, SeriesStore

TENANT = "acme"
METRIC = "latency_ms"


def rule(**overrides):
    row = {
        "tenant": TENANT,
        "metric": METRIC,
        "labels": {},
        "comparator": "<",
        "threshold": 10.0,
        "for_ms": 0,
        "window_ms": 60000,
        "agg": "avg",
        "severity": "warning",
        "annotations": {},
    }
    row.update(overrides)
    return row
def route(**overrides):
    row = {"tenant": TENANT, "target": "pager"}
    row.update(overrides)
    return row
class EngineCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-notify-")
        self.root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(self.root)
        self.engine = AlertEngine(self.store, self.root)
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def reload(self):
        self.engine = AlertEngine(SeriesStore(self.root), self.root)
class TestRouteValidation(EngineCase):
    def test_defaults_and_generated_ids(self):
        first = self.engine.add_route(route())
        second = self.engine.add_route(route(target="email"))
        self.assertEqual((first["id"], second["id"]), ("route-0001", "route-0002"))
        self.assertEqual(first["severities"], ["info", "warning", "critical"])
        self.assertEqual(first["events"], ["firing", "resolved"])
        self.assertIsNone(first["repeat_ms"])
        self.assertEqual(first["labels"], [])
    def test_explicit_id_and_normalisation(self):
        row = self.engine.add_route(route(id="r9", labels={"host": "a"},
                                          severities=["critical", "critical"],
                                          events=["resolved"], repeat_ms=0))
        self.assertEqual(row["id"], "r9")
        self.assertEqual(row["labels"], [["host", "a"]])
        self.assertEqual(row["severities"], ["critical"])
        self.assertEqual(row["events"], ["resolved"])
        self.assertEqual(row["repeat_ms"], 0)
    def test_malformed_routes_rejected(self):
        for bad in ("nope", {}, route(tenant=""), route(target=""), route(id=""),
                    route(labels={"a": None}), route(severities=[]),
                    route(severities=["urgent"]), route(events=[]),
                    route(events=["pending"]), route(events="firing"),
                    route(repeat_ms=-1), route(repeat_ms="soon")):
            with self.assertRaises(ObsError, msg=repr(bad)):
                self.engine.add_route(bad)
    def test_duplicate_id_conflicts(self):
        self.engine.add_route(route(id="r1"))
        with self.assertRaises(ObsError) as ctx:
            self.engine.add_route(route(id="r1"))
        self.assertTrue(str(ctx.exception).startswith("conflict"))
        self.assertEqual(len(self.engine.list_routes()), 1)
    def test_generated_id_skips_taken_names(self):
        self.engine.add_route(route(id="route-0001"))
        self.assertEqual(self.engine.add_route(route())["id"], "route-0002")
    def test_list_and_delete(self):
        self.engine.add_route(route(id="a"))
        self.engine.add_route(route(id="b", tenant="other"))
        self.assertEqual([r["id"] for r in self.engine.list_routes()], ["a", "b"])
        self.assertEqual([r["id"] for r in self.engine.list_routes("other")], ["b"])
        self.assertEqual(self.engine.del_route("a"), {"deleted": "a"})
        with self.assertRaises(ObsError) as ctx:
            self.engine.del_route("a")
        self.assertTrue(str(ctx.exception).startswith("unknown route"))
    def test_routes_persist(self):
        self.engine.add_route(route(id="r1", repeat_ms=1000))
        self.reload()
        self.assertEqual([r["id"] for r in self.engine.list_routes()], ["r1"])
class NotifyCase(EngineCase):
    def setUp(self):
        super().setUp()
        self.store.write(TENANT, METRIC, {}, [[0, 5.0]])
        self.engine.add_rule(rule())
    def events(self, rows=None):
        return [row["event"] for row in (rows or self.engine.list_notifications())]
class TestNotificationGeneration(NotifyCase):
    def test_evaluate_still_returns_four_lists(self):
        self.engine.add_route(route())
        out = self.engine.evaluate(1000)
        self.assertEqual(sorted(out), ["firing", "inhibited", "resolved", "silenced"])
        self.assertEqual(len(out["firing"]), 1)
    def test_first_firing_notifies_once_per_matching_route(self):
        self.engine.add_route(route(id="a"))
        self.engine.add_route(route(id="b", target="email"))
        self.engine.add_route(route(id="c", tenant="other"))
        self.engine.evaluate(1000)
        rows = self.engine.list_notifications()
        self.assertEqual([row["route_id"] for row in rows], ["a", "b"])
        row = rows[0]
        self.assertEqual(row["id"], "notification-00001")
        self.assertEqual(row["event"], "firing")
        self.assertEqual(row["tenant"], TENANT)
        self.assertEqual(row["target"], "pager")
        self.assertEqual(row["created_ms"], 1000)
        self.assertEqual(row["alert_id"], row["alert"]["id"])
        self.assertEqual(row["alert"]["state"], "firing")
        self.assertFalse(row["acked"])
        self.engine.evaluate(2000)
        self.assertEqual(len(self.engine.list_notifications()), 2)
    def test_repeat_only_with_repeat_ms_after_interval(self):
        self.engine.add_route(route(id="a", repeat_ms=5000))
        self.engine.add_route(route(id="b"))  # no repeat
        self.engine.evaluate(1000)
        self.engine.evaluate(5999)
        self.assertEqual(len(self.engine.list_notifications()), 2)
        self.engine.evaluate(6000)
        rows = self.engine.list_notifications()
        self.assertEqual(len(rows), 3)
        self.assertEqual((rows[-1]["route_id"], rows[-1]["created_ms"]), ("a", 6000))
    def test_no_firing_notification_while_pending(self):
        self.engine.del_rule("rule-0001")  # replace the immediate rule from setUp
        self.engine.add_rule(rule(for_ms=60000, window_ms=60000))
        self.engine.add_route(route())
        out = self.engine.evaluate(1000)
        self.assertEqual(out["firing"], [])
        self.assertEqual(self.engine.list_notifications(), [])
    def test_silence_suppresses_and_recovery_renotifies(self):
        self.engine.add_route(route())
        self.engine.evaluate(1000)
        self.engine.add_silence(TENANT, {}, 2000, 3000, "maint")
        self.assertEqual(len(self.engine.evaluate(2500)["silenced"]), 1)
        self.assertEqual(len(self.engine.list_notifications()), 1)
        self.assertEqual(len(self.engine.evaluate(4000)["firing"]), 1)
        self.assertEqual(self.events(), ["firing", "firing"])
    def test_inhibition_suppresses_and_recovery_renotifies(self):
        self.store.write(TENANT, "other_metric", {}, [[0, 1.0]])
        self.engine.add_rule(rule(id="r2", metric="other_metric", severity="critical"))
        self.engine.add_inhibition("critical", "warning")
        self.engine.add_route(route())
        out = self.engine.evaluate(1000)
        self.assertEqual(len(out["inhibited"]), 1)
        self.assertEqual(self.events(), ["firing"])  # only the critical source alert
        # source resolves -> the inhibited alert recovers to firing and re-notifies
        self.store.write(TENANT, "other_metric", {}, [[1000, 50.0]])
        out = self.engine.evaluate(61000)
        self.assertEqual(len(out["firing"]), 1)
        self.assertEqual(self.events(), ["firing", "resolved", "firing"])
    def test_resolved_notifies_exactly_once(self):
        self.engine.add_route(route())
        self.engine.evaluate(1000)
        self.store.write(TENANT, METRIC, {}, [[1000, 50.0]])
        self.assertEqual(len(self.engine.evaluate(61000)["resolved"]), 1)
        self.assertEqual(self.events(), ["firing", "resolved"])
        self.engine.evaluate(62000)
        self.assertEqual(len(self.engine.list_notifications()), 2)
    def test_route_matching_by_labels_severity_and_events(self):
        self.engine.add_route(route(id="labels", labels={"host": "a"}))       # no match
        self.engine.add_route(route(id="sev", severities=["critical"]))       # no match
        self.engine.add_route(route(id="resolved-only", events=["resolved"])) # no firing
        self.engine.add_route(route(id="all"))
        self.engine.evaluate(1000)
        self.assertEqual([row["route_id"] for row in self.engine.list_notifications()], ["all"])
        self.store.write(TENANT, METRIC, {}, [[1000, 50.0]])
        self.engine.evaluate(61000)
        self.assertEqual([row["route_id"] for row in self.engine.list_notifications()],
                         ["all", "all", "resolved-only"])
    def test_refire_after_resolution_notifies_again(self):
        self.engine.add_route(route())
        self.engine.evaluate(1000)
        self.store.write(TENANT, METRIC, {}, [[1000, 50.0]])
        self.engine.evaluate(61000)
        self.store.write(TENANT, METRIC, {}, [[125000, 1.0]])
        self.engine.evaluate(200000)
        self.assertEqual(self.events(), ["firing", "resolved", "firing"])
class TestNotificationQueue(NotifyCase):
    def setUp(self):
        super().setUp()
        self.engine.add_route(route(id="a"))
        self.engine.evaluate(1000)
    def test_list_filters_and_never_consumes(self):
        rows = self.engine.list_notifications()
        self.assertEqual(len(rows), 1)
        alert_id = rows[0]["alert_id"]
        self.assertEqual(len(self.engine.list_notifications(tenant=TENANT)), 1)
        self.assertEqual(self.engine.list_notifications(tenant="other"), [])
        self.assertEqual(len(self.engine.list_notifications(route_id="a")), 1)
        self.assertEqual(self.engine.list_notifications(route_id="b"), [])
        self.assertEqual(len(self.engine.list_notifications(alert_id=alert_id)), 1)
        self.assertEqual(len(self.engine.list_notifications(acked=False)), 1)
        self.assertEqual(self.engine.list_notifications(acked=True), [])
        self.assertEqual(len(self.engine.list_notifications()), 1)  # still there
    def test_ack_is_idempotent_and_unknown_is_rejected(self):
        nid = self.engine.list_notifications()[0]["id"]
        first = self.engine.ack_notification(nid)
        self.assertTrue(first["acked"])
        self.assertEqual(self.engine.ack_notification(nid), first)
        with self.assertRaises(ObsError) as ctx:
            self.engine.ack_notification("notification-99999")
        self.assertTrue(str(ctx.exception).startswith("unknown notification"))
    def test_notifications_and_dedup_survive_restart(self):
        self.engine.ack_notification("notification-00001")
        self.reload()
        rows = self.engine.list_notifications()
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["acked"])
        self.engine.evaluate(2000)  # same firing state: no duplicate after reload
        self.assertEqual(len(self.engine.list_notifications()), 1)
    def test_deleted_route_keeps_its_notifications(self):
        self.engine.del_route("a")
        self.assertEqual(len(self.engine.list_notifications(route_id="a")), 1)
        self.engine.evaluate(2000)
        self.assertEqual(len(self.engine.list_notifications()), 1)
if __name__ == "__main__":
    unittest.main()
