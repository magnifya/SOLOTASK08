"""Tests for obsd.http_app: live ThreadingHTTPServer over a real socket."""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from obsd import AlertEngine, SeriesStore, create_server


class HttpCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-http-")
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
        shutil.rmtree(self.tmp, ignore_errors=True)
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
    def write(self, labels, samples, metric="latency_ms", tenant="acme", status=202):
        code, body = self.request("POST", "/v1/series", {
            "tenant": tenant, "metric": metric, "labels": labels, "samples": samples})
        self.assertEqual(code, status)
        return body
class TestHttpApi(HttpCase):
    def test_healthz(self):
        code, body = self.request("GET", "/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"ok": True})
    def test_write_then_query_with_step_and_agg(self):
        first = self.write({"host": "a"}, [[0, 1.0], [1000, 3.0], [2000, 5.0]])
        self.assertEqual((first["written"], first["duplicates"]), (3, 0))
        second = self.write({"host": "a"}, [[0, 1.0], [1000, 3.0], [2000, 5.0]])
        self.assertEqual((second["written"], second["duplicates"]), (0, 3))
        code, body = self.request(
            "GET", "/v1/query?tenant=acme&metric=latency_ms&label.host=a&step=1000&agg=avg")
        self.assertEqual(code, 200)
        self.assertEqual(body["series"][0]["labels"], {"host": "a"})
        self.assertEqual(body["series"][0]["points"], [[0, 1.0], [1000, 3.0], [2000, 5.0]])
    def test_conflicting_write_returns_409(self):
        self.write({}, [[1000, 1.0]])
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "latency_ms", "labels": {}, "samples": [[1000, 2.0]]})
        self.assertEqual(code, 409)
        self.assertIn("conflict", body["error"])
    def test_malformed_request_returns_400_json_error(self):
        code, body = self.request("POST", "/v1/series", {"metric": "latency_ms"})
        self.assertEqual(code, 400)
        self.assertIn("missing field", body["error"])
        code, body = self.request("GET", "/v1/nope")
        self.assertEqual(code, 404)
        self.assertIn("not found", body["error"])
    def test_rule_evaluate_alerts_and_silence(self):
        self.write({}, [[60000, 5.0], [65000, 5.0]])
        code, rule = self.request("POST", "/v1/rules", {
            "id": "r1", "tenant": "acme", "metric": "latency_ms", "labels": {},
            "comparator": "<", "threshold": 10.0, "for_ms": 0, "window_ms": 30000,
            "agg": "avg", "severity": "warning", "annotations": {"summary": "slow"}})
        self.assertEqual(code, 201)
        self.assertEqual(rule["id"], "r1")
        code, listed = self.request("GET", "/v1/rules?tenant=acme")
        self.assertEqual(code, 200)
        self.assertEqual(len(listed["rules"]), 1)

        code, evaluated = self.request("POST", "/v1/evaluate", {"now_ms": 68000})
        self.assertEqual(code, 200)
        self.assertEqual(len(evaluated["firing"]), 1)
        alert_id = evaluated["firing"][0]["id"]

        # Same condition again: same alert id (dedup), occurrences incremented.
        code, again = self.request("POST", "/v1/evaluate", {"now_ms": 69000})
        self.assertEqual(again["firing"][0]["id"], alert_id)
        self.assertEqual(again["firing"][0]["occurrences"], 2)

        code, alerts = self.request("GET", "/v1/alerts?tenant=acme&state=firing")
        self.assertEqual(code, 200)
        self.assertEqual([row["id"] for row in alerts["alerts"]], [alert_id])

        code, silence = self.request("POST", "/v1/silences", {
            "tenant": "acme", "labels": {}, "starts_ms": 0, "ends_ms": 90000,
            "reason": "maintenance"})
        self.assertEqual(code, 201)
        code, silenced = self.request("POST", "/v1/evaluate", {"now_ms": 70000})
        self.assertEqual(silenced["firing"], [])
        self.assertEqual(len(silenced["silenced"]), 1)
        self.assertEqual(silenced["silenced"][0]["silenced_by"], silence["id"])
    def test_evaluate_without_now_ms_is_rejected(self):
        code, body = self.request("POST", "/v1/evaluate", {})
        self.assertEqual(code, 400)
        self.assertIn("now_ms", body["error"])
    def test_group_by_query_and_errors(self):
        self.write({"host": "a"}, [[0, 1.0], [1000, 3.0]])
        self.write({"host": "b"}, [[1000, 9.0]])
        code, body = self.request(
            "GET", "/v1/query?tenant=acme&metric=latency_ms&agg=avg&group_by=%5B%22host%22%5D")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"series": [
            {"labels": {"host": "a"}, "points": [[0, 2.0]]},
            {"labels": {"host": "b"}, "points": [[1000, 9.0]]}]})
        # An empty group_by array merges every matching series into one group.
        code, body = self.request(
            "GET", "/v1/query?tenant=acme&metric=latency_ms&agg=avg&group_by=%5B%5D")
        self.assertEqual(body, {"series": [{"labels": {}, "points": [[0, 13.0 / 3.0]]}]})
        # Malformed group_by values are 400 with the usual error object.
        for group_by in ("[host", "%22host%22", "%5B%22host%22%2C%22host%22%5D", "%5B%5D"):
            code, body = self.request(
                "GET", "/v1/query?tenant=acme&metric=latency_ms&group_by=" + group_by)
            self.assertEqual(code, 400, group_by)
            self.assertIn("error", body)
        code, body = self.request(
            "GET", "/v1/query?tenant=acme&metric=latency_ms&agg=median&group_by=%5B%5D")
        self.assertEqual(code, 400)
        self.assertIn("error", body)
    def test_quotas_set_get_usage_and_enforcement(self):
        code, body = self.request("GET", "/v1/quotas?tenant=acme")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"tenant": "acme", "max_series": None, "max_points": None,
                                "series": 0, "points": 0})
        code, body = self.request("POST", "/v1/quotas",
                                  {"tenant": "acme", "max_series": 1, "max_points": 2})
        self.assertEqual(code, 200)
        self.assertEqual(body, {"tenant": "acme", "max_series": 1, "max_points": 2,
                                "series": 0, "points": 0})
        self.write({}, [[1000, 1.0], [2000, 2.0]])
        code, body = self.request("GET", "/v1/quotas?tenant=acme")
        self.assertEqual((code, body["series"], body["points"]), (200, 1, 2))
        # A batch exceeding max_points is rejected with 409 and changes nothing.
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "latency_ms", "labels": {},
            "samples": [[2000, 2.0], [3000, 3.0]]})
        self.assertEqual(code, 409)
        self.assertIn("quota exceeded", body["error"])
        code, body = self.request("GET", "/v1/quotas?tenant=acme")
        self.assertEqual((body["series"], body["points"]), (1, 2))
        # Duplicates and overwrites still succeed at the limit.
        self.write({}, [[1000, 1.0]])
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "latency_ms", "labels": {},
            "samples": [[1000, 9.0]], "overwrite": True})
        self.assertEqual(code, 202)
        # A second series hits max_series with 409.
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "latency_ms", "labels": {"host": "b"},
            "samples": [[1000, 1.0]]})
        self.assertEqual(code, 409)
        self.assertIn("max_series", body["error"])
    def test_invalid_quota_requests_return_400_and_keep_config(self):
        self.request("POST", "/v1/quotas", {"tenant": "acme", "max_series": 1, "max_points": 4})
        for bad in (True, "1", 1.5, -1):
            code, body = self.request("POST", "/v1/quotas",
                                      {"tenant": "acme", "max_series": bad, "max_points": 4})
            self.assertEqual(code, 400, repr(bad))
            self.assertIn("error", body)
        code, body = self.request("POST", "/v1/quotas",
                                  {"tenant": "", "max_series": 1, "max_points": 1})
        self.assertEqual(code, 400)
        code, body = self.request("GET", "/v1/quotas")
        self.assertEqual(code, 400)
        code, body = self.request("GET", "/v1/quotas?tenant=acme")
        self.assertEqual((code, body["max_series"], body["max_points"]), (200, 1, 4))
    def test_slo_set_and_status(self):
        self.write({}, [[index * 1000, 1.0 if index else 0.0] for index in range(10)])
        code, slo = self.request("POST", "/v1/slos", {
            "tenant": "acme", "name": "availability", "metric": "latency_ms", "labels": {},
            "good_comparator": ">=", "threshold": 1.0, "target_ratio": 0.9, "window_ms": 60000})
        self.assertEqual(code, 201)
        self.assertEqual(slo["name"], "availability")
        code, status = self.request(
            "GET", "/v1/slos/status?tenant=acme&name=availability&now_ms=60000")
        self.assertEqual(code, 200)
        self.assertEqual((status["total"], status["good"], status["bad"]), (10, 9, 1))
        self.assertAlmostEqual(status["ratio"], 0.9)
        self.assertAlmostEqual(status["error_budget"], 0.0)
        self.assertAlmostEqual(status["burn_rate"], 1.0)
        self.assertTrue(status["met"])
    def test_notification_routes_and_queue(self):
        self.write({}, [[0, 5.0]])
        code, _ = self.request("POST", "/v1/rules", {
            "tenant": "acme", "metric": "latency_ms", "labels": {}, "comparator": "<",
            "threshold": 10.0, "for_ms": 0, "window_ms": 60000, "agg": "avg",
            "severity": "warning", "annotations": {}})
        self.assertEqual(code, 201)
        # Validation failures are 400, duplicates 409, unknown routes 404.
        for bad in ({}, {"tenant": "acme"},
                    {"tenant": "acme", "target": "t", "events": ["bogus"]},
                    {"tenant": "acme", "target": "t", "severities": []},
                    {"tenant": "acme", "target": "t", "repeat_ms": -1}):
            code, body = self.request("POST", "/v1/notification-routes", bad)
            self.assertEqual(code, 400, repr(bad))
            self.assertIn("error", body)
        code, route = self.request("POST", "/v1/notification-routes",
                                   {"tenant": "acme", "target": "pager"})
        self.assertEqual(code, 201)
        self.assertEqual(route["id"], "route-0001")
        self.assertEqual(route["events"], ["firing", "resolved"])
        code, body = self.request("POST", "/v1/notification-routes",
                                  {"id": "route-0001", "tenant": "acme", "target": "x"})
        self.assertEqual(code, 409)
        code, listed = self.request("GET", "/v1/notification-routes?tenant=acme")
        self.assertEqual([row["id"] for row in listed["routes"]], ["route-0001"])
        # Evaluating queues one firing notification for the matching route.
        code, evaluated = self.request("POST", "/v1/evaluate", {"now_ms": 1000})
        self.assertEqual(len(evaluated["firing"]), 1)
        code, notes = self.request("GET", "/v1/notifications?tenant=acme&acked=false")
        self.assertEqual(code, 200)
        self.assertEqual(len(notes["notifications"]), 1)
        note = notes["notifications"][0]
        self.assertEqual((note["route_id"], note["event"], note["created_ms"]),
                         ("route-0001", "firing", 1000))
        code, empty = self.request("GET", "/v1/notifications?acked=true")
        self.assertEqual(empty["notifications"], [])
        code, body = self.request("GET", "/v1/notifications?acked=maybe")
        self.assertEqual(code, 400)
        # Ack is idempotent; unknown ids are 404.
        code, acked = self.request("POST", "/v1/notifications/%s/ack" % note["id"])
        self.assertEqual(code, 200)
        self.assertTrue(acked["acked"])
        code, again = self.request("POST", "/v1/notifications/%s/ack" % note["id"])
        self.assertEqual((code, again), (200, acked))
        code, body = self.request("POST", "/v1/notifications/notification-99999/ack")
        self.assertEqual(code, 404)
        # Deleting the route keeps the queued notification.
        code, deleted = self.request("DELETE", "/v1/notification-routes/route-0001")
        self.assertEqual((code, deleted), (200, {"deleted": "route-0001"}))
        code, body = self.request("DELETE", "/v1/notification-routes/route-0001")
        self.assertEqual(code, 404)
        code, notes = self.request("GET", "/v1/notifications?route_id=route-0001")
        self.assertEqual(len(notes["notifications"]), 1)
if __name__ == "__main__":
    unittest.main()
