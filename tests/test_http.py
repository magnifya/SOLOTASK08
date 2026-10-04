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


class TestQuotasHttp(HttpCase):
    def quota(self, tenant):
        return self.request("GET", "/v1/quotas?tenant=%s" % tenant)

    def set_quota(self, payload):
        return self.request("POST", "/v1/quotas", payload)

    def test_unconfigured_tenant_reports_null_limits_and_zero_usage(self):
        code, body = self.quota("acme")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"tenant": "acme", "max_series": None, "max_points": None,
                                "series": 0, "points": 0})

    def test_set_and_query_quota_returns_200_with_five_fields(self):
        code, body = self.set_quota({"tenant": "acme", "max_series": 1, "max_points": 2})
        self.assertEqual(code, 200)
        self.assertEqual(body, {"tenant": "acme", "max_series": 1, "max_points": 2,
                                "series": 0, "points": 0})
        code, body = self.set_quota({"tenant": "globex", "max_points": 5})  # omitted field
        self.assertEqual(code, 200)
        self.assertIsNone(body["max_series"])
        self.assertEqual(body["max_points"], 5)
        code, body = self.quota("acme")
        self.assertEqual(code, 200)
        self.assertEqual((body["series"], body["points"]), (0, 0))

    def test_invalid_quota_requests_are_400_and_do_not_change_config(self):
        self.set_quota({"tenant": "acme", "max_series": 1, "max_points": 2})
        for bad in (
            {"max_series": 1, "max_points": 1},
            {"tenant": "", "max_series": 1, "max_points": 1},
            {"tenant": "acme", "max_series": True},
            {"tenant": "acme", "max_points": "2"},
            {"tenant": "acme", "max_series": 0.5},
            {"tenant": "acme", "max_points": -1},
        ):
            code, body = self.set_quota(bad)
            self.assertEqual(code, 400, bad)
            self.assertIn("error", body)
        code, body = self.request("GET", "/v1/quotas")
        self.assertEqual(code, 400)
        code, body = self.quota("acme")
        self.assertEqual((body["max_series"], body["max_points"]), (1, 2))

    def test_writes_are_limited_and_409_rejects_whole_batch(self):
        self.set_quota({"tenant": "acme", "max_series": 1, "max_points": 2})
        body = self.write({}, [[1000, 1.0], [1000, 1.0], [2000, 2.0]])
        self.assertEqual((body["written"], body["duplicates"]), (2, 1))
        code, body = self.quota("acme")
        self.assertEqual((body["series"], body["points"]), (1, 2))
        # New series rejected (series dimension), no partial series/points.
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "other", "labels": {}, "samples": [[1000, 1.0]]})
        self.assertEqual(code, 409)
        self.assertIn("quota", body["error"])
        # New point rejected (points dimension).
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "latency_ms", "labels": {}, "samples": [[3000, 3.0]]})
        self.assertEqual(code, 409)
        # Pure replay still succeeds while at the cap.
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "latency_ms", "labels": {}, "samples": [[1000, 1.0]]})
        self.assertEqual(code, 202)
        self.assertEqual((body["written"], body["duplicates"]), (0, 1))
        code, status = self.quota("acme")
        self.assertEqual(status, {"tenant": "acme", "max_series": 1, "max_points": 2,
                                  "series": 1, "points": 2})

    def test_quota_does_not_touch_other_tenants(self):
        self.set_quota({"tenant": "acme", "max_series": 0, "max_points": 0})
        code, body = self.request("POST", "/v1/series", {
            "tenant": "globex", "metric": "latency_ms", "labels": {},
            "samples": [[1000, 1.0]]})
        self.assertEqual(code, 202, body)
        code, body = self.quota("globex")
        self.assertEqual((body["series"], body["points"], body["max_points"]), (1, 1, None))


if __name__ == "__main__":
    unittest.main()
