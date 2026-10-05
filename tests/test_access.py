"""Tests for obsd.access: principals, authorization and the audit log."""

import hashlib
import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from obsd import AccessControl, AlertEngine, AuditLog, ObsError, SeriesStore, \
    create_server


class AccessControlCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-access-")
        self.root = os.path.join(self.tmp, "data")
        self.access = AccessControl(self.root)
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestPrincipals(AccessControlCase):
    def test_disabled_until_first_principal(self):
        self.assertFalse(self.access.enabled)
        self.access.create_principal("ops", "secret", "admin", ["acme"])
        self.assertTrue(self.access.enabled)

    def test_create_list_and_token_never_stored(self):
        record = self.access.create_principal("viewer-1", "tok-abc", "viewer",
                                              ["acme", "acme", "eu"])
        self.assertEqual(record, {"id": "viewer-1", "role": "viewer",
                                  "tenants": ["acme", "eu"]})
        self.assertNotIn("tok-abc", json.dumps(record))
        self.assertEqual(self.access.list_principals(), [record])
        with open(self.access.principals_path, "r", encoding="utf-8") as handle:
            on_disk = handle.read()
        self.assertNotIn("tok-abc", on_disk)
        self.assertIn(hashlib.sha256(b"tok-abc").hexdigest(), on_disk)

    def test_create_validation(self):
        for bad in (("", "t", "viewer", ["a"]), ("x", "", "viewer", ["a"]),
                    ("x", "t", "root", ["a"]), ("x", "t", "viewer", []),
                    ("x", "t", "viewer", ["a", ""])):
            with self.assertRaises(ObsError, msg=repr(bad)):
                self.access.create_principal(*bad)
        self.assertEqual(self.access.list_principals(), [])

    def test_duplicate_id_conflicts(self):
        self.access.create_principal("ops", "one", "admin", ["acme"])
        with self.assertRaises(ObsError) as ctx:
            self.access.create_principal("ops", "two", "viewer", ["acme"])
        self.assertIn("conflict", str(ctx.exception))

    def test_revoke_and_unknown_principal(self):
        self.access.create_principal("ops", "secret", "admin", ["acme"])
        self.assertIsNotNone(self.access.authenticate("secret"))
        self.access.revoke_principal("ops")
        self.assertEqual(self.access.list_principals(), [])
        self.assertIsNone(self.access.authenticate("secret"))
        for principal_id in ("ops", "nobody"):
            with self.assertRaises(ObsError) as ctx:
                self.access.revoke_principal(principal_id)
            self.assertIn("unknown principal", str(ctx.exception))
        # Revoking the last principal keeps access control enabled.
        self.assertTrue(self.access.enabled)

    def test_principals_survive_reopen(self):
        self.access.create_principal("ops", "secret", "admin", ["acme"])
        reopened = AccessControl(self.root)
        self.assertTrue(reopened.enabled)
        self.assertEqual(reopened.authenticate("secret")["id"], "ops")
        self.assertIsNone(reopened.authenticate("wrong"))


class HttpCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-http-access-")
        self.root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(self.root)
        self.engine = AlertEngine(self.store, self.root)
        self.access = AccessControl(self.root)
        self.audit = AuditLog(self.root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0,
                                    access=self.access, audit=self.audit)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.tmp, ignore_errors=True)
    def request(self, method, path, payload=None, token=None):
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=body, method=method)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        if token is not None:
            req.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))
    def principal(self, principal_id, token, role, tenants):
        return self.access.create_principal(principal_id, token, role, tenants)


class TestAnonymousWhenDisabled(HttpCase):
    def test_no_config_means_no_401_or_403(self):
        code, body = self.request("GET", "/healthz")
        self.assertEqual((code, body), (200, {"ok": True}))
        code, _ = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1, 2.0]]})
        self.assertEqual(code, 202)
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        self.assertEqual(body["series"][0]["points"], [[1, 2.0]])
        code, _ = self.request("GET", "/v1/stats")
        self.assertEqual(code, 200)
        # The audit console is reachable anonymously while disabled.
        code, body = self.request("GET", "/v1/audit")
        self.assertEqual(code, 200)
        self.assertTrue(body["entries"])
        self.assertTrue(all(entry["principal_id"] is None
                            for entry in body["entries"]))


class TestAuthentication(HttpCase):
    def setUp(self):
        super().setUp()
        self.principal("admin", "adm-token", "admin", ["acme"])
        self.principal("writer", "wrt-token", "writer", ["acme"])
        self.principal("viewer", "vie-token", "viewer", ["acme"])
    def test_healthz_stays_public(self):
        code, body = self.request("GET", "/healthz")
        self.assertEqual((code, body), (200, {"ok": True}))
    def test_missing_malformed_and_wrong_tokens_are_401(self):
        for headers in ({}, {"Authorization": "vie-token"},
                        {"Authorization": "Bearer"},
                        {"Authorization": "Token vie-token"},
                        {"Authorization": "Bearer nope"}):
            req = urllib.request.Request(self.base + "/v1/stats", method="GET")
            for key, value in headers.items():
                req.add_header(key, value)
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    code = response.status
            except urllib.error.HTTPError as exc:
                code, body = exc.code, json.loads(exc.read().decode("utf-8"))
            self.assertEqual(code, 401, repr(headers))
            self.assertEqual(body, {"error": "unauthorized"})
    def test_roles_and_scope(self):
        # viewer reads in scope, cannot write, cannot read other tenants.
        code, _ = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1, 1.0]]},
            token="wrt-token")
        self.assertEqual(code, 202)
        code, _ = self.request("GET", "/v1/query?tenant=acme&metric=m",
                               token="vie-token")
        self.assertEqual(code, 200)
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[2, 2.0]]},
            token="vie-token")
        self.assertEqual((code, body), (403, {"error": "forbidden"}))
        code, _ = self.request("GET", "/v1/query?tenant=other&metric=m",
                               token="vie-token")
        self.assertEqual(code, 403)
        # writer cannot do admin operations.
        code, _ = self.request("POST", "/v1/quotas",
                               {"tenant": "acme", "max_series": 1}, token="wrt-token")
        self.assertEqual(code, 403)
        code, _ = self.request("GET", "/v1/stats", token="wrt-token")
        self.assertEqual(code, 403)
        code, _ = self.request("POST", "/v1/evaluate", {"now_ms": 10},
                               token="wrt-token")
        self.assertEqual(code, 403)
        # admin does everything, across tenants.
        code, _ = self.request("POST", "/v1/quotas",
                               {"tenant": "acme", "max_series": 10}, token="adm-token")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/stats", token="adm-token")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/query?tenant=other&metric=m",
                               token="adm-token")
        self.assertEqual(code, 200)
    def test_cross_tenant_reads_need_admin(self):
        code, _ = self.request("GET", "/v1/alerts", token="vie-token")
        self.assertEqual(code, 403)
        code, _ = self.request("GET", "/v1/alerts?tenant=acme", token="vie-token")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/alerts", token="adm-token")
        self.assertEqual(code, 200)
    def test_batch_scope_checked_before_writing(self):
        code, body = self.request("POST", "/v1/series/batch", {"entries": [
            {"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]},
            {"tenant": "other", "metric": "m", "samples": [[1, 1.0]]}]},
            token="wrt-token")
        self.assertEqual((code, body), (403, {"error": "forbidden"}))
        # Nothing was written: the batch stayed atomic.
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m",
                                  token="adm-token")
        self.assertEqual(body, {"series": []})
        code, body = self.request("POST", "/v1/series/batch", {"entries": [
            {"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]}]},
            token="wrt-token")
        self.assertEqual(code, 202)
    def test_revoked_token_stops_working(self):
        self.principal("temp", "tmp-token", "viewer", ["acme"])
        code, _ = self.request("GET", "/v1/alerts?tenant=acme", token="tmp-token")
        self.assertEqual(code, 200)
        self.access.revoke_principal("temp")
        code, _ = self.request("GET", "/v1/alerts?tenant=acme", token="tmp-token")
        self.assertEqual(code, 401)
    def test_cli_side_changes_reach_a_running_server(self):
        # A second AccessControl (what a CLI invocation would use) edits the
        # same directory; the server's instance picks the change up.
        external = AccessControl(self.root)
        external.create_principal("late", "late-token", "viewer", ["acme"])
        code, _ = self.request("GET", "/v1/alerts?tenant=acme", token="late-token")
        self.assertEqual(code, 200)
        external.revoke_principal("late")
        code, _ = self.request("GET", "/v1/alerts?tenant=acme", token="late-token")
        self.assertEqual(code, 401)


class TestAudit(HttpCase):
    def setUp(self):
        super().setUp()
        self.principal("admin", "adm-token", "admin", ["acme"])
        self.principal("viewer", "vie-token", "viewer", ["acme"])
    def test_every_request_is_recorded(self):
        self.request("GET", "/healthz")  # never audited
        self.request("GET", "/v1/stats")  # 401
        self.request("GET", "/v1/stats", token="vie-token")  # 403
        self.request("GET", "/v1/stats", token="adm-token")  # 200
        self.request("GET", "/v1/nope", token="adm-token")  # 404
        code, body = self.request("GET", "/v1/audit", token="adm-token")
        self.assertEqual(code, 200)
        entries = body["entries"]
        # The audit request itself is logged after its response is sent.
        self.assertEqual([entry["seq"] for entry in entries], [1, 2, 3, 4])
        self.assertEqual(
            [(entry["principal_id"], entry["path"], entry["outcome"], entry["status"])
             for entry in entries],
            [(None, "/v1/stats", "denied", 401),
             ("viewer", "/v1/stats", "denied", 403),
             ("admin", "/v1/stats", "allowed", 200),
             ("admin", "/v1/nope", "failed", 404)])
        self.assertTrue(all(entry["tenant"] is None for entry in entries))
        code, body = self.request("GET", "/v1/audit?after_seq=4", token="adm-token")
        self.assertEqual([(entry["path"], entry["outcome"])
                          for entry in body["entries"]],
                         [("/v1/audit", "allowed")])
    def test_tenant_extraction_and_filters(self):
        self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1, 1.0]]},
            token="adm-token")
        self.request("POST", "/v1/series/batch", {"entries": [
            {"tenant": "acme", "metric": "m", "samples": [[2, 2.0]]},
            {"tenant": "eu", "metric": "m", "samples": [[2, 2.0]]}]},
            token="adm-token")
        code, body = self.request("GET", "/v1/audit?tenant=acme", token="adm-token")
        tenants = [entry["tenant"] for entry in body["entries"]]
        self.assertIn("acme", tenants)
        self.assertNotIn(None, tenants)
        code, body = self.request("GET", "/v1/audit?outcome=allowed&limit=1",
                                  token="adm-token")
        self.assertEqual(len(body["entries"]), 1)
        seq = body["entries"][0]["seq"]
        code, body = self.request("GET", "/v1/audit?after_seq=%d" % seq,
                                  token="adm-token")
        self.assertTrue(all(entry["seq"] > seq for entry in body["entries"]))
        code, body = self.request("GET", "/v1/audit?principal_id=admin",
                                  token="adm-token")
        self.assertTrue(all(entry["principal_id"] == "admin"
                            for entry in body["entries"]))
    def test_audit_requires_admin_and_valid_filters(self):
        code, _ = self.request("GET", "/v1/audit", token="vie-token")
        self.assertEqual(code, 403)
        for query in ("outcome=bogus", "limit=0", "limit=-1", "limit=x",
                      "after_seq=x"):
            code, body = self.request("GET", "/v1/audit?" + query, token="adm-token")
            self.assertEqual((code, sorted(body)), (400, ["error"]), query)
    def test_audit_survives_restart_and_hides_tokens(self):
        self.request("GET", "/v1/stats", token="adm-token")
        reopened = AuditLog(self.root)
        entries = reopened.query()
        self.assertEqual([entry["seq"] for entry in entries], [1])
        self.assertEqual(entries[0]["principal_id"], "admin")
        with open(self.audit.audit_path, "r", encoding="utf-8") as handle:
            self.assertNotIn("adm-token", handle.read())


if __name__ == "__main__":
    unittest.main()
