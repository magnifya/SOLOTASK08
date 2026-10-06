"""Tests for obsd.access and the access-controlled HTTP surface."""

import contextlib
import io
import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from obsd import AccessControl, AlertEngine, SeriesStore, create_server
from obsd.cli import main as cli_main
from obsd.tsdb import ObsError


class AccessControlCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-access-")
        self.root = os.path.join(self.tmp, "data")
        self.access = AccessControl(self.root)
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestAccessControl(AccessControlCase):
    def test_disabled_until_first_principal(self):
        self.assertFalse(self.access.enabled())
        self.assertEqual(self.access.list_principals(), [])
        self.access.create_principal("admin", "secret", "admin", ["acme"])
        self.assertTrue(self.access.enabled())

    def test_create_list_revoke_roundtrip(self):
        created = self.access.create_principal("viewer-1", "tok", "viewer",
                                               ["acme", "acme", "globex"])
        self.assertEqual(created, {"id": "viewer-1", "role": "viewer",
                                   "tenants": ["acme", "globex"]})
        self.assertNotIn("tok", json.dumps(created))
        self.assertEqual(self.access.list_principals(), [created])
        self.assertEqual(self.access.revoke_principal("viewer-1"),
                         {"revoked": "viewer-1"})
        self.assertEqual(self.access.list_principals(), [])
        # The config keeps existing (and enforcing) with zero principals.
        self.assertTrue(self.access.enabled())

    def test_validation_and_conflicts(self):
        for bad in ({}, {"token": "t"}, {"token": "t", "role": "viewer"},
                    {"token": "t", "role": "viewer", "tenants": []}):
            with self.assertRaises(ObsError, msg=repr(bad)):
                self.access.create_principal(bad.get("id"), bad.get("token"),
                                             bad.get("role"),
                                             bad.get("tenants", ["acme"]))
        for args in (("", "t", "viewer", ["acme"]),
                     ("p", "", "viewer", ["acme"]),
                     ("p", "t", "root", ["acme"]),
                     ("p", "t", "viewer", []),
                     ("p", "t", "viewer", [""])):
            with self.assertRaises(ObsError, msg=repr(args)):
                self.access.create_principal(*args)
        self.access.create_principal("p", "t", "viewer", ["acme"])
        with self.assertRaisesRegex(ObsError, "conflict"):
            self.access.create_principal("p", "other", "admin", ["acme"])
        with self.assertRaisesRegex(ObsError, "unknown principal"):
            self.access.revoke_principal("ghost")

    def test_only_digest_is_persisted(self):
        self.access.create_principal("p", "super-secret-token", "admin", ["acme"])
        with open(self.access.access_path, "r", encoding="utf-8") as handle:
            raw = handle.read()
        self.assertNotIn("super-secret-token", raw)
        self.assertIn("token_sha256", raw)
        self.assertNotIn("token", json.dumps(self.access.list_principals()))

    def test_authenticate_and_restart(self):
        self.access.create_principal("p", "tok", "writer", ["acme"])
        self.assertIsNone(self.access.authenticate("wrong"))
        self.assertIsNone(self.access.authenticate(""))
        self.assertEqual(self.access.authenticate("tok")["id"], "p")
        reopened = AccessControl(self.root)
        self.assertTrue(reopened.enabled())
        self.assertEqual(reopened.authenticate("tok")["role"], "writer")
        reopened.revoke_principal("p")
        self.assertIsNone(AccessControl(self.root).authenticate("tok"))

    def test_audit_seq_filters_limit_and_restart(self):
        self.assertEqual(self.access.query_audit(), [])
        self.access.record(None, "GET", "/v1/query", "acme", "denied", 401)
        self.access.record("p", "POST", "/v1/series", "acme", "allowed", 202)
        self.access.record("p", "GET", "/v1/stats", None, "failed", 400)
        entries = self.access.query_audit()
        self.assertEqual([row["seq"] for row in entries], [1, 2, 3])
        self.assertEqual(entries[0], {"seq": 1, "principal_id": None,
                                      "method": "GET", "path": "/v1/query",
                                      "tenant": "acme", "outcome": "denied",
                                      "status": 401})
        self.assertEqual([r["seq"] for r in self.access.query_audit(tenant="acme")],
                         [1, 2])
        self.assertEqual([r["seq"] for r in self.access.query_audit(principal_id="p")],
                         [2, 3])
        self.assertEqual([r["seq"] for r in self.access.query_audit(outcome="denied")],
                         [1])
        self.assertEqual([r["seq"] for r in self.access.query_audit(after_seq=1)],
                         [2, 3])
        self.assertEqual([r["seq"] for r in self.access.query_audit(limit=2)], [1, 2])
        self.assertEqual([r["seq"] for r in
                          self.access.query_audit(after_seq=1, limit=1)], [2])
        for bad in ({"outcome": "bogus"}, {"after_seq": -1}, {"after_seq": 1.5},
                    {"limit": 0}, {"limit": -2}, {"limit": 1.5}, {"limit": True}):
            with self.assertRaises(ObsError, msg=repr(bad)):
                self.access.query_audit(**bad)
        # The log survives a restart and seq keeps increasing.
        reopened = AccessControl(self.root)
        self.assertEqual(len(reopened.query_audit()), 3)
        reopened.record("p", "GET", "/v1/alerts", None, "allowed", 200)
        self.assertEqual(reopened.query_audit()[-1]["seq"], 4)
        raw = open(reopened.audit_path, encoding="utf-8").read()
        self.assertNotIn("tok", raw)


class HttpAccessCase(unittest.TestCase):
    access = None
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-http-access-")
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.access = AccessControl(root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0,
                                    access=self.access)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.tmp, ignore_errors=True)
    def request(self, method, path, payload=None, token=None, raw_headers=None):
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=body, method=method)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        if token is not None:
            req.add_header("Authorization", "Bearer " + token)
        for key, value in (raw_headers or {}).items():
            req.add_header(key, value)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))
    def make_principal(self, pid, token, role, tenants):
        return self.access.create_principal(pid, token, role, tenants)


class TestHttpAccessDisabled(HttpAccessCase):
    def test_anonymous_surface_is_unchanged(self):
        code, body = self.request("GET", "/healthz")
        self.assertEqual((code, body), (200, {"ok": True}))
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1, 2.0]]})
        self.assertEqual(code, 202)
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        self.assertEqual(body["series"][0]["points"], [[1, 2.0]])
        code, body = self.request("GET", "/v1/stats")
        self.assertEqual(code, 200)
        # Type errors are rejected as usual but leave no audit records behind.
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1, 2.0]],
            "overwrite": None})
        self.assertEqual((code, body), (400, {"error": "overwrite must be a boolean"}))
        code, body = self.request("POST", "/v1/inhibitions", {
            "source_severity": "critical", "target_severity": "warning",
            "same_labels": None})
        self.assertEqual((code, body), (400, {"error": "same_labels must be a boolean"}))
        # No 401/403 and no audit records while access control is off.
        self.assertFalse(os.path.exists(self.access.audit_path))
        code, body = self.request("GET", "/v1/audit")
        self.assertEqual((code, body), (200, {"entries": []}))


class TestHttpAccessEnabled(HttpAccessCase):
    def setUp(self):
        super().setUp()
        self.make_principal("root", "admin-tok", "admin", ["acme"])
        self.make_principal("reader", "view-tok", "viewer", ["acme"])
        self.make_principal("writer", "write-tok", "writer", ["acme"])
        self.make_principal("outsider", "out-tok", "writer", ["globex"])
    def admin(self, method, path, payload=None):
        return self.request(method, path, payload, token="admin-tok")
    def test_healthz_stays_public_and_unaudited(self):
        code, body = self.request("GET", "/healthz")
        self.assertEqual((code, body), (200, {"ok": True}))
        self.assertEqual(self.access.query_audit(), [])
    def test_missing_malformed_and_wrong_token_are_401(self):
        for headers in (None, {}, {"Authorization": "view-tok"},
                        {"Authorization": "Bearer"},
                        {"Authorization": "Basic view-tok"},
                        {"Authorization": "Bearer wrong"},
                        {"Authorization": "Bearer view-tok extra"}):
            code, body = self.request("GET", "/v1/alerts?tenant=acme",
                                      raw_headers=headers)
            self.assertEqual((code, body), (401, {"error": "unauthorized"}))
        entries = self.access.query_audit()
        self.assertEqual(len(entries), 7)
        self.assertTrue(all(row["principal_id"] is None for row in entries))
        self.assertTrue(all(row["outcome"] == "denied" for row in entries))
        self.assertTrue(all(row["status"] == 401 for row in entries))
    def test_viewer_reads_own_tenant_only(self):
        self.admin("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1, 2.0]]})
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m",
                                  token="view-tok")
        self.assertEqual(code, 200)
        self.assertEqual(body["series"][0]["points"], [[1, 2.0]])
        # Writes, other tenants and cross-tenant reads are forbidden.
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[2, 3.0]]},
            token="view-tok")
        self.assertEqual((code, body), (403, {"error": "forbidden"}))
        code, _ = self.request("GET", "/v1/query?tenant=globex&metric=m",
                               token="view-tok")
        self.assertEqual(code, 403)
        code, _ = self.request("GET", "/v1/alerts", token="view-tok")
        self.assertEqual(code, 403)
        code, _ = self.request("GET", "/v1/audit", token="view-tok")
        self.assertEqual(code, 403)
    def test_writer_scope_and_admin_only_endpoints(self):
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1, 2.0]]},
            token="write-tok")
        self.assertEqual(code, 202)
        code, _ = self.request("POST", "/v1/rules", {
            "tenant": "acme", "metric": "m", "labels": {}, "comparator": ">",
            "threshold": 1.0, "window_ms": 1000, "agg": "avg",
            "severity": "info", "annotations": {}}, token="write-tok")
        self.assertEqual(code, 201)
        # Out-of-scope tenant and admin-only operations are forbidden.
        code, _ = self.request("POST", "/v1/series", {
            "tenant": "globex", "metric": "m", "labels": {}, "samples": [[1, 1.0]]},
            token="write-tok")
        self.assertEqual(code, 403)
        for method, path, payload in (
                ("POST", "/v1/quotas", {"tenant": "acme", "max_series": 1}),
                ("GET", "/v1/quotas?tenant=acme", None),
                ("POST", "/v1/evaluate", {"now_ms": 1000}),
                ("GET", "/v1/stats", None),
                ("POST", "/v1/inhibitions", {"source_severity": "critical",
                                             "target_severity": "info"})):
            code, body = self.request(method, path, payload, token="write-tok")
            self.assertEqual((code, body), (403, {"error": "forbidden"}))
        # The outsider's own tenant works; acme does not.
        code, _ = self.request("POST", "/v1/series", {
            "tenant": "globex", "metric": "m", "labels": {}, "samples": [[1, 1.0]]},
            token="out-tok")
        self.assertEqual(code, 202)
        code, _ = self.request("GET", "/v1/query?tenant=acme&metric=m",
                               token="out-tok")
        self.assertEqual(code, 403)
    def test_admin_does_everything_including_cross_tenant(self):
        code, body = self.admin("POST", "/v1/series", {
            "tenant": "globex", "metric": "m", "labels": {}, "samples": [[1, 1.0]]})
        self.assertEqual(code, 202)
        code, body = self.admin("GET", "/v1/alerts")
        self.assertEqual(code, 200)
        code, body = self.admin("POST", "/v1/evaluate", {"now_ms": 1000})
        self.assertEqual(code, 200)
        code, body = self.admin("GET", "/v1/stats")
        self.assertEqual(code, 200)
    def test_batch_is_checked_per_entry_before_writing(self):
        entries = [
            {"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]},
            {"tenant": "globex", "metric": "m", "samples": [[1, 1.0]]}]
        code, body = self.request("POST", "/v1/series/batch", {"entries": entries},
                                  token="write-tok")
        self.assertEqual((code, body), (403, {"error": "forbidden"}))
        # Nothing was written: the rejection is atomic.
        code, body = self.admin("GET", "/v1/query?tenant=acme&metric=m")
        self.assertEqual(body, {"series": []})
        # A batch fully inside the caller's scope (even multi-tenant) passes.
        self.make_principal("multi", "multi-tok", "writer", ["acme", "globex"])
        code, body = self.request("POST", "/v1/series/batch", {"entries": entries},
                                  token="multi-tok")
        self.assertEqual(code, 202)
        self.assertEqual(body["written"], 2)
    def test_business_errors_are_unchanged_when_authorized(self):
        code, body = self.admin("POST", "/v1/series", {"metric": "m"})
        self.assertEqual(code, 400)
        self.assertIn("missing field", body["error"])
        code, body = self.admin("GET", "/v1/nope")
        self.assertEqual(code, 404)
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m&agg=bogus",
                                  token="view-tok")
        self.assertEqual(code, 400)
    def test_type_errors_are_audited_as_failed_400(self):
        # The authenticated principal's type errors are recorded as failed/400.
        code, body = self.admin("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1, 1.0]],
            "overwrite": None})
        self.assertEqual((code, body), (400, {"error": "overwrite must be a boolean"}))
        code, body = self.admin("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1, 1.0]],
            "now_ms": True})
        self.assertEqual(code, 400)
        code, body = self.admin("POST", "/v1/inhibitions", {
            "source_severity": "critical", "target_severity": "warning",
            "same_labels": None})
        self.assertEqual((code, body), (400, {"error": "same_labels must be a boolean"}))
        entries = self.access.query_audit()
        self.assertEqual([(row["principal_id"], row["outcome"], row["status"])
                          for row in entries],
                         [("root", "failed", 400)] * 3)
        self.assertEqual([row["tenant"] for row in entries],
                         ["acme", "acme", None])
        # The rejections created nothing.
        code, body = self.admin("GET", "/v1/stats")
        self.assertEqual((body["store"]["series"], body["store"]["points"],
                          body["store"]["writes"]), (0, 0, 0))
        self.assertEqual(self.engine.list_inhibitions(), [])
    def test_audit_log_records_everything(self):
        self.request("GET", "/v1/alerts?tenant=acme")                      # 401
        self.request("GET", "/v1/alerts?tenant=globex", token="view-tok")  # 403
        self.request("GET", "/v1/alerts?tenant=acme", token="view-tok")    # 200
        self.admin("GET", "/v1/nope")                                      # 404
        self.admin("GET", "/v1/stats")                                     # 200
        entries = self.access.query_audit()
        self.assertEqual([row["seq"] for row in entries], [1, 2, 3, 4, 5])
        self.assertEqual([(row["principal_id"], row["outcome"], row["status"])
                          for row in entries],
                         [(None, "denied", 401), ("reader", "denied", 403),
                          ("reader", "allowed", 200), ("root", "failed", 404),
                          ("root", "allowed", 200)])
        self.assertEqual(entries[0]["tenant"], "acme")
        self.assertEqual(entries[1]["tenant"], "globex")
        self.assertIsNone(entries[4]["tenant"])
        self.assertEqual(entries[2]["method"], "GET")
        self.assertEqual(entries[2]["path"], "/v1/alerts")
        self.assertNotIn("tok", json.dumps(entries))
    def test_audit_endpoint_filters_and_pagination(self):
        self.request("GET", "/v1/alerts?tenant=acme")                      # 401
        self.request("GET", "/v1/alerts?tenant=acme", token="view-tok")    # 200
        self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {},
            "samples": [[1, 1.0]]}, token="write-tok")                     # 202
        code, body = self.admin("GET", "/v1/audit")
        self.assertEqual(code, 200)
        self.assertEqual([row["seq"] for row in body["entries"]], [1, 2, 3])
        # Every audit request is itself audited after responding, so compare
        # against a snapshot taken just before each call.
        for query, filters in (("outcome=denied", {"outcome": "denied"}),
                               ("principal_id=writer", {"principal_id": "writer"}),
                               ("tenant=acme&limit=2", {"tenant": "acme", "limit": 2}),
                               ("after_seq=2", {"after_seq": 2}),
                               ("after_seq=1&limit=1", {"after_seq": 1, "limit": 1})):
            expected = self.access.query_audit(**filters)
            code, body = self.admin("GET", "/v1/audit?" + query)
            self.assertEqual(code, 200, query)
            self.assertEqual(body["entries"], expected, query)
        for query in ("outcome=bogus", "after_seq=-1", "after_seq=x",
                      "limit=0", "limit=-1", "limit=x"):
            code, body = self.admin("GET", "/v1/audit?" + query)
            self.assertEqual(code, 400, query)
            self.assertIn("error", body)
    def test_principals_and_audit_survive_restart(self):
        self.admin("GET", "/v1/stats")
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.access = AccessControl(root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0,
                                    access=self.access)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()
        code, _ = self.request("GET", "/v1/stats")
        self.assertEqual(code, 401)
        code, body = self.admin("GET", "/v1/audit")
        self.assertEqual([row["seq"] for row in body["entries"]], [1, 2])
        self.assertEqual(body["entries"][0]["outcome"], "allowed")
        self.assertEqual(body["entries"][1]["outcome"], "denied")
    def test_cli_created_principal_is_enforced_without_restart(self):
        self.access.revoke_principal("root")
        code, _ = self.request("GET", "/v1/stats", token="admin-tok")
        self.assertEqual(code, 401)
        self.access.create_principal("root2", "new-tok", "admin", ["acme"])
        code, _ = self.request("GET", "/v1/stats", token="new-tok")
        self.assertEqual(code, 200)


class TestPrincipalCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-access-cli-")
        self.data_dir = os.path.join(self.tmp, "data")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", self.data_dir] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()
    def test_create_list_revoke(self):
        code, out, err = self.run_cli(
            "principal-create", "--id", "reader", "--token", "s3cret",
            "--role", "viewer", "--tenant", "acme", "--tenant", "globex")
        self.assertEqual(code, 0)
        created = json.loads(out)
        self.assertEqual(created, {"id": "reader", "role": "viewer",
                                   "tenants": ["acme", "globex"]})
        self.assertNotIn("s3cret", out)
        code, out, _ = self.run_cli("principal-list")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), {"principals": [created]})
        # Duplicate id is a conflict-style JSON error on stderr.
        code, out, err = self.run_cli(
            "principal-create", "--id", "reader", "--token", "x",
            "--role", "admin", "--tenant", "acme")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("conflict", json.loads(err)["error"])
        # Unknown principal is an unknown-style JSON error.
        code, _, err = self.run_cli("principal-revoke", "--id", "ghost")
        self.assertEqual(code, 1)
        self.assertIn("unknown principal", json.loads(err)["error"])
        code, out, _ = self.run_cli("principal-revoke", "--id", "reader")
        self.assertEqual((code, json.loads(out)), (0, {"revoked": "reader"}))
        code, out, _ = self.run_cli("principal-list")
        self.assertEqual(json.loads(out), {"principals": []})
    def test_create_validation_errors(self):
        code, _, err = self.run_cli(
            "principal-create", "--id", "p", "--token", "", "--role", "viewer",
            "--tenant", "acme")
        self.assertEqual(code, 1)
        self.assertIn("token", json.loads(err)["error"])
        code, _, err = self.run_cli(
            "principal-create", "--id", "p", "--token", "t", "--role", "bogus",
            "--tenant", "acme")
        self.assertEqual(code, 1)
        # argparse prints its usage first; the JSON error is the last line.
        self.assertIn("error", json.loads(err.splitlines()[-1]))


if __name__ == "__main__":
    unittest.main()
