"""Tests for the persistent revision and read-consistency tokens."""

import base64
import contextlib
import hashlib
import hmac
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


def forge_token(secret, tenant, revision):
    """Build a syntactically valid token with an arbitrary payload."""
    payload = json.dumps({"revision": revision, "tenant": tenant, "v": 1},
                         sort_keys=True, separators=(",", ":")).encode("utf-8")
    body = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    signature = hmac.new(secret.encode("ascii"), body.encode("ascii"),
                         hashlib.sha256).hexdigest()
    return "obsd1.%s.%s" % (body, signature)


class ConsistencyCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-consistency-")
        self.root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(self.root)
        self.engine = AlertEngine(self.store, self.root)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def reopen(self):
        self.store = SeriesStore(self.root)
        self.engine = AlertEngine(self.store, self.root)
        return self.store

    def revision(self):
        return self.store.consistency_token("acme")["revision"]

    def secret(self):
        # Minting a token first guarantees the secret exists on disk.
        self.store.consistency_token("acme")
        with open(os.path.join(self.root, "revision.json"), "r",
                  encoding="utf-8") as handle:
            return json.load(handle)["secret"]

    def write(self, samples=[[1000, 1.0]], **kwargs):
        return self.store.write("acme", "latency_ms", {"host": "a"}, samples, **kwargs)


class TestRevision(ConsistencyCase):
    def test_revision_starts_at_zero(self):
        token = self.store.consistency_token("acme")
        self.assertEqual(token["revision"], 0)
        self.assertEqual(token["tenant"], "acme")
        self.assertTrue(token["token"].startswith("obsd1."))

    def test_successful_write_bumps_revision_once(self):
        self.write()
        self.assertEqual(self.revision(), 1)
        self.write(samples=[[2000, 2.0]])
        self.assertEqual(self.revision(), 2)

    def test_pure_duplicate_write_does_not_bump(self):
        self.write()
        self.write()  # identical samples: pure duplicate
        self.assertEqual(self.revision(), 1)

    def test_overwrite_bumps(self):
        self.write()
        self.write(overwrite=True, samples=[[1000, 9.0]])
        self.assertEqual(self.revision(), 2)

    def test_conflict_and_validation_failure_do_not_bump(self):
        self.write()
        with self.assertRaises(ObsError):
            self.write(samples=[[1000, 5.0]])  # conflict
        with self.assertRaises(ObsError):
            self.write(samples=[["x", 1.0]])  # malformed sample
        with self.assertRaises(ObsError):
            self.store.write("acme", "m", {}, [[1, 1.0]], now="not-an-int")
        self.assertEqual(self.revision(), 1)

    def test_quota_rejection_does_not_bump(self):
        self.store.set_quota("acme", 0, None)
        with self.assertRaises(ObsError):
            self.write()
        self.assertEqual(self.revision(), 0)

    def test_batch_bumps_once_per_commit(self):
        self.store.write_batch([
            {"tenant": "acme", "metric": "m1", "samples": [[1, 1.0]]},
            {"tenant": "acme", "metric": "m2", "samples": [[1, 1.0]]}])
        self.assertEqual(self.revision(), 1)
        # Pure duplicate batch: no bump.
        self.store.write_batch([{"tenant": "acme", "metric": "m1",
                                 "samples": [[1, 1.0]]}])
        self.assertEqual(self.revision(), 1)

    def test_failed_batch_leaves_no_revision_trace(self):
        self.write()
        with self.assertRaises(ObsError):
            self.store.write_batch([
                {"tenant": "acme", "metric": "m2", "samples": [[1, 1.0]]},
                {"tenant": "acme", "metric": "latency_ms", "labels": {"host": "a"},
                 "samples": [[1000, 2.0]]}])  # conflicts with the stored value
        self.assertEqual(self.revision(), 1)
        self.assertEqual(self.store.stats()["series"], 1)

    def test_replay_bumps_only_when_applied(self):
        snapshot = self.store.export_snapshot("acme", "latency_ms")
        self.store.replay_snapshot(snapshot)  # empty entries: no bump
        self.assertEqual(self.revision(), 0)
        self.write()
        snapshot = self.store.export_snapshot("acme", "latency_ms")
        other = SeriesStore(os.path.join(self.tmp, "other"))
        other.replay_snapshot(snapshot, dry_run=True)
        self.assertEqual(other.consistency_token("acme")["revision"], 0)
        other.replay_snapshot(snapshot)
        self.assertEqual(other.consistency_token("acme")["revision"], 1)
        other.replay_snapshot(snapshot)  # pure duplicate replay
        self.assertEqual(other.consistency_token("acme")["revision"], 1)

    def test_replay_rejection_leaves_no_revision_trace(self):
        self.write()
        snapshot = self.store.export_snapshot("acme", "latency_ms")
        snapshot["snapshot_id"] = "0" * 64
        with self.assertRaises(ObsError):
            self.store.replay_snapshot(snapshot)
        self.assertEqual(self.revision(), 1)

    def test_retention_run_bumps_only_when_samples_drop(self):
        self.store.set_retention("acme", 500)
        self.write(samples=[[1000, 1.0], [3000, 2.0]])
        self.assertEqual(self.revision(), 1)
        result = self.store.run_retention(2500, tenant="acme", dry_run=True)
        self.assertEqual(result["tenants"][0]["dropped"], 1)
        self.assertEqual(self.revision(), 1)
        self.store.run_retention(2500, tenant="acme")
        self.assertEqual(self.revision(), 2)
        # A second run deletes nothing: no bump.
        self.store.run_retention(2500, tenant="acme")
        self.assertEqual(self.revision(), 2)

    def test_reads_do_not_bump(self):
        self.write()
        self.store.query("acme", "latency_ms")
        self.store.export_snapshot("acme", "latency_ms")
        self.engine.set_slo("acme", "avail", "latency_ms", {}, ">=", 1, 0.9, 60000)
        self.engine.slo_status("avail", 5000, tenant="acme")
        self.store.stats()
        self.assertEqual(self.revision(), 1)

    def test_revision_survives_restart(self):
        self.write()
        self.write(samples=[[2000, 2.0]])
        self.assertEqual(self.revision(), 2)
        store = self.reopen()
        self.assertEqual(store.consistency_token("acme")["revision"], 2)
        store.write("acme", "latency_ms", {"host": "a"}, [[3000, 3.0]])
        self.assertEqual(store.consistency_token("acme")["revision"], 3)
        self.assertEqual(self.reopen().consistency_token("acme")["revision"], 3)

    def test_second_instance_sees_commits(self):
        other = SeriesStore(self.root)
        self.write()
        # The other instance folds in the persisted revision on demand.
        self.assertEqual(other.consistency_token("acme")["revision"], 1)
        token = self.store.consistency_token("acme")
        # A read gated on the fresh token succeeds on the other instance too.
        other.query("acme", "latency_ms", read_token=token["token"])


class TestReturnRevision(ConsistencyCase):
    def test_write_return_revision(self):
        result = self.write(return_revision=True)
        self.assertEqual(result["revision"], 1)
        result = self.write(return_revision=True)  # pure duplicate
        self.assertEqual(result["revision"], 1)
        result = self.write()
        self.assertNotIn("revision", result)

    def test_write_return_revision_must_be_boolean(self):
        for bad in (None, 1, "true", 0.0):
            with self.assertRaises(ObsError):
                self.write(return_revision=bad)
        self.assertEqual(self.store.stats()["points"], 0)
        self.assertEqual(self.revision(), 0)

    def test_batch_return_revision(self):
        result = self.store.write_batch(
            [{"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]}],
            return_revision=True)
        self.assertEqual(result["revision"], 1)
        result = self.store.write_batch(
            [{"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]}],
            return_revision=True)
        self.assertEqual(result["revision"], 1)
        with self.assertRaises(ObsError):
            self.store.write_batch(
                [{"tenant": "acme", "metric": "m", "samples": [[2, 2.0]]}],
                return_revision=None)
        self.assertEqual(self.revision(), 1)

    def test_replay_return_revision(self):
        self.write()
        snapshot = self.store.export_snapshot("acme", "latency_ms")
        other = SeriesStore(os.path.join(self.tmp, "other"))
        result = other.replay_snapshot(snapshot, return_revision=True)
        self.assertEqual(result["revision"], 1)
        result = other.replay_snapshot(snapshot, return_revision=True)
        self.assertEqual(result["revision"], 1)
        result = other.replay_snapshot(snapshot, dry_run=True,
                                       return_revision=True)
        self.assertEqual(result["revision"], 1)
        with self.assertRaises(ObsError):
            other.replay_snapshot(snapshot, return_revision="yes")

    def test_retention_run_return_revision(self):
        self.store.set_retention("acme", 500)
        self.write(samples=[[1000, 1.0], [3000, 2.0]])
        result = self.store.run_retention(2500, tenant="acme",
                                          return_revision=True)
        self.assertEqual(result["revision"], 2)
        result = self.store.run_retention(2500, tenant="acme",
                                          return_revision=True)
        self.assertEqual(result["revision"], 2)  # nothing deleted: no commit
        result = self.store.run_retention(2500, tenant="acme", dry_run=True,
                                          return_revision=True)
        self.assertEqual(result["revision"], 2)
        result = self.store.run_retention(2500, tenant="acme")
        self.assertNotIn("revision", result)
        with self.assertRaises(ObsError):
            self.store.run_retention(2500, tenant="acme", return_revision=None)


class TestReadToken(ConsistencyCase):
    def token(self, tenant="acme"):
        return self.store.consistency_token(tenant)["token"]

    def test_query_with_valid_token(self):
        self.write()
        rows = self.store.query("acme", "latency_ms", read_token=self.token())
        self.assertEqual(rows[0]["points"], [[1000, 1.0]])

    def test_export_with_valid_token(self):
        self.write()
        snapshot = self.store.export_snapshot("acme", "latency_ms",
                                              read_token=self.token())
        self.assertEqual(len(snapshot["entries"]), 1)

    def test_slo_status_with_valid_token(self):
        self.write()
        self.engine.set_slo("acme", "avail", "latency_ms", {}, ">=", 1, 0.9, 60000)
        status = self.engine.slo_status("avail", 5000, tenant="acme",
                                        read_token=self.token())
        self.assertEqual(status["total"], 1)

    def test_tampered_token_rejected(self):
        self.write()
        token = self.token()
        for bad in (token[:-1] + ("a" if token[-1] != "a" else "b"),
                    token.replace("obsd1.", "obsd2.", 1),
                    token + ".extra",
                    "not-a-token",
                    "",
                    42):
            with self.assertRaises(ObsError) as caught:
                self.store.query("acme", "latency_ms", read_token=bad)
            self.assertEqual(str(caught.exception), "invalid read token")
        # None means "no token": the read proceeds ungated.
        self.assertEqual(len(self.store.query("acme", "latency_ms",
                                              read_token=None)), 1)

    def test_cross_tenant_token_rejected(self):
        self.write()
        token = self.store.consistency_token("globex")["token"]
        with self.assertRaises(ObsError) as caught:
            self.store.query("acme", "latency_ms", read_token=token)
        self.assertEqual(str(caught.exception), "invalid read token")
        with self.assertRaises(ObsError):
            self.store.export_snapshot("acme", "latency_ms", read_token=token)

    def test_foreign_secret_rejected(self):
        other = SeriesStore(os.path.join(self.tmp, "other"))
        other.write("acme", "latency_ms", {}, [[1, 1.0]])
        token = other.consistency_token("acme")["token"]
        with self.assertRaises(ObsError) as caught:
            self.store.query("acme", "latency_ms", read_token=token)
        self.assertEqual(str(caught.exception), "invalid read token")

    def test_future_revision_unavailable(self):
        self.write()
        token = forge_token(self.secret(), "acme", self.revision() + 100)
        with self.assertRaises(ObsError) as caught:
            self.store.query("acme", "latency_ms", read_token=token)
        self.assertEqual(str(caught.exception), "read revision unavailable")
        with self.assertRaises(ObsError):
            self.store.export_snapshot("acme", "latency_ms", read_token=token)

    def test_token_survives_restart(self):
        self.write()
        token = self.token()
        store = self.reopen()
        rows = store.query("acme", "latency_ms", read_token=token)
        self.assertEqual(rows[0]["points"], [[1000, 1.0]])

    def test_monotonic_reads_with_same_token(self):
        self.write()
        token = self.token()
        first = self.store.query("acme", "latency_ms", read_token=token)
        self.write(samples=[[2000, 2.0]])
        second = self.store.query("acme", "latency_ms", read_token=token)
        self.assertEqual(len(first[0]["points"]), 1)
        self.assertEqual(len(second[0]["points"]), 2)

    def test_slo_status_token_tenant_mismatch(self):
        self.write()
        self.engine.set_slo("acme", "avail", "latency_ms", {}, ">=", 1, 0.9, 60000)
        token = self.store.consistency_token("globex")["token"]
        with self.assertRaises(ObsError) as caught:
            self.engine.slo_status("avail", 5000, tenant="acme", read_token=token)
        self.assertEqual(str(caught.exception), "invalid read token")


class TestConsistencyHttp(ConsistencyCase):
    def setUp(self):
        super().setUp()
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        super().tearDown()

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

    def post_series(self, payload, status=202, token=None):
        code, body = self.request("POST", "/v1/series", payload, token=token)
        self.assertEqual(code, status)
        return body

    def test_consistency_token_endpoint(self):
        code, body = self.request("GET", "/v1/consistency-token?tenant=acme")
        self.assertEqual(code, 200)
        self.assertEqual(body["revision"], 0)
        self.assertEqual(body["tenant"], "acme")
        self.assertTrue(body["token"].startswith("obsd1."))
        code, body = self.request("GET", "/v1/consistency-token")
        self.assertEqual(code, 400)

    def test_write_return_revision_over_http(self):
        body = self.post_series({"tenant": "acme", "metric": "m",
                                 "samples": [[1, 1.0]], "return_revision": True})
        self.assertEqual(body["revision"], 1)
        body = self.post_series({"tenant": "acme", "metric": "m",
                                 "samples": [[1, 1.0]], "return_revision": True})
        self.assertEqual(body["revision"], 1)  # duplicate: current revision
        body = self.post_series({"tenant": "acme", "metric": "m",
                                 "samples": [[2, 2.0]]})
        self.assertNotIn("revision", body)

    def test_return_revision_null_rejected_over_http(self):
        for path, payload in (
                ("/v1/series", {"tenant": "acme", "metric": "m",
                                "samples": [[1, 1.0]], "return_revision": None}),
                ("/v1/series/batch",
                 {"entries": [{"tenant": "acme", "metric": "m",
                               "samples": [[1, 1.0]]}], "return_revision": None}),
                ("/v1/retention/run",
                 {"now_ms": 1000, "return_revision": None})):
            code, body = self.request("POST", path, payload)
            self.assertEqual(code, 400)
            self.assertEqual(body["error"], "return_revision must be a boolean")
        self.assertEqual(self.store.stats()["points"], 0)
        self.assertEqual(self.revision(), 0)

    def test_batch_and_replay_return_revision_over_http(self):
        code, body = self.request("POST", "/v1/series/batch", {
            "entries": [{"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]}],
            "return_revision": True})
        self.assertEqual(code, 202)
        self.assertEqual(body["revision"], 1)
        code, snapshot = self.request("GET", "/v1/export?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        code, body = self.request("POST", "/v1/replay", dict(
            snapshot, return_revision=True))
        self.assertEqual(code, 202)
        self.assertEqual(body["revision"], 1)  # pure duplicate replay

    def test_retention_run_return_revision_over_http(self):
        self.post_series({"tenant": "acme", "metric": "m",
                          "samples": [[1000, 1.0], [3000, 2.0]]})
        self.request("POST", "/v1/retention/policies",
                     {"tenant": "acme", "retention_ms": 500})
        code, body = self.request("POST", "/v1/retention/run",
                                  {"now_ms": 2500, "tenant": "acme",
                                   "return_revision": True})
        self.assertEqual(code, 200)
        self.assertEqual(body["revision"], 2)

    def test_query_with_read_token_over_http(self):
        self.post_series({"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]})
        code, body = self.request("GET", "/v1/consistency-token?tenant=acme")
        token = body["token"]
        code, body = self.request(
            "GET", "/v1/query?tenant=acme&metric=m&read_token=" + token)
        self.assertEqual(code, 200)
        self.assertEqual(len(body["series"]), 1)
        code, body = self.request(
            "GET", "/v1/export?tenant=acme&metric=m&read_token=" + token)
        self.assertEqual(code, 200)
        self.assertEqual(len(body["entries"]), 1)

    def test_invalid_read_token_over_http(self):
        self.post_series({"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]})
        code, body = self.request(
            "GET", "/v1/query?tenant=acme&metric=m&read_token=bogus")
        self.assertEqual(code, 400)
        self.assertEqual(body, {"error": "invalid read token"})
        code, body = self.request("GET", "/v1/consistency-token?tenant=globex")
        token = body["token"]
        code, body = self.request(
            "GET", "/v1/query?tenant=acme&metric=m&read_token=" + token)
        self.assertEqual(code, 400)
        self.assertEqual(body, {"error": "invalid read token"})

    def test_unavailable_revision_over_http(self):
        self.post_series({"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]})
        token = forge_token(self.secret(), "acme", self.revision() + 100)
        code, body = self.request(
            "GET", "/v1/query?tenant=acme&metric=m&read_token=" + token)
        self.assertEqual(code, 409)
        self.assertEqual(body, {"error": "read revision unavailable"})

    def test_slo_status_read_token_over_http(self):
        self.post_series({"tenant": "acme", "metric": "m", "samples": [[1, 1.0]]})
        code, _ = self.request("POST", "/v1/slos", {
            "tenant": "acme", "name": "avail", "metric": "m", "labels": {},
            "good_comparator": ">=", "threshold": 1, "target_ratio": 0.9,
            "window_ms": 60000})
        self.assertEqual(code, 201)
        code, body = self.request("GET", "/v1/consistency-token?tenant=acme")
        token = body["token"]
        code, body = self.request(
            "GET", "/v1/slos/status?tenant=acme&name=avail&now_ms=5000"
                    "&read_token=" + token)
        self.assertEqual(code, 200)
        self.assertEqual(body["total"], 1)
        code, body = self.request(
            "GET", "/v1/slos/status?tenant=acme&name=avail&now_ms=5000"
                    "&read_token=bogus")
        self.assertEqual(code, 400)
        self.assertEqual(body, {"error": "invalid read token"})

    def test_access_control_and_audit(self):
        access = AccessControl(self.root)
        access.create_principal("viewer", "view-tok", "viewer", ["acme"])
        access.create_principal("other", "other-tok", "viewer", ["globex"])
        server = create_server(self.store, self.engine, "127.0.0.1", 0,
                               access=access)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = "http://127.0.0.1:%d" % server.server_address[1]
        try:
            # The new endpoint requires authentication like every other.
            req = urllib.request.Request(base + "/v1/consistency-token?tenant=acme")
            try:
                urllib.request.urlopen(req, timeout=10)
                self.fail("expected 401")
            except urllib.error.HTTPError as exc:
                self.assertEqual(exc.code, 401)
            # A principal covering the tenant may mint a token.
            req = urllib.request.Request(base + "/v1/consistency-token?tenant=acme")
            req.add_header("Authorization", "Bearer view-tok")
            with urllib.request.urlopen(req, timeout=10) as response:
                body = json.loads(response.read().decode("utf-8"))
            self.assertEqual(body["tenant"], "acme")
            # A principal scoped to another tenant may not.
            req = urllib.request.Request(base + "/v1/consistency-token?tenant=acme")
            req.add_header("Authorization", "Bearer other-tok")
            try:
                urllib.request.urlopen(req, timeout=10)
                self.fail("expected 403")
            except urllib.error.HTTPError as exc:
                self.assertEqual(exc.code, 403)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        # Every request above was audited.
        entries = access.query_audit()
        self.assertEqual([row["status"] for row in entries], [401, 200, 403])
        self.assertTrue(all(row["path"] == "/v1/consistency-token"
                            for row in entries))


class TestConsistencyCli(ConsistencyCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", self.root] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_consistency_token_command(self):
        code, out, _ = self.run_cli("consistency-token", "--tenant", "acme")
        self.assertEqual(code, 0)
        body = json.loads(out)
        self.assertEqual(body["revision"], 0)
        self.assertEqual(body["tenant"], "acme")
        self.assertTrue(body["token"].startswith("obsd1."))

    def test_write_return_revision_flag(self):
        code, out, _ = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                    "--sample", "1000:1.0", "--return-revision")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["revision"], 1)
        code, out, _ = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                    "--sample", "2000:2.0")
        self.assertEqual(json.loads(out).get("revision"), None)

    def test_write_batch_return_revision_flag(self):
        code, out, _ = self.run_cli(
            "write-batch", "--return-revision",
            "--entries", '[{"tenant":"acme","metric":"m","samples":[[1,1.0]]}]')
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["revision"], 1)

    def test_query_with_read_token_flag(self):
        self.run_cli("write", "--tenant", "acme", "--metric", "m",
                     "--sample", "1000:1.0")
        _, out, _ = self.run_cli("consistency-token", "--tenant", "acme")
        token = json.loads(out)["token"]
        code, out, _ = self.run_cli("query", "--tenant", "acme", "--metric", "m",
                                    "--read-token", token)
        self.assertEqual(code, 0)
        self.assertEqual(len(json.loads(out)["series"]), 1)
        code, _, err = self.run_cli("query", "--tenant", "acme", "--metric", "m",
                                    "--read-token", "bogus")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(err), {"error": "invalid read token"})

    def test_export_and_replay_flags(self):
        self.run_cli("write", "--tenant", "acme", "--metric", "m",
                     "--sample", "1000:1.0")
        _, out, _ = self.run_cli("consistency-token", "--tenant", "acme")
        token = json.loads(out)["token"]
        code, out, _ = self.run_cli("export", "--tenant", "acme", "--metric", "m",
                                    "--read-token", token)
        self.assertEqual(code, 0)
        snapshot = out
        code, out, _ = self.run_cli("replay", "--snapshot", snapshot,
                                    "--return-revision")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["revision"], 1)  # duplicate replay

    def test_retention_run_return_revision_flag(self):
        self.run_cli("retention-set", "--tenant", "acme", "--retention-ms", "500")
        self.run_cli("write", "--tenant", "acme", "--metric", "m",
                     "--sample", "1000:1.0", "--sample", "3000:2.0")
        code, out, _ = self.run_cli("retention-run", "--tenant", "acme",
                                    "--now-ms", "2500", "--return-revision")
        self.assertEqual(code, 0)
        body = json.loads(out)
        self.assertEqual(body["tenants"][0]["dropped"], 1)
        self.assertEqual(body["revision"], 2)

    def test_slo_status_read_token_flag(self):
        self.run_cli("write", "--tenant", "acme", "--metric", "m",
                     "--sample", "1000:1.0")
        self.run_cli("slo", "set", "--tenant", "acme", "--name", "avail",
                     "--metric", "m", "--good-comparator", ">=", "--threshold", "1",
                     "--target-ratio", "0.9", "--window-ms", "60000")
        _, out, _ = self.run_cli("consistency-token", "--tenant", "acme")
        token = json.loads(out)["token"]
        code, out, _ = self.run_cli("slo", "status", "--tenant", "acme",
                                    "--name", "avail", "--now-ms", "5000",
                                    "--read-token", token)
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["total"], 1)


if __name__ == "__main__":
    unittest.main()
