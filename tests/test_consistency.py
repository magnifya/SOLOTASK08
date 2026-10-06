"""Tests for the persistent revision, consistency tokens and consistent reads."""

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


class ConsistencyCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-consistency-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))


class TestRevision(ConsistencyCase):
    def test_mutations_bump_revision_exactly_once(self):
        store = self.store()
        self.assertEqual(store.current_revision(), 0)
        store.write("acme", "m", {}, [[1000, 1.0]])
        self.assertEqual(store.current_revision(), 1)
        store.write_batch([{"tenant": "acme", "metric": "m",
                            "samples": [[2000, 2.0]]}])
        self.assertEqual(store.current_revision(), 2)
        snapshot = store.export_snapshot("acme", "m")
        # Replaying the same entries is a pure duplicate: no bump.
        store.replay_snapshot(snapshot)
        self.assertEqual(store.current_revision(), 2)
        import hashlib
        from obsd.tsdb import _snapshot_bytes
        other = store.export_snapshot("acme", "m")
        other["entries"] = [dict(entry, metric="m3") for entry in other["entries"]]
        other["snapshot_id"] = hashlib.sha256(_snapshot_bytes(
            {"version": 1, "entries": other["entries"]})).hexdigest()
        store.replay_snapshot(other)
        self.assertEqual(store.current_revision(), 3)
        store.set_retention("acme", 500)
        store.run_retention(2000, tenant="acme")
        self.assertEqual(store.current_revision(), 4)

    def test_non_commits_never_bump(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        self.assertEqual(store.current_revision(), 1)
        # Pure duplicate single write and batch.
        store.write("acme", "m", {}, [[1000, 1.0]])
        store.write_batch([{"tenant": "acme", "metric": "m",
                            "samples": [[1000, 1.0]]}])
        # Validation failure, conflict and quota rejection.
        for call in (
                lambda: store.write("acme", "m", {}, []),
                lambda: store.write("acme", "m", {}, [[1000, 2.0]]),
                lambda: store.write_batch([{"tenant": "acme", "metric": "m",
                                            "samples": [[1000, 9.0]]}]),
                lambda: store.replay_snapshot({"version": 2}),
                lambda: store.write("acme", "m", {}, [[2000, 1.0]], now=1000)):
            with self.assertRaises(ObsError):
                call()
        store.set_quota("acme", 1, None)
        with self.assertRaises(ObsError):
            store.write("acme", "other", {}, [[1000, 1.0]])
        # Dry runs and no-op retention runs.
        snapshot = store.export_snapshot("acme", "m")
        store.replay_snapshot(snapshot, dry_run=True)
        store.set_retention("acme", 100)
        report = store.run_retention(50, tenant="acme")  # cutoff -50: nothing drops
        self.assertEqual(report["tenants"][0]["dropped"], 0)
        store.run_retention(10**9, tenant="acme", dry_run=True)
        self.assertEqual(store.current_revision(), 1)

    def test_reads_do_not_move_revision(self):
        store = self.store()
        engine = AlertEngine(store, os.path.join(self.tmp, "data"))
        store.write("acme", "m", {}, [[1000, 1.0]])
        engine.set_slo("acme", "avail", "m", {}, ">", 0, 0.9, 1000)
        store.query("acme", "m")
        store.export_snapshot("acme", "m")
        engine.slo_status("avail", 1000)
        store.consistency_token("acme")
        self.assertEqual(store.current_revision(), 1)

    def test_revision_survives_restart(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        store.write("acme", "m", {}, [[2000, 2.0]])
        reopened = self.store()
        self.assertEqual(reopened.current_revision(), 2)
        result = reopened.write("acme", "m", {}, [[3000, 3.0]], return_revision=True)
        self.assertEqual(result["revision"], 3)

    def test_failed_batch_leaves_no_revision_trace(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        with self.assertRaises(ObsError):
            store.write_batch([
                {"tenant": "acme", "metric": "m", "samples": [[2000, 2.0]]},
                {"tenant": "acme", "metric": "m", "samples": [[1000, 9.0]]}])
        self.assertEqual(store.current_revision(), 1)
        self.assertEqual(store.query("acme", "m")[0]["points"], [[1000, 1.0]])


class TestReturnRevision(ConsistencyCase):
    def test_write_result_shape(self):
        store = self.store()
        plain = store.write("acme", "m", {}, [[1000, 1.0]])
        self.assertEqual(set(plain), {"series_id", "written", "duplicates"})
        result = store.write("acme", "m", {}, [[2000, 2.0]], return_revision=True)
        self.assertEqual(result["revision"], 2)
        duplicate = store.write("acme", "m", {}, [[2000, 2.0]], return_revision=True)
        self.assertEqual((duplicate["written"], duplicate["revision"]), (0, 2))

    def test_batch_replay_retention_result_shape(self):
        store = self.store()
        result = store.write_batch(
            [{"tenant": "acme", "metric": "m", "samples": [[1000, 1.0]]}],
            return_revision=True)
        self.assertEqual(result["revision"], 1)
        snapshot = store.export_snapshot("acme", "m")
        replay = store.replay_snapshot(snapshot, return_revision=True)
        self.assertEqual((replay["written"], replay["revision"]), (0, 1))
        store.set_retention("acme", 500)
        report = store.run_retention(2000, tenant="acme", return_revision=True)
        self.assertEqual(report["revision"], 2)
        report = store.run_retention(2000, tenant="acme", return_revision=True)
        self.assertEqual(report["revision"], 2)

    def test_return_revision_must_be_a_boolean(self):
        store = self.store()
        for bad in (None, 0, 1, "true", []):
            with self.assertRaises(ObsError):
                store.write("acme", "m", {}, [[1000, 1.0]], return_revision=bad)
            with self.assertRaises(ObsError):
                store.write_batch([{"tenant": "acme", "metric": "m",
                                    "samples": [[1000, 1.0]]}], return_revision=bad)
            with self.assertRaises(ObsError):
                store.replay_snapshot({"version": 1}, return_revision=bad)
            with self.assertRaises(ObsError):
                store.run_retention(1000, return_revision=bad)
        self.assertEqual(store.current_revision(), 0)
        self.assertEqual(store.stats()["points"], 0)


class TestReadToken(ConsistencyCase):
    def test_token_roundtrip_and_tenant_binding(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        token = store.consistency_token("acme")
        self.assertEqual((token["tenant"], token["revision"]), ("acme", 1))
        rows = store.query("acme", "m", read_token=token["token"])
        self.assertEqual(rows[0]["points"], [[1000, 1.0]])
        store.export_snapshot("acme", "m", read_token=token["token"])
        with self.assertRaises(ObsError) as ctx:
            store.query("globex", "m", read_token=token["token"])
        self.assertEqual(str(ctx.exception), "invalid read token")

    def test_invalid_tokens_are_rejected(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        token = store.consistency_token("acme")["token"]
        for bad in ("garbage", token[:-1] + ("0" if token[-1] != "0" else "1"),
                    token.replace(".", ":", 1), 123, {"x": 1}):
            with self.assertRaises(ObsError) as ctx:
                store.query("acme", "m", read_token=bad)
            self.assertEqual(str(ctx.exception), "invalid read token")

    def test_unavailable_revision_is_a_distinct_error(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        # A well-formed token for a revision this instance has not reached.
        future = store._encode_token("acme", store.current_revision() + 10)
        with self.assertRaises(ObsError) as ctx:
            store.query("acme", "m", read_token=future)
        self.assertEqual(str(ctx.exception), "read revision unavailable")
        with self.assertRaises(ObsError) as ctx:
            store.export_snapshot("acme", "m", read_token=future)
        self.assertEqual(str(ctx.exception), "read revision unavailable")

    def test_token_sees_newer_but_never_older(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        token = store.consistency_token("acme")["token"]
        store.write("acme", "m", {}, [[2000, 2.0]])
        rows = store.query("acme", "m", read_token=token)
        self.assertEqual(rows[0]["points"], [[1000, 1.0], [2000, 2.0]])

    def test_tokens_verify_after_restart(self):
        store = self.store()
        store.write("acme", "m", {}, [[1000, 1.0]])
        token = store.consistency_token("acme")["token"]
        reopened = self.store()
        rows = reopened.query("acme", "m", read_token=token)
        self.assertEqual(rows[0]["points"], [[1000, 1.0]])
        with self.assertRaises(ObsError):
            reopened.query("acme", "m", read_token=token[:-1] + "0")

    def test_slo_status_read_token(self):
        store = self.store()
        engine = AlertEngine(store, os.path.join(self.tmp, "data"))
        store.write("acme", "m", {}, [[1000, 1.0]])
        engine.set_slo("acme", "avail", "m", {}, ">", 0, 0.9, 1000)
        token = store.consistency_token("acme")["token"]
        status = engine.slo_status("avail", 1000, read_token=token)
        self.assertEqual(status["total"], 1)
        with self.assertRaises(ObsError) as ctx:
            engine.slo_status("avail", 1000, read_token="garbage")
        self.assertEqual(str(ctx.exception), "invalid read token")
        other = store.consistency_token("globex")["token"]
        with self.assertRaises(ObsError) as ctx:
            engine.slo_status("avail", 1000, read_token=other)
        self.assertEqual(str(ctx.exception), "invalid read token")


class TestConsistencyCli(ConsistencyCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", os.path.join(self.tmp, "cli")] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_token_write_and_read_roundtrip(self):
        code, out, _ = self.run_cli("consistency-token", "--tenant", "acme")
        self.assertEqual(code, 0)
        token = json.loads(out)
        self.assertEqual((token["tenant"], token["revision"]), ("acme", 0))
        code, out, _ = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                    "--sample", "1000:1.0", "--return-revision")
        self.assertEqual(json.loads(out)["revision"], 1)
        code, out, _ = self.run_cli("write", "--tenant", "acme", "--metric", "m",
                                    "--sample", "2000:2.0")
        self.assertNotIn("revision", json.loads(out))
        code, out, _ = self.run_cli("consistency-token", "--tenant", "acme")
        token = json.loads(out)
        self.assertEqual(token["revision"], 2)
        code, out, _ = self.run_cli("query", "--tenant", "acme", "--metric", "m",
                                    "--read-token", token["token"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["series"][0]["points"],
                         [[1000, 1.0], [2000, 2.0]])
        code, _, err = self.run_cli("query", "--tenant", "acme", "--metric", "m",
                                    "--read-token", "garbage")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(err)["error"], "invalid read token")

    def test_batch_replay_retention_flags(self):
        code, out, _ = self.run_cli(
            "write-batch", "--return-revision",
            "--entries", '[{"tenant":"acme","metric":"m","samples":[[1000,1.0]]}]')
        self.assertEqual(json.loads(out)["revision"], 1)
        code, out, _ = self.run_cli("export", "--tenant", "acme", "--metric", "m")
        code, out, _ = self.run_cli("replay", "--snapshot", out, "--return-revision")
        result = json.loads(out)
        self.assertEqual((result["written"], result["revision"]), (0, 1))
        code, out, _ = self.run_cli("retention-set", "--tenant", "acme",
                                    "--retention-ms", "500")
        code, out, _ = self.run_cli("retention-run", "--tenant", "acme",
                                    "--now-ms", "2000", "--return-revision")
        self.assertEqual(json.loads(out)["revision"], 2)


class TestConsistencyHttp(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-consistency-http-")
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
    def request(self, method, path, payload=None, token=None):
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=body, method=method)
        if token is not None:
            req.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_token_endpoint_and_return_revision(self):
        code, body = self.request("GET", "/v1/consistency-token?tenant=acme")
        self.assertEqual(code, 200)
        self.assertEqual((body["tenant"], body["revision"]), ("acme", 0))
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {},
            "samples": [[1000, 1.0]], "return_revision": True})
        self.assertEqual((code, body["revision"]), (202, 1))
        code, body = self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[2000, 2.0]]})
        self.assertNotIn("revision", body)
        code, body = self.request("GET", "/v1/consistency-token?tenant=acme")
        self.assertEqual(body["revision"], 2)

    def test_return_revision_rejects_null_and_non_boolean(self):
        for bad in (None, "yes", 1):
            code, body = self.request("POST", "/v1/series", {
                "tenant": "acme", "metric": "m", "labels": {},
                "samples": [[1000, 1.0]], "return_revision": bad})
            self.assertEqual(code, 400)
            self.assertIn("return_revision", body["error"])
        self.assertEqual(self.store.current_revision(), 0)
        self.assertEqual(self.store.stats()["points"], 0)
        code, body = self.request("POST", "/v1/retention/run", {
            "now_ms": 1000, "tenant": "acme", "return_revision": None})
        self.assertEqual(code, 400)

    def test_read_token_status_codes(self):
        self.request("POST", "/v1/series", {
            "tenant": "acme", "metric": "m", "labels": {}, "samples": [[1000, 1.0]]})
        _, token = self.request("GET", "/v1/consistency-token?tenant=acme")
        code, _ = self.request("GET", "/v1/query?tenant=acme&metric=m&read_token="
                               + token["token"])
        self.assertEqual(code, 200)
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m&read_token=bad")
        self.assertEqual((code, body["error"]), (400, "invalid read token"))
        code, body = self.request("GET", "/v1/query?tenant=globex&metric=m&read_token="
                                  + token["token"])
        self.assertEqual((code, body["error"]), (400, "invalid read token"))
        future = self.store._encode_token("acme", self.store.current_revision() + 5)
        code, body = self.request("GET", "/v1/query?tenant=acme&metric=m&read_token="
                                  + future)
        self.assertEqual((code, body["error"]), (409, "read revision unavailable"))

    def test_token_endpoint_is_tenant_scoped(self):
        self.access.create_principal("viewer", "tok-view", "viewer", ["acme"])
        code, _ = self.request("GET", "/v1/consistency-token?tenant=acme")
        self.assertEqual(code, 401)
        code, body = self.request("GET", "/v1/consistency-token?tenant=acme",
                                  token="tok-view")
        self.assertEqual(code, 200)
        code, _ = self.request("GET", "/v1/consistency-token?tenant=globex",
                               token="tok-view")
        self.assertEqual(code, 403)
        code, body = self.request("GET", "/v1/audit?tenant=acme", token="tok-view")
        self.assertEqual(code, 403)  # audit itself is admin-only
        self.access.create_principal("admin", "tok-admin", "admin", ["acme"])
        code, body = self.request("GET", "/v1/audit", token="tok-admin")
        self.assertIn("/v1/consistency-token",
                      [entry["path"] for entry in body["entries"]])


if __name__ == "__main__":
    unittest.main()
