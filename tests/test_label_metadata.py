"""Tests for read-only label metadata discovery.

Covers ``SeriesStore.label_names``/``label_values`` (registered-series scope,
empty series, exact and matcher filters, Unicode ordering, read-only
snapshots and read tokens), the HTTP ``GET /v1/label-names`` and
``GET /v1/label-values`` entries (unified ``label metadata invalid`` errors,
tenant-scoped access and audit) and the matching CLI subcommands.
"""

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
import urllib.parse
import urllib.request

from obsd import AccessControl, AlertEngine, SeriesStore, create_server
from obsd.cli import main as cli_main
from obsd.tsdb import ObsError

M = lambda key, op, value: {"key": key, "op": op, "value": value}


class MetadataCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-labels-")
        self.root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(self.root)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def seed(self):
        # Five acme/m series (one with no labels, one with an empty value),
        # plus an unrelated tenant and metric that must never be visible.
        self.store.write("acme", "m", {"host": "api-a", "zone": "x"},
                         [[1000, 1.0]])
        self.store.write("acme", "m", {"host": "api-b", "zone": "x"},
                         [[1000, 2.0]])
        self.store.write("acme", "m", {"host": "web-1", "zone": "y"},
                         [[1000, 3.0]])
        self.store.write("acme", "m", {"host": "", "zone": ""}, [[1000, 4.0]])
        self.store.write("acme", "m", {}, [[1000, 5.0]])
        self.store.write("acme", "other", {"only": "here"}, [[1000, 6.0]])
        self.store.write("globex", "m", {"host": "foreign", "secret": "z"},
                         [[1000, 9.0]])


class TestLabelMetadataStore(MetadataCase):
    def test_names_and_values_shapes_and_ordering(self):
        self.seed()
        names = self.store.label_names("acme", "m")
        self.assertEqual(names,
                         {"tenant": "acme", "metric": "m",
                          "labels": ["host", "zone"]})
        values = self.store.label_values("acme", "m", "host")
        self.assertEqual(values,
                         {"tenant": "acme", "metric": "m", "label": "host",
                          "values": ["", "api-a", "api-b", "web-1"]})
        self.assertEqual(self.store.label_values("acme", "m", "zone")["values"],
                         ["", "x", "y"])

    def test_names_and_values_deduplicate(self):
        self.seed()
        # zone=x repeats on two series but is reported once.
        self.assertEqual(self.store.label_values("acme", "m", "zone")["values"],
                         ["", "x", "y"])
        repeated = self.store.label_values("acme", "m", "host")
        self.assertEqual(len(repeated["values"]), len(set(repeated["values"])))

    def test_unicode_code_point_ordering(self):
        self.store.write("acme", "u", {"k": "中"}, [[1, 1.0]])
        self.store.write("acme", "u", {"k": "é"}, [[1, 1.0]])
        self.store.write("acme", "u", {"k": "a"}, [[1, 1.0]])
        self.store.write("acme", "u", {"名": "v"}, [[1, 1.0]])
        # Unicode code-point order: é (U+00E9) sorts before 中 (U+4E2D).
        self.assertEqual(self.store.label_names("acme", "u")["labels"],
                         ["k", "名"])
        self.assertEqual(self.store.label_values("acme", "u", "k")["values"],
                         ["a", "é", "中"])

    def test_empty_value_kept_but_missing_label_produces_nothing(self):
        self.seed()
        # "" is a real value for the series that carries host="".
        self.assertEqual(self.store.label_values("acme", "m", "host")["values"][0],
                         "")
        # A label present on none of the series yields an empty list, not 404.
        self.assertEqual(self.store.label_values("acme", "m", "nope"),
                         {"tenant": "acme", "metric": "m", "label": "nope",
                          "values": []})

    def test_empty_label_series_participates_in_name_counts(self):
        self.seed()
        # Drop every acme sample: the series stay registered and their labels
        # still participate in name discovery.
        self.store.enforce_retention("acme", 10_000)
        self.assertEqual(self.store.stats()["points"], 1)  # globex point kept
        self.assertEqual(self.store.stats()["series"], 7)
        self.assertEqual(self.store.label_names("acme", "m")["labels"],
                         ["host", "zone"])
        self.assertEqual(self.store.label_values("acme", "m", "host")["values"],
                         ["", "api-a", "api-b", "web-1"])

    def test_no_matching_series_returns_empty_arrays(self):
        self.assertEqual(self.store.label_names("acme", "m"),
                         {"tenant": "acme", "metric": "m", "labels": []})
        self.seed()
        self.assertEqual(self.store.label_names("acme", "missing")["labels"], [])
        self.assertEqual(self.store.label_names("nobody", "m")["labels"], [])

    def test_scoped_to_tenant_and_metric(self):
        self.seed()
        self.assertEqual(self.store.label_names("globex", "m")["labels"],
                         ["host", "secret"])
        self.assertEqual(self.store.label_names("acme", "other")["labels"],
                         ["only"])
        self.assertEqual(
            self.store.label_values("globex", "m", "host")["values"],
            ["foreign"])

    def test_exact_label_filters_apply_to_series_labels(self):
        self.seed()
        self.assertEqual(
            self.store.label_names("acme", "m", labels={"zone": "x"})["labels"],
            ["host", "zone"])
        self.assertEqual(
            self.store.label_values("acme", "m", "host",
                                   labels={"zone": "x"})["values"],
            ["api-a", "api-b"])
        # The no-label series and the host-less filters leave nothing.
        self.assertEqual(
            self.store.label_values("acme", "m", "host",
                                   labels={"zone": "y"})["values"],
            ["web-1"])

    def test_matchers_filter_series_before_collection(self):
        self.seed()
        names = self.store.label_names(
            "acme", "m", matchers=[M("host", "=~", "api-.*")])
        self.assertEqual(names["labels"], ["host", "zone"])
        values = self.store.label_values(
            "acme", "m", "host", matchers=[M("zone", "!=", "x")])
        # zone y (web-1), the zone="" series and the missing-zone/no-label
        # series all satisfy != x; only the first two carry a host.
        self.assertEqual(values["values"], ["", "web-1"])
        # Contradictory matchers: no series, empty arrays.
        self.assertEqual(self.store.label_names(
            "acme", "m", matchers=[M("host", "=", "api-a"),
                                   M("host", "!=", "api-a")])["labels"], [])
        self.assertEqual(self.store.label_values(
            "acme", "m", "host",
            matchers=[M("host", "=", "api-a"), M("host", "!=", "api-a")]),
            {"tenant": "acme", "metric": "m", "label": "host", "values": []})

    def test_matchers_combine_with_exact_labels_as_and(self):
        self.seed()
        self.assertEqual(
            self.store.label_values("acme", "m", "host",
                                    labels={"zone": "x"},
                                    matchers=[M("host", "!=", "api-b")])[
                "values"],
            ["api-a"])

    def test_validation_uses_one_error_message(self):
        self.seed()
        calls = [
            lambda: self.store.label_names(None, "m"),
            lambda: self.store.label_names("", "m"),
            lambda: self.store.label_names(123, "m"),
            lambda: self.store.label_names("acme", None),
            lambda: self.store.label_names("acme", ""),
            lambda: self.store.label_names("acme", 7),
            lambda: self.store.label_values("acme", "m", None),
            lambda: self.store.label_values("acme", "m", ""),
            lambda: self.store.label_values("acme", "m", 5),
        ]
        for call in calls:
            with self.assertRaises(ObsError) as caught:
                call()
            self.assertEqual(str(caught.exception), "label metadata invalid")

    def test_matchers_validated_even_without_candidate_series(self):
        # At store level malformed matchers raise ObsError (the exact matcher
        # wording, like ``query``); HTTP/CLI normalise every such failure to
        # "label metadata invalid". What matters here is that validation runs
        # before the empty registry is scanned.
        for bad in (
                [{"key": "k", "op": "~", "value": ""}],
                [{"key": "k", "op": "=~", "value": "("}],
                [{"key": "", "op": "=", "value": ""}],
                [{"op": "=", "value": ""}],
                [{"key": "k", "op": "=", "value": 1}],
                [{"key": "k", "op": "="}],
                [{"key": "k", "op": "=", "value": "", "extra": 1}],
                ["not-an-object"],
                "not-a-list",
        ):
            with self.assertRaises(ObsError, msg=repr(bad)):
                self.store.label_names("acme", "nonexistent", matchers=bad)
            with self.assertRaises(ObsError, msg=repr(bad)):
                self.store.label_values("acme", "nonexistent", "k",
                                        matchers=bad)

    def test_read_is_state_free(self):
        self.seed()
        before = self.store.stats()
        revision = self.store.consistency_token("acme")["revision"]
        for _ in range(5):
            self.store.label_names("acme", "m")
            self.store.label_values("acme", "m", "host")
            self.store.label_names("acme", "missing",
                                   matchers=[M("host", "=~", ".*")])
        self.assertEqual(self.store.stats(), before)
        self.assertEqual(self.store.consistency_token("acme")["revision"],
                         revision)

    def test_snapshot_is_consistent_under_concurrent_writes(self):
        self.seed()
        stop = threading.Event()

        def writer():
            n = 0
            while not stop.is_set():
                n += 1
                try:
                    self.store.write("acme", "m", {"host": "w%d" % n},
                                     [[1000 + n, 1.0]])
                except ObsError:
                    pass

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            for _ in range(200):
                names = self.store.label_names("acme", "m")
                self.assertEqual(names["labels"], sorted(names["labels"]))
                values = self.store.label_values("acme", "m", "host")
                self.assertEqual(values["values"], sorted(values["values"]))
                self.assertEqual(len(values["values"]),
                                 len(set(values["values"])))
        finally:
            stop.set()
            thread.join()

    def secret(self):
        self.store.consistency_token("acme")
        with open(os.path.join(self.root, "revision.json"), encoding="utf-8") as f:
            return json.load(f)["secret"]

    def forge(self, revision, tenant="acme"):
        payload = json.dumps({"revision": revision, "tenant": tenant, "v": 1},
                             sort_keys=True, separators=(",", ":")).encode()
        body = base64.urlsafe_b64encode(payload).decode().rstrip("=")
        signature = hmac.new(self.secret().encode(), body.encode(),
                             hashlib.sha256).hexdigest()
        return "obsd1.%s.%s" % (body, signature)

    def test_read_token_semantics(self):
        self.seed()
        token = self.store.consistency_token("acme")["token"]
        self.assertEqual(
            self.store.label_names("acme", "m", read_token=token)["labels"],
            ["host", "zone"])
        self.assertEqual(
            self.store.label_values("acme", "m", "host",
                                    read_token=token)["values"][0], "")
        for bad in ("bogus", "not-a-token", 42):
            for call in (lambda: self.store.label_names("acme", "m",
                                                        read_token=bad),
                         lambda: self.store.label_values("acme", "m", "host",
                                                         read_token=bad)):
                with self.assertRaises(ObsError) as caught:
                    call()
                self.assertEqual(str(caught.exception), "invalid read token")
        cross = self.store.consistency_token("globex")["token"]
        with self.assertRaises(ObsError) as caught:
            self.store.label_names("acme", "m", read_token=cross)
        self.assertEqual(str(caught.exception), "invalid read token")
        future = self.forge(self.store.consistency_token("acme")["revision"]
                            + 100)
        for call in (lambda: self.store.label_names("acme", "m",
                                                    read_token=future),
                     lambda: self.store.label_values("acme", "m", "host",
                                                     read_token=future)):
            with self.assertRaises(ObsError) as caught:
                call()
            self.assertEqual(str(caught.exception),
                             "read revision unavailable")


class TestLabelMetadataHttp(MetadataCase):
    def setUp(self):
        super().setUp()
        self.engine = AlertEngine(self.store, self.root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        super().tearDown()

    def request(self, path, token=None):
        req = urllib.request.Request(self.base + path)
        if token is not None:
            req.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(
                    response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_names_and_values_endpoints(self):
        self.seed()
        code, body = self.request("/v1/label-names?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"tenant": "acme", "metric": "m",
                                "labels": ["host", "zone"]})
        code, body = self.request(
            "/v1/label-values?tenant=acme&metric=m&name=host")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"tenant": "acme", "metric": "m",
                                "label": "host",
                                "values": ["", "api-a", "api-b", "web-1"]})

    def test_empty_results_are_200_empty_arrays(self):
        code, body = self.request("/v1/label-names?tenant=acme&metric=m")
        self.assertEqual((code, body),
                         (200, {"tenant": "acme", "metric": "m", "labels": []}))
        self.seed()
        code, body = self.request(
            "/v1/label-values?tenant=acme&metric=m&name=absent")
        self.assertEqual(code, 200)
        self.assertEqual(body["values"], [])

    def test_filters_over_http(self):
        self.seed()
        encoded = urllib.parse.quote(
            '[{"key":"host","op":"=~","value":"api-.*"}]')
        code, body = self.request(
            "/v1/label-values?tenant=acme&metric=m&name=host&matchers="
            + encoded)
        self.assertEqual(code, 200)
        self.assertEqual(body["values"], ["api-a", "api-b"])
        code, body = self.request(
            "/v1/label-values?tenant=acme&metric=m&name=host&label.zone=x")
        self.assertEqual(code, 200)
        self.assertEqual(body["values"], ["api-a", "api-b"])

    def test_invalid_requests_are_400_label_metadata_invalid(self):
        self.seed()
        paths = (
            "/v1/label-names?metric=m",
            "/v1/label-names?tenant=acme",
            "/v1/label-names?tenant=&metric=m",
            "/v1/label-values?tenant=acme&metric=m",
            "/v1/label-values?tenant=acme&metric=m&name=",
            "/v1/label-names?tenant=acme&metric=m&start=1",
            "/v1/label-names?tenant=acme&metric=m&step=1",
            "/v1/label-names?tenant=acme&metric=m&agg=sum",
            "/v1/label-names?tenant=acme&metric=m&group_by=%5B%5D",
            "/v1/label-names?tenant=acme&metric=m&window=1",
            "/v1/label-values?tenant=acme&metric=m&name=host&bogus=1",
            "/v1/label-names?tenant=acme&metric=nope&matchers=",
            "/v1/label-names?tenant=acme&metric=nope&matchers=null",
            "/v1/label-names?tenant=acme&metric=nope&matchers="
            + urllib.parse.quote('[{"key":"k","op":"~","value":""}]'),
            "/v1/label-values?tenant=acme&metric=nope&name=k&matchers="
            + urllib.parse.quote('[{"key":"host","op":"=","value":1}]'),
        )
        for path in paths:
            code, body = self.request(path)
            self.assertEqual(code, 400, path)
            self.assertEqual(body, {"error": "label metadata invalid"}, path)

    def test_read_token_failures_over_http(self):
        self.seed()
        code, body = self.request(
            "/v1/label-names?tenant=acme&metric=m&read_token=bogus")
        self.assertEqual((code, body), (400, {"error": "invalid read token"}))
        code, body = self.request(
            "/v1/label-values?tenant=acme&metric=m&name=host"
            "&read_token=bogus")
        self.assertEqual(code, 400)
        self.assertEqual(body, {"error": "invalid read token"})
        secret_path = os.path.join(self.root, "revision.json")
        self.store.consistency_token("acme")
        with open(secret_path, encoding="utf-8") as handle:
            secret = json.load(handle)["secret"]
        payload = json.dumps(
            {"revision": 999, "tenant": "acme", "v": 1},
            sort_keys=True, separators=(",", ":")).encode()
        body64 = base64.urlsafe_b64encode(payload).decode().rstrip("=")
        signature = hmac.new(secret.encode(), body64.encode(),
                             hashlib.sha256).hexdigest()
        token = "obsd1.%s.%s" % (body64, signature)
        code, body = self.request(
            "/v1/label-names?tenant=acme&metric=m&read_token=" + token)
        self.assertEqual((code, body),
                         (409, {"error": "read revision unavailable"}))

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
            def call(path, token=None):
                req = urllib.request.Request(base + path)
                if token is not None:
                    req.add_header("Authorization", "Bearer " + token)
                try:
                    with urllib.request.urlopen(req, timeout=10) as response:
                        return response.status
                except urllib.error.HTTPError as exc:
                    return exc.code

            self.seed()
            self.assertEqual(call("/v1/label-names?tenant=acme&metric=m"), 401)
            self.assertEqual(
                call("/v1/label-names?tenant=acme&metric=m", "view-tok"), 200)
            self.assertEqual(
                call("/v1/label-values?tenant=acme&metric=m&name=host",
                     "view-tok"), 200)
            self.assertEqual(
                call("/v1/label-names?tenant=acme&metric=m", "other-tok"), 403)
            # A read with no single tenant is not a viewer operation.
            self.assertEqual(call("/v1/label-names?metric=m", "view-tok"), 403)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        entries = [row for row in access.query_audit()
                   if row["path"] in ("/v1/label-names", "/v1/label-values")]
        self.assertEqual([row["status"] for row in entries],
                         [401, 200, 200, 403, 403])
        self.assertTrue(all(row["outcome"] in ("denied", "allowed")
                            for row in entries))


class TestLabelMetadataCli(MetadataCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        data_dir = os.path.join(self.tmp, "cli")
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", data_dir] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def seed_cli(self):
        for labels in ("host=api-a,zone=x", "host=api-b,zone=x",
                       "host=web-1,zone=y", "host=,zone=", ""):
            argv = ["write", "--tenant", "acme", "--metric", "m",
                    "--sample", "1000:1"]
            if labels:
                for pair in labels.split(","):
                    argv += ["--label", pair]
            code, _, err = self.run_cli(*argv)
            self.assertEqual(code, 0, err)

    def test_commands_emit_one_json_line(self):
        self.seed_cli()
        code, out, err = self.run_cli("label-names", "--tenant", "acme",
                                      "--metric", "m")
        self.assertEqual(code, 0, err)
        self.assertEqual(len(out.splitlines()), 1)
        self.assertEqual(json.loads(out),
                         {"tenant": "acme", "metric": "m",
                          "labels": ["host", "zone"]})
        code, out, err = self.run_cli("label-values", "--tenant", "acme",
                                      "--metric", "m", "--name", "host")
        self.assertEqual(code, 0, err)
        self.assertEqual(len(out.splitlines()), 1)
        self.assertEqual(json.loads(out),
                         {"tenant": "acme", "metric": "m", "label": "host",
                          "values": ["", "api-a", "api-b", "web-1"]})

    def test_empty_results_are_empty_arrays(self):
        code, out, err = self.run_cli("label-names", "--tenant", "acme",
                                      "--metric", "m")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["labels"], [])
        self.seed_cli()
        code, out, err = self.run_cli("label-values", "--tenant", "acme",
                                      "--metric", "m", "--name", "absent")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["values"], [])

    def test_filters(self):
        self.seed_cli()
        code, out, err = self.run_cli(
            "label-values", "--tenant", "acme", "--metric", "m", "--name",
            "host", "--matchers", '[{"key":"host","op":"=~","value":"api-.*"}]')
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["values"], ["api-a", "api-b"])
        code, out, err = self.run_cli(
            "label-names", "--tenant", "acme", "--metric", "m",
            "--label", "zone=x")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["labels"], ["host", "zone"])

    def test_invalid_arguments_are_one_error_line(self):
        self.seed_cli()
        cases = (
            ["label-names", "--metric", "m"],
            ["label-names", "--tenant", "acme"],
            ["label-values", "--tenant", "acme", "--metric", "m"],
            ["label-values", "--tenant", "acme", "--metric", "m", "--name", ""],
            ["label-names", "--tenant", "acme", "--metric", "m",
             "--matchers", "bad"],
            ["label-names", "--tenant", "acme", "--metric", "nope",
             "--matchers", '[{"key":"k","op":"~","value":""}]'],
        )
        for argv in cases:
            code, out, err = self.run_cli(*argv)
            self.assertEqual(code, 1, argv)
            self.assertEqual(out, "", argv)
            lines = err.strip().splitlines()
            self.assertEqual(json.loads(lines[-1]),
                             {"error": "label metadata invalid"}, argv)

    def test_read_token_flags(self):
        self.seed_cli()
        code, out, _ = self.run_cli("consistency-token", "--tenant", "acme")
        token = json.loads(out)["token"]
        code, out, err = self.run_cli("label-names", "--tenant", "acme",
                                      "--metric", "m", "--read-token", token)
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["labels"], ["host", "zone"])
        code, _, err = self.run_cli("label-values", "--tenant", "acme",
                                    "--metric", "m", "--name", "host",
                                    "--read-token", "bogus")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(err), {"error": "invalid read token"})


if __name__ == "__main__":
    unittest.main()
