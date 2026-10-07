"""Tests for the read-only label metadata discovery (label names/values)."""

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


def forge_token(secret, tenant, revision):
    """Build a syntactically valid token with an arbitrary payload."""
    payload = json.dumps({"revision": revision, "tenant": tenant, "v": 1},
                         sort_keys=True, separators=(",", ":")).encode("utf-8")
    body = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    signature = hmac.new(secret.encode("ascii"), body.encode("ascii"),
                         hashlib.sha256).hexdigest()
    return "obsd1.%s.%s" % (body, signature)


class LabelMetadataCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-labelmeta-")
        self.root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(self.root)
        self.engine = AlertEngine(self.store, self.root)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def seed(self):
        self.store.write("acme", "latency_ms",
                         {"host": "api-a", "region": "us", "env": "prod"},
                         [[1000, 1.0]])
        self.store.write("acme", "latency_ms",
                         {"host": "api-b", "region": "eu", "env": ""},
                         [[1000, 2.0]])
        self.store.write("acme", "latency_ms",
                         {"host": "worker", "zone": "z1"}, [[1000, 3.0]])
        # Other tenants and metrics stay invisible to the queries below.
        self.store.write("acme", "errors_total", {"host": "hidden"},
                         [[1000, 1.0]])
        self.store.write("globex", "latency_ms", {"host": "hidden"},
                         [[1000, 1.0]])

    def empty_series(self):
        """Register a series and then drop all its samples via retention."""
        self.store.write("acme", "latency_ms",
                         {"host": "gone", "region": "us"}, [[500, 4.0]])
        self.store.set_retention("acme", 1000)
        self.store.run_retention(2000, tenant="acme")


class TestLabelNames(LabelMetadataCase):
    def test_names_are_deduplicated_sorted_and_scoped(self):
        self.seed()
        result = self.store.label_names("acme", "latency_ms")
        self.assertEqual(result, {"tenant": "acme", "metric": "latency_ms",
                                  "labels": ["env", "host", "region", "zone"]})

    def test_empty_series_contribute_names(self):
        self.seed()
        self.empty_series()
        result = self.store.label_names("acme", "latency_ms")
        self.assertEqual(result["labels"], ["env", "host", "region", "zone"])
        # The emptied series stays registered; only its sample is gone.
        self.assertEqual(self.store.get_quota("acme")["points"], 4)

    def test_exact_labels_and_matchers_filter_first(self):
        self.seed()
        result = self.store.label_names("acme", "latency_ms",
                                        labels={"region": "us"})
        self.assertEqual(result["labels"], ["env", "host", "region"])
        result = self.store.label_names(
            "acme", "latency_ms",
            matchers=[{"key": "host", "op": "=~", "value": "api-.*"}])
        self.assertEqual(result["labels"], ["env", "host", "region"])
        result = self.store.label_names(
            "acme", "latency_ms",
            matchers=[{"key": "zone", "op": "!=", "value": "z1"}])
        self.assertEqual(result["labels"], ["env", "host", "region"])

    def test_no_matching_series_is_an_empty_list(self):
        self.seed()
        result = self.store.label_names("acme", "latency_ms",
                                        labels={"host": "nope"})
        self.assertEqual(result["labels"], [])
        result = self.store.label_names("acme", "unknown_metric")
        self.assertEqual(result, {"tenant": "acme", "metric": "unknown_metric",
                                  "labels": []})

    def test_unicode_sort_order(self):
        self.store.write("acme", "m", {"Zeta": "1", "alpha": "1", "Ä": "1"},
                         [[1, 1.0]])
        result = self.store.label_names("acme", "m")
        self.assertEqual(result["labels"], sorted(["Zeta", "alpha", "Ä"]))


class TestLabelValues(LabelMetadataCase):
    def test_values_are_deduplicated_sorted_and_keep_empty_string(self):
        self.seed()
        result = self.store.label_values("acme", "latency_ms", "env")
        self.assertEqual(result, {"tenant": "acme", "metric": "latency_ms",
                                  "label": "env", "values": ["", "prod"]})
        result = self.store.label_values("acme", "latency_ms", "host")
        self.assertEqual(result["values"], ["api-a", "api-b", "worker"])

    def test_series_without_the_label_contribute_nothing(self):
        self.seed()
        result = self.store.label_values("acme", "latency_ms", "zone")
        self.assertEqual(result["values"], ["z1"])
        result = self.store.label_values("acme", "latency_ms", "missing")
        self.assertEqual(result["values"], [])

    def test_filters_apply_to_series_labels_first(self):
        self.seed()
        result = self.store.label_values("acme", "latency_ms", "host",
                                         labels={"region": "us"})
        self.assertEqual(result["values"], ["api-a"])
        result = self.store.label_values(
            "acme", "latency_ms", "host",
            matchers=[{"key": "env", "op": "!~", "value": "prod"}])
        self.assertEqual(result["values"], ["api-b", "worker"])

    def test_no_matching_series_is_an_empty_list(self):
        self.seed()
        result = self.store.label_values("acme", "latency_ms", "host",
                                         labels={"host": "nope"})
        self.assertEqual(result, {"tenant": "acme", "metric": "latency_ms",
                                  "label": "host", "values": []})


class TestLabelMetadataValidation(LabelMetadataCase):
    def assert_invalid(self, call, *args, **kwargs):
        with self.assertRaises(ObsError) as caught:
            call(*args, **kwargs)
        self.assertEqual(str(caught.exception), "label metadata invalid")

    def test_scope_fields_must_be_non_empty_strings(self):
        self.seed()
        for bad in (None, "", 42, ["acme"]):
            self.assert_invalid(self.store.label_names, bad, "latency_ms")
            self.assert_invalid(self.store.label_names, "acme", bad)
            self.assert_invalid(self.store.label_values, bad, "latency_ms", "h")
            self.assert_invalid(self.store.label_values, "acme", bad, "h")
            self.assert_invalid(self.store.label_values, "acme", "latency_ms",
                                bad)

    def test_malformed_matchers_are_the_unified_error(self):
        self.seed()
        bad_matchers = (
            "not-a-list",
            [{"key": "host", "op": "=", "value": "a", "extra": 1}],
            [{"key": "", "op": "=", "value": "a"}],
            [{"key": "host", "op": "?", "value": "a"}],
            [{"key": "host", "op": "=~", "value": "("}],
        )
        for matchers in bad_matchers:
            self.assert_invalid(self.store.label_names, "acme", "latency_ms",
                                matchers=matchers)
            self.assert_invalid(self.store.label_values, "acme", "latency_ms",
                                "host", matchers=matchers)

    def test_matchers_are_validated_without_candidate_series(self):
        # No series at all: a malformed matcher list still fails.
        self.assert_invalid(self.store.label_names, "acme", "latency_ms",
                            matchers=[{"key": "h", "op": "=~", "value": "("}])
        self.assert_invalid(self.store.label_values, "acme", "latency_ms",
                            "host", matchers="junk")

    def test_reads_never_change_state(self):
        self.seed()
        before = (self.store.stats(), self.store.consistency_token("acme"))
        self.store.label_names("acme", "latency_ms")
        self.store.label_values("acme", "latency_ms", "host")
        for call, args in ((self.store.label_names, ("acme", "latency_ms")),
                           (self.store.label_values,
                            ("acme", "latency_ms", "host"))):
            with self.assertRaises(ObsError):
                call(*args, matchers="junk")
        after = (self.store.stats(), self.store.consistency_token("acme"))
        self.assertEqual(before[0], after[0])
        self.assertEqual(before[1]["revision"], after[1]["revision"])


class TestLabelMetadataTokens(LabelMetadataCase):
    def secret(self):
        self.store.consistency_token("acme")
        with open(os.path.join(self.root, "revision.json"), "r",
                  encoding="utf-8") as handle:
            return json.load(handle)["secret"]

    def test_valid_token_reads(self):
        self.seed()
        token = self.store.consistency_token("acme")["token"]
        result = self.store.label_names("acme", "latency_ms", read_token=token)
        self.assertIn("host", result["labels"])
        result = self.store.label_values("acme", "latency_ms", "host",
                                         read_token=token)
        self.assertEqual(len(result["values"]), 3)

    def test_forged_cross_tenant_and_malformed_tokens_are_400(self):
        self.seed()
        good = self.store.consistency_token("acme")["token"]
        foreign = SeriesStore(os.path.join(self.tmp, "foreign"))
        foreign.consistency_token("acme")
        bad_tokens = (
            good[:-1] + ("a" if good[-1] != "a" else "b"),
            self.store.consistency_token("globex")["token"],
            foreign.consistency_token("acme")["token"],
            "not-a-token",
            "",
        )
        for token in bad_tokens:
            for call, args in ((self.store.label_names, ("acme", "latency_ms")),
                               (self.store.label_values,
                                ("acme", "latency_ms", "host"))):
                with self.assertRaises(ObsError) as caught:
                    call(*args, read_token=token)
                self.assertEqual(str(caught.exception), "invalid read token")

    def test_unavailable_revision_is_409_semantics(self):
        self.seed()
        revision = self.store.consistency_token("acme")["revision"]
        token = forge_token(self.secret(), "acme", revision + 100)
        for call, args in ((self.store.label_names, ("acme", "latency_ms")),
                           (self.store.label_values,
                            ("acme", "latency_ms", "host"))):
            with self.assertRaises(ObsError) as caught:
                call(*args, read_token=token)
            self.assertEqual(str(caught.exception), "read revision unavailable")


class LabelMetadataHttpCase(unittest.TestCase):
    access = None

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-labelmeta-http-")
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

    def seed(self):
        for labels in ({"host": "api-a", "region": "us"},
                       {"host": "api-b", "region": "eu"},
                       {"host": "worker", "zone": "z1"}):
            code, _ = self.request("POST", "/v1/series", {
                "tenant": "acme", "metric": "latency_ms", "labels": labels,
                "samples": [[1000, 1.0]]})
            self.assertEqual(code, 202)


class TestLabelMetadataHttp(LabelMetadataHttpCase):
    def test_label_names_endpoint(self):
        self.seed()
        code, body = self.request(
            "GET", "/v1/label-names?tenant=acme&metric=latency_ms")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"tenant": "acme", "metric": "latency_ms",
                                "labels": ["host", "region", "zone"]})

    def test_label_values_endpoint(self):
        self.seed()
        code, body = self.request(
            "GET", "/v1/label-values?tenant=acme&metric=latency_ms&name=region")
        self.assertEqual(code, 200)
        self.assertEqual(body, {"tenant": "acme", "metric": "latency_ms",
                                "label": "region", "values": ["eu", "us"]})

    def test_filters_via_query_string(self):
        self.seed()
        code, body = self.request(
            "GET", "/v1/label-names?tenant=acme&metric=latency_ms&label.region=us")
        self.assertEqual((code, body["labels"]), (200, ["host", "region"]))
        matchers = json.dumps([{"key": "host", "op": "=~", "value": "api-.*"}])
        code, body = self.request(
            "GET", "/v1/label-values?tenant=acme&metric=latency_ms&name=region"
                   "&matchers=" + urllib.parse.quote(matchers))
        self.assertEqual((code, body["values"]), (200, ["eu", "us"]))

    def test_no_match_is_200_with_empty_array(self):
        self.seed()
        code, body = self.request(
            "GET", "/v1/label-names?tenant=acme&metric=nope")
        self.assertEqual((code, body["labels"]), (200, []))
        code, body = self.request(
            "GET", "/v1/label-values?tenant=acme&metric=latency_ms&name=nope")
        self.assertEqual((code, body["values"]), (200, []))

    def test_missing_and_unknown_fields_are_the_unified_400(self):
        self.seed()
        for path in ("/v1/label-names?metric=latency_ms",
                     "/v1/label-names?tenant=acme",
                     "/v1/label-names?tenant=acme&metric=latency_ms&bogus=1",
                     "/v1/label-names?tenant=acme&metric=latency_ms&name=host",
                     "/v1/label-values?tenant=acme&metric=latency_ms",
                     "/v1/label-values?tenant=acme&name=host",
                     "/v1/label-values?metric=latency_ms&name=host",
                     "/v1/label-values?tenant=acme&metric=latency_ms&name=host"
                     "&start=0"):
            code, body = self.request("GET", path)
            self.assertEqual((code, body), (400, {"error": "label metadata invalid"}),
                             msg=path)

    def test_bad_matchers_are_the_unified_400_even_without_candidates(self):
        bad_regex = urllib.parse.quote(
            json.dumps([{"key": "h", "op": "=~", "value": "("}]))
        for path in ("/v1/label-names?tenant=acme&metric=latency_ms&matchers=junk",
                     "/v1/label-names?tenant=acme&metric=latency_ms&matchers=",
                     "/v1/label-values?tenant=acme&metric=latency_ms&name=host"
                     "&matchers=" + bad_regex):
            code, body = self.request("GET", path)
            self.assertEqual((code, body), (400, {"error": "label metadata invalid"}),
                             msg=path)

    def test_read_token_failures(self):
        self.seed()
        _, token_body = self.request("GET", "/v1/consistency-token?tenant=acme")
        token = token_body["token"]
        code, body = self.request(
            "GET", "/v1/label-names?tenant=acme&metric=latency_ms&read_token="
                   + token)
        self.assertEqual(code, 200)
        tampered = token[:-1] + ("a" if token[-1] != "a" else "b")
        code, body = self.request(
            "GET", "/v1/label-names?tenant=acme&metric=latency_ms&read_token="
                   + tampered)
        self.assertEqual((code, body), (400, {"error": "invalid read token"}))
        _, other = self.request("GET", "/v1/consistency-token?tenant=globex")
        code, body = self.request(
            "GET", "/v1/label-values?tenant=acme&metric=latency_ms&name=host"
                   "&read_token=" + other["token"])
        self.assertEqual((code, body), (400, {"error": "invalid read token"}))

    def test_unavailable_revision_is_409(self):
        self.seed()
        self.request("GET", "/v1/consistency-token?tenant=acme")
        with open(os.path.join(self.store.root, "revision.json"), "r",
                  encoding="utf-8") as handle:
            secret = json.load(handle)["secret"]
        revision = self.store.consistency_token("acme")["revision"]
        token = forge_token(secret, "acme", revision + 100)
        code, body = self.request(
            "GET", "/v1/label-names?tenant=acme&metric=latency_ms&read_token="
                   + token)
        self.assertEqual((code, body), (409, {"error": "read revision unavailable"}))


class TestLabelMetadataHttpAccess(LabelMetadataHttpCase):
    def setUp(self):
        super().setUp()
        # Seed directly through the store: once principals exist every HTTP
        # request needs a token.
        for labels in ({"host": "api-a", "region": "us"},
                       {"host": "api-b", "region": "eu"},
                       {"host": "worker", "zone": "z1"}):
            self.store.write("acme", "latency_ms", labels, [[1000, 1.0]])
        self.access.create_principal("viewer", "view-tok", "viewer", ["acme"])
        self.access.create_principal("admin", "admin-tok", "admin", ["acme"])

    def test_anonymous_is_denied_and_audited(self):
        code, body = self.request("GET",
                                  "/v1/label-names?tenant=acme&metric=latency_ms")
        self.assertEqual((code, body), (401, {"error": "unauthorized"}))

    def test_viewer_reads_own_tenant_only(self):
        code, body = self.request(
            "GET", "/v1/label-names?tenant=acme&metric=latency_ms",
            token="view-tok")
        self.assertEqual(code, 200)
        self.assertEqual(body["labels"], ["host", "region", "zone"])
        code, body = self.request(
            "GET", "/v1/label-values?tenant=acme&metric=latency_ms&name=host",
            token="view-tok")
        self.assertEqual(code, 200)
        code, body = self.request(
            "GET", "/v1/label-names?tenant=globex&metric=latency_ms",
            token="view-tok")
        self.assertEqual((code, body), (403, {"error": "forbidden"}))

    def test_reads_are_audited_with_the_tenant(self):
        self.request("GET", "/v1/label-names?tenant=acme&metric=latency_ms",
                     token="view-tok")
        self.request("GET", "/v1/label-values?tenant=acme&metric=latency_ms"
                            "&name=host", token="view-tok")
        entries = [row for row in self.access.query_audit()
                   if row["path"].startswith("/v1/label-")]
        self.assertEqual([row["path"] for row in entries],
                         ["/v1/label-names", "/v1/label-values"])
        self.assertTrue(all(row["tenant"] == "acme" for row in entries))
        self.assertTrue(all(row["outcome"] == "allowed" for row in entries))
        self.assertTrue(all(row["principal_id"] == "viewer" for row in entries))


class TestLabelMetadataCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-labelmeta-cli-")
        self.data_dir = os.path.join(self.tmp, "data")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", self.data_dir] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def seed(self):
        for args in (("write", "--tenant", "acme", "--metric", "latency_ms",
                      "--label", "host=api-a", "--label", "region=us",
                      "--sample", "1000:1.0"),
                     ("write", "--tenant", "acme", "--metric", "latency_ms",
                      "--label", "host=api-b", "--label", "region=eu",
                      "--sample", "1000:2.0")):
            code, _, err = self.run_cli(*args)
            self.assertEqual((code, err), (0, ""))

    def test_label_names_single_line_json(self):
        self.seed()
        code, out, err = self.run_cli("label-names", "--tenant", "acme",
                                      "--metric", "latency_ms")
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(len(out.splitlines()), 1)
        self.assertEqual(json.loads(out), {"tenant": "acme",
                                           "metric": "latency_ms",
                                           "labels": ["host", "region"]})

    def test_label_values_single_line_json(self):
        self.seed()
        code, out, err = self.run_cli("label-values", "--tenant", "acme",
                                      "--metric", "latency_ms", "--name",
                                      "region")
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(json.loads(out)["values"], ["eu", "us"])
        code, out, err = self.run_cli("label-values", "--tenant", "acme",
                                      "--metric", "latency_ms", "--name",
                                      "host", "--label", "region=us")
        self.assertEqual(json.loads(out)["values"], ["api-a"])

    def test_missing_name_is_the_unified_error(self):
        self.seed()
        code, out, err = self.run_cli("label-values", "--tenant", "acme",
                                      "--metric", "latency_ms")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertEqual(json.loads(err), {"error": "label metadata invalid"})

    def test_bad_matchers_are_the_unified_error(self):
        self.seed()
        code, _, err = self.run_cli("label-names", "--tenant", "acme",
                                    "--metric", "latency_ms", "--matchers",
                                    "junk")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(err), {"error": "label metadata invalid"})

    def test_read_token_roundtrip(self):
        self.seed()
        code, out, _ = self.run_cli("consistency-token", "--tenant", "acme")
        token = json.loads(out)["token"]
        code, out, err = self.run_cli("label-names", "--tenant", "acme",
                                      "--metric", "latency_ms", "--read-token",
                                      token)
        self.assertEqual((code, err), (0, ""))
        tampered = token[:-1] + ("a" if token[-1] != "a" else "b")
        code, out, err = self.run_cli("label-names", "--tenant", "acme",
                                      "--metric", "latency_ms", "--read-token",
                                      tampered)
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(err), {"error": "invalid read token"})


if __name__ == "__main__":
    unittest.main()
