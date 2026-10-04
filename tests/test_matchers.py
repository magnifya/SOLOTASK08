"""Tests for composable label matchers (=, !=, =~, !~) across all entries.

Covers SeriesStore.query (raw, bucketed, grouped and sliding-window), the HTTP
GET /v1/query ``matchers`` parameter and the CLI ``--matchers`` option.
"""

import contextlib
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

from obsd import AlertEngine, ObsError, SeriesStore, create_server
from obsd.cli import main as cli_main
from obsd.tsdb import parse_matchers_text

M = lambda key, op, value: {"key": key, "op": op, "value": value}


def _sig(labels):
    """Order-independent signature of a label dict."""
    return tuple(sorted(labels.items()))


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-match-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))


class TestMatcherSemantics(StoreCase):
    def fill(self, store):
        store.write("acme", "m", {"host": "api-a", "zone": "x"}, [[1000, 1.0]])
        store.write("acme", "m", {"host": "api-b", "zone": "x"}, [[1000, 2.0]])
        store.write("acme", "m", {"host": "xapi-a", "zone": "y"}, [[1000, 3.0]])
        store.write("acme", "m", {"host": "", "zone": ""}, [[1000, 4.0]])
        store.write("acme", "m", {"zone": "x"}, [[1000, 5.0]])
        store.write("other", "m", {"host": "api-a"}, [[1000, 9.0]])

    def labels(self, store, **over):
        rows = store.query("acme", "m", **over)
        return sorted(_sig(dict(row["labels"])) for row in rows)

    def test_exact_equality_and_negation(self):
        store = self.store()
        self.fill(store)
        self.assertEqual(self.labels(store, matchers=[M("host", "=", "api-a")]),
                         [_sig({"host": "api-a", "zone": "x"})])
        self.assertEqual(self.labels(store, matchers=[M("host", "!=", "api-a")]),
                         [_sig({"host": "", "zone": ""}),
                          _sig({"host": "api-b", "zone": "x"}),
                          _sig({"host": "xapi-a", "zone": "y"}),
                          _sig({"zone": "x"})])

    def test_missing_key_fails_equality_passes_negation(self):
        store = self.store()
        self.fill(store)
        # The host-less series (zone x only) matches != but never =.
        self.assertIn(_sig({"zone": "x"}),
                      self.labels(store, matchers=[M("host", "!=", "api-a")]))
        self.assertNotIn(_sig({"zone": "x"}),
                         self.labels(store, matchers=[M("host", "=", "anything")]))

    def test_regex_full_match_is_anchored(self):
        store = self.store()
        self.fill(store)
        got = self.labels(store, matchers=[M("host", "=~", "api-.*")])
        self.assertEqual(got, [_sig({"host": "api-a", "zone": "x"}),
                               _sig({"host": "api-b", "zone": "x"})])
        # Anchored: xapi-a does not match api-.*; the missing key neither.
        self.assertEqual(
            self.labels(store, matchers=[M("host", "=~", "xapi-a")]),
            [_sig({"host": "xapi-a", "zone": "y"})])

    def test_regex_negation_and_case_sensitivity(self):
        store = self.store()
        self.fill(store)
        got = set(self.labels(store, matchers=[M("host", "!~", "api-.*")]))
        # Missing key and non-matching values all pass !~.
        self.assertEqual(got, {_sig({"host": "", "zone": ""}),
                               _sig({"host": "xapi-a", "zone": "y"}),
                               _sig({"zone": "x"})})
        self.assertEqual(self.labels(store, matchers=[M("host", "=~", "API-.*")]), [])

    def test_empty_regex_only_matches_present_empty_value(self):
        store = self.store()
        self.fill(store)
        # Present and empty: the "" host series. The missing-host one is out.
        self.assertEqual(self.labels(store, matchers=[M("host", "=~", "")]),
                         [_sig({"host": "", "zone": ""})])
        # !~ matches the missing key but not the empty value.
        got = set(self.labels(store, matchers=[M("host", "!~", "")]))
        self.assertNotIn(_sig({"host": "", "zone": ""}), got)
        self.assertIn(_sig({"zone": "x"}), got)
        # Exact "" equality behaves the same for present keys.
        self.assertEqual(self.labels(store, matchers=[M("host", "=", "")]),
                         [_sig({"host": "", "zone": ""})])

    def test_multiple_matchers_on_same_and_different_keys_are_and_ed(self):
        store = self.store()
        self.fill(store)
        self.assertEqual(self.labels(store, matchers=[
            M("host", "=~", "api-.*"), M("zone", "=", "x")]),
            [_sig({"host": "api-a", "zone": "x"}),
             _sig({"host": "api-b", "zone": "x"})])
        # Two conditions on one key: intersection.
        self.assertEqual(self.labels(store, matchers=[
            M("host", "=~", "api-.*"), M("host", "!=", "api-b")]),
            [_sig({"host": "api-a", "zone": "x"})])

    def test_duplicate_matchers_dont_duplicate_series_and_order_is_irrelevant(self):
        store = self.store()
        self.fill(store)
        a = self.labels(store, matchers=[M("host", "=~", "api-.*"),
                                         M("zone", "=", "x")])
        b = self.labels(store, matchers=[M("zone", "=", "x"),
                                         M("host", "=~", "api-.*")])
        self.assertEqual(a, b)
        c = self.labels(store, matchers=[M("host", "=~", "api-.*"),
                                         M("host", "=~", "api-.*")])
        self.assertEqual(c, a)

    def test_contradictory_matchers_return_empty(self):
        store = self.store()
        self.fill(store)
        self.assertEqual(self.labels(store, matchers=[
            M("host", "=", "api-a"), M("host", "!=", "api-a")]), [])
        self.assertEqual(self.labels(store, matchers=[
            M("host", "=~", "api-.*"), M("host", "!~", "api-.*")]), [])

    def test_matchers_combine_with_exact_labels_as_and(self):
        store = self.store()
        self.fill(store)
        self.assertEqual(self.labels(store, labels={"zone": "x"},
                                     matchers=[M("host", "=~", "api-.*")]),
                         [_sig({"host": "api-a", "zone": "x"}),
                          _sig({"host": "api-b", "zone": "x"})])
        self.assertEqual(self.labels(store, labels={"host": "api-a"},
                                     matchers=[M("zone", "!=", "x")]), [])

    def test_matchers_apply_before_grouping_including_non_group_keys(self):
        store = self.store()
        self.fill(store)
        rows = store.query("acme", "m", agg="sum", group_by=["zone"],
                           matchers=[M("host", "=~", "api-.*")])
        # Only the two api-* series survive; both are zone x, and the
        # missing-host and xapi-a series never form groups.
        self.assertEqual([row["labels"] for row in rows], [{"zone": "x"}])
        self.assertEqual(rows[0]["points"], [[1000, 3.0]])

    def test_matchers_work_with_buckets_and_windows(self):
        store = self.store()
        store.write("acme", "m", {"host": "api-a"},
                    [[1000, 1.0], [1500, 2.0], [2000, 4.0]])
        store.write("acme", "m", {"host": "web-1"}, [[2000, 8.0]])
        rows = store.query("acme", "m", step_ms=1000, agg="sum",
                           matchers=[M("host", "=~", "api-.*")])
        self.assertEqual(rows[0]["points"], [[1000, 3.0], [2000, 4.0]])
        rows = store.query("acme", "m", start_ms=1000, end_ms=2000, step_ms=1000,
                           window_ms=1000, agg="sum",
                           matchers=[M("host", "!=", "web-1")])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["labels"], {"host": "api-a"})
        # t=1000 window (0,1000]: the 1000 sample only (1500 is after t);
        # t=2000 window (1000,2000]: 1500 and 2000 pool to 6.0.
        self.assertEqual(rows[0]["points"], [[1000, 1.0], [2000, 6.0]])
        # Contradictory matchers give an empty window result, not null grids.
        self.assertEqual(store.query(
            "acme", "m", start_ms=1000, end_ms=2000, step_ms=1000,
            window_ms=1000, agg="sum",
            matchers=[M("host", "=", "api-a"), M("host", "!=", "api-a")]), [])

    def test_matchers_scope_to_requested_tenant_and_metric_only(self):
        store = self.store()
        self.fill(store)
        rows = store.query("acme", "m", matchers=[M("host", "=", "api-a")])
        self.assertEqual(len(rows), 1)
        self.assertEqual(store.query("other", "m", matchers=[M("host", "=~", ".*")])[0]
                         ["labels"], {"host": "api-a"})
        # tenant/metric/series_id are not implicit labels.
        self.assertEqual(store.query("acme", "m", matchers=[M("tenant", "=~", ".*")]), [])
        self.assertEqual(store.query("acme", "m", matchers=[M("series_id", "=~", ".*")]), [])
        self.assertEqual(store.query("acme", "m", matchers=[M("metric", "=", "m")]), [])

    def test_omitted_or_empty_matchers_add_no_condition(self):
        store = self.store()
        self.fill(store)
        baseline = self.labels(store)
        self.assertEqual(self.labels(store, matchers=None), baseline)
        self.assertEqual(self.labels(store, matchers=[]), baseline)

    def test_matcher_filtering_keeps_missing_vs_empty_groups_distinct(self):
        store = self.store()
        self.fill(store)
        # .* matches present hosts only (empty included); missing-host excluded.
        rows = store.query("acme", "m", agg="sum", group_by=["zone"],
                           matchers=[M("host", "=~", ".*")])
        self.assertEqual([row["labels"] for row in rows],
                         [{"zone": ""}, {"zone": "x"}, {"zone": "y"}])


class TestMatcherValidation(StoreCase):
    def test_no_candidates_still_validates_every_matcher(self):
        store = self.store()
        for bad in (
                [{"key": "k", "op": "~", "value": ""}],
                [{"key": "k", "op": "=~", "value": "("}],
                [{"key": "", "op": "=", "value": ""}],
                [{"op": "=", "value": ""}],
                [{"key": "k", "op": "=", "value": 1}],
                [{"key": "k", "op": "=", "value": None}],
                [{"key": "k", "op": "="}],
                [{"key": "k", "op": "=", "value": "", "extra": 1}],
                ["not-an-object"],
                "not-a-list",
                {"key": "k", "op": "=", "value": ""},
        ):
            with self.assertRaises(ObsError, msg=repr(bad)):
                store.query("acme", "nonexistent-metric", matchers=bad)
        # A later invalid matcher is checked when an earlier one matches nothing.
        with self.assertRaises(ObsError):
            store.query("acme", "m", matchers=[
                M("nope", "=", "nope"), {"key": "k", "op": "=", "value": 1}])
        # And with no candidate series at all.
        with self.assertRaises(ObsError):
            store.query("acme", "nope", matchers=[M("k", "=", "v"),
                                                  {"key": "k", "op": "~", "value": ""}])

    def test_python_none_is_omitted_but_explicit_null_text_is_illegal(self):
        self.assertIsNone(parse_matchers_text(None))
        self.assertEqual(parse_matchers_text("[]"), [])
        for text in ("", "   ", "null", "garbage", "{}", '"x"'):
            with self.assertRaises(ObsError, msg=repr(text)):
                parse_matchers_text(text)


class TestMatcherPersistence(StoreCase):
    def test_results_stable_after_reopen_and_query_is_read_only(self):
        store = self.store("keep")
        store.write("acme", "m", {"host": "api-a"}, [[1000, 1.0], [2000, 2.0]])
        store.write("acme", "m", {"host": "web-1"}, [[1000, 9.0]])
        expected = store.query("acme", "m", group_by=[], agg="sum",
                               matchers=[M("host", "=~", "api-.*")])
        before = store.stats()
        for _ in range(3):
            store.query("acme", "m", matchers=[M("host", "=~", "api-.*")])
        self.assertEqual(store.stats(), before)
        reopened = self.store("keep")
        self.assertEqual(reopened.query("acme", "m", group_by=[], agg="sum",
                                        matchers=[M("host", "=~", "api-.*")]),
                         expected)


class TestMatcherHttp(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-match-http-")
        root = os.path.join(self.tmp, "data")
        self.store = SeriesStore(root)
        self.engine = AlertEngine(self.store, root)
        self.server = create_server(self.store, self.engine, "127.0.0.1", 0)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.store.write("acme", "m", {"host": "api-a"}, [[1000, 1.0]])
        self.store.write("acme", "m", {"host": "web-1"}, [[1000, 9.0]])

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def request(self, path):
        try:
            with urllib.request.urlopen(self.base + path, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_matchers_filter_query(self):
        encoded = urllib.parse.quote('[{"key":"host","op":"=~","value":"api-.*"}]')
        code, body = self.request(
            "/v1/query?tenant=acme&metric=m&matchers=" + encoded)
        self.assertEqual(code, 200)
        self.assertEqual(body, {"series": [
            {"labels": {"host": "api-a"}, "points": [[1000, 1.0]]}]})
        # Empty array adds no condition.
        code, body = self.request("/v1/query?tenant=acme&metric=m&matchers=%5B%5D")
        self.assertEqual(code, 200)
        self.assertEqual(len(body["series"]), 2)
        # Combines with label.* as AND.
        code, body = self.request(
            "/v1/query?tenant=acme&metric=m&label.host=api-a&matchers=" +
            urllib.parse.quote('[{"key":"host","op":"!=","value":"api-a"}]'))
        self.assertEqual(code, 200)
        self.assertEqual(body["series"], [])
        # No matchers parameter keeps the legacy behaviour.
        code, body = self.request("/v1/query?tenant=acme&metric=m")
        self.assertEqual(code, 200)
        self.assertEqual(len(body["series"]), 2)

    def test_bad_matchers_are_400_json(self):
        paths = (
            "matchers=",
            "matchers=null",
            "matchers=" + urllib.parse.quote("[not json"),
            "matchers=" + urllib.parse.quote("{}"),
            "matchers=" + urllib.parse.quote('"x"'),
            "matchers=" + urllib.parse.quote('[{"key":"host","op":"~","value":""}]'),
            "matchers=" + urllib.parse.quote('[{"key":"","op":"=","value":""}]'),
            "matchers=" + urllib.parse.quote('[{"key":"host","op":"=","value":1}]'),
            "matchers=" + urllib.parse.quote('[{"key":"host","op":"=~","value":"("}]'),
            "matchers=" + urllib.parse.quote(
                '[{"key":"host","op":"=","value":"a","extra":1}]'),
            "matchers=" + urllib.parse.quote('[{"op":"=","value":"a"}]'),
        )
        for suffix in paths:
            code, body = self.request("/v1/query?tenant=acme&metric=nope&" + suffix)
            self.assertEqual(code, 400, suffix)
            self.assertIn("error", body)


class TestMatcherCli(StoreCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        data_dir = os.path.join(self.tmp, "cli")
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", data_dir] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_matchers_query_success(self):
        code, _, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "m",
            "--label", "host=api-a", "--sample", "1000:1")
        self.assertEqual(code, 0, err)
        code, _, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "m",
            "--label", "host=web-1", "--sample", "1000:9")
        self.assertEqual(code, 0, err)
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--matchers", '[{"key":"host","op":"=~","value":"api-.*"}]')
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out), {"series": [
            {"labels": {"host": "api-a"}, "points": [[1000, 1.0]]}]})
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m", "--agg", "sum",
            "--group-by", "[]",
            "--matchers", '[{"key":"host","op":"!=","value":"web-1"}]')
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out),
                         {"series": [{"labels": {}, "points": [[1000, 1.0]]}]})

    def test_matchers_query_failures(self):
        for bad in ('', 'null', 'garbage', '{}',
                    '[{"key":"host","op":"~","value":""}]',
                    '[{"key":"host","op":"=~","value":"("}]',
                    '[{"key":"host","op":"=","value":1}]',
                    '[{"key":"","op":"=","value":""}]',
                    '[{"key":"host","op":"="}]'):
            code, out, err = self.run_cli(
                "query", "--tenant", "acme", "--metric", "m", "--matchers", bad)
            self.assertEqual(code, 1, repr(bad))
            self.assertEqual(out, "")
            line = err.strip().splitlines()[-1]
            self.assertIn("error", json.loads(line))


if __name__ == "__main__":
    unittest.main()
