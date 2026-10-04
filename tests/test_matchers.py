"""Tests for composable label matchers (=, !=, =~, !~) across all query paths.

Covers SeriesStore.query (raw, fixed buckets, grouped, sliding window),
GET /v1/query and the CLI ``query --matchers``.
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
import urllib.request

from obsd import AlertEngine, ObsError, SeriesStore, create_server
from obsd.cli import main as cli_main
from obsd.tsdb import compile_matchers, matchers_hold


def m(key, op, value):
    return {"key": key, "op": op, "value": value}


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="obsd-match-")
    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
    def store(self, sub="data"):
        return SeriesStore(os.path.join(self.tmp, sub))

    def seed(self, store):
        store.write("acme", "m", {"host": "api-a", "zone": "z1"},
                    [[1000, 1.0], [2000, 2.0]])
        store.write("acme", "m", {"host": "xapi-a", "zone": "z1"},
                    [[1000, 3.0]])
        store.write("acme", "m", {"host": "api-b", "zone": ""},
                    [[1000, 4.0]])
        store.write("acme", "m", {"host": "api-c"},
                    [[1000, 5.0]])
        store.write("acme", "m", {"host": "API-a"},
                    [[1000, 6.0]])
        store.write("acme", "other", {"host": "api-a"}, [[1000, 7.0]])
        store.write("globex", "m", {"host": "api-a"}, [[1000, 8.0]])
        return store


class TestMatcherPrimitives(unittest.TestCase):
    def test_none_and_empty_list_add_nothing(self):
        self.assertEqual(compile_matchers(None), [])
        self.assertEqual(compile_matchers([]), [])

    def test_compiled_forms(self):
        compiled = compile_matchers([m("k", "=", "v"), m("k", "!=", "v"),
                                     m("k", "=~", "v.*"), m("k", "!~", "v.*")])
        self.assertEqual([item[1] for item in compiled], ["=", "!=", "=~", "!~"])
        self.assertIsNone(compiled[0][3])
        self.assertIsNotNone(compiled[2][3])

    def test_hold_equality_and_missing_key(self):
        self.assertTrue(matchers_hold({"k": "v"}, compile_matchers([m("k", "=", "v")])))
        self.assertFalse(matchers_hold({"k": "w"}, compile_matchers([m("k", "=", "v")])))
        # Missing key fails = and =~ ...
        self.assertFalse(matchers_hold({}, compile_matchers([m("k", "=", "v")])))
        self.assertFalse(matchers_hold({}, compile_matchers([m("k", "=~", ".*")])))
        # ... but satisfies != and !~.
        self.assertTrue(matchers_hold({}, compile_matchers([m("k", "!=", "v")])))
        self.assertTrue(matchers_hold({}, compile_matchers([m("k", "!~", "v.*")])))
        self.assertFalse(matchers_hold({"k": "v"},
                                       compile_matchers([m("k", "!=", "v")])))
        self.assertFalse(matchers_hold({"k": "vxy"},
                                       compile_matchers([m("k", "!~", "v.*")])))

    def test_hold_regex_is_full_match_case_sensitive_unicode(self):
        self.assertTrue(matchers_hold({"host": "api-a"},
                                      compile_matchers([m("host", "=~", "api-.*")])))
        self.assertFalse(matchers_hold({"host": "xapi-a"},
                                       compile_matchers([m("host", "=~", "api-.*")])))
        self.assertFalse(matchers_hold({"host": "API-a"},
                                       compile_matchers([m("host", "=~", "api-.*")])))
        self.assertTrue(matchers_hold({"host": "café"},
                                      compile_matchers([m("host", "=~", "caf.")])))
        # Empty regex matches only a present, empty value.
        self.assertTrue(matchers_hold({"k": ""},
                                      compile_matchers([m("k", "=~", "")])))
        self.assertFalse(matchers_hold({"k": "x"},
                                       compile_matchers([m("k", "=~", "")])))
        self.assertFalse(matchers_hold({},
                                      compile_matchers([m("k", "=~", "")])))
        self.assertTrue(matchers_hold({},
                                     compile_matchers([m("k", "!~", "")])))

    def test_empty_value_is_a_real_value_distinct_from_missing(self):
        compiled = compile_matchers([m("zone", "=", "")])
        self.assertTrue(matchers_hold({"zone": ""}, compiled))
        self.assertFalse(matchers_hold({"zone": "z1"}, compiled))
        self.assertFalse(matchers_hold({}, compiled))


class TestMatcherValidation(unittest.TestCase):
    def expect_error(self, value):
        with self.assertRaises(ObsError, msg=repr(value)):
            compile_matchers(value)

    def test_good_shapes_accepted(self):
        compile_matchers([m("k", "=", "")])
        compile_matchers([m("k", "!~", "")])

    def test_bad_shapes_rejected(self):
        self.expect_error({"key": "k", "op": "=", "value": "v"})  # object, not list
        self.expect_error("x")
        self.expect_error(42)
        self.expect_error(True)
        for bad in ("x", 42, None, True, ["x"]):
            self.expect_error([bad])
        self.expect_error([{"op": "=", "value": "v"}])           # missing key
        self.expect_error([{"key": "k", "value": "v"}])          # missing op
        self.expect_error([{"key": "k", "op": "="}])             # missing value
        self.expect_error([{"key": "k", "op": "=", "value": "v", "x": 1}])  # extra
        self.expect_error([{"key": "", "op": "=", "value": "v"}])  # empty key
        self.expect_error([{"key": 1, "op": "=", "value": "v"}])
        self.expect_error([{"key": None, "op": "=", "value": "v"}])
        self.expect_error([{"key": "k", "op": "<", "value": "v"}])
        self.expect_error([{"key": "k", "op": "==", "value": "v"}])
        self.expect_error([{"key": "k", "op": "~", "value": "v"}])
        self.expect_error([{"key": "k", "op": None, "value": "v"}])
        self.expect_error([{"key": "k", "op": "=", "value": None}])
        self.expect_error([{"key": "k", "op": "=", "value": 1}])
        self.expect_error([{"key": "k", "op": "=~", "value": "("}])
        self.expect_error([{"key": "k", "op": "!~", "value": "[a"}])

    def test_validation_runs_with_no_candidate_series(self):
        store = SeriesStore(tempfile.mkdtemp(prefix="obsd-match-empty-"))
        try:
            with self.assertRaises(ObsError):
                store.query("acme", "m", matchers=[m("k", "=~", "(")])
        finally:
            shutil.rmtree(store.root, ignore_errors=True)


class TestQueryMatchers(StoreCase):
    def hosts(self, rows):
        return sorted(row["labels"]["host"] for row in rows)

    def test_raw_query_filters_with_each_op(self):
        store = self.seed(self.store())
        rows = store.query("acme", "m", matchers=[m("host", "=~", "api-.*")])
        # api-a, api-b, api-c match; xapi-a and API-a do not; other tenant and
        # other metric never appear.
        self.assertEqual(self.hosts(rows), ["api-a", "api-b", "api-c"])
        rows = store.query("acme", "m", matchers=[m("host", "!~", "api-.*")])
        self.assertEqual(self.hosts(rows), ["API-a", "xapi-a"])
        rows = store.query("acme", "m", matchers=[m("host", "=", "api-a")])
        self.assertEqual(self.hosts(rows), ["api-a"])
        # Missing key satisfies !=: api-c and API-a have no zone.
        rows = store.query("acme", "m", matchers=[m("zone", "!=", "z1")])
        self.assertEqual(self.hosts(rows), ["API-a", "api-b", "api-c"])
        # Empty-string value is not missing: = "" finds only the empty zone.
        rows = store.query("acme", "m", matchers=[m("zone", "=", "")])
        self.assertEqual(self.hosts(rows), ["api-b"])
        rows = store.query("acme", "m", matchers=[m("zone", "!=", "")])
        self.assertEqual(self.hosts(rows), ["API-a", "api-a", "api-c", "xapi-a"])

    def test_matchers_and_exact_labels_combine_by_and(self):
        store = self.seed(self.store())
        rows = store.query("acme", "m", labels={"zone": "z1"},
                           matchers=[m("host", "=~", "api-.*")])
        self.assertEqual(self.hosts(rows), ["api-a"])
        # Contradiction with the exact filter: empty result.
        rows = store.query("acme", "m", labels={"host": "api-a"},
                           matchers=[m("host", "!=", "api-a")])
        self.assertEqual(rows, [])

    def test_multiple_matchers_same_key_are_and_order_and_duplicates(self):
        store = self.seed(self.store())
        spec = [m("host", "=~", "api-.*"), m("host", "!=", "api-b")]
        rows_a = store.query("acme", "m", matchers=spec)
        rows_b = store.query("acme", "m", matchers=list(reversed(spec)))
        self.assertEqual(rows_a, rows_b)
        self.assertEqual(self.hosts(rows_a), ["api-a", "api-c"])
        # Duplicate matcher does not duplicate rows.
        rows = store.query("acme", "m", matchers=[m("host", "=", "api-a"),
                                                  m("host", "=", "api-a")])
        self.assertEqual(self.hosts(rows), ["api-a"])
        # Contradictory matchers: empty.
        rows = store.query("acme", "m",
                           matchers=[m("host", "=~", "api-.*"),
                                     m("host", "!~", "api-.*")])
        self.assertEqual(rows, [])

    def test_matchers_scope_to_tenant_and_metric_only(self):
        store = self.seed(self.store())
        rows = store.query("globex", "m", matchers=[m("host", "=~", ".*")])
        self.assertEqual(self.hosts(rows), ["api-a"])
        rows = store.query("acme", "other", matchers=[m("host", "=~", ".*")])
        self.assertEqual(self.hosts(rows), ["api-a"])
        # tenant/metric/series_id are not implicit label keys.
        rows = store.query("acme", "m", matchers=[m("tenant", "=~", ".*")])
        self.assertEqual(rows, [])
        rows = store.query("acme", "m", matchers=[m("metric", "=", "m")])
        self.assertEqual(rows, [])
        rows = store.query("acme", "m", matchers=[m("series_id", "!=", "")])
        # !="..." on a missing key matches every series, so this proves only
        # that series_id does not exist as a label (an empty regex =~ does not).
        self.assertEqual(len(rows), 5)

    def test_fixed_buckets_keep_semantics(self):
        store = self.seed(self.store())
        rows = store.query("acme", "m", matchers=[m("host", "=", "api-a")],
                           start_ms=1000, end_ms=3000, step_ms=1000, agg="sum")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["labels"], {"host": "api-a", "zone": "z1"})
        # Fixed buckets run to the last non-empty bucket (no trailing null).
        self.assertEqual(rows[0]["points"], [[1000, 1.0], [2000, 2.0]])

    def test_grouped_filters_before_grouping_with_unlisted_keys(self):
        store = self.seed(self.store())
        # Filter on zone (not in group_by): only api-a has zone z1 and a
        # matching host regex; xapi-a is cut by the regex. No step: both api-a
        # samples pool into one sum point at its earliest timestamp.
        rows = store.query("acme", "m", agg="sum", group_by=["zone"],
                           matchers=[m("host", "=~", "api-.*"),
                                     m("zone", "=~", "z.")])
        self.assertEqual(rows, [{"labels": {"zone": "z1"},
                                 "points": [[1000, 3.0]]}])
        # != "" keeps missing distinct from empty: groups are z1 and missing;
        # the empty-zone api-b is excluded while zone-less api-c is included.
        rows = store.query("acme", "m", agg="count", group_by=["zone"],
                           matchers=[m("host", "=~", "api-.*"),
                                     m("zone", "!=", "")])
        labels = [row["labels"] for row in rows]
        self.assertEqual(labels, [{}, {"zone": "z1"}])
        # Empty group_by merges the surviving series only.
        rows = store.query("acme", "m", agg="sum", group_by=[],
                           matchers=[m("host", "=", "api-a")])
        self.assertEqual(rows, [{"labels": {}, "points": [[1000, 3.0]]}])

    def test_sliding_windows_share_the_filter(self):
        store = self.seed(self.store())
        rows = store.query("acme", "m", matchers=[m("host", "=~", "api-.*")],
                           start_ms=1000, end_ms=2000, step_ms=1000,
                           window_ms=1000, agg="sum")
        by_host = {row["labels"]["host"]: row["points"] for row in rows}
        self.assertEqual(set(by_host), {"api-a", "api-b", "api-c"})
        self.assertEqual(by_host["api-a"], [[1000, 1.0], [2000, 2.0]])
        self.assertEqual(by_host["api-b"], [[1000, 4.0], [2000, None]])
        # Grouped windows filter first, then pool.
        rows = store.query("acme", "m", agg="sum", group_by=["zone"],
                           matchers=[m("host", "=~", "api-a")],
                           start_ms=1000, end_ms=2000, step_ms=1000,
                           window_ms=1000)
        self.assertEqual(rows, [{"labels": {"zone": "z1"},
                                 "points": [[1000, 1.0], [2000, 2.0]]}])
        # No match: empty list, not synthetic grids.
        rows = store.query("acme", "m", matchers=[m("host", "=", "nope")],
                           start_ms=1000, end_ms=2000, step_ms=1000,
                           window_ms=1000, agg="sum")
        self.assertEqual(rows, [])

    def test_none_and_empty_matchers_change_nothing(self):
        store = self.seed(self.store())
        baseline = store.query("acme", "m")
        self.assertEqual(store.query("acme", "m", matchers=None), baseline)
        self.assertEqual(store.query("acme", "m", matchers=[]), baseline)

    def test_query_is_read_only_and_stable_after_reopen(self):
        store = self.store("keep")
        self.seed(store)
        spec = [m("host", "=~", "api-.*"), m("zone", "!=", "z1")]
        expected = store.query("acme", "m", matchers=spec,
                               start_ms=0, end_ms=5000, step_ms=1000,
                               agg="sum", group_by=["host"], window_ms=2000)
        before = store.stats()
        for _ in range(5):
            store.query("acme", "m", matchers=spec,
                        start_ms=0, end_ms=5000, step_ms=1000,
                        agg="sum", group_by=["host"], window_ms=2000)
        self.assertEqual(store.stats(), before)
        reopened = self.store("keep")
        self.assertEqual(reopened.query("acme", "m", matchers=spec,
                                        start_ms=0, end_ms=5000, step_ms=1000,
                                        agg="sum", group_by=["host"],
                                        window_ms=2000), expected)

    def test_concurrent_writes_keep_snapshot_consistent_with_matchers(self):
        store = self.store()
        store.write("acme", "m", {"host": "a"}, [[1000, 1.0]])
        stop = threading.Event()

        def flip():
            value = 2.0
            while not stop.is_set():
                store.write("acme", "m", {"host": "a"}, [[1000, value]], overwrite=True)
                value = 5.0 - value  # 2.0 <-> 3.0

        thread = threading.Thread(target=flip)
        thread.start()
        try:
            for _ in range(400):
                rows = store.query("acme", "m", matchers=[m("host", "=", "a")])
                self.assertEqual(rows[0]["points"], [[1000, rows[0]["points"][0][1]]])
                self.assertIn(rows[0]["points"][0][1], (1.0, 2.0, 3.0))
        finally:
            stop.set()
            thread.join(timeout=5)


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
        self.store.write("acme", "m", {"host": "xapi-a"}, [[1000, 2.0]])

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

    Q = "/v1/query?tenant=acme&metric=m"

    def test_regex_filter_over_http(self):
        import urllib.parse
        text = json.dumps([m("host", "=~", "api-.*")])
        code, body = self.request(self.Q + "&matchers=" + urllib.parse.quote(text))
        self.assertEqual(code, 200)
        self.assertEqual(body, {"series": [{"labels": {"host": "api-a"},
                                            "points": [[1000, 1.0]]}]})

    def test_omitted_and_empty_array_add_no_condition(self):
        code, body = self.request(self.Q)
        self.assertEqual(code, 200)
        self.assertEqual(len(body["series"]), 2)
        code, body = self.request(self.Q + "&matchers=%5B%5D")
        self.assertEqual(code, 200)
        self.assertEqual(len(body["series"]), 2)

    def test_bad_matchers_are_400_json(self):
        bad = {
            "invalid json": "matchers=%5B",
            "blank text": "matchers=",
            "explicit null": "matchers=null",
            "string not array": "matchers=%22host%22",
            "object not array": "matchers=%7B%22key%22%3A%22k%22%7D",
            "number element": "matchers=%5B1%5D",
            "missing field": "matchers=%5B%7B%22key%22%3A%22k%22%2C%22op%22%3A%22%3D%22%7D%5D",
            "extra field": "matchers=%5B%7B%22key%22%3A%22k%22%2C%22op%22%3A%22%3D%22%2C%22value%22%3A%22v%22%2C%22x%22%3A1%7D%5D",
            "empty key": "matchers=%5B%7B%22key%22%3A%22%22%2C%22op%22%3A%22%3D%22%2C%22value%22%3A%22v%22%7D%5D",
            "bad op": "matchers=%5B%7B%22key%22%3A%22k%22%2C%22op%22%3A%22%3C%22%2C%22value%22%3A%22v%22%7D%5D",
            "bad value type": "matchers=%5B%7B%22key%22%3A%22k%22%2C%22op%22%3A%22%3D%22%2C%22value%22%3A1%7D%5D",
            "bad regex": "matchers=%5B%7B%22key%22%3A%22k%22%2C%22op%22%3A%22%3D~%22%2C%22value%22%3A%22(%22%7D%5D",
        }
        for label, suffix in bad.items():
            code, body = self.request(self.Q + "&" + suffix)
            self.assertEqual(code, 400, label)
            self.assertIn("error", body, label)

    def test_validation_runs_with_no_candidate_series(self):
        import urllib.parse
        text = urllib.parse.quote(json.dumps([m("k", "=~", "(")]))
        code, body = self.request("/v1/query?tenant=acme&metric=absent&matchers=" + text)
        self.assertEqual(code, 400)
        self.assertIn("error", body)


class TestMatcherCli(StoreCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        data_dir = os.path.join(self.tmp, "cli")
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["--data-dir", data_dir] + list(argv))
        return code, out.getvalue().strip(), err.getvalue().strip()

    def test_cli_matchers_success(self):
        code, _, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "m",
            "--label", "host=api-a", "--sample", "1000:1")
        self.assertEqual(code, 0, err)
        code, _, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "m",
            "--label", "host=xapi-a", "--sample", "1000:2")
        self.assertEqual(code, 0, err)
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--matchers", json.dumps([m("host", "=~", "api-.*")]))
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out),
                         {"series": [{"labels": {"host": "api-a"},
                                      "points": [[1000, 1.0]]}]})
        # Combined with --label and a negation matcher.
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m",
            "--label", "host=api-a",
            "--matchers", json.dumps([m("host", "!=", "api-a")]))
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["series"], [])

    def test_cli_bad_matchers_fail_with_one_json_error_line(self):
        for bad in ("[", "", "null", '"host"', '{}',
                    json.dumps([1]),
                    json.dumps([{"key": "k", "op": "=", "value": 1}]),
                    json.dumps([{"key": "k", "op": "<", "value": "v"}]),
                    json.dumps([{"key": "k", "op": "=~", "value": "("}]),
                    json.dumps([{"key": "", "op": "=", "value": "v"}])):
            code, out, err = self.run_cli(
                "query", "--tenant", "acme", "--metric", "m",
                "--matchers", bad)
            self.assertEqual(code, 1, repr(bad))
            self.assertEqual(out, "", repr(bad))
            lines = err.splitlines()
            self.assertEqual(len(lines), 1, repr(bad))
            self.assertIn("error", json.loads(lines[0]), repr(bad))

    def test_cli_omitted_matchers_unchanged(self):
        code, _, err = self.run_cli(
            "write", "--tenant", "acme", "--metric", "m", "--sample", "1000:1")
        self.assertEqual(code, 0, err)
        code, out, err = self.run_cli(
            "query", "--tenant", "acme", "--metric", "m")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["series"][0]["labels"], {})


if __name__ == "__main__":
    unittest.main()
