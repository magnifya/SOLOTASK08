"""obsd.cli - single-line JSON command line interface."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .alerts import AlertEngine
from .http_app import run_server
from .tsdb import ObsError, SeriesStore

DEFAULT_DATA_DIR = "./obsd_data"
AGGS = ["sum", "avg", "min", "max", "count"]
COMPS = [">", ">=", "<", "<=", "==", "!="]
SEVERITIES = ["info", "warning", "critical"]


def _emit(payload):
    sys.stdout.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    sys.stdout.flush()
def _fail(message):
    sys.stderr.write(json.dumps({"error": str(message)}, sort_keys=True,
                                separators=(",", ":")) + "\n")
    sys.stderr.flush()
    return 1
def _labels(pairs):
    out = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise ObsError("label %r must look like key=value" % pair)
        key, value = pair.split("=", 1)
        out[key] = value
    return out
def _samples(entries):
    out = []
    for entry in entries or []:
        stamp, _, value = entry.partition(":")
        try:
            out.append([int(stamp), float(value)])
        except ValueError:
            raise ObsError("sample %r must look like timestamp_ms:value" % entry)
    if not out:
        raise ObsError("at least one --sample timestamp_ms:value is required")
    return out
def _common(parser, label_help="label matcher key=value"):
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--metric", required=True)
    parser.add_argument("--label", action="append", default=[], help=label_help)
    return parser
def _build_parser():
    parser = argparse.ArgumentParser(prog="obsd",
                                     description="metrics ingestion, alerting and SLO backend")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR,
                        help="state directory (default ./obsd_data)")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)

    quota_set = sub.add_parser("quota-set", help="set per-tenant write limits")
    quota_set.add_argument("--tenant", required=True)
    quota_set.add_argument("--max-series", type=int, default=None,
                           help="max registered series (omit for unlimited)")
    quota_set.add_argument("--max-points", type=int, default=None,
                           help="max distinct points across the tenant (omit for unlimited)")

    quota_get = sub.add_parser("quota-get", help="show a tenant's limits and usage")
    quota_get.add_argument("--tenant", required=True)

    write = _common(sub.add_parser("write", help="write samples into a series"))
    write.add_argument("--sample", action="append", default=[], metavar="TS:VALUE")
    write.add_argument("--now-ms", type=int, default=None)
    write.add_argument("--overwrite", action="store_true")

    query = _common(sub.add_parser("query", help="range query with optional bucketing"))
    query.add_argument("--start", type=int, default=None)
    query.add_argument("--end", type=int, default=None)
    query.add_argument("--step", type=int, default=None)
    query.add_argument("--agg", choices=AGGS, default=None)
    query.add_argument("--group-by", default=None,
                       help="JSON array of label keys to group series by (requires --agg)")

    rule = _common(sub.add_parser("rule-add", help="add an alert rule"))
    rule.add_argument("--id", default=None)
    rule.add_argument("--comparator", required=True, choices=COMPS)
    rule.add_argument("--threshold", type=float, required=True)
    rule.add_argument("--for-ms", type=int, default=0)
    rule.add_argument("--window-ms", type=int, required=True)
    rule.add_argument("--agg", required=True, choices=AGGS)
    rule.add_argument("--severity", required=True, choices=SEVERITIES)
    rule.add_argument("--annotation", action="append", default=[])

    evaluate = sub.add_parser("eval", help="evaluate all rules at a timestamp")
    evaluate.add_argument("--now-ms", type=int, required=True)

    alerts = sub.add_parser("alerts", help="list alerts")
    alerts.add_argument("--tenant", default=None)
    alerts.add_argument("--state", default=None)

    silence = sub.add_parser("silence-add", help="add a silence window")
    silence.add_argument("--tenant", required=True)
    silence.add_argument("--label", action="append", default=[])
    silence.add_argument("--starts-ms", type=int, required=True)
    silence.add_argument("--ends-ms", type=int, required=True)
    silence.add_argument("--reason", default="")

    slo = sub.add_parser("slo", help="SLO management")
    slo_sub = slo.add_subparsers(dest="slo_command", required=True)
    slo_set = _common(slo_sub.add_parser("set"))
    slo_set.add_argument("--name", required=True)
    slo_set.add_argument("--good-comparator", required=True, choices=COMPS)
    slo_set.add_argument("--threshold", type=float, required=True)
    slo_set.add_argument("--target-ratio", type=float, required=True)
    slo_set.add_argument("--window-ms", type=int, required=True)
    slo_status = slo_sub.add_parser("status")
    slo_status.add_argument("--name", required=True)
    slo_status.add_argument("--tenant", default=None)
    slo_status.add_argument("--now-ms", type=int, required=True)
    return parser
def _run(args, store, engine):
    if args.command == "serve":
        return run_server(store, engine, args.host, args.port)
    if args.command == "quota-set":
        _emit(store.set_quota(args.tenant, args.max_series, args.max_points))
    elif args.command == "quota-get":
        _emit(store.get_quota(args.tenant))
    elif args.command == "write":
        _emit(store.write(args.tenant, args.metric, _labels(args.label),
                          _samples(args.sample), now=args.now_ms, overwrite=args.overwrite))
    elif args.command == "query":
        group_by = None
        if args.group_by is not None:
            try:
                group_by = json.loads(args.group_by)
            except ValueError:
                raise ObsError("group_by must be a JSON array of label keys")
        rows = store.query(args.tenant, args.metric, labels=_labels(args.label),
                           start_ms=args.start, end_ms=args.end, step_ms=args.step,
                           agg=args.agg, group_by=group_by)
        _emit({"series": [{"labels": row["labels"], "points": row["points"]} for row in rows]})
    elif args.command == "rule-add":
        _emit(engine.add_rule({
            "id": args.id, "tenant": args.tenant, "metric": args.metric,
            "labels": _labels(args.label), "comparator": args.comparator,
            "threshold": args.threshold, "for_ms": args.for_ms, "window_ms": args.window_ms,
            "agg": args.agg, "severity": args.severity,
            "annotations": _labels(args.annotation)}))
    elif args.command == "eval":
        _emit(engine.evaluate(args.now_ms))
    elif args.command == "alerts":
        _emit({"alerts": engine.list_alerts(args.tenant, args.state)})
    elif args.command == "silence-add":
        _emit(engine.add_silence(args.tenant, _labels(args.label), args.starts_ms,
                                 args.ends_ms, args.reason))
    elif args.command == "slo" and args.slo_command == "set":
        _emit(engine.set_slo(args.tenant, args.name, args.metric, _labels(args.label),
                             args.good_comparator, args.threshold, args.target_ratio,
                             args.window_ms))
    elif args.command == "slo":
        _emit(engine.slo_status(args.name, args.now_ms, tenant=args.tenant))
    else:
        return _fail("unknown command: %s" % args.command)
    return 0
def main(argv=None):
    """Dispatch one CLI invocation; 0 on success, non-zero + JSON error otherwise."""
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return _fail("invalid arguments (exit %s)" % exc.code)
    try:
        data_dir = os.path.abspath(args.data_dir)
        store = SeriesStore(data_dir)
        return _run(args, store, AlertEngine(store, data_dir))
    except ObsError as exc:
        return _fail(exc)
    except (OSError, ValueError) as exc:
        return _fail("%s: %s" % (type(exc).__name__, exc))
if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
