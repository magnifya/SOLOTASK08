"""obsd.cli - single-line JSON command line interface."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .access import AccessControl
from .alerts import AlertEngine
from .http_app import run_server
from .tsdb import ObsError, SeriesStore, parse_matchers_text

DEFAULT_DATA_DIR = "./obsd_data"
AGGS = ["sum", "avg", "min", "max", "count"]
# Sliding-window counter aggregates are query-only; rule-add keeps AGGS.
QUERY_AGGS = AGGS + ["increase", "rate"]
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

    retention_set = sub.add_parser("retention-set",
                                   help="set a tenant's retention policy")
    retention_set.add_argument("--tenant", required=True)
    retention_set.add_argument("--retention-ms", type=int, default=None,
                               help="keep samples with timestamp_ms >= now-retention_ms "
                                    "(omit for no cleanup)")

    retention_get = sub.add_parser("retention-get",
                                   help="show a tenant's retention policy and usage")
    retention_get.add_argument("--tenant", required=True)

    retention_run = sub.add_parser("retention-run",
                                   help="run retention for one tenant or every "
                                        "configured tenant")
    retention_run.add_argument("--tenant", default=None,
                               help="process only this tenant (default: all tenants "
                                    "with a configured policy, in lexicographic order)")
    retention_run.add_argument("--now-ms", type=int, required=True)
    retention_run.add_argument("--dry-run", action="store_true",
                               help="report only; nothing is deleted or compacted")
    retention_run.add_argument("--return-revision", action="store_true",
                               help="include the commit revision in the result")

    ds_set = sub.add_parser("downsampling-set",
                            help="create or replace a tenant/metric "
                                 "downsampling policy")
    ds_set.add_argument("--tenant", required=True)
    ds_set.add_argument("--metric", required=True)
    ds_set.add_argument("--step-ms", type=int, required=True,
                        help="fixed bucket resolution in ms (positive integer)")
    ds_set.add_argument("--agg", action="append", required=True, choices=AGGS,
                        help="declare an aggregation; repeat for several")

    ds_get = sub.add_parser("downsampling-get",
                            help="show a tenant/metric downsampling policy")
    ds_get.add_argument("--tenant", required=True)
    ds_get.add_argument("--metric", required=True)

    ds_run = sub.add_parser("downsampling-run",
                            help="(re)compute persisted downsampled buckets "
                                 "for one policy or every configured policy")
    ds_run.add_argument("--tenant", default=None,
                        help="process only this tenant's policies")
    ds_run.add_argument("--metric", default=None,
                        help="process only policies for this metric")
    ds_run.add_argument("--now-ms", type=int, required=True,
                        help="process raw samples up to and including this ms")
    ds_run.add_argument("--dry-run", action="store_true",
                        help="report only; nothing is stored and the revision "
                             "does not move")
    ds_run.add_argument("--return-revision", action="store_true",
                        help="include the commit revision in the result")

    ds_query = _common(sub.add_parser("downsampled-query",
                                      help="query persisted downsampled buckets "
                                           "(step fixed by the policy)"))
    ds_query.add_argument("--start", type=int, default=None)
    ds_query.add_argument("--end", type=int, default=None)
    ds_query.add_argument("--agg", choices=QUERY_AGGS, default=None)
    ds_query.add_argument("--matchers", default=None, metavar="JSON_ARRAY",
                          help='JSON array of {"key","op","value"} matchers; '
                               'op is one of =, !=, =~, !~ (AND with --label)')
    ds_query.add_argument("--read-token", default=None,
                          help="consistency token the read must catch up to")

    consistency_token = sub.add_parser("consistency-token",
                                       help="mint a read-consistency token for a tenant")
    consistency_token.add_argument("--tenant", required=True)

    write = _common(sub.add_parser("write", help="write samples into a series"))
    write.add_argument("--sample", action="append", default=[], metavar="TS:VALUE")
    write.add_argument("--now-ms", type=int, default=None)
    write.add_argument("--overwrite", action="store_true")
    write.add_argument("--return-revision", action="store_true",
                       help="include the commit revision in the result")

    write_batch = sub.add_parser("write-batch",
                                 help="atomically write many series from a JSON array")
    write_batch.add_argument("--entries", required=True, metavar="JSON_ARRAY",
                             help='JSON array of {"tenant","metric","samples",'
                                  '"labels"?} entry objects')
    write_batch.add_argument("--now-ms", type=int, default=None)
    write_batch.add_argument("--overwrite", action="store_true")
    write_batch.add_argument("--return-revision", action="store_true",
                             help="include the commit revision in the result")

    query = _common(sub.add_parser("query", help="range query with optional bucketing"))
    query.add_argument("--start", type=int, default=None)
    query.add_argument("--end", type=int, default=None)
    query.add_argument("--step", type=int, default=None)
    query.add_argument("--window-ms", type=int, default=None,
                       help="sliding-window length in ms; requires --start/--end/--step/--agg")
    query.add_argument("--agg", choices=QUERY_AGGS, default=None)
    query.add_argument("--group-by", default=None, metavar="JSON_ARRAY",
                       help="JSON array of label keys for cross-series grouping "
                            "(requires --agg; [] merges all matching series)")
    query.add_argument("--matchers", default=None, metavar="JSON_ARRAY",
                       help='JSON array of {"key","op","value"} matchers; '
                            'op is one of =, !=, =~, !~ (AND with --label)')
    query.add_argument("--read-token", default=None,
                       help="consistency token the read must catch up to")

    export = _common(sub.add_parser("export", help="export a verifiable snapshot "
                                                   "of raw samples"))
    export.add_argument("--start", type=int, default=None)
    export.add_argument("--end", type=int, default=None)
    export.add_argument("--matchers", default=None, metavar="JSON_ARRAY",
                        help='JSON array of {"key","op","value"} matchers; '
                             'op is one of =, !=, =~, !~ (AND with --label)')
    export.add_argument("--read-token", default=None,
                        help="consistency token the read must catch up to")

    label_names = sub.add_parser("label-names",
                                 help="list distinct label names of the "
                                      "registered series of one tenant/metric")
    label_names.add_argument("--tenant", default=None)
    label_names.add_argument("--metric", default=None)
    label_names.add_argument("--label", action="append", default=[],
                             help="label matcher key=value")
    label_names.add_argument("--matchers", default=None, metavar="JSON_ARRAY",
                             help='JSON array of {"key","op","value"} matchers; '
                                  'op is one of =, !=, =~, !~ (AND with --label)')
    label_names.add_argument("--read-token", default=None,
                             help="consistency token the read must catch up to")

    label_values = sub.add_parser("label-values",
                                  help="list distinct values of one label of "
                                       "the registered series of one tenant/metric")
    label_values.add_argument("--tenant", default=None)
    label_values.add_argument("--metric", default=None)
    label_values.add_argument("--name", default=None,
                              help="label name; must be non-empty")
    label_values.add_argument("--label", action="append", default=[],
                              help="label matcher key=value")
    label_values.add_argument("--matchers", default=None, metavar="JSON_ARRAY",
                              help='JSON array of {"key","op","value"} matchers; '
                                   'op is one of =, !=, =~, !~ (AND with --label)')
    label_values.add_argument("--read-token", default=None,
                              help="consistency token the read must catch up to")

    replay = sub.add_parser("replay", help="replay an exported snapshot into "
                                           "this store")
    replay.add_argument("--snapshot", required=True, metavar="JSON",
                        help="snapshot object as printed by the export command")
    replay.add_argument("--now-ms", type=int, default=None)
    replay.add_argument("--overwrite", action="store_true")
    replay.add_argument("--dry-run", action="store_true",
                        help="validate and count only; nothing is applied")
    replay.add_argument("--return-revision", action="store_true",
                        help="include the commit revision in the result")

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

    route = sub.add_parser("route-add", help="add a notification route")
    route.add_argument("--tenant", required=True)
    route.add_argument("--target", required=True)
    route.add_argument("--id", default=None)
    route.add_argument("--label", action="append", default=[],
                       help="exact label matcher key=value (subset match)")
    route.add_argument("--severity", action="append", default=None, choices=SEVERITIES,
                       help="limit to these severities (default: all)")
    route.add_argument("--event", action="append", default=None,
                       choices=["firing", "resolved"],
                       help="limit to these events (default: firing and resolved)")
    route.add_argument("--repeat-ms", type=int, default=None,
                       help="repeat a firing notification after this many ms "
                            "(default: no repeat)")

    route_list = sub.add_parser("route-list", help="list notification routes")
    route_list.add_argument("--tenant", default=None)

    notifications = sub.add_parser("notification-list", help="list queued notifications")
    notifications.add_argument("--tenant", default=None)
    notifications.add_argument("--route-id", default=None)
    notifications.add_argument("--alert-id", default=None)
    notifications.add_argument("--acked", default=None, choices=["true", "false"])

    ack = sub.add_parser("notification-ack", help="acknowledge a notification")
    ack.add_argument("--id", required=True)

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
    slo_status.add_argument("--read-token", default=None,
                            help="consistency token the read must catch up to")

    principal_create = sub.add_parser("principal-create",
                                      help="create an access principal (token is "
                                           "stored only as its SHA-256 digest)")
    principal_create.add_argument("--id", required=True)
    principal_create.add_argument("--token", required=True)
    principal_create.add_argument("--role", required=True,
                                  choices=["viewer", "writer", "admin"])
    principal_create.add_argument("--tenant", action="append", required=True,
                                  help="tenant scope; repeat for several")

    sub.add_parser("principal-list", help="list access principals")

    principal_revoke = sub.add_parser("principal-revoke",
                                      help="revoke an access principal")
    principal_revoke.add_argument("--id", required=True)
    return parser
def _run(args, store, engine, access):
    if args.command == "serve":
        return run_server(store, engine, args.host, args.port, access=access)
    if args.command == "principal-create":
        _emit(access.create_principal(args.id, args.token, args.role, args.tenant))
    elif args.command == "principal-list":
        _emit({"principals": access.list_principals()})
    elif args.command == "principal-revoke":
        _emit(access.revoke_principal(args.id))
    elif args.command == "quota-set":
        _emit(store.set_quota(args.tenant, args.max_series, args.max_points))
    elif args.command == "quota-get":
        _emit(store.get_quota(args.tenant))
    elif args.command == "retention-set":
        _emit(store.set_retention(args.tenant, args.retention_ms))
    elif args.command == "retention-get":
        _emit(store.get_retention(args.tenant))
    elif args.command == "retention-run":
        _emit(store.run_retention(args.now_ms, tenant=args.tenant,
                                  dry_run=args.dry_run,
                                  return_revision=args.return_revision))
    elif args.command == "downsampling-set":
        _emit(store.set_downsampling_policy(args.tenant, args.metric,
                                            args.step_ms, args.agg))
    elif args.command == "downsampling-get":
        _emit(store.get_downsampling_policy(args.tenant, args.metric))
    elif args.command == "downsampling-run":
        _emit(store.run_downsampling(args.now_ms, tenant=args.tenant,
                                     metric=args.metric, dry_run=args.dry_run,
                                     return_revision=args.return_revision))
    elif args.command == "downsampled-query":
        matchers = parse_matchers_text(args.matchers, "--matchers")
        rows = store.query_downsampled(args.tenant, args.metric,
                                       labels=_labels(args.label),
                                       start_ms=args.start, end_ms=args.end,
                                       agg=args.agg, matchers=matchers,
                                       read_token=args.read_token)
        _emit({"series": [{"labels": row["labels"], "points": row["points"]}
                          for row in rows]})
    elif args.command == "consistency-token":
        _emit(store.consistency_token(args.tenant))
    elif args.command == "write":
        _emit(store.write(args.tenant, args.metric, _labels(args.label),
                          _samples(args.sample), now=args.now_ms,
                          overwrite=args.overwrite,
                          return_revision=args.return_revision))
    elif args.command == "write-batch":
        try:
            entries = json.loads(args.entries)
        except ValueError:
            raise ObsError("--entries must be a JSON array of entry objects")
        _emit(store.write_batch(entries, now=args.now_ms, overwrite=args.overwrite,
                                return_revision=args.return_revision))
    elif args.command == "query":
        group_by = None
        if args.group_by is not None:
            try:
                group_by = json.loads(args.group_by)
            except ValueError:
                raise ObsError("--group-by must be a JSON array of label keys")
        matchers = parse_matchers_text(args.matchers, "--matchers")
        rows = store.query(args.tenant, args.metric, labels=_labels(args.label),
                           start_ms=args.start, end_ms=args.end, step_ms=args.step,
                           agg=args.agg, group_by=group_by, window_ms=args.window_ms,
                           matchers=matchers, read_token=args.read_token)
        _emit({"series": [{"labels": row["labels"], "points": row["points"]} for row in rows]})
    elif args.command == "export":
        matchers = parse_matchers_text(args.matchers, "--matchers")
        _emit(store.export_snapshot(args.tenant, args.metric,
                                    labels=_labels(args.label),
                                    start_ms=args.start, end_ms=args.end,
                                    matchers=matchers, read_token=args.read_token))
    elif args.command in ("label-names", "label-values"):
        # Missing/empty tenant, metric and name, and malformed matchers all
        # surface as the same one-line "label metadata invalid" error as the
        # HTTP 400; read-token failures keep their own wording.
        try:
            matchers = parse_matchers_text(args.matchers, "--matchers")
            if args.command == "label-names":
                body = store.label_names(args.tenant, args.metric,
                                         labels=_labels(args.label),
                                         matchers=matchers,
                                         read_token=args.read_token)
            else:
                body = store.label_values(args.tenant, args.metric, args.name,
                                          labels=_labels(args.label),
                                          matchers=matchers,
                                          read_token=args.read_token)
        except ObsError as exc:
            message = str(exc)
            if message not in ("invalid read token", "read revision unavailable"):
                raise ObsError("label metadata invalid")
            raise
        _emit(body)
    elif args.command == "replay":
        try:
            snapshot = json.loads(args.snapshot)
        except ValueError:
            raise ObsError("--snapshot must be a JSON object as produced by export")
        _emit(store.replay_snapshot(snapshot, now_ms=args.now_ms,
                                    overwrite=args.overwrite,
                                    dry_run=args.dry_run,
                                    return_revision=args.return_revision))
    elif args.command == "rule-add":
        _emit(engine.add_rule({
            "id": args.id, "tenant": args.tenant, "metric": args.metric,
            "labels": _labels(args.label), "comparator": args.comparator,
            "threshold": args.threshold, "for_ms": args.for_ms, "window_ms": args.window_ms,
            "agg": args.agg, "severity": args.severity,
            "annotations": _labels(args.annotation)}))
    elif args.command == "eval":
        _emit(engine.evaluate(args.now_ms))
    elif args.command == "route-add":
        _emit(engine.add_route({
            "id": args.id, "tenant": args.tenant, "target": args.target,
            "labels": _labels(args.label), "severities": args.severity,
            "events": args.event, "repeat_ms": args.repeat_ms}))
    elif args.command == "route-list":
        _emit({"routes": engine.list_routes(args.tenant)})
    elif args.command == "notification-list":
        _emit({"notifications": engine.list_notifications(
            tenant=args.tenant, route_id=args.route_id, alert_id=args.alert_id,
            acked=None if args.acked is None else args.acked == "true")})
    elif args.command == "notification-ack":
        _emit(engine.ack_notification(args.id))
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
        _emit(engine.slo_status(args.name, args.now_ms, tenant=args.tenant,
                                read_token=args.read_token))
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
        return _run(args, store, AlertEngine(store, data_dir), AccessControl(data_dir))
    except ObsError as exc:
        return _fail(exc)
    except (OSError, ValueError) as exc:
        return _fail("%s: %s" % (type(exc).__name__, exc))
if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
