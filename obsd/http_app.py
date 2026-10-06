"""obsd.http_app - stdlib ThreadingHTTPServer JSON API."""

from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .alerts import AlertEngine
from .tsdb import ObsError, SeriesStore, parse_matchers_text

__all__ = ["ObsdHTTPServer", "create_server", "make_handler", "run_server"]

# Role ranks and the capability each endpoint needs: a viewer reads, a writer
# also writes within its tenant scope, an admin does everything including
# cross-tenant operations.
_ROLE_RANK = {"viewer": 1, "writer": 2, "admin": 3}
_CAPABILITY_RANK = {"read": 1, "write": 2, "admin": 3}


def _int(value, field, default=None):
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ObsError("%s must be an integer" % field)
def _float(value, field):
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ObsError("%s must be a number" % field)
def _require(payload, key):
    if payload.get(key) is None:
        raise ObsError("missing field: %s" % key)
    return payload[key]
def _only(payload, keys):
    """Reject any field outside ``keys`` (a typo must not pass silently)."""
    for key in payload:
        if key not in keys:
            raise ObsError("unexpected field: %s" % key)
def _labels(payload):
    value = payload.get("labels") or {}
    if not isinstance(value, dict):
        raise ObsError("labels must be an object")
    return value
def _group_by(value):
    """``group_by`` arrives as a JSON array of label keys, or is absent."""
    if value is None or value == "":
        return None
    try:
        return json.loads(value)
    except ValueError:
        raise ObsError("group_by must be a JSON array of label keys")
def _tenant_set(value):
    return {value} if isinstance(value, str) and value else set()
def _scope_tenant(value):
    return value if isinstance(value, str) and value else None
def _batch_scope(payload):
    """Per-entry tenants of a batch write; ``set()`` when not determinable."""
    entries = payload.get("entries")
    if isinstance(entries, list) and entries and all(
            isinstance(entry, dict) for entry in entries):
        return set().union(*(_tenant_set(entry.get("tenant")) for entry in entries))
    return set()
def _resource_tenant(rows, key, row_id):
    """Tenant of one stored resource, or ``None`` when it does not exist."""
    for row in rows:
        if row.get("id") == row_id:
            return row.get("tenant")
    return None
def _request_scope(engine, method, path, params, payload):
    """Classify one request as ``(capability, tenants, audit_tenant)``.

    ``tenants`` is ``None`` when no tenant check applies (the capability alone
    decides); otherwise it is the set of tenants the principal must cover. An
    empty set means the request has no single tenant — a cross-tenant
    operation only an admin may run. ``audit_tenant`` is the single tenant to
    record, or ``None``.
    """
    if method == "GET":
        if path in ("/v1/query", "/v1/export", "/v1/consistency-token"):
            return "read", _tenant_set(params.get("tenant")), params.get("tenant")
        if path in ("/v1/rules", "/v1/notification-routes", "/v1/notifications",
                    "/v1/alerts", "/v1/slos", "/v1/slos/status"):
            return "read", _tenant_set(params.get("tenant")), params.get("tenant")
        if path == "/v1/retention/policies":
            return "read", _tenant_set(params.get("tenant")), params.get("tenant")
        if path in ("/v1/downsampling/policies", "/v1/query/downsampled"):
            return "read", _tenant_set(params.get("tenant")), params.get("tenant")
        if path in ("/v1/quotas", "/v1/stats", "/v1/audit"):
            return "admin", None, None
    elif method == "POST":
        if path == "/v1/series":
            return "write", _tenant_set(payload.get("tenant")), \
                _scope_tenant(payload.get("tenant"))
        if path == "/v1/series/batch":
            tenants = _batch_scope(payload)
            return "write", tenants, next(iter(tenants)) if len(tenants) == 1 else None
        if path == "/v1/replay":
            # Same per-entry tenant scoping as a batch write: any entry outside
            # the caller's scope rejects the whole replay with 403.
            tenants = _batch_scope(payload)
            return "write", tenants, next(iter(tenants)) if len(tenants) == 1 else None
        if path in ("/v1/rules", "/v1/notification-routes", "/v1/silences",
                    "/v1/slos"):
            return "write", _tenant_set(payload.get("tenant")), \
                _scope_tenant(payload.get("tenant"))
        if path == "/v1/retention/policies":
            return "write", _tenant_set(payload.get("tenant")), \
                _scope_tenant(payload.get("tenant"))
        if path == "/v1/retention/run":
            # With a tenant it is an ordinary tenant-scoped write; without one
            # it sweeps every configured tenant — a cross-tenant operation
            # only an admin may run (an empty scope set permits admins only).
            tenant = payload.get("tenant")
            return ("write", _tenant_set(tenant), _scope_tenant(tenant)) \
                if tenant is not None else ("write", set(), None)
        if path == "/v1/downsampling/policies":
            return "write", _tenant_set(payload.get("tenant")), \
                _scope_tenant(payload.get("tenant"))
        if path == "/v1/downsampling/run":
            # Same shape as a retention run: tenant-scoped write with a
            # tenant, admin-only cross-tenant run without one.
            tenant = payload.get("tenant")
            return ("write", _tenant_set(tenant), _scope_tenant(tenant)) \
                if tenant is not None else ("write", set(), None)
        if path in ("/v1/quotas", "/v1/inhibitions", "/v1/evaluate"):
            return "admin", None, None
        if path.startswith("/v1/notifications/") and path.endswith("/ack"):
            row_id = path[len("/v1/notifications/"):-len("/ack")]
            tenant = _resource_tenant(engine.list_notifications(), "id", row_id)
            # An unknown notification keeps the business 404; only a real one
            # is checked against the caller's scope.
            return ("write", _tenant_set(tenant), tenant) if tenant is not None \
                else ("write", None, None)
    elif method == "DELETE":
        if path.startswith("/v1/rules/"):
            tenant = _resource_tenant(engine.list_rules(), "id", path.rsplit("/", 1)[-1])
            return ("write", _tenant_set(tenant), tenant) if tenant is not None \
                else ("write", None, None)
        if path.startswith("/v1/notification-routes/"):
            tenant = _resource_tenant(engine.list_routes(), "id", path.rsplit("/", 1)[-1])
            return ("write", _tenant_set(tenant), tenant) if tenant is not None \
                else ("write", None, None)
    # Unknown paths carry no tenant scope; dispatch answers 404.
    return "read", None, None
def _permitted(principal, capability, tenants):
    """True when ``principal``'s role covers ``capability`` and ``tenants``."""
    if _ROLE_RANK.get(principal["role"], 0) < _CAPABILITY_RANK[capability]:
        return False
    if principal["role"] == "admin" or tenants is None:
        return True
    return bool(tenants) and all(tenant in principal["tenants"] for tenant in tenants)
def dispatch(store, engine, method, path, params, payload, access=None):
    """Pure routing: (status, body) or ObsError. No sockets involved."""
    labels = {key[6:]: value for key, value in params.items() if key.startswith("label.")}
    if (method, path) == ("GET", "/healthz"):
        return 200, {"ok": True}
    if (method, path) == ("GET", "/v1/consistency-token"):
        return 200, store.consistency_token(_require(params, "tenant"))
    if (method, path) == ("GET", "/v1/stats"):
        return 200, {"store": store.stats(), "alerts": len(engine.list_alerts())}
    if (method, path) == ("GET", "/v1/audit"):
        entries = [] if access is None else access.query_audit(
            tenant=params.get("tenant"), principal_id=params.get("principal_id"),
            outcome=params.get("outcome"),
            after_seq=_int(params.get("after_seq"), "after_seq"),
            limit=_int(params.get("limit"), "limit"))
        return 200, {"entries": entries}
    if (method, path) == ("POST", "/v1/quotas"):
        # Omitted limits default to unlimited; the store validates tenant and
        # limits and raises ObsError (-> 400) without touching the config.
        return 200, store.set_quota(_require(payload, "tenant"),
                                    payload.get("max_series"),
                                    payload.get("max_points"))
    if (method, path) == ("GET", "/v1/quotas"):
        return 200, store.get_quota(_require(params, "tenant"))
    if (method, path) == ("POST", "/v1/retention/policies"):
        # An omitted retention_ms means null (no cleanup); the store validates
        # the tenant and the policy and raises ObsError (-> 400) without
        # touching the persisted config.
        _only(payload, {"tenant", "retention_ms"})
        return 200, store.set_retention(_require(payload, "tenant"),
                                        payload.get("retention_ms"))
    if (method, path) == ("GET", "/v1/retention/policies"):
        return 200, store.get_retention(_require(params, "tenant"))
    if (method, path) == ("POST", "/v1/retention/run"):
        # ``dry_run``/``return_revision`` pass through uncoerced: the store
        # accepts only real booleans, just like ``now_ms`` accepts only
        # non-bool integers.
        _only(payload, {"tenant", "now_ms", "dry_run", "return_revision"})
        return 200, store.run_retention(_require(payload, "now_ms"),
                                        tenant=payload.get("tenant"),
                                        dry_run=payload.get("dry_run", False),
                                        return_revision=payload.get("return_revision", False))
    if (method, path) == ("POST", "/v1/downsampling/policies"):
        # Every validation failure of a policy is the same 400 error; fields
        # pass through uncoerced so the store judges their types.
        for key in payload:
            if key not in ("tenant", "metric", "step_ms", "aggregations"):
                raise ObsError("downsampling policy invalid")
        return 200, store.set_downsampling_policy(
            payload.get("tenant"), payload.get("metric"),
            payload.get("step_ms"), payload.get("aggregations"))
    if (method, path) == ("GET", "/v1/downsampling/policies"):
        return 200, store.get_downsampling_policy(_require(params, "tenant"),
                                                  _require(params, "metric"))
    if (method, path) == ("POST", "/v1/downsampling/run"):
        # ``now_ms``/``tenant``/``metric``/``dry_run`` pass through uncoerced:
        # the store accepts only a non-bool integer ``now_ms``, non-empty
        # string filters and a real boolean ``dry_run``.
        for key in payload:
            if key not in ("now_ms", "tenant", "metric", "dry_run"):
                raise ObsError("downsampling run invalid")
        return 200, store.run_downsampling(payload.get("now_ms"),
                                           tenant=payload.get("tenant"),
                                           metric=payload.get("metric"),
                                           dry_run=payload.get("dry_run", False))
    if (method, path) == ("GET", "/v1/query/downsampled"):
        # The step comes from the policy; sliding windows and grouping are
        # rejected rather than silently ignored.
        if params.get("window") not in (None, "") \
                or params.get("group_by") not in (None, ""):
            raise ObsError("downsampled query unsupported")
        rows = store.query_downsampled(
            _require(params, "tenant"), _require(params, "metric"), labels=labels,
            start_ms=_int(params.get("start"), "start"),
            end_ms=_int(params.get("end"), "end"),
            agg=params.get("agg") or None,
            matchers=parse_matchers_text(params.get("matchers")),
            read_token=params.get("read_token"))
        return 200, {"series": [{"labels": row["labels"], "points": row["points"]}
                                for row in rows]}
    if (method, path) == ("POST", "/v1/series"):
        # ``now_ms``, ``overwrite`` and ``return_revision`` pass through
        # uncoerced: the store accepts only None or a non-bool integer for
        # ``now_ms`` and only real booleans for the flags (omitted defaults to
        # False; an explicit null is rejected, not treated as False).
        result = store.write(_require(payload, "tenant"), _require(payload, "metric"),
                             _labels(payload), _require(payload, "samples"),
                             now=payload.get("now_ms"),
                             overwrite=payload.get("overwrite", False),
                             return_revision=payload.get("return_revision", False))
        body = {"written": result["written"], "duplicates": result["duplicates"],
                "series_id": result["series_id"]}
        if "revision" in result:
            body["revision"] = result["revision"]
        return 202, body
    if (method, path) == ("POST", "/v1/series/batch"):
        # ``overwrite``/``return_revision`` are passed through uncoerced: the
        # store accepts only real booleans, just like ``now_ms`` accepts only
        # non-bool integers.
        result = store.write_batch(_require(payload, "entries"),
                                   now=payload.get("now_ms"),
                                   overwrite=payload.get("overwrite", False),
                                   return_revision=payload.get("return_revision", False))
        body = {"written": result["written"], "duplicates": result["duplicates"],
                "results": result["results"]}
        if "revision" in result:
            body["revision"] = result["revision"]
        return 202, body
    if (method, path) == ("POST", "/v1/replay"):
        # The snapshot travels in the body together with the replay options;
        # ``overwrite``/``dry_run``/``return_revision`` pass through uncoerced
        # (booleans only).
        snapshot = {key: payload[key]
                    for key in ("version", "snapshot_id", "entries") if key in payload}
        result = store.replay_snapshot(snapshot, now_ms=payload.get("now_ms"),
                                       overwrite=payload.get("overwrite", False),
                                       dry_run=payload.get("dry_run", False),
                                       return_revision=payload.get("return_revision", False))
        return (202 if result["applied"] else 200), result
    if (method, path) == ("GET", "/v1/query"):
        rows = store.query(_require(params, "tenant"), _require(params, "metric"), labels=labels,
                           start_ms=_int(params.get("start"), "start"),
                           end_ms=_int(params.get("end"), "end"),
                           step_ms=_int(params.get("step"), "step"),
                           agg=params.get("agg") or None,
                           group_by=_group_by(params.get("group_by")),
                           window_ms=_int(params.get("window"), "window"),
                           matchers=parse_matchers_text(params.get("matchers")),
                           read_token=params.get("read_token"))
        return 200, {"series": [{"labels": row["labels"], "points": row["points"]}
                                for row in rows]}
    if (method, path) == ("GET", "/v1/export"):
        return 200, store.export_snapshot(
            _require(params, "tenant"), _require(params, "metric"), labels=labels,
            start_ms=_int(params.get("start"), "start"),
            end_ms=_int(params.get("end"), "end"),
            matchers=parse_matchers_text(params.get("matchers")),
            read_token=params.get("read_token"))
    if (method, path) == ("POST", "/v1/rules"):
        return 201, engine.add_rule(payload)
    if (method, path) == ("GET", "/v1/rules"):
        return 200, {"rules": engine.list_rules(params.get("tenant"))}
    if (method, path) == ("POST", "/v1/notification-routes"):
        return 201, engine.add_route(payload)
    if (method, path) == ("GET", "/v1/notification-routes"):
        return 200, {"routes": engine.list_routes(params.get("tenant"))}
    if (method, path) == ("GET", "/v1/notifications"):
        acked = params.get("acked")
        if acked is not None:
            if acked not in ("true", "false"):
                raise ObsError("acked must be true or false")
            acked = acked == "true"
        return 200, {"notifications": engine.list_notifications(
            tenant=params.get("tenant"), route_id=params.get("route_id"),
            alert_id=params.get("alert_id"), acked=acked)}
    if method == "POST" and path.startswith("/v1/notifications/") \
            and path.endswith("/ack"):
        return 200, engine.ack_notification(
            path[len("/v1/notifications/"):-len("/ack")])
    if (method, path) == ("POST", "/v1/evaluate"):
        return 200, engine.evaluate(_int(_require(payload, "now_ms"), "now_ms"))
    if (method, path) == ("GET", "/v1/alerts"):
        return 200, {"alerts": engine.list_alerts(params.get("tenant"), params.get("state"))}
    if (method, path) == ("POST", "/v1/silences"):
        return 201, engine.add_silence(_require(payload, "tenant"), _labels(payload),
                                       _require(payload, "starts_ms"),
                                       _require(payload, "ends_ms"), payload.get("reason", ""))
    if (method, path) == ("POST", "/v1/inhibitions"):
        # ``same_labels`` passes through uncoerced: the engine accepts only
        # real booleans (omitted defaults to True; an explicit null is
        # rejected, not treated as True).
        return 201, engine.add_inhibition(_require(payload, "source_severity"),
                                          _require(payload, "target_severity"),
                                          payload.get("same_labels", True))
    if (method, path) == ("POST", "/v1/slos"):
        return 201, engine.set_slo(_require(payload, "tenant"), _require(payload, "name"),
                                   _require(payload, "metric"), _labels(payload),
                                   _require(payload, "good_comparator"),
                                   _float(_require(payload, "threshold"), "threshold"),
                                   _float(_require(payload, "target_ratio"), "target_ratio"),
                                   _require(payload, "window_ms"))
    if (method, path) == ("GET", "/v1/slos"):
        return 200, {"slos": engine.list_slos(params.get("tenant"))}
    if (method, path) == ("GET", "/v1/slos/status"):
        now = _int(params.get("now_ms"), "now_ms", int(time.time() * 1000))
        return 200, engine.slo_status(_require(params, "name"), now,
                                      tenant=params.get("tenant"),
                                      read_token=params.get("read_token"))
    raise ObsError("not found: %s %s" % (method, path))
def _status_for(message):
    if message.startswith("not found"):
        return 404
    if message == "downsampling policy unavailable":
        return 404
    # Unknown rules/alerts/SLOs/comparators are missing resources (404); an
    # unknown aggregation is a malformed request (400).
    if message.startswith("unknown") and not message.startswith("unknown aggregation"):
        return 404
    return 409 if message.startswith(("conflict", "quota exceeded",
                                      "read revision unavailable")) else 400
def make_handler(store, engine, access=None):
    class ObsdHandler(BaseHTTPRequestHandler):
        server_version = "obsd/0.1"
        sys_version = ""
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            """Keep stdout clean: every line printed is a JSON payload."""
        def _read_json(self):
            length = _int(self.headers.get("Content-Length"), "Content-Length", 0) or 0
            if not length:
                return {}
            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise ObsError("invalid JSON body")
            if not isinstance(payload, dict):
                raise ObsError("JSON body must be an object")
            return payload
        def _send(self, status, body):
            data = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        def _bearer_token(self):
            """The token of ``Authorization: Bearer <token>``, else ``None``."""
            header = self.headers.get("Authorization")
            if not header:
                return None
            parts = header.split()
            if len(parts) != 2 or parts[0] != "Bearer":
                return None
            return parts[1]
        def _handle(self, method, call):
            parsed = urlparse(self.path)
            params = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
            # Blank values are dropped like every other parameter, except an
            # explicit empty matchers is an illegal value rather than omission.
            blank_params = parse_qs(parsed.query, keep_blank_values=True)
            if "matchers" in blank_params:
                params["matchers"] = blank_params["matchers"][-1]
            # /healthz is always public and never audited. While no access
            # config exists every request stays anonymous and unaudited.
            guarded = access is not None and parsed.path != "/healthz"
            if guarded:
                access.refresh()
                guarded = access.enabled()
            principal = None
            audit_tenant = params.get("tenant")
            if guarded:
                principal = access.authenticate(self._bearer_token())
                if principal is None:
                    self._send(401, {"error": "unauthorized"})
                    access.record(None, method, parsed.path, audit_tenant,
                                  "denied", 401)
                    return
            try:
                payload = self._read_json() if method == "POST" else {}
            except ObsError as exc:
                status = _status_for(str(exc))
                self._send(status, {"error": str(exc)})
                if guarded:
                    access.record(principal["id"], method, parsed.path,
                                  audit_tenant, "failed", status)
                return
            if guarded:
                capability, tenants, audit_tenant = _request_scope(
                    engine, method, parsed.path, params, payload)
                if not _permitted(principal, capability, tenants):
                    self._send(403, {"error": "forbidden"})
                    access.record(principal["id"], method, parsed.path,
                                  audit_tenant, "denied", 403)
                    return
            try:
                status, body = call(parsed.path, params, payload)
            except ObsError as exc:
                status, body = _status_for(str(exc)), {"error": str(exc)}
            except Exception as exc:  # pragma: no cover - defensive
                status, body = 500, {"error": "internal error: %s" % exc}
            self._send(status, body)
            if guarded:
                access.record(principal["id"], method, parsed.path, audit_tenant,
                              "allowed" if status < 400 else "failed", status)
        def do_GET(self):
            self._handle("GET", lambda path, params, payload:
                         dispatch(store, engine, "GET", path, params, payload,
                                  access=access))
        def do_POST(self):
            self._handle("POST", lambda path, params, payload:
                         dispatch(store, engine, "POST", path, params, payload,
                                  access=access))
        def do_DELETE(self):
            def call(path, params, payload):
                if path.startswith("/v1/rules/"):
                    return 200, engine.del_rule(path.rsplit("/", 1)[-1])
                if path.startswith("/v1/notification-routes/"):
                    return 200, engine.del_route(path.rsplit("/", 1)[-1])
                raise ObsError("not found: DELETE %s" % path)
            self._handle("DELETE", call)
    return ObsdHandler
class ObsdHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, store, engine, access=None):
        self.store = store
        self.engine = engine
        self.access = access
        super().__init__(address, make_handler(store, engine, access))
def create_server(store, engine, host="127.0.0.1", port=8080, access=None):
    """Build (but do not start) a ThreadingHTTPServer bound to ``host:port``."""
    if not isinstance(store, SeriesStore):
        raise ObsError("store must be a SeriesStore")
    if not isinstance(engine, AlertEngine):
        raise ObsError("engine must be an AlertEngine")
    return ObsdHTTPServer((host, int(port)), store, engine, access)
def run_server(store, engine, host="127.0.0.1", port=8080, access=None):
    """Serve until interrupted; prints one JSON startup line on stdout."""
    server = create_server(store, engine, host, port, access=access)
    print(json.dumps({"ok": True, "listening": "http://%s:%d" % server.server_address[:2],
                      "data_dir": store.root}, sort_keys=True, separators=(",", ":")), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
