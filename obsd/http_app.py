"""obsd.http_app - stdlib ThreadingHTTPServer JSON API."""

from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .access import AccessControl, AuditLog, Forbidden, Unauthorized, \
    authorize, request_tenant
from .alerts import AlertEngine
from .tsdb import ObsError, SeriesStore, parse_matchers_text

__all__ = ["ObsdHTTPServer", "create_server", "make_handler", "run_server"]


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
def dispatch(store, engine, method, path, params, payload, audit=None):
    """Pure routing: (status, body) or ObsError. No sockets involved."""
    labels = {key[6:]: value for key, value in params.items() if key.startswith("label.")}
    if (method, path) == ("GET", "/healthz"):
        return 200, {"ok": True}
    if (method, path) == ("GET", "/v1/audit"):
        if audit is None:
            raise ObsError("not found: %s %s" % (method, path))
        return 200, {"entries": audit.query(
            tenant=params.get("tenant") or None,
            principal_id=params.get("principal_id") or None,
            outcome=params.get("outcome") or None,
            after_seq=_int(params.get("after_seq"), "after_seq"),
            limit=_int(params.get("limit"), "limit"))}
    if (method, path) == ("GET", "/v1/stats"):
        return 200, {"store": store.stats(), "alerts": len(engine.list_alerts())}
    if (method, path) == ("POST", "/v1/quotas"):
        # Omitted limits default to unlimited; the store validates tenant and
        # limits and raises ObsError (-> 400) without touching the config.
        return 200, store.set_quota(_require(payload, "tenant"),
                                    payload.get("max_series"),
                                    payload.get("max_points"))
    if (method, path) == ("GET", "/v1/quotas"):
        return 200, store.get_quota(_require(params, "tenant"))
    if (method, path) == ("POST", "/v1/series"):
        result = store.write(_require(payload, "tenant"), _require(payload, "metric"),
                             _labels(payload), _require(payload, "samples"),
                             now=payload.get("now_ms"),
                             overwrite=bool(payload.get("overwrite", False)))
        return 202, {"written": result["written"], "duplicates": result["duplicates"],
                     "series_id": result["series_id"]}
    if (method, path) == ("POST", "/v1/series/batch"):
        # ``overwrite`` is passed through uncoerced: the store accepts only
        # real booleans, just like ``now_ms`` accepts only non-bool integers.
        result = store.write_batch(_require(payload, "entries"),
                                   now=payload.get("now_ms"),
                                   overwrite=payload.get("overwrite", False))
        return 202, {"written": result["written"], "duplicates": result["duplicates"],
                     "results": result["results"]}
    if (method, path) == ("GET", "/v1/query"):
        rows = store.query(_require(params, "tenant"), _require(params, "metric"), labels=labels,
                           start_ms=_int(params.get("start"), "start"),
                           end_ms=_int(params.get("end"), "end"),
                           step_ms=_int(params.get("step"), "step"),
                           agg=params.get("agg") or None,
                           group_by=_group_by(params.get("group_by")),
                           window_ms=_int(params.get("window"), "window"),
                           matchers=parse_matchers_text(params.get("matchers")))
        return 200, {"series": [{"labels": row["labels"], "points": row["points"]}
                                for row in rows]}
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
        return 201, engine.add_inhibition(_require(payload, "source_severity"),
                                          _require(payload, "target_severity"),
                                          bool(payload.get("same_labels", True)))
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
        return 200, engine.slo_status(_require(params, "name"), now, tenant=params.get("tenant"))
    raise ObsError("not found: %s %s" % (method, path))
def _status_for(message):
    if message.startswith("not found"):
        return 404
    # Unknown rules/alerts/SLOs/comparators are missing resources (404); an
    # unknown aggregation is a malformed request (400).
    if message.startswith("unknown") and not message.startswith("unknown aggregation"):
        return 404
    return 409 if message.startswith(("conflict", "quota exceeded")) else 400
def _bearer_token(header):
    """Extract the token from ``Authorization: Bearer <token>`` or ``None``."""
    if not header:
        return None
    parts = header.split()
    if len(parts) != 2 or parts[0] != "Bearer" or not parts[1]:
        return None
    return parts[1]
def make_handler(store, engine, access=None, audit=None):
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
        def _handle(self, method, call):
            parsed = urlparse(self.path)
            params = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
            # Blank values are dropped like every other parameter, except an
            # explicit empty matchers is an illegal value rather than omission.
            blank_params = parse_qs(parsed.query, keep_blank_values=True)
            if "matchers" in blank_params:
                params["matchers"] = blank_params["matchers"][-1]
            path = parsed.path
            public = path == "/healthz"  # always open, never audited
            guarded = not public and access is not None and access.enabled
            principal = None
            payload = {}
            try:
                if guarded:
                    principal = access.authenticate(
                        _bearer_token(self.headers.get("Authorization")))
                    if principal is None:
                        raise Unauthorized()
                if method == "POST":
                    payload = self._read_json()
                if guarded:
                    authorize(engine, principal, method, path, params, payload)
                status, body = call(path, params, payload)
            except Unauthorized:
                status, body = 401, {"error": "unauthorized"}
            except Forbidden:
                status, body = 403, {"error": "forbidden"}
            except ObsError as exc:
                status, body = _status_for(str(exc)), {"error": str(exc)}
            except Exception as exc:  # pragma: no cover - defensive
                status, body = 500, {"error": "internal error: %s" % exc}
            self._send(status, body)
            if not public and audit is not None:
                try:
                    tenant = request_tenant(engine, method, path, params, payload)
                except Exception:  # pragma: no cover - defensive
                    tenant = None
                outcome = "denied" if status in (401, 403) \
                    else "allowed" if 200 <= status < 300 else "failed"
                audit.append(principal["id"] if principal else None,
                             method, path, tenant, outcome, status)
        def do_GET(self):
            self._handle("GET", lambda path, params, payload:
                         dispatch(store, engine, "GET", path, params, payload, audit))
        def do_POST(self):
            self._handle("POST", lambda path, params, payload:
                         dispatch(store, engine, "POST", path, params, payload, audit))
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

    def __init__(self, address, store, engine, access=None, audit=None):
        self.store = store
        self.engine = engine
        self.access = access
        self.audit = audit
        super().__init__(address, make_handler(store, engine, access, audit))
def create_server(store, engine, host="127.0.0.1", port=8080, access=None, audit=None):
    """Build (but do not start) a ThreadingHTTPServer bound to ``host:port``."""
    if not isinstance(store, SeriesStore):
        raise ObsError("store must be a SeriesStore")
    if not isinstance(engine, AlertEngine):
        raise ObsError("engine must be an AlertEngine")
    if access is None:
        access = AccessControl(store.root)
    if audit is None:
        audit = AuditLog(store.root)
    return ObsdHTTPServer((host, int(port)), store, engine, access, audit)
def run_server(store, engine, host="127.0.0.1", port=8080, access=None, audit=None):
    """Serve until interrupted; prints one JSON startup line on stdout."""
    server = create_server(store, engine, host, port, access=access, audit=audit)
    print(json.dumps({"ok": True, "listening": "http://%s:%d" % server.server_address[:2],
                      "data_dir": store.root}, sort_keys=True, separators=(",", ":")), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
