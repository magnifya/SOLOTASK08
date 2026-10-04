"""obsd.http_app - stdlib ThreadingHTTPServer JSON API."""

from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .alerts import AlertEngine
from .tsdb import ObsError, SeriesStore

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
def dispatch(store, engine, method, path, params, payload):
    """Pure routing: (status, body) or ObsError. No sockets involved."""
    labels = {key[6:]: value for key, value in params.items() if key.startswith("label.")}
    if (method, path) == ("GET", "/healthz"):
        return 200, {"ok": True}
    if (method, path) == ("GET", "/v1/stats"):
        return 200, {"store": store.stats(), "alerts": len(engine.list_alerts())}
    if (method, path) == ("POST", "/v1/series"):
        result = store.write(_require(payload, "tenant"), _require(payload, "metric"),
                             _labels(payload), _require(payload, "samples"),
                             now=payload.get("now_ms"),
                             overwrite=bool(payload.get("overwrite", False)))
        return 202, {"written": result["written"], "duplicates": result["duplicates"],
                     "series_id": result["series_id"]}
    if (method, path) == ("GET", "/v1/query"):
        rows = store.query(_require(params, "tenant"), _require(params, "metric"), labels=labels,
                           start_ms=_int(params.get("start"), "start"),
                           end_ms=_int(params.get("end"), "end"),
                           step_ms=_int(params.get("step"), "step"),
                           agg=params.get("agg") or None)
        return 200, {"series": [{"labels": row["labels"], "points": row["points"]}
                                for row in rows]}
    if (method, path) == ("POST", "/v1/rules"):
        return 201, engine.add_rule(payload)
    if (method, path) == ("GET", "/v1/rules"):
        return 200, {"rules": engine.list_rules(params.get("tenant"))}
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
    if message.startswith(("not found", "unknown")):
        return 404
    return 409 if message.startswith("conflict") else 400
def make_handler(store, engine):
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
            try:
                payload = self._read_json() if method == "POST" else {}
                status, body = call(parsed.path, params, payload)
            except ObsError as exc:
                self._send(_status_for(str(exc)), {"error": str(exc)})
                return
            except Exception as exc:  # pragma: no cover - defensive
                self._send(500, {"error": "internal error: %s" % exc})
                return
            self._send(status, body)
        def do_GET(self):
            self._handle("GET", lambda path, params, payload:
                         dispatch(store, engine, "GET", path, params, payload))
        def do_POST(self):
            self._handle("POST", lambda path, params, payload:
                         dispatch(store, engine, "POST", path, params, payload))
        def do_DELETE(self):
            def call(path, params, payload):
                if not path.startswith("/v1/rules/"):
                    raise ObsError("not found: DELETE %s" % path)
                return 200, engine.del_rule(path.rsplit("/", 1)[-1])
            self._handle("DELETE", call)
    return ObsdHandler
class ObsdHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, store, engine):
        self.store = store
        self.engine = engine
        super().__init__(address, make_handler(store, engine))
def create_server(store, engine, host="127.0.0.1", port=8080):
    """Build (but do not start) a ThreadingHTTPServer bound to ``host:port``."""
    if not isinstance(store, SeriesStore):
        raise ObsError("store must be a SeriesStore")
    if not isinstance(engine, AlertEngine):
        raise ObsError("engine must be an AlertEngine")
    return ObsdHTTPServer((host, int(port)), store, engine)
def run_server(store, engine, host="127.0.0.1", port=8080):
    """Serve until interrupted; prints one JSON startup line on stdout."""
    server = create_server(store, engine, host, port)
    print(json.dumps({"ok": True, "listening": "http://%s:%d" % server.server_address[:2],
                      "data_dir": store.root}, sort_keys=True, separators=(",", ":")), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
