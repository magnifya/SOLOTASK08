"""obsd.access - persisted access principals, authorization and audit log.

Principals are managed through the local CLI and stored in
``<root>/principals.json``; only the SHA-256 digest of each token is kept, so
the raw token never touches disk, CLI output or audit records. Once that file
exists every non-``/healthz`` HTTP request must authenticate; while it is
absent the server stays fully anonymous and never answers 401/403.

Every non-``/healthz`` HTTP request (authenticated or not, allowed or not)
appends one record to ``<root>/audit.jsonl`` with a monotonically increasing
``seq``; the log survives restarts and is read back by ``GET /v1/audit``.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading

from .tsdb import ObsError, _atomic_write

__all__ = ["AccessControl", "AuditLog", "Unauthorized", "Forbidden",
           "request_tenant", "authorize", "ROLES", "OUTCOMES"]

ROLES = ("viewer", "writer", "admin")
ROLE_RANK = {"viewer": 1, "writer": 2, "admin": 3}
OUTCOMES = ("allowed", "denied", "failed")


class Unauthorized(Exception):
    """The request carries no usable credential (-> 401)."""


class Forbidden(Exception):
    """The principal is authenticated but not allowed here (-> 403)."""


def _digest(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _public(record):
    """The externally visible view of a principal: never the token, never
    its digest."""
    return {"id": record["id"], "role": record["role"],
            "tenants": list(record["tenants"])}


class AccessControl:
    """Principal registry persisted as ``<root>/principals.json``.

    The store is *enabled* exactly when that file exists: creating the first
    principal turns access control on, and it stays on (denying every token)
    even after the last principal is revoked.
    """

    def __init__(self, root):
        if not isinstance(root, (str, os.PathLike)) or not str(root):
            raise ObsError("root must be a non-empty path")
        self.root = os.path.abspath(str(root))
        self.principals_path = os.path.join(self.root, "principals.json")
        self._lock = threading.RLock()
        self._principals = {}
        self._signature = None
        self._load()

    @property
    def enabled(self):
        return os.path.exists(self.principals_path)

    # ------------------------------------------------------------- persistence
    def _file_signature(self):
        try:
            stat = os.stat(self.principals_path)
            return (stat.st_mtime_ns, stat.st_size)
        except OSError:
            return None

    def _load(self):
        self._principals = {}
        if not os.path.exists(self.principals_path):
            self._signature = None
            return
        try:
            with open(self.principals_path, "r", encoding="utf-8") as handle:
                rows = json.load(handle)
        except (OSError, ValueError) as exc:
            raise ObsError("cannot read principals config: %s" % exc)
        if not isinstance(rows, list):
            raise ObsError("cannot read principals config: root must be an array")
        for row in rows:
            if not isinstance(row, dict) \
                    or not isinstance(row.get("id"), str) or not row["id"] \
                    or not isinstance(row.get("token_sha256"), str) \
                    or row.get("role") not in ROLES \
                    or not isinstance(row.get("tenants"), list) or not row["tenants"] \
                    or not all(isinstance(t, str) and t for t in row["tenants"]):
                raise ObsError("cannot read principals config: malformed entry")
            self._principals[row["id"]] = {
                "id": row["id"], "token_sha256": row["token_sha256"],
                "role": row["role"], "tenants": list(row["tenants"]),
                "revoked": bool(row.get("revoked"))}
        self._signature = self._file_signature()

    def _reload_if_changed(self):
        """Pick up CLI edits (create/revoke) made while a server is running."""
        if self._file_signature() != self._signature:
            self._load()

    def _save(self):
        _atomic_write(self.principals_path, json.dumps(
            [self._principals[key] for key in sorted(self._principals)],
            sort_keys=True, separators=(",", ":")))
        self._signature = self._file_signature()

    # -------------------------------------------------------------- principals
    def create_principal(self, principal_id, token, role, tenants):
        """Register a principal; only the token's SHA-256 digest is stored."""
        if not isinstance(principal_id, str) or not principal_id:
            raise ObsError("principal id must be a non-empty string")
        if not isinstance(token, str) or not token:
            raise ObsError("token must be a non-empty string")
        if role not in ROLES:
            raise ObsError("role must be one of %s" % (list(ROLES),))
        if not isinstance(tenants, (list, tuple)) \
                or not all(isinstance(t, str) and t for t in tenants):
            raise ObsError("tenants must be an array of non-empty strings")
        tenants = sorted(set(tenants))
        if not tenants:
            raise ObsError("at least one tenant scope is required")
        with self._lock:
            if principal_id in self._principals:
                raise ObsError("conflict: principal id already exists: %s" % principal_id)
            record = {"id": principal_id, "token_sha256": _digest(token),
                      "role": role, "tenants": tenants, "revoked": False}
            self._principals[principal_id] = record
            self._save()
            return _public(record)

    def list_principals(self):
        """Active principals sorted by id; revoked ones are hidden."""
        with self._lock:
            return [_public(self._principals[key]) for key in sorted(self._principals)
                    if not self._principals[key]["revoked"]]

    def revoke_principal(self, principal_id):
        """Disable a principal's token; unknown or already-revoked ids 404."""
        with self._lock:
            record = self._principals.get(principal_id)
            if record is None or record["revoked"]:
                raise ObsError("unknown principal: %s" % principal_id)
            record["revoked"] = True
            self._save()
            return _public(record)

    def authenticate(self, token):
        """Resolve a bearer token to an active principal, or ``None``."""
        if not isinstance(token, str) or not token:
            return None
        digest = _digest(token)
        with self._lock:
            self._reload_if_changed()
            for key in sorted(self._principals):
                record = self._principals[key]
                if not record["revoked"] and record["token_sha256"] == digest:
                    return _public(record)
        return None


# ------------------------------------------------------------ authorization
def _lookup_tenant(rows, wanted):
    for row in rows:
        if row.get("id") == wanted:
            return row.get("tenant")
    return None


def request_tenant(engine, method, path, params, payload):
    """The single tenant a request touches, or ``None`` when there is none.

    Used both for authorization and for the audit record's ``tenant`` field.
    """
    if method == "POST" and path == "/v1/series/batch":
        entries = payload.get("entries")
        if not isinstance(entries, list):
            return None
        tenants = set()
        for entry in entries:
            if isinstance(entry, dict):
                tenant = entry.get("tenant")
                if isinstance(tenant, str) and tenant:
                    tenants.add(tenant)
        return tenants.pop() if len(tenants) == 1 else None
    if method == "DELETE" and path.startswith("/v1/rules/"):
        return _lookup_tenant(engine.list_rules(), path.rsplit("/", 1)[-1])
    if method == "DELETE" and path.startswith("/v1/notification-routes/"):
        return _lookup_tenant(engine.list_routes(), path.rsplit("/", 1)[-1])
    if method == "POST" and path.startswith("/v1/notifications/") \
            and path.endswith("/ack"):
        note_id = path[len("/v1/notifications/"):-len("/ack")]
        return _lookup_tenant(engine.list_notifications(), note_id)
    tenant = payload.get("tenant") if method == "POST" else params.get("tenant")
    return tenant if isinstance(tenant, str) and tenant else None


def _required_role(method, path):
    """Minimum role for a route, or ``None`` when any principal may proceed."""
    if (method, path) in (("GET", "/v1/stats"), ("POST", "/v1/quotas"),
                          ("GET", "/v1/quotas"), ("POST", "/v1/evaluate"),
                          ("POST", "/v1/inhibitions"), ("GET", "/v1/audit")):
        return "admin"
    if (method, path) in (("POST", "/v1/series"), ("POST", "/v1/series/batch"),
                          ("POST", "/v1/rules"), ("POST", "/v1/notification-routes"),
                          ("POST", "/v1/silences"), ("POST", "/v1/slos")):
        return "writer"
    if method == "DELETE" and (path.startswith("/v1/rules/")
                               or path.startswith("/v1/notification-routes/")):
        return "writer"
    if method == "POST" and path.startswith("/v1/notifications/") \
            and path.endswith("/ack"):
        return "writer"
    if (method, path) in (("GET", "/v1/query"), ("GET", "/v1/rules"),
                          ("GET", "/v1/notification-routes"),
                          ("GET", "/v1/notifications"), ("GET", "/v1/alerts"),
                          ("GET", "/v1/slos"), ("GET", "/v1/slos/status")):
        return "viewer"
    return None


# Reads that span every tenant when no ``tenant`` parameter narrows them;
# such cross-tenant reads are reserved for admin.
_CROSS_TENANT_READS = (("GET", "/v1/rules"), ("GET", "/v1/notification-routes"),
                       ("GET", "/v1/notifications"), ("GET", "/v1/alerts"),
                       ("GET", "/v1/slos"), ("GET", "/v1/slos/status"))


def authorize(engine, principal, method, path, params, payload):
    """Raise ``Forbidden`` unless ``principal`` may perform this request.

    Role order is viewer < writer < admin. Tenant scope binds viewers and
    writers; admin additionally performs cross-tenant operations and is not
    scope-checked.
    """
    role = principal["role"]
    required = _required_role(method, path)
    if required is not None and ROLE_RANK[role] < ROLE_RANK[required]:
        raise Forbidden()
    if role == "admin":
        return
    tenants = set(principal["tenants"])
    if (method, path) in _CROSS_TENANT_READS and not params.get("tenant"):
        raise Forbidden()
    if method == "POST" and path == "/v1/series/batch":
        # Every entry's tenant is checked before anything is written, so a
        # batch stays atomic: one out-of-scope entry rejects the whole batch.
        entries = payload.get("entries")
        if isinstance(entries, list):
            for entry in entries:
                if isinstance(entry, dict):
                    tenant = entry.get("tenant")
                    if isinstance(tenant, str) and tenant and tenant not in tenants:
                        raise Forbidden()
        return
    tenant = request_tenant(engine, method, path, params, payload)
    if tenant is not None and tenant not in tenants:
        raise Forbidden()


# ------------------------------------------------------------------ audit
class AuditLog:
    """Append-only audit log persisted as ``<root>/audit.jsonl``.

    One record per non-healthz HTTP request: ``seq`` (monotonic, continuing
    across restarts), ``principal_id`` (``None`` when unauthenticated),
    ``method``, ``path``, ``tenant`` (``None`` when no single tenant applies),
    ``outcome`` (allowed/denied/failed) and the final HTTP ``status``.
    """

    def __init__(self, root):
        if not isinstance(root, (str, os.PathLike)) or not str(root):
            raise ObsError("root must be a non-empty path")
        self.root = os.path.abspath(str(root))
        self.audit_path = os.path.join(self.root, "audit.jsonl")
        self._lock = threading.RLock()
        self._entries = []
        self._seq = 0
        self._load()

    def _load(self):
        if not os.path.exists(self.audit_path):
            return
        with open(self.audit_path, "r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(row, dict) or not isinstance(row.get("seq"), int):
                    continue
                self._entries.append(row)
                self._seq = max(self._seq, row["seq"])

    def append(self, principal_id, method, path, tenant, outcome, status):
        """Append one record and fsync it; returns the stored entry."""
        with self._lock:
            self._seq += 1
            entry = {"seq": self._seq, "principal_id": principal_id,
                     "method": method, "path": path, "tenant": tenant,
                     "outcome": outcome, "status": int(status)}
            line = json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n"
            with open(self.audit_path, "a", encoding="utf-8") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
            self._entries.append(entry)
            return dict(entry)

    def query(self, tenant=None, principal_id=None, outcome=None,
              after_seq=None, limit=None):
        """Filter entries (always ascending by ``seq``) with optional paging."""
        if outcome is not None and outcome not in OUTCOMES:
            raise ObsError("outcome must be one of %s" % (list(OUTCOMES),))
        if after_seq is not None and (isinstance(after_seq, bool)
                                      or not isinstance(after_seq, int)):
            raise ObsError("after_seq must be an integer")
        if limit is not None and (isinstance(limit, bool)
                                  or not isinstance(limit, int) or limit <= 0):
            raise ObsError("limit must be a positive integer")
        with self._lock:
            rows = [dict(entry) for entry in self._entries
                    if (tenant is None or entry["tenant"] == tenant)
                    and (principal_id is None or entry["principal_id"] == principal_id)
                    and (outcome is None or entry["outcome"] == outcome)
                    and (after_seq is None or entry["seq"] > after_seq)]
        rows.sort(key=lambda entry: entry["seq"])
        if limit is not None:
            rows = rows[:limit]
        return rows
