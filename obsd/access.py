"""obsd.access - persistent multi-tenant principals and request audit log.

Standard library only. Principals live under ``root`` as ``access.json``;
only the SHA-256 digest of each token is persisted, never the token itself.
Every audited HTTP request appends one JSON line to ``audit.jsonl`` with a
monotonically increasing ``seq``, so the log survives restarts.

Access control is *disabled* while no config exists: the store then behaves
exactly as before (fully anonymous, no 401/403, no audit records). The first
``create_principal`` writes ``access.json`` and turns enforcement on; the
config keeps existing (and enforcing) even when the last principal is later
revoked.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading

from .tsdb import ObsError, _atomic_write

__all__ = ["AccessControl", "ROLES", "OUTCOMES"]

ROLES = ("viewer", "writer", "admin")
OUTCOMES = ("allowed", "denied", "failed")


class AccessControl:
    """Principal registry plus append-only audit log persisted under ``root``.

    Layout::

        <root>/access.json   {"principals": [{"id","token_sha256","role",
                                              "tenants"}, ...]}
        <root>/audit.jsonl   one {"seq","principal_id","method","path",
                              "tenant","outcome","status"} object per line
    """

    def __init__(self, root):
        if not isinstance(root, (str, os.PathLike)) or not str(root):
            raise ObsError("root must be a non-empty path")
        self.root = os.path.abspath(str(root))
        self.access_path = os.path.join(self.root, "access.json")
        self.audit_path = os.path.join(self.root, "audit.jsonl")
        self._lock = threading.RLock()
        self._principals = {}
        self._configured = False
        self._access_sig = None
        self._audit = []
        os.makedirs(self.root, exist_ok=True)
        self._load_access()
        self._load_audit()

    # ------------------------------------------------------------- persistence
    def _load_access(self):
        if not os.path.exists(self.access_path):
            return
        try:
            with open(self.access_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, ValueError) as exc:
            raise ObsError("cannot read access config: %s" % exc)
        rows = data.get("principals") if isinstance(data, dict) else data
        if not isinstance(rows, list):
            raise ObsError("cannot read access config: principals must be a list")
        principals = {}
        for row in rows:
            if not isinstance(row, dict) or not row.get("id"):
                raise ObsError("cannot read access config: malformed principal")
            principals[row["id"]] = {
                "id": row["id"], "token_sha256": row.get("token_sha256"),
                "role": row.get("role"), "tenants": list(row.get("tenants") or [])}
        self._principals = principals
        self._configured = True
        self._access_sig = self._signature()

    def _load_audit(self):
        """Reload the audit log; unreadable lines are skipped, never fatal."""
        if not os.path.exists(self.audit_path):
            return
        with open(self.audit_path, "r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if isinstance(row, dict) and isinstance(row.get("seq"), int):
                    self._audit.append(row)
        self._audit.sort(key=lambda row: row["seq"])

    def _signature(self):
        try:
            stat = os.stat(self.access_path)
        except OSError:
            return None
        return (stat.st_mtime_ns, stat.st_size)

    def _save_access(self):
        rows = [self._principals[key] for key in sorted(self._principals)]
        _atomic_write(self.access_path, json.dumps(
            {"principals": rows}, sort_keys=True, separators=(",", ":")))
        self._access_sig = self._signature()

    def refresh(self):
        """Reload the principal config when ``access.json`` changed on disk.

        Principals are managed through the local CLI while the server keeps
        running, so a running server picks up creations and revocations
        without a restart. A corrupt or vanished file keeps the last good
        in-memory state rather than dropping enforcement.
        """
        with self._lock:
            signature = self._signature()
            if signature is None or signature == self._access_sig:
                return
            try:
                self._load_access()
            except ObsError:
                return

    def enabled(self):
        """True once an access config exists, even with zero principals left."""
        return self._configured

    # -------------------------------------------------------------- principals
    @staticmethod
    def _public(row):
        """The externally visible shape: never the token, never its digest."""
        return {"id": row["id"], "role": row["role"],
                "tenants": list(row["tenants"])}

    def create_principal(self, principal_id, token, role, tenants):
        """Register a principal; only the token's SHA-256 digest is stored.

        The returned record (like every output and audit record) never
        contains the token. A duplicate id is a conflict.
        """
        if not isinstance(principal_id, str) or not principal_id:
            raise ObsError("principal id must be a non-empty string")
        if not isinstance(token, str) or not token:
            raise ObsError("token must be a non-empty string")
        if role not in ROLES:
            raise ObsError("role must be one of %s" % (ROLES,))
        if not isinstance(tenants, (list, tuple)) or not tenants:
            raise ObsError("at least one tenant scope is required")
        scopes = []
        for tenant in tenants:
            if not isinstance(tenant, str) or not tenant:
                raise ObsError("principal tenant scopes must be non-empty strings")
            if tenant not in scopes:
                scopes.append(tenant)
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        with self._lock:
            if principal_id in self._principals:
                raise ObsError("conflict: principal id already exists: %s"
                               % principal_id)
            row = {"id": principal_id, "token_sha256": digest, "role": role,
                   "tenants": scopes}
            self._principals[principal_id] = row
            self._configured = True
            self._save_access()
        return self._public(row)

    def list_principals(self):
        with self._lock:
            return [self._public(self._principals[key])
                    for key in sorted(self._principals)]

    def revoke_principal(self, principal_id):
        """Remove a principal; its token stops working immediately."""
        with self._lock:
            if principal_id not in self._principals:
                raise ObsError("unknown principal: %s" % principal_id)
            del self._principals[principal_id]
            self._save_access()
        return {"revoked": principal_id}

    def authenticate(self, token):
        """The principal record for ``token``, or ``None`` when it matches none."""
        if not isinstance(token, str) or not token:
            return None
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        with self._lock:
            for row in self._principals.values():
                if row["token_sha256"] == digest:
                    return self._public(row)
        return None

    # ------------------------------------------------------------------- audit
    def record(self, principal_id, method, path, tenant, outcome, status):
        """Append one audit entry with the next ``seq`` and persist it at once.

        ``principal_id`` is ``None`` when the request could not be
        authenticated, ``tenant`` is ``None`` when the request has no single
        tenant. Entries never contain credentials.
        """
        if outcome not in OUTCOMES:
            raise ObsError("outcome must be one of %s" % (OUTCOMES,))
        with self._lock:
            seq = (self._audit[-1]["seq"] if self._audit else 0) + 1
            entry = {"seq": seq, "principal_id": principal_id, "method": method,
                     "path": path, "tenant": tenant, "outcome": outcome,
                     "status": int(status)}
            self._audit.append(entry)
            line = json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n"
            with open(self.audit_path, "a", encoding="utf-8") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
        return dict(entry)

    def query_audit(self, tenant=None, principal_id=None, outcome=None,
                    after_seq=None, limit=None):
        """Filtered read of the audit log, always ascending by ``seq``.

        ``after_seq`` keeps only entries with ``seq > after_seq`` (with
        ``limit`` this pages the log); ``limit`` must be a positive integer.
        Invalid filters raise ``ObsError`` (``400`` over HTTP).
        """
        if outcome is not None and outcome not in OUTCOMES:
            raise ObsError("outcome must be one of %s" % (OUTCOMES,))
        if after_seq is not None and (isinstance(after_seq, bool)
                                      or not isinstance(after_seq, int)
                                      or after_seq < 0):
            raise ObsError("after_seq must be a non-negative integer")
        if limit is not None and (isinstance(limit, bool)
                                  or not isinstance(limit, int) or limit <= 0):
            raise ObsError("limit must be a positive integer")
        with self._lock:
            rows = [dict(row) for row in self._audit
                    if (tenant is None or row["tenant"] == tenant)
                    and (principal_id is None or row["principal_id"] == principal_id)
                    and (outcome is None or row["outcome"] == outcome)
                    and (after_seq is None or row["seq"] > after_seq)]
        if limit is not None:
            rows = rows[:limit]
        return rows
