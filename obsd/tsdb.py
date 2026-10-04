"""obsd.tsdb - series identity, idempotent writes, bucketed query, retention.

Standard library only. All logic takes an explicit clock (``now``) where time
matters; nothing here calls time.time().
"""

from __future__ import annotations

import hashlib
import json
import os
import threading

__all__ = ["ObsError", "SeriesStore", "AGGREGATES", "canonical_identity", "series_id_for"]

AGGREGATES = ("sum", "avg", "min", "max", "count")


class ObsError(Exception):
    """Raised for any rejected observation or request."""


def _num(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _labels_list(labels):
    """Normalise labels to a list of ``[key, value]`` pairs sorted by key."""
    if labels is None:
        return []
    if isinstance(labels, dict):
        items = list(labels.items())
    elif isinstance(labels, (list, tuple)):
        items = []
        for entry in labels:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                raise ObsError("labels entries must be [key, value] pairs")
            items.append((entry[0], entry[1]))
    else:
        raise ObsError("labels must be an object or a list of pairs")
    pairs = []
    for key, value in items:
        if not isinstance(key, str) or value is None:
            raise ObsError("label keys must be strings and values must not be null")
        pairs.append([key, str(value)])
    pairs.sort(key=lambda pair: pair[0])
    if len({key for key, _ in pairs}) != len(pairs):
        raise ObsError("duplicate label key")
    return pairs


def as_labels(labels):
    """Normalise labels to a plain dict."""
    return {key: value for key, value in _labels_list(labels)}


def canonical_identity(tenant, metric, labels):
    """Canonical newline-joined identity string of a series."""
    if not isinstance(tenant, str) or not tenant:
        raise ObsError("tenant must be a non-empty string")
    if not isinstance(metric, str) or not metric:
        raise ObsError("metric must be a non-empty string")
    return "\n".join([tenant, metric] +
                     ["%s=%s" % (key, value) for key, value in _labels_list(labels)])


def series_id_for(tenant, metric, labels):
    """64-char sha256 id of a series, independent of label insertion order."""
    return hashlib.sha256(canonical_identity(tenant, metric, labels).encode("utf-8")).hexdigest()


def matches_labels(series_labels, matcher):
    """True when every matcher pair is present in ``series_labels``."""
    if not matcher:
        return True
    if isinstance(matcher, (list, tuple)):
        matcher = as_labels(matcher)
    return all(str(series_labels.get(key)) == str(value) for key, value in matcher.items())


def _aggregate(values, agg):
    if agg == "sum":
        return float(sum(values))
    if agg == "avg":
        return sum(values) / float(len(values))
    if agg == "min":
        return float(min(values))
    if agg == "max":
        return float(max(values))
    if agg == "count":
        return len(values)
    raise ObsError("unknown aggregation: %r" % (agg,))


def _atomic_write(path, text):
    """Write ``text`` to ``path`` through a temp file plus os.replace."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path + ".tmp", "w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(path + ".tmp", path)


def _line(stamp, value):
    return json.dumps({"t": stamp, "v": value}, separators=(",", ":")) + "\n"


class SeriesStore:
    """Append-mostly time series store persisted under ``root``.

    Layout::

        <root>/series.json                 registry: series_id -> series row
        <root>/points/<series_id>.jsonl    one ``{"t":ts,"v":value}`` per line
        <root>/quotas.json                 per-tenant max_series/max_points
    """

    def __init__(self, root):
        if not isinstance(root, (str, os.PathLike)) or not str(root):
            raise ObsError("root must be a non-empty path")
        self.root = os.path.abspath(str(root))
        self.points_dir = os.path.join(self.root, "points")
        self.series_path = os.path.join(self.root, "series.json")
        self.quotas_path = os.path.join(self.root, "quotas.json")
        self._lock = threading.RLock()
        self._series, self._samples, self._writes = {}, {}, 0
        self._quotas = {}
        os.makedirs(self.points_dir, exist_ok=True)
        self._load()
        self._load_quotas()

    def _points_path(self, sid):
        return os.path.join(self.points_dir, sid + ".jsonl")

    # ------------------------------------------------------------- persistence
    def _load(self):
        rows = []
        if os.path.exists(self.series_path):
            try:
                with open(self.series_path, "r", encoding="utf-8") as handle:
                    rows = json.load(handle)
            except (OSError, ValueError) as exc:
                raise ObsError("cannot read series registry: %s" % exc)
        for row in rows:
            sid = row.get("series_id")
            if not sid:
                continue
            self._series[sid] = {"series_id": sid, "tenant": row.get("tenant"),
                                 "metric": row.get("metric"),
                                 "labels": [[str(k), str(v)] for k, v in row.get("labels", [])],
                                 "identity": row.get("identity")}
            self._samples[sid] = self._read_points(sid)

    def _read_points(self, sid):
        """Reload one point file; the last line for a timestamp wins.

        Overwrites append a fresh line, so repeated writes of the same
        timestamp collapse to one occupied point on reload.
        """
        by_stamp = {}
        path = self._points_path(sid)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                        by_stamp[int(row["t"])] = float(row["v"])
                    except (ValueError, KeyError, TypeError):
                        continue
        return sorted(by_stamp.items())

    def _save_registry(self):
        _atomic_write(self.series_path, json.dumps(
            [self._series[sid] for sid in sorted(self._series)],
            sort_keys=True, separators=(",", ":")))

    def _rewrite_points(self, sid):
        _atomic_write(self._points_path(sid),
                      "".join(_line(ts, value) for ts, value in self._samples[sid]))

    # ------------------------------------------------------------------ quotas
    def _load_quotas(self):
        """Reload per-tenant limits; a directory without a quota file has none."""
        if not os.path.exists(self.quotas_path):
            return
        try:
            with open(self.quotas_path, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except (OSError, ValueError) as exc:
            raise ObsError("cannot read quota config: %s" % exc)
        if not isinstance(raw, dict):
            raise ObsError("quota config must be an object of tenants")
        for tenant, limits in raw.items():
            if not isinstance(tenant, str) or not tenant or not isinstance(limits, dict):
                raise ObsError("quota config entries must map a non-empty tenant to limits")
            max_series = limits.get("max_series")
            max_points = limits.get("max_points")
            self._quotas[tenant] = {
                "max_series": None if max_series is None else self._valid_limit(max_series, "max_series"),
                "max_points": None if max_points is None else self._valid_limit(max_points, "max_points")}

    @staticmethod
    def _valid_limit(value, field):
        """A limit is a non-negative int (never bool/float/str) or None."""
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ObsError("%s must be a non-negative integer or null" % field)
        if value < 0:
            raise ObsError("%s must be a non-negative integer or null" % field)
        return value

    @staticmethod
    def _valid_tenant(tenant):
        if not isinstance(tenant, str) or not tenant:
            raise ObsError("tenant must be a non-empty string")
        return tenant

    def _save_quotas(self):
        _atomic_write(self.quotas_path, json.dumps(
            {tenant: self._quotas[tenant] for tenant in sorted(self._quotas)},
            sort_keys=True, separators=(",", ":")))

    def set_quota(self, tenant, max_series, max_points):
        """Configure a tenant's limits atomically; None means unlimited.

        Validation happens before any state change, so a bad request never
        alters the stored configuration. Setting a quota never creates a
        series.
        """
        self._valid_tenant(tenant)
        max_series = self._valid_limit(max_series, "max_series")
        max_points = self._valid_limit(max_points, "max_points")
        with self._lock:
            self._quotas[tenant] = {"max_series": max_series, "max_points": max_points}
            self._save_quotas()
            return self.quota_status(tenant)

    def _tenant_usage(self, tenant):
        series_count = points_count = 0
        for sid, row in self._series.items():
            if row["tenant"] == tenant:
                series_count += 1
                points_count += len(self._samples[sid])
        return series_count, points_count

    def quota_status(self, tenant):
        """Limits (None when unconfigured) and current usage for one tenant."""
        self._valid_tenant(tenant)
        with self._lock:
            limits = self._quotas.get(tenant, {"max_series": None, "max_points": None})
            series_count, points_count = self._tenant_usage(tenant)
            return {"tenant": tenant, "max_series": limits["max_series"],
                    "max_points": limits["max_points"],
                    "series": series_count, "points": points_count}

    # ------------------------------------------------------------------- write
    def write(self, tenant, metric, labels, samples, now=None, overwrite=False):
        """Store ``samples`` = ``[(timestamp_ms, value), ...]``.

        Idempotent: an identical ``(series, timestamp)`` value is a duplicate
        and changes nothing. A differing value for a stored timestamp raises
        ``ObsError`` unless ``overwrite=True``.

        Existing input validation and conflict rules run first; the batch is
        then checked against the tenant's quota by its net increase in distinct
        series/timestamps. Any exceeded dimension rejects the whole batch with
        ``ObsError`` (HTTP 409): no new series, sample or write counter change
        remains.
        """
        if not isinstance(samples, (list, tuple)):
            raise ObsError("samples must be a list of [timestamp_ms, value] pairs")
        clean = []
        for entry in samples:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                raise ObsError("each sample must be a [timestamp_ms, value] pair")
            stamp, value = entry
            if not _num(stamp) or not _num(value):
                raise ObsError("sample timestamp and value must be numbers")
            if now is not None and int(stamp) > int(now):
                raise ObsError("sample timestamp %d is in the future (now=%d)"
                               % (int(stamp), int(now)))
            clean.append((int(stamp), float(value)))
        if not clean:
            raise ObsError("samples must not be empty")
        pairs = _labels_list(labels)
        identity = canonical_identity(tenant, metric, pairs)
        sid = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        with self._lock:
            existed = sid in self._series
            stored = dict(self._samples.get(sid, ()))
            merged = dict(stored)
            written = duplicates = 0
            fresh = []
            for stamp, value in clean:
                if stamp in merged and merged[stamp] != value and not overwrite:
                    raise ObsError("conflict for %s at timestamp %d: stored=%r incoming=%r"
                                   % (sid, stamp, merged[stamp], value))
                if stamp in merged and merged[stamp] == value:
                    duplicates += 1
                    continue
                merged[stamp] = value
                fresh.append((stamp, value))
                written += 1
            # Net increase: a new series counts once, and only distinct
            # timestamps not already stored occupy new points.
            new_series = 0 if existed else 1
            new_points = len({stamp for stamp, _ in clean} - set(stored))
            limits = self._quotas.get(tenant)
            if limits is not None:
                cur_series, cur_points = self._tenant_usage(tenant)
                if (new_series and limits["max_series"] is not None
                        and cur_series + new_series > limits["max_series"]):
                    raise ObsError(
                        "conflict: tenant %r quota exceeded: series %d + %d > max_series %d"
                        % (tenant, cur_series, new_series, limits["max_series"]))
                if (new_points and limits["max_points"] is not None
                        and cur_points + new_points > limits["max_points"]):
                    raise ObsError(
                        "conflict: tenant %r quota exceeded: points %d + %d > max_points %d"
                        % (tenant, cur_points, new_points, limits["max_points"]))
            if not existed:
                self._series[sid] = {"series_id": sid, "tenant": tenant, "metric": metric,
                                     "labels": pairs, "identity": identity}
                self._samples[sid] = []
            if fresh:
                self._samples[sid] = sorted(merged.items())
                with open(self._points_path(sid), "a", encoding="utf-8") as handle:
                    handle.write("".join(_line(ts, value) for ts, value in sorted(fresh)))
                    handle.flush()
                    os.fsync(handle.fileno())
                self._writes += 1
                self._save_registry()
        return {"series_id": sid, "written": written, "duplicates": duplicates}

    # ------------------------------------------------------------------- query
    def _samples_for(self, tenant, metric, labels):
        with self._lock:
            ids = sorted(sid for sid, row in self._series.items()
                         if row["tenant"] == tenant and row["metric"] == metric
                         and matches_labels(dict(row["labels"]), labels))
            return [(sid, list(self._samples[sid])) for sid in ids]

    def _scan(self, tenant, metric, labels, start_ms, end_ms):
        """Raw samples of matching series inside ``[start, end]`` (internal).

        Used where exact event counts matter (SLO numerator/denominator) and by
        rule evaluation to find candidate series.
        """
        lo = None if start_ms is None else int(start_ms)
        hi = None if end_ms is None else int(end_ms)
        out = []
        for sid, samples in self._samples_for(tenant, metric, labels):
            rows = [item for item in samples
                    if (lo is None or item[0] >= lo) and (hi is None or item[0] <= hi)]
            if rows:
                out.append((sid, rows))
        return out

    def query(self, tenant, metric, labels=None, start_ms=None, end_ms=None,
              step_ms=None, agg=None):
        """Raw or bucketed points per matching series.

        ``step_ms`` buckets are left-closed/right-open on epoch multiples and
        empty buckets are emitted as ``None``. A series with no sample in range
        is still listed, with an empty ``points`` list, unless ``agg`` is used
        without ``step_ms`` (then it yields nothing and is omitted).
        """
        if agg is not None and agg not in AGGREGATES:
            raise ObsError("unknown aggregation: %r" % (agg,))
        if step_ms is not None and (not _num(step_ms) or int(step_ms) <= 0):
            raise ObsError("step_ms must be a positive number")
        lo = None if start_ms is None else int(start_ms)
        hi = None if end_ms is None else int(end_ms)
        if lo is not None and hi is not None and hi < lo:
            raise ObsError("end_ms must be >= start_ms")
        out = []
        for sid, samples in self._samples_for(tenant, metric, labels):
            rows = [item for item in samples
                    if (lo is None or item[0] >= lo) and (hi is None or item[0] <= hi)]
            points = self._bucket(rows, step_ms, agg, lo, hi)
            if points is None or (not points and agg is not None and step_ms is None):
                continue
            out.append({"series_id": sid, "labels": dict(self._series[sid]["labels"]),
                        "points": points})
        return out

    def _bucket(self, rows, step_ms, agg, lo, hi):
        """``None`` = nothing to report, ``[]`` = known empty, else the points."""
        if step_ms is None:
            if agg is None:
                return [[stamp, value] for stamp, value in rows]
            return None if not rows else [[rows[0][0], _aggregate([v for _, v in rows], agg)]]
        step = int(step_ms)
        groups = {}
        for stamp, value in rows:
            groups.setdefault(stamp - (stamp % step), []).append(value)
        if not groups:
            return None
        keys = sorted(groups)
        start = max(keys[0], lo - (lo % step)) if lo is not None else keys[0]
        end = min(keys[-1], hi - (hi % step)) if hi is not None else keys[-1]
        return [[key, _aggregate(groups[key], agg) if key in groups else None]
                for key in range(start, end + 1, step)]

    def rollup(self, tenant, metric, labels, window_ms, agg):
        """Downsampled series over the whole stored range, ordered by series_id."""
        if agg not in AGGREGATES:
            raise ObsError("unknown aggregation: %r" % (agg,))
        if not _num(window_ms) or int(window_ms) <= 0:
            raise ObsError("window_ms must be a positive number")
        return [{"series_id": item["series_id"], "labels": item["labels"],
                 "points": [point for point in item["points"] if point[1] is not None]}
                for item in self.query(tenant, metric, labels=labels,
                                       step_ms=int(window_ms), agg=agg)]

    # --------------------------------------------------------------- retention
    def enforce_retention(self, tenant, cutoff_ms):
        """Drop every sample with ``timestamp_ms < cutoff_ms`` for ``tenant``."""
        cutoff = int(cutoff_ms)
        dropped = affected = 0
        with self._lock:
            for sid in sorted(self._series):
                if self._series[sid]["tenant"] != tenant:
                    continue
                keep = [item for item in self._samples[sid] if item[0] >= cutoff]
                removed = len(self._samples[sid]) - len(keep)
                if removed:
                    self._samples[sid] = keep
                    self._rewrite_points(sid)
                    dropped += removed
                    affected += 1
        return {"dropped": dropped, "series": affected, "cutoff_ms": cutoff}

    def stats(self):
        with self._lock:
            per_tenant = {}
            points = 0
            for sid, row in self._series.items():
                count = len(self._samples[sid])
                points += count
                entry = per_tenant.setdefault(row["tenant"], {"series": 0, "points": 0})
                entry["series"] += 1
                entry["points"] += count
        return {"series": len(self._series), "points": points, "writes": self._writes,
                "tenants": per_tenant}
