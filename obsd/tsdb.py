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
        <root>/quotas.json                 tenant -> {"max_series", "max_points"}
    """

    def __init__(self, root):
        if not isinstance(root, (str, os.PathLike)) or not str(root):
            raise ObsError("root must be a non-empty path")
        self.root = os.path.abspath(str(root))
        self.points_dir = os.path.join(self.root, "points")
        self.series_path = os.path.join(self.root, "series.json")
        self.quota_path = os.path.join(self.root, "quotas.json")
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
        """Reload one point file, skipping unreadable lines.

        Repeated lines for the same timestamp (left behind by historical
        overwrites) collapse to the last value stored, so occupancy counts each
        distinct timestamp once after a reopen.
        """
        rows = {}
        path = self._points_path(sid)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                        rows[int(row["t"])] = float(row["v"])
                    except (ValueError, KeyError, TypeError):
                        continue
        return sorted(rows.items())

    def _save_registry(self):
        _atomic_write(self.series_path, json.dumps(
            [self._series[sid] for sid in sorted(self._series)],
            sort_keys=True, separators=(",", ":")))

    def _rewrite_points(self, sid):
        _atomic_write(self._points_path(sid),
                      "".join(_line(ts, value) for ts, value in self._samples[sid]))

    # ------------------------------------------------------------------ quotas
    def _load_quotas(self):
        """Reload ``quotas.json``; a missing file means every tenant is unlimited."""
        if not os.path.exists(self.quota_path):
            return
        try:
            with open(self.quota_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, ValueError) as exc:
            raise ObsError("cannot read quota config: %s" % exc)
        if not isinstance(data, dict):
            raise ObsError("cannot read quota config: root must be an object")
        for tenant, row in data.items():
            if not isinstance(tenant, str) or not tenant or not isinstance(row, dict):
                raise ObsError("cannot read quota config: malformed entry")
            self._quotas[tenant] = {
                "max_series": self._clean_limit(row.get("max_series"), "max_series"),
                "max_points": self._clean_limit(row.get("max_points"), "max_points")}

    @staticmethod
    def _clean_limit(value, field):
        """A limit is a non-negative integer or ``None`` (unlimited)."""
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ObsError("%s must be a non-negative integer or null" % field)
        if value < 0:
            raise ObsError("%s must be a non-negative integer or null" % field)
        return value

    @staticmethod
    def _clean_tenant(tenant):
        if not isinstance(tenant, str) or not tenant:
            raise ObsError("tenant must be a non-empty string")
        return tenant

    def _save_quotas(self):
        _atomic_write(self.quota_path, json.dumps(
            {tenant: self._quotas[tenant] for tenant in sorted(self._quotas)},
            sort_keys=True, separators=(",", ":")))

    def set_quota(self, tenant, max_series, max_points):
        """Configure the per-tenant limits; ``None`` means unlimited.

        Limits may be lowered below current usage; pure duplicate/overwrite
        writes stay allowed afterwards. Configuring a quota never creates a
        series.
        """
        tenant = self._clean_tenant(tenant)
        max_series = self._clean_limit(max_series, "max_series")
        max_points = self._clean_limit(max_points, "max_points")
        with self._lock:
            self._quotas[tenant] = {"max_series": max_series, "max_points": max_points}
            self._save_quotas()
            return self.get_quota(tenant)

    def get_quota(self, tenant):
        """Return limits and current usage for ``tenant`` in one snapshot."""
        tenant = self._clean_tenant(tenant)
        with self._lock:
            limits = self._quotas.get(tenant, {"max_series": None, "max_points": None})
            series = points = 0
            for sid, row in self._series.items():
                if row["tenant"] == tenant:
                    series += 1
                    points += len(self._samples[sid])
            return {"tenant": tenant, "max_series": limits["max_series"],
                    "max_points": limits["max_points"], "series": series, "points": points}

    # ------------------------------------------------------------------- write
    def write(self, tenant, metric, labels, samples, now=None, overwrite=False):
        """Store ``samples`` = ``[(timestamp_ms, value), ...]``.

        Idempotent: an identical ``(series, timestamp)`` value is a duplicate
        and changes nothing. A differing value for a stored timestamp raises
        ``ObsError`` unless ``overwrite=True``.

        The whole batch is checked against the tenant's configured quotas
        *after* input validation and conflict rules, using the net new series
        and distinct new timestamps the batch would add (same-value replays,
        overwrites and timestamps repeated inside the batch add nothing). A
        batch that would exceed either limit is rejected wholesale with
        ``ObsError``: no series, points or write counter change is kept.
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
        # Labels/tenant are validated before the lock so conflicts and quota
        # breaches never surface as identity errors.
        pairs = _labels_list(labels)
        identity = canonical_identity(tenant, metric, pairs)
        sid = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        with self._lock:
            existed = sid in self._series
            current = dict(self._samples.get(sid, ()))
            merged = dict(current)
            written = duplicates = 0
            new_stamps = set()
            overwritten = False
            for stamp, value in clean:
                if stamp in merged:
                    if merged[stamp] != value:
                        if not overwrite:
                            raise ObsError(
                                "conflict for %s at timestamp %d: stored=%r incoming=%r"
                                % (sid, stamp, merged[stamp], value))
                        if stamp in current:
                            overwritten = True
                    else:
                        duplicates += 1
                        continue
                else:
                    new_stamps.add(stamp)
                merged[stamp] = value
                written += 1

            quota = self._quotas.get(tenant)
            if quota is not None:
                used_series = used_points = 0
                for sid_, row in self._series.items():
                    if row["tenant"] == tenant:
                        used_series += 1
                        used_points += len(self._samples[sid_])
                new_series = 0 if existed else 1
                new_points = len(new_stamps)
                limit_series = quota["max_series"]
                limit_points = quota["max_points"]
                if new_series and limit_series is not None \
                        and used_series + new_series > limit_series:
                    raise ObsError(
                        "quota exceeded for tenant %r: series %d+%d > max_series %s"
                        % (tenant, used_series, new_series, limit_series))
                if new_points and limit_points is not None \
                        and used_points + new_points > limit_points:
                    raise ObsError(
                        "quota exceeded for tenant %r: points %d+%d > max_points %s"
                        % (tenant, used_points, new_points, limit_points))

            if not existed:
                self._series[sid] = {"series_id": sid, "tenant": tenant, "metric": metric,
                                     "labels": pairs, "identity": identity}
                self._samples[sid] = []
            if written:
                self._samples[sid] = sorted(merged.items())
                if overwritten:
                    # An overwrite replaced a stored value: rewrite the file so a
                    # timestamp never occupies more than one physical line.
                    self._rewrite_points(sid)
                else:
                    fresh = sorted((stamp, merged[stamp]) for stamp in new_stamps)
                    with open(self._points_path(sid), "a", encoding="utf-8") as handle:
                        handle.write("".join(_line(ts, value) for ts, value in fresh))
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
              step_ms=None, agg=None, group_by=None):
        """Raw or bucketed points per matching series.

        ``step_ms`` buckets are left-closed/right-open on epoch multiples and
        empty buckets are emitted as ``None``. A series with no sample in range
        is still listed, with an empty ``points`` list, unless ``agg`` is used
        without ``step_ms`` (then it yields nothing and is omitted).

        ``group_by=None`` keeps this per-series behaviour. Otherwise it must
        be a list of distinct non-empty label keys (``[]`` merges every match
        into one group) and ``agg`` is required: the raw in-range samples of
        all series in a group are aggregated together and each result row
        carries only ``labels`` (the group keys present on those series) and
        ``points``.
        """
        if agg is not None and agg not in AGGREGATES:
            raise ObsError("unknown aggregation: %r" % (agg,))
        if step_ms is not None and (not _num(step_ms) or int(step_ms) <= 0):
            raise ObsError("step_ms must be a positive number")
        lo = None if start_ms is None else int(start_ms)
        hi = None if end_ms is None else int(end_ms)
        if lo is not None and hi is not None and hi < lo:
            raise ObsError("end_ms must be >= start_ms")
        if group_by is not None:
            return self._query_grouped(tenant, metric, labels, lo, hi,
                                       step_ms, agg, group_by)
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

    def _query_grouped(self, tenant, metric, labels, lo, hi, step_ms, agg, group_by):
        """Cross-series aggregation: one row per distinct ``group_by`` tuple.

        The raw in-range samples of every series in a group are pooled before
        aggregating (no per-series pre-averaging), so samples from different
        series at the same timestamp each count. A group key absent from a
        series simply does not appear in that group's labels; a missing key
        and an empty-string value therefore land in different groups.
        """
        if not isinstance(group_by, (list, tuple)):
            raise ObsError("group_by must be a list of label keys")
        keys = list(group_by)
        for key in keys:
            if not isinstance(key, str) or not key:
                raise ObsError("group_by entries must be non-empty strings")
        if len(set(keys)) != len(keys):
            raise ObsError("group_by entries must not contain duplicates")
        if agg is None:
            raise ObsError("group_by requires agg")
        groups = {}
        for sid, samples in self._samples_for(tenant, metric, labels):
            series_labels = dict(self._series[sid]["labels"])
            group_labels = {key: series_labels[key] for key in keys
                            if key in series_labels}
            token = tuple(sorted(group_labels.items()))
            entry = groups.setdefault(token, {"labels": group_labels, "rows": []})
            entry["rows"].extend(item for item in samples
                                 if (lo is None or item[0] >= lo)
                                 and (hi is None or item[0] <= hi))
        out = []
        for token in sorted(groups):
            entry = groups[token]
            rows = sorted(entry["rows"])
            if step_ms is None:
                points = [] if not rows else \
                    [[rows[0][0], _aggregate([value for _, value in rows], agg)]]
            else:
                points = self._bucket(rows, step_ms, agg, lo, hi) or []
            out.append({"labels": entry["labels"], "points": points})
        return out

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
