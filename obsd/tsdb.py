"""obsd.tsdb - series identity, idempotent writes, bucketed query, retention.

Standard library only. All logic takes an explicit clock (``now``) where time
matters; nothing here calls time.time().
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading

__all__ = ["ObsError", "SeriesStore", "AGGREGATES", "COUNTER_AGGREGATES",
           "canonical_identity", "series_id_for"]

AGGREGATES = ("sum", "avg", "min", "max", "count")
# Sliding-window-only counter aggregates: accepted by ``SeriesStore.query``
# (and the HTTP/CLI query entries built on it) when ``window_ms`` is given.
# Rollup, alert rules and SLOs keep validating against ``AGGREGATES`` alone.
COUNTER_AGGREGATES = ("increase", "rate")
MATCHER_OPS = ("=", "!=", "=~", "!~")


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


def compile_matchers(matchers):
    """Validate ``matchers`` and return ``(key, op, value, pattern)`` tuples.

    Each element is an object with exactly ``key`` (non-empty string), ``op``
    (one of ``=``, ``!=``, ``=~``, ``!~``) and ``value`` (string, possibly
    empty). ``None`` and an empty list add no condition; regex patterns are
    compiled here, so an invalid pattern is rejected before any series is read.
    """
    if matchers is None:
        return ()
    if not isinstance(matchers, (list, tuple)):
        raise ObsError("matchers must be a list of matcher objects")
    compiled = []
    for element in matchers:
        if not isinstance(element, dict):
            raise ObsError("each matcher must be an object with key, op and value")
        if set(element) != {"key", "op", "value"}:
            raise ObsError("each matcher must contain exactly key, op and value")
        key, op, value = element["key"], element["op"], element["value"]
        if not isinstance(key, str) or not key:
            raise ObsError("matcher key must be a non-empty string")
        if op not in MATCHER_OPS:
            raise ObsError("matcher op must be one of %s" % (MATCHER_OPS,))
        if not isinstance(value, str):
            raise ObsError("matcher value must be a string")
        pattern = None
        if op in ("=~", "!~"):
            try:
                pattern = re.compile(value)
            except re.error as exc:
                raise ObsError("invalid matcher regex %r: %s" % (value, exc))
        compiled.append((key, op, value, pattern))
    return tuple(compiled)


def matchers_hold(series_labels, compiled):
    """True when every compiled matcher holds for ``series_labels``.

    A missing key never satisfies ``=``/``=~`` but always satisfies ``!=``/
    ``!~``; regex matchers anchor with ``fullmatch`` so the pattern covers the
    whole value. Only real labels are considered.
    """
    for key, op, value, pattern in compiled:
        present = key in series_labels
        if op == "=":
            if not present or series_labels[key] != value:
                return False
        elif op == "!=":
            if present and series_labels[key] == value:
                return False
        elif op == "=~":
            if not present or pattern.fullmatch(series_labels[key]) is None:
                return False
        else:  # "!~"
            if present and pattern.fullmatch(series_labels[key]) is not None:
                return False
    return True


def parse_matchers_text(text, field="matchers"):
    """Decode a JSON array text of matcher objects; ``None``/absent adds none.

    Used by the HTTP and CLI entry points, where the parameter arrives as a
    string. Returns the raw decoded list (validated later by
    :func:`compile_matchers`, which runs even with no candidate series). Empty
    text and an explicit JSON ``null`` are illegal here (unlike omitting the
    parameter or passing Python ``None`` to ``query``).
    """
    if text is None:
        return None
    if not isinstance(text, str) or not text:
        raise ObsError("%s must be a JSON array of matcher objects" % field)
    try:
        data = json.loads(text)
    except ValueError:
        raise ObsError("%s must be a JSON array of matcher objects" % field)
    if data is None:
        raise ObsError("%s must be a JSON array of matcher objects" % field)
    if not isinstance(data, list):
        raise ObsError("%s must be a JSON array of matcher objects" % field)
    return data


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

    # ---------------------------------------------------- multi-series writes
    @staticmethod
    def _clean_entry_samples(samples, now):
        """Validate one batch entry's samples against the same rules as ``write``.

        Returns ``[(int_stamp, float_value), ...]`` in input order. ``now`` is
        either ``None`` (the future check is skipped) or an integer already
        validated by the caller.
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
        return clean

    def write_batch(self, entries, now=None, overwrite=False):
        """Atomically write samples for several series in one transaction.

        ``entries`` is a non-empty array of objects, each with required
        ``tenant`` (non-empty string), ``metric`` (non-empty string),
        ``samples`` (non-empty ``[[timestamp_ms, value], ...]``) and optional
        ``labels`` (an object, defaulting to ``{}``); no other fields are
        allowed. The batch may span tenants and metrics and contain the same
        series more than once. ``now`` is ``None`` (no future-sample check) or
        an integer; ``overwrite`` is a boolean (default ``False``).

        The whole request is validated first (every entry's structure, identity
        and samples), conflicts are evaluated second, and per-tenant quotas
        third. Entries and their samples are processed in input order, so a
        later entry sees series and timestamps accumulated by earlier entries
        in the same batch: an identical ``(series, timestamp)`` value is a
        duplicate, a differing value conflicts unless ``overwrite=True``, in
        which case it replaces and counts as written.

        Quotas use each tenant's net new series and distinct new timestamps;
        duplicates, overwrites and timestamps repeated within the batch add no
        occupancy, and only dimensions that actually increase are checked (so
        limits lowered below current usage still allow replays). Any rejection
        leaves the store untouched. On success the point files and registry are
        committed together under the store lock, and ``stats()["writes"]``
        increases by exactly one when the batch wrote anything (a batch made
        entirely of duplicates changes nothing).

        Returns
        ``{"written": n, "duplicates": m, "results": [{"series_id",
        "written", "duplicates"}, ...]}`` with the per-entry results in input
        order and totals equal to their sum.
        """
        if now is not None and (not isinstance(now, int) or isinstance(now, bool)):
            raise ObsError("now must be an integer or null")
        if not isinstance(overwrite, bool):
            raise ObsError("overwrite must be a boolean")
        if not isinstance(entries, (list, tuple)) or not entries:
            raise ObsError("entries must be a non-empty array")
        allowed = {"tenant", "metric", "samples", "labels"}
        # Phase 1: validate every entry (structure, identity and samples) before
        # any conflict or quota rule is examined.
        prepared = []
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                raise ObsError("entries[%d] must be an object" % index)
            extra = set(entry) - allowed
            if extra:
                raise ObsError("entries[%d] has unknown field: %s"
                               % (index, sorted(extra)[0]))
            tenant = entry.get("tenant")
            metric = entry.get("metric")
            if "samples" not in entry:
                raise ObsError("missing field: entries[%d].samples" % index)
            if not isinstance(tenant, str) or not tenant:
                raise ObsError("entries[%d].tenant must be a non-empty string" % index)
            if not isinstance(metric, str) or not metric:
                raise ObsError("entries[%d].metric must be a non-empty string" % index)
            labels = entry.get("labels")
            if labels is None:
                labels = {}
            elif not isinstance(labels, dict):
                raise ObsError("entries[%d].labels must be an object" % index)
            pairs = _labels_list(labels)
            identity = canonical_identity(tenant, metric, pairs)
            sid = hashlib.sha256(identity.encode("utf-8")).hexdigest()
            samples = self._clean_entry_samples(entry["samples"], now)
            prepared.append({"sid": sid, "tenant": tenant, "metric": metric,
                             "pairs": pairs, "identity": identity, "samples": samples})

        with self._lock:
            # Phases 2-4 run under one lock acquisition on private working
            # copies, so concurrent readers and writers see either the whole
            # pre-batch or the whole post-batch state. A raised exception drops
            # the copies without touching the committed state.
            states = {}

            def state_for(item):
                sid = item["sid"]
                state = states.get(sid)
                if state is None:
                    state = {
                        "row": {"series_id": sid, "tenant": item["tenant"],
                                "metric": item["metric"], "labels": item["pairs"],
                                "identity": item["identity"]},
                        "existed": sid in self._series,
                        # Working copy of the series' timestamps; later entries
                        # see everything earlier entries merged.
                        "merged": dict(self._samples.get(sid, ())),
                        # Timestamps with no physical line before this batch.
                        "new_stamps": set(),
                        "overwritten": False}
                    states[sid] = state
                return state

            # Phase 2: conflicts in entry and sample input order.
            results = []
            total_written = total_duplicates = 0
            for item in prepared:
                state = state_for(item)
                merged = state["merged"]
                written = duplicates = 0
                for stamp, value in item["samples"]:
                    if stamp in merged:
                        if merged[stamp] != value:
                            if not overwrite:
                                raise ObsError(
                                    "conflict for %s at timestamp %d: stored=%r incoming=%r"
                                    % (item["sid"], stamp, merged[stamp], value))
                            if stamp not in state["new_stamps"]:
                                # Replacing a value already on disk requires a
                                # full point-file rewrite at commit time.
                                state["overwritten"] = True
                        else:
                            duplicates += 1
                            continue
                    else:
                        state["new_stamps"].add(stamp)
                    merged[stamp] = value
                    written += 1
                results.append({"series_id": item["sid"], "written": written,
                                "duplicates": duplicates})
                total_written += written
                total_duplicates += duplicates

            # Phase 3: per-tenant net new series and distinct new timestamps.
            tenants = {state["row"]["tenant"] for state in states.values()}
            for tenant in tenants:
                quota = self._quotas.get(tenant)
                if quota is None:
                    continue
                used_series = used_points = 0
                for sid_, row in self._series.items():
                    if row["tenant"] == tenant:
                        used_series += 1
                        used_points += len(self._samples[sid_])
                tenant_states = [st for st in states.values()
                                 if st["row"]["tenant"] == tenant]
                add_series = sum(1 for st in tenant_states if not st["existed"])
                add_points = sum(len(st["new_stamps"]) for st in tenant_states)
                limit_series = quota["max_series"]
                limit_points = quota["max_points"]
                if add_series and limit_series is not None \
                        and used_series + add_series > limit_series:
                    raise ObsError(
                        "quota exceeded for tenant %r: series %d+%d > max_series %s"
                        % (tenant, used_series, add_series, limit_series))
                if add_points and limit_points is not None \
                        and used_points + add_points > limit_points:
                    raise ObsError(
                        "quota exceeded for tenant %r: points %d+%d > max_points %s"
                        % (tenant, used_points, add_points, limit_points))

            # Phase 4: commit. Pure-duplicate batches have nothing to write and
            # do not bump the writes counter.
            if total_written:
                registry_changed = False
                for sid in sorted(states):
                    state = states[sid]
                    if not state["new_stamps"] and not state["overwritten"]:
                        continue
                    merged = state["merged"]
                    if not state["existed"]:
                        self._series[sid] = state["row"]
                        self._samples[sid] = []
                        registry_changed = True
                    self._samples[sid] = sorted(merged.items())
                    if state["overwritten"]:
                        # Replaced values must not leave a timestamp on two
                        # physical lines; rewrite the whole file once.
                        self._rewrite_points(sid)
                    else:
                        fresh = sorted((stamp, merged[stamp])
                                       for stamp in state["new_stamps"])
                        with open(self._points_path(sid), "a",
                                  encoding="utf-8") as handle:
                            handle.write("".join(_line(ts, value)
                                                 for ts, value in fresh))
                            handle.flush()
                            os.fsync(handle.fileno())
                self._writes += 1
                if registry_changed:
                    self._save_registry()

        return {"written": total_written, "duplicates": total_duplicates,
                "results": results}

    # ------------------------------------------------------------------- query
    def _matching_series(self, tenant, metric, labels, compiled_matchers=()):
        """Snapshot of every matching series: sid, labels and raw samples.

        Copied under one lock acquisition so a query can never mix samples from
        before and after a concurrent write or retention sweep. Exact ``labels``
        and the compiled ``matchers`` both filter the same raw series labels
        before any aggregation or grouping takes place.
        """
        with self._lock:
            ids = sorted(sid for sid, row in self._series.items()
                         if row["tenant"] == tenant and row["metric"] == metric
                         and matches_labels(dict(row["labels"]), labels)
                         and matchers_hold(dict(row["labels"]), compiled_matchers))
            return [(sid, dict(self._series[sid]["labels"]), list(self._samples[sid]))
                    for sid in ids]

    def _samples_for(self, tenant, metric, labels, compiled_matchers=()):
        return [(sid, samples) for sid, _, samples in
                self._matching_series(tenant, metric, labels, compiled_matchers)]

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
              step_ms=None, agg=None, group_by=None, window_ms=None, matchers=None):
        """Raw, bucketed or sliding-window points per matching series.

        ``matchers`` is an optional list of ``{"key", "op", "value"}`` objects
        (``None`` or ``[]`` adds no condition). Supported ops: ``=``, ``!=``
        (exact whole-string equality/negation), ``=~``, ``!~`` (Python ``re``
        full-match/negation, case-sensitive, Unicode-aware). Matchers combine
        with each other and with ``labels`` by logical AND, apply to the raw
        series labels before bucketing/grouping (keys absent from
        ``group_by`` included), and are validated up front even when no series
        could match. A missing key fails ``=``/``=~`` and passes ``!=``/``!~``.

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

        With ``window_ms`` the query switches to sliding-window mode: it
        requires ``start_ms``, ``end_ms``, ``step_ms`` and ``agg``, all integer
        milliseconds (step and window strictly positive). Evaluation times are
        ``start_ms + k*step_ms`` not exceeding ``end_ms``; the point timestamp
        is the evaluation time and its value aggregates the raw samples in
        ``(t - window_ms, t]`` (left edge excluded, right edge included). The
        first window reads history before ``start_ms``, but never anything past
        ``t``. Empty windows, including ``count``, emit ``None``. Every matching
        series (or group) gets the full time grid even when every window is
        empty; no matching series yields an empty list.

        ``agg`` may also be ``increase`` or ``rate`` (sliding-window mode only;
        without ``window_ms`` they are rejected). These treat each matching
        series as a counter: per window, adjacent in-window readings contribute
        their difference when non-decreasing, and a drop is treated as a
        counter reset contributing only the later reading (the first reading
        contributes nothing). ``increase`` is the sum of those deltas, ``rate``
        that sum divided by the seconds between the window's first and last
        sample. Only samples actually inside the window are used: no boundary
        borrowing, no interpolation, no extrapolation. A window with fewer
        than two distinct sample timestamps yields ``None``; unchanged readings
        yield zero. Grouped queries compute each series independently and then
        sum the non-``None`` per-series results of the group (never
        differencing across series); a group whose series are all ``None`` at
        a time yields ``None`` there. Any matched sample that actually falls
        into an output window and is negative or non-finite fails the whole
        query with ``ObsError``; samples outside every window or filtered out
        by labels/matchers are never inspected.
        """
        # Compile before anything else so all matchers are validated even when
        # another argument (or the absence of candidate series) would yield an
        # empty result.
        compiled_matchers = compile_matchers(matchers)
        if agg is not None and agg not in AGGREGATES + COUNTER_AGGREGATES:
            raise ObsError("unknown aggregation: %r" % (agg,))
        if agg in COUNTER_AGGREGATES and window_ms is None:
            raise ObsError("aggregation %r requires window_ms" % (agg,))
        if window_ms is not None:
            lo, hi, step, window = self._window_params(
                start_ms, end_ms, step_ms, window_ms, agg)
            return self._query_windows(tenant, metric, labels, lo, hi, step,
                                       agg, group_by, window, compiled_matchers)
        if step_ms is not None and (not _num(step_ms) or int(step_ms) <= 0):
            raise ObsError("step_ms must be a positive number")
        lo = None if start_ms is None else int(start_ms)
        hi = None if end_ms is None else int(end_ms)
        if lo is not None and hi is not None and hi < lo:
            raise ObsError("end_ms must be >= start_ms")
        if group_by is not None:
            return self._query_grouped(tenant, metric, labels, lo, hi,
                                       step_ms, agg, group_by, compiled_matchers)
        out = []
        for sid, samples in self._samples_for(tenant, metric, labels,
                                              compiled_matchers):
            rows = [item for item in samples
                    if (lo is None or item[0] >= lo) and (hi is None or item[0] <= hi)]
            points = self._bucket(rows, step_ms, agg, lo, hi)
            if points is None or (not points and agg is not None and step_ms is None):
                continue
            out.append({"series_id": sid, "labels": dict(self._series[sid]["labels"]),
                        "points": points})
        return out

    @staticmethod
    def _window_params(start_ms, end_ms, step_ms, window_ms, agg):
        """Validate sliding-window arguments; return ``(start, end, step, window)``."""
        if not isinstance(window_ms, int) or isinstance(window_ms, bool) or window_ms <= 0:
            raise ObsError("window_ms must be a positive integer")
        if not isinstance(start_ms, int) or isinstance(start_ms, bool):
            raise ObsError("sliding-window query requires an integer start_ms")
        if not isinstance(end_ms, int) or isinstance(end_ms, bool):
            raise ObsError("sliding-window query requires an integer end_ms")
        if not isinstance(step_ms, int) or isinstance(step_ms, bool) or step_ms <= 0:
            raise ObsError("sliding-window query requires a positive integer step_ms")
        if end_ms < start_ms:
            raise ObsError("end_ms must be >= start_ms")
        if agg is None:
            raise ObsError("sliding-window query requires agg")
        return start_ms, end_ms, step_ms, window_ms

    def _window_grid(self, start_ms, end_ms, step_ms):
        """The ``start_ms + k*step_ms`` evaluation times not past ``end_ms``."""
        return range(start_ms, end_ms + 1, step_ms)

    def _window_values(self, rows, window, times, agg):
        """Aggregate the samples of ``(t - window, t]`` for each ``t``.

        ``rows`` is a timestamp-sorted sample list shared by every time. A
        sample at the evaluation time is included; one exactly one window
        length earlier is excluded. Empty windows (no samples, regardless of
        the aggregation, even ``count``) become ``None``.
        """
        points = []
        index = 0
        for t in times:
            left = t - window
            while index < len(rows) and rows[index][0] <= left:
                index += 1
            values = []
            for cursor in range(index, len(rows)):
                stamp = rows[cursor][0]
                if stamp > t:
                    break
                values.append(rows[cursor][1])
            points.append([t, None if not values else _aggregate(values, agg)])
        return points

    @staticmethod
    def _counter_value(windowed, agg):
        """``increase``/``rate`` of one series over one window's samples.

        ``windowed`` is the timestamp-sorted samples of a single series inside
        ``(t - window, t]``. Every sample actually in the window must be a
        non-negative finite number, else the whole query fails. Fewer than two
        samples (hence fewer than two distinct timestamps) yield ``None``.
        """
        for stamp, value in windowed:
            if not math.isfinite(value) or value < 0:
                raise ObsError(
                    "aggregation %r requires non-negative finite samples, "
                    "got %r at timestamp %d" % (agg, value, stamp))
        if len(windowed) < 2:
            return None
        total = 0.0
        previous = windowed[0][1]
        for _, value in windowed[1:]:
            # A drop means the counter reset: count only the later reading.
            total += value - previous if value >= previous else value
            previous = value
        if agg == "increase":
            return total
        span_seconds = (windowed[-1][0] - windowed[0][0]) / 1000.0
        return total / span_seconds

    def _window_counter_points(self, rows, window, times, agg):
        """Per-time ``increase``/``rate`` for one series over the time grid."""
        points = []
        index = 0
        for t in times:
            left = t - window
            while index < len(rows) and rows[index][0] <= left:
                index += 1
            windowed = []
            for cursor in range(index, len(rows)):
                stamp = rows[cursor][0]
                if stamp > t:
                    break
                windowed.append(rows[cursor])
            points.append([t, self._counter_value(windowed, agg)])
        return points

    def _query_windows(self, tenant, metric, labels, lo, hi, step_ms, agg,
                       group_by, window, compiled_matchers=()):
        if group_by is not None:
            if not isinstance(group_by, (list, tuple)):
                raise ObsError("group_by must be a list of label keys")
            keys = list(group_by)
            for key in keys:
                if not isinstance(key, str) or not key:
                    raise ObsError("group_by entries must be non-empty strings")
            if len(set(keys)) != len(keys):
                raise ObsError("group_by entries must not contain duplicates")
        times = list(self._window_grid(lo, hi, step_ms))
        matched = self._matching_series(tenant, metric, labels, compiled_matchers)
        counter = agg in COUNTER_AGGREGATES
        if group_by is None:
            out = []
            for sid, series_labels, samples in matched:
                if counter:
                    points = self._window_counter_points(samples, window, times, agg)
                else:
                    points = self._window_values(samples, window, times, agg)
                out.append({"series_id": sid, "labels": dict(series_labels),
                            "points": points})
            return out
        groups = {}
        for sid, series_labels, samples in matched:
            group_labels = {key: series_labels[key] for key in keys
                            if key in series_labels}
            token = tuple(sorted(group_labels.items()))
            entry = groups.setdefault(token, {"labels": group_labels, "rows": []})
            # The first window reads history before start_ms; no window reads
            # anything past its own evaluation time, so no upper filter applies.
            if counter:
                # Counter aggregates are computed per series, never across
                # series; the group sums the per-series results afterwards.
                entry["rows"].append(
                    self._window_counter_points(samples, window, times, agg))
            else:
                entry["rows"].extend(samples)
        out = []
        for token in sorted(groups):
            entry = groups[token]
            if counter:
                points = []
                for position, t in enumerate(times):
                    values = [column[position][1] for column in entry["rows"]
                              if column[position][1] is not None]
                    points.append([t, sum(values) if values else None])
            else:
                rows = sorted(entry["rows"])
                points = self._window_values(rows, window, times, agg)
            out.append({"labels": entry["labels"], "points": points})
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

    def _query_grouped(self, tenant, metric, labels, lo, hi, step_ms, agg, group_by,
                       compiled_matchers=()):
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
        for sid, samples in self._samples_for(tenant, metric, labels,
                                              compiled_matchers):
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
