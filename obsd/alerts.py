"""obsd.alerts - rules, evaluation, silence/inhibition, dedup and SLO math.

All time is injected through ``now_ms``; nothing here reads the wall clock.
"""

from __future__ import annotations

import copy
import json
import os
import threading

from .tsdb import ObsError, _atomic_write, _labels_list, matches_labels

__all__ = ["AlertEngine", "COMPARATORS", "AGGREGATES", "SEVERITIES", "STATE_FIRING",
           "STATE_SILENCED", "STATE_INHIBITED", "STATE_RESOLVED", "EVENTS"]
COMPARATORS = (">", ">=", "<", "<=", "==", "!=")
AGGREGATES = ("sum", "avg", "min", "max", "count")
SEVERITIES = ("info", "warning", "critical")
SEVERITY_RANK = {"info": 1, "warning": 2, "critical": 3}
STATE_FIRING = "firing"
STATE_SILENCED = "silenced"
STATE_INHIBITED = "inhibited"
STATE_RESOLVED = "resolved"
ACTIVE_STATES = (STATE_FIRING, STATE_SILENCED, STATE_INHIBITED)
EVENTS = ("firing", "resolved")
_FILES = ("rules", "alerts", "slos", "silences", "inhibitions", "counters",
          "routes", "notifications", "notify_state")


def _is_num(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)
def _require_int(value, field):
    if not _is_num(value) or int(value) != value:
        raise ObsError("%s must be an integer" % field)
    return int(value)
def compare(value, comparator, threshold):
    """Apply ``comparator`` to a bucket value; ``None`` never satisfies."""
    if value is None:
        return False
    if comparator == ">":
        return value > threshold
    if comparator == ">=":
        return value >= threshold
    if comparator == "<":
        return value < threshold
    if comparator == "<=":
        return value <= threshold
    if comparator == "==":
        return value == threshold
    if comparator == "!=":
        return value != threshold
    raise ObsError("unknown comparator: %r" % (comparator,))
class AlertEngine:
    """Rule evaluation with dedup, silences, inhibition and SLO tracking.

    Persisted under ``root`` as rules.json, alerts.json, slos.json,
    silences.json, inhibitions.json, counters.json, routes.json,
    notifications.json and notify_state.json (atomic writes); a new
    instance reloads all of it, so alerts and rules survive a restart.
    """

    def __init__(self, store, root):
        if store is None:
            raise ObsError("store is required")
        if not isinstance(root, (str, os.PathLike)) or not str(root):
            raise ObsError("root must be a non-empty path")
        self.store = store
        self.root = os.path.abspath(str(root))
        self._lock = threading.RLock()
        self._rules, self._silences, self._inhibitions, self._slos, self._alerts = {}, {}, {}, {}, {}
        self._routes, self._notifications, self._notify_state = {}, {}, {}
        self._counters = {"rule": 0, "silence": 0, "inhibition": 0, "alert": 0,
                          "route": 0, "notification": 0}
        os.makedirs(self.root, exist_ok=True)
        self._load()
    # ------------------------------------------------------------- persistence
    def _path(self, name):
        return os.path.join(self.root, name + ".json")
    def _read(self, name, default):
        path = self._path(name)
        if not os.path.exists(path):
            return default
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, ValueError) as exc:
            raise ObsError("cannot read %s.json: %s" % (name, exc))
    def _write(self, name, rows):
        _atomic_write(self._path(name), json.dumps(rows, sort_keys=True, separators=(",", ":")))
    def _dump(self, name, mapping):
        self._write(name, [mapping[key] for key in sorted(mapping)])
    def _load(self):
        rules = self._read("rules", [])
        for rule in rules.values() if isinstance(rules, dict) else rules:
            self._rules[rule["id"]] = rule
        for row in self._read("silences", []):
            self._silences[row["id"]] = row
        for row in self._read("inhibitions", []):
            self._inhibitions[row["id"]] = row
        for row in self._read("slos", []):
            self._slos[(row["tenant"], row["name"])] = row
        for row in self._read("alerts", []):
            row["firing"] = bool(row.get("firing")) and row.get("state") != STATE_RESOLVED
            self._alerts[(row["rule_id"], row["series_id"])] = row
        for row in self._read("routes", []):
            self._routes[row["id"]] = row
        for row in self._read("notifications", []):
            self._notifications[row["id"]] = row
        for row in self._read("notify_state", []):
            self._notify_state[(row["route_id"], row["alert_id"])] = {
                "firing": bool(row.get("firing")), "last_ms": row.get("last_ms")}
        for key, value in self._read("counters", {}).items():
            if key in self._counters:
                self._counters[key] = max(int(value), self._counters[key])
    # ------------------------------------------------------------------ rules
    def add_rule(self, rule):
        """Validate and store a rule; ``id`` is generated when omitted."""
        if not isinstance(rule, dict):
            raise ObsError("rule must be an object")
        with self._lock:
            self._counters["rule"] += 1
            rid = rule.get("id") or "rule-%04d" % self._counters["rule"]
            clean = self._validate(rule, rid)
            self._rules[rid] = clean
            self._dump("rules", self._rules)
        return copy.deepcopy(clean)
    def _validate(self, rule, rid):
        if not isinstance(rid, str) or not rid:
            raise ObsError("rule id must be a non-empty string")
        for field in ("tenant", "metric"):
            if not isinstance(rule.get(field), str) or not rule[field]:
                raise ObsError("rule %s must be a non-empty string" % field)
        for field, allowed in (("comparator", COMPARATORS), ("agg", AGGREGATES),
                               ("severity", SEVERITIES)):
            if rule.get(field) not in allowed:
                raise ObsError("rule %s must be one of %s" % (field, allowed))
        if not _is_num(rule.get("threshold")):
            raise ObsError("rule threshold must be a number")
        window = _require_int(rule.get("window_ms"), "rule window_ms")
        if window <= 0:
            raise ObsError("rule window_ms must be positive")
        hold = _require_int(rule.get("for_ms", 0), "rule for_ms")
        if hold < 0:
            raise ObsError("rule for_ms must be >= 0")
        annotations = rule.get("annotations") or {}
        if not isinstance(annotations, dict):
            raise ObsError("rule annotations must be an object")
        return {"id": rid, "tenant": rule["tenant"], "metric": rule["metric"],
                "labels": _labels_list(rule.get("labels")), "comparator": rule["comparator"],
                "threshold": float(rule["threshold"]), "for_ms": hold, "window_ms": window,
                "agg": rule["agg"], "severity": rule["severity"],
                "annotations": {str(k): str(v) for k, v in annotations.items()},
                "error": rule.get("error")}
    def list_rules(self, tenant=None):
        with self._lock:
            rows = [copy.deepcopy(self._rules[key]) for key in sorted(self._rules)]
        return [row for row in rows if tenant is None or row["tenant"] == tenant]
    def del_rule(self, rule_id):
        with self._lock:
            if rule_id not in self._rules:
                raise ObsError("unknown rule: %s" % rule_id)
            del self._rules[rule_id]
            self._dump("rules", self._rules)
        return {"deleted": rule_id}
    # --------------------------------------------------------------- silences
    def add_silence(self, tenant, matcher_labels, starts_ms, ends_ms, reason=""):
        """Record a silence window; it suppresses matching alerts inside it."""
        if not isinstance(tenant, str) or not tenant:
            raise ObsError("silence tenant must be a non-empty string")
        start = _require_int(starts_ms, "silence starts_ms")
        end = _require_int(ends_ms, "silence ends_ms")
        if end <= start:
            raise ObsError("silence ends_ms must be greater than starts_ms")
        with self._lock:
            self._counters["silence"] += 1
            row = {"id": "silence-%04d" % self._counters["silence"], "tenant": tenant,
                   "labels": _labels_list(matcher_labels), "starts_ms": start,
                   "ends_ms": end, "reason": str(reason or "")}
            self._silences[row["id"]] = row
            self._dump("silences", self._silences)
        return copy.deepcopy(row)
    def list_silences(self, tenant=None):
        with self._lock:
            rows = [copy.deepcopy(self._silences[key]) for key in sorted(self._silences)]
        return [row for row in rows if tenant is None or row["tenant"] == tenant]
    def _active_silence(self, tenant, labels, now_ms):
        for key in sorted(self._silences):
            row = self._silences[key]
            if row["tenant"] == tenant and row["starts_ms"] <= now_ms <= row["ends_ms"] \
                    and matches_labels(labels, dict(row["labels"])):
                return row
        return None
    # ------------------------------------------------------------- inhibition
    def add_inhibition(self, source_severity, target_severity, same_labels=True):
        """Higher-severity firing alerts suppress matching lower-severity ones.

        ``same_labels`` must be a real boolean (omitted defaults to ``True``);
        ``None``, numbers, strings and other types raise ``ObsError`` before
        any id is assigned, any counter incremented or ``inhibitions.json``
        written.
        """
        for field, value in (("source_severity", source_severity),
                             ("target_severity", target_severity)):
            if value not in SEVERITIES:
                raise ObsError("%s must be one of %s" % (field, SEVERITIES))
        if SEVERITY_RANK[source_severity] <= SEVERITY_RANK[target_severity]:
            raise ObsError("source_severity must be strictly higher than target_severity")
        if not isinstance(same_labels, bool):
            raise ObsError("same_labels must be a boolean")
        with self._lock:
            self._counters["inhibition"] += 1
            row = {"id": "inhibition-%04d" % self._counters["inhibition"],
                   "source_severity": source_severity, "target_severity": target_severity,
                   "same_labels": same_labels}
            self._inhibitions[row["id"]] = row
            self._dump("inhibitions", self._inhibitions)
        return copy.deepcopy(row)
    def list_inhibitions(self):
        with self._lock:
            return [copy.deepcopy(self._inhibitions[key]) for key in sorted(self._inhibitions)]
    def _inhibited(self, severity, tenant, labels, rule_id, series_id):
        """Id of the firing alert that inhibits this one, or None.

        Equal label sets (the empty set included) are required when
        ``same_labels`` is set; otherwise the source set must be a subset.
        """
        wanted = frozenset((key, str(value)) for key, value in labels.items())
        rules = [row for row in self._inhibitions.values() if row["target_severity"] == severity]
        if not rules:
            return None
        for key in sorted(self._alerts):
            other = self._alerts[key]
            if other["state"] != STATE_FIRING or other["tenant"] != tenant:
                continue
            if (other["rule_id"], other["series_id"]) == (rule_id, series_id):
                continue
            other_labels = frozenset((k, str(v)) for k, v in other["labels"].items())
            for row in rules:
                if SEVERITY_RANK[other["severity"]] < SEVERITY_RANK[row["source_severity"]]:
                    continue
                if other_labels == wanted or (not row["same_labels"] and other_labels <= wanted):
                    return other["id"]
        return None
    # -------------------------------------------------------- notification routes
    def add_route(self, route):
        """Validate and store a notification route; ``id`` is generated when omitted."""
        if not isinstance(route, dict):
            raise ObsError("route must be an object")
        with self._lock:
            clean = self._validate_route(route)
            rid = clean["id"]
            if rid is None:
                while True:
                    self._counters["route"] += 1
                    rid = "route-%04d" % self._counters["route"]
                    if rid not in self._routes:
                        break
                clean["id"] = rid
            elif rid in self._routes:
                raise ObsError("conflict: route id already exists: %s" % rid)
            self._routes[rid] = clean
            self._dump("routes", self._routes)
        return copy.deepcopy(clean)
    def _validate_route(self, route):
        rid = route.get("id")
        if rid is not None and (not isinstance(rid, str) or not rid):
            raise ObsError("route id must be a non-empty string")
        for field in ("tenant", "target"):
            if not isinstance(route.get(field), str) or not route[field]:
                raise ObsError("route %s must be a non-empty string" % field)
        severities = self._route_choice(route.get("severities"), SEVERITIES, "severities")
        events = self._route_choice(route.get("events"), EVENTS, "events")
        repeat = route.get("repeat_ms")
        if repeat is not None:
            repeat = _require_int(repeat, "route repeat_ms")
            if repeat < 0:
                raise ObsError("route repeat_ms must be >= 0")
        return {"id": rid, "tenant": route["tenant"], "target": route["target"],
                "labels": _labels_list(route.get("labels")), "severities": severities,
                "events": events, "repeat_ms": repeat}
    @staticmethod
    def _route_choice(value, allowed, field):
        """A route selector: absent means all of ``allowed``; otherwise a non-empty
        array of allowed values, normalised to ``allowed`` order without duplicates."""
        if value is None:
            return list(allowed)
        if not isinstance(value, list) or not value:
            raise ObsError("route %s must be a non-empty array" % field)
        for item in value:
            if item not in allowed:
                raise ObsError("route %s must be one of %s" % (field, allowed))
        return [item for item in allowed if item in value]
    def list_routes(self, tenant=None):
        with self._lock:
            rows = [copy.deepcopy(self._routes[key]) for key in sorted(self._routes)]
        return [row for row in rows if tenant is None or row["tenant"] == tenant]
    def del_route(self, route_id):
        """Delete a route; its historical notifications are kept."""
        with self._lock:
            if route_id not in self._routes:
                raise ObsError("unknown route: %s" % route_id)
            del self._routes[route_id]
            self._dump("routes", self._routes)
        return {"deleted": route_id}
    # ------------------------------------------------------------- notifications
    def _matching_routes(self, alert):
        return [self._routes[rid] for rid in sorted(self._routes)
                if self._routes[rid]["tenant"] == alert["tenant"]
                and alert["severity"] in self._routes[rid]["severities"]
                and matches_labels(alert["labels"], dict(self._routes[rid]["labels"]))]
    def _route_event(self, alert, kind, now):
        """Apply one alert transition to every matching route's dedup state.

        ``kind`` is ``"firing"`` (alert is firing), ``"resolved"`` (alert just
        resolved) or ``None`` (pending, silenced or inhibited: no notification,
        and the firing episode ends so a later recovery re-notifies).
        """
        for route in self._matching_routes(alert):
            key = (route["id"], alert["id"])
            state = self._notify_state.get(key)
            if kind is None:
                if state is not None:
                    state["firing"] = False
                continue
            if state is None:
                state = self._notify_state[key] = {"firing": False, "last_ms": None}
            if kind == "firing":
                if "firing" in route["events"]:
                    if not state["firing"]:
                        self._emit_notification(route, alert, "firing", now)
                        state["last_ms"] = now
                    elif route["repeat_ms"] is not None and state["last_ms"] is not None \
                            and now - state["last_ms"] >= route["repeat_ms"]:
                        self._emit_notification(route, alert, "firing", now)
                        state["last_ms"] = now
                state["firing"] = True
            else:  # resolved: exactly once per resolution
                if "resolved" in route["events"]:
                    self._emit_notification(route, alert, "resolved", now)
                state["firing"] = False
    def _emit_notification(self, route, alert, event, now):
        self._counters["notification"] += 1
        row = {"id": "notification-%05d" % self._counters["notification"],
               "route_id": route["id"], "alert_id": alert["id"],
               "tenant": route["tenant"], "target": route["target"], "event": event,
               "created_ms": now, "alert": copy.deepcopy(alert), "acked": False}
        self._notifications[row["id"]] = row
    def _save_notify_state(self):
        self._write("notify_state",
                    [{"route_id": key[0], "alert_id": key[1],
                      "firing": state["firing"], "last_ms": state["last_ms"]}
                     for key, state in sorted(self._notify_state.items())])
    def list_notifications(self, tenant=None, route_id=None, alert_id=None, acked=None):
        """Filtered read of the notification queue; never consumes entries."""
        with self._lock:
            rows = [copy.deepcopy(self._notifications[key]) for key in sorted(self._notifications)]
        return [row for row in rows
                if (tenant is None or row["tenant"] == tenant)
                and (route_id is None or row["route_id"] == route_id)
                and (alert_id is None or row["alert_id"] == alert_id)
                and (acked is None or row["acked"] == acked)]
    def ack_notification(self, notification_id):
        """Mark a notification acknowledged; re-acking returns the same record."""
        with self._lock:
            row = self._notifications.get(notification_id)
            if row is None:
                raise ObsError("unknown notification: %s" % notification_id)
            if not row["acked"]:
                row["acked"] = True
                self._dump("notifications", self._notifications)
            return copy.deepcopy(row)
    # --------------------------------------------------------------- evaluate
    def _condition(self, rule, series_id, now_ms):
        """Buckets of one series inside the rule window, plus the hold decision.

        The window is ``[bucket_start(now_ms) - window_ms, now_ms]`` with
        ``bucket_start(t) = t - (t % window_ms)``: the running bucket plus the
        whole previous one. The condition holds when every non-empty bucket in
        the window satisfies the comparator; ``held_ms`` spans the unbroken run
        of satisfying buckets ending at the last bucket.
        """
        step = rule["window_ms"]
        buckets = labels = None
        for item in self.store.query(
                rule["tenant"], rule["metric"], labels=dict(rule["labels"]),
                start_ms=now_ms - (now_ms % step) - step, end_ms=now_ms,
                step_ms=step, agg=rule["agg"]):
            if item["series_id"] == series_id:
                buckets, labels = item["points"], item["labels"]
                break
        values = [point[1] for point in buckets or [] if point[1] is not None]
        if not values:
            return None
        satisfied = [compare(value, rule["comparator"], rule["threshold"]) for value in values]
        if not all(satisfied):
            return {"hold": False, "labels": labels, "observed": values[-1]}
        run = 0
        for index in range(len(satisfied) - 1, -1, -1):
            if not satisfied[index]:
                run = index + 1
                break
        held = int(buckets[-1][0]) - int(buckets[run][0])
        return {"hold": True, "labels": labels, "observed": values[-1],
                "run_start": int(buckets[run][0]), "held_ms": held,
                "for_satisfied": held >= rule["for_ms"]}
    def evaluate(self, now_ms):
        """Evaluate every rule at ``now_ms`` -> firing/silenced/inhibited/resolved."""
        now = _require_int(now_ms, "now_ms")
        out = {STATE_FIRING: [], STATE_SILENCED: [], STATE_INHIBITED: [], STATE_RESOLVED: []}
        with self._lock:
            for rule in self.list_rules():
                if rule.get("error"):
                    raise ObsError("cannot evaluate rule %s: %s" % (rule["id"], rule["error"]))
                step = rule["window_ms"]
                for sid, _ in self.store._scan(
                        rule["tenant"], rule["metric"], dict(rule["labels"]),
                        now - (now % step) - step, now):
                    state = self._condition(rule, sid, now)
                    if state is None:
                        continue
                    key = (rule["id"], sid)
                    alert = self._alerts.get(key)
                    if not state["hold"]:
                        if alert is not None and alert["state"] in ACTIVE_STATES:
                            alert.update(state=STATE_RESOLVED, firing=False, resolved_ms=now)
                            out[STATE_RESOLVED].append(copy.deepcopy(alert))
                            self._route_event(alert, "resolved", now)
                        continue
                    if alert is None:
                        alert = {"id": self._next_alert_id(), "rule_id": rule["id"],
                                 "tenant": rule["tenant"], "metric": rule["metric"],
                                 "series_id": sid, "labels": dict(state["labels"]),
                                 "severity": rule["severity"], "occurrences": 0,
                                 "annotations": copy.deepcopy(rule["annotations"]),
                                 "state": STATE_FIRING}
                        self._alerts[key] = alert
                    alert.update(occurrences=int(alert.get("occurrences", 0)) + 1, firing=True,
                                 since_ms=state["run_start"], last_eval_ms=now,
                                 observed=state["observed"], held_ms=state["held_ms"])
                    if not state["for_satisfied"]:
                        self._route_event(alert, None, now)
                        continue
                    silence = self._active_silence(rule["tenant"], state["labels"], now)
                    blocker = None if silence else self._inhibited(
                        alert["severity"], rule["tenant"], state["labels"], rule["id"], sid)
                    if silence is not None:
                        alert.update(state=STATE_SILENCED, silenced_by=silence["id"],
                                     silence_reason=silence["reason"])
                        out[STATE_SILENCED].append(copy.deepcopy(alert))
                        self._route_event(alert, None, now)
                    elif blocker is not None:
                        alert.update(state=STATE_INHIBITED, inhibited_by=blocker)
                        out[STATE_INHIBITED].append(copy.deepcopy(alert))
                        self._route_event(alert, None, now)
                    else:
                        alert.update(state=STATE_FIRING)
                        alert.pop("silenced_by", None)
                        alert.pop("inhibited_by", None)
                        out[STATE_FIRING].append(copy.deepcopy(alert))
                        self._route_event(alert, "firing", now)
            self._write("alerts", [self._alerts[k] for k in
                                   sorted(self._alerts, key=lambda k: (k[0], k[1]))])
            self._dump("notifications", self._notifications)
            self._save_notify_state()
            self._write("counters", self._counters)
        for rows in out.values():
            rows.sort(key=lambda row: row["id"])
        return out
    def _next_alert_id(self):
        self._counters["alert"] += 1
        return "alert-%05d" % self._counters["alert"]
    # ----------------------------------------------------------------- alerts
    def list_alerts(self, tenant=None, state=None):
        with self._lock:
            rows = [copy.deepcopy(self._alerts[key]) for key in sorted(self._alerts)]
        return [row for row in rows
                if (tenant is None or row["tenant"] == tenant)
                and (state is None or row["state"] == state)]
    def get_alert(self, alert_id):
        with self._lock:
            for row in self._alerts.values():
                if row["id"] == alert_id:
                    return copy.deepcopy(row)
        raise ObsError("unknown alert: %s" % alert_id)
    # -------------------------------------------------------------------- SLO
    def set_slo(self, tenant, name, metric, labels, good_comparator, threshold,
                target_ratio, window_ms):
        """Define a good-events ratio SLO over a sliding ``window_ms``."""
        for field, value in (("tenant", tenant), ("name", name), ("metric", metric)):
            if not isinstance(value, str) or not value:
                raise ObsError("slo %s must be a non-empty string" % field)
        if good_comparator not in COMPARATORS:
            raise ObsError("good_comparator must be one of %s" % (COMPARATORS,))
        if not _is_num(threshold):
            raise ObsError("slo threshold must be a number")
        if not _is_num(target_ratio) or not 0.0 < float(target_ratio) <= 1.0:
            raise ObsError("target_ratio must be in (0, 1]")
        window = _require_int(window_ms, "slo window_ms")
        if window <= 0:
            raise ObsError("slo window_ms must be positive")
        row = {"tenant": tenant, "name": name, "metric": metric,
               "labels": _labels_list(labels), "good_comparator": good_comparator,
               "threshold": float(threshold), "target_ratio": float(target_ratio),
               "window_ms": window}
        with self._lock:
            self._slos[(tenant, name)] = row
            self._dump("slos", self._slos)
        return copy.deepcopy(row)
    def _find_slo(self, name, tenant=None):
        with self._lock:
            if tenant is not None and (tenant, name) in self._slos:
                return copy.deepcopy(self._slos[(tenant, name)])
            matches = [self._slos[key] for key in sorted(self._slos) if key[1] == name]
        if not matches:
            raise ObsError("unknown slo: %s" % name)
        if len(matches) > 1 and tenant is None:
            raise ObsError("slo %s exists for several tenants; pass tenant" % name)
        return matches[0]
    def list_slos(self, tenant=None):
        with self._lock:
            rows = [copy.deepcopy(self._slos[key]) for key in sorted(self._slos)]
        return [row for row in rows if tenant is None or row["tenant"] == tenant]
    def slo_status(self, name, now_ms, tenant=None, read_token=None):
        """Event-ratio SLO status over ``[now_ms - window_ms, now_ms]``.

        Every stored sample in the window is one event: ``total`` counts them,
        ``good`` counts those satisfying the good comparator, ``bad = total -
        good``.

            ratio        = good / total                            (0.0 if total == 0)
            error_budget = max(0, 1 - (1 - ratio) / (1 - target))  (target < 1)
            burn_rate    = (bad / total) / (1 - target)            (0.0 if target >= 1)
            met          = ratio >= target

        With ``read_token`` given the token is validated against the SLO's
        tenant (see ``SeriesStore.check_read_token``) before any sample is
        read; the budget math then runs on one coherent snapshot and never
        changes the revision.
        """
        now = _require_int(now_ms, "now_ms")
        slo = self._find_slo(name, tenant)
        if read_token is not None:
            self.store.check_read_token(read_token, slo["tenant"])
        total = good = 0
        for _, rows in self.store._scan(slo["tenant"], slo["metric"], dict(slo["labels"]),
                                        now - slo["window_ms"], now):
            for _, value in rows:
                total += 1
                if compare(value, slo["good_comparator"], slo["threshold"]):
                    good += 1
        bad = total - good
        target = float(slo["target_ratio"])
        ratio = (good / float(total)) if total else 0.0
        allowed_bad = 1.0 - target
        if allowed_bad > 0.0:
            error_budget = max(0.0, 1.0 - ((1.0 - ratio) / allowed_bad))
        else:
            error_budget = 1.0 if ratio >= 1.0 else 0.0
        burn_rate = ((bad / float(total)) / allowed_bad) if (allowed_bad > 0.0 and total) else 0.0
        return {"tenant": slo["tenant"], "name": slo["name"], "window_ms": slo["window_ms"],
                "total": total, "good": good, "bad": bad, "ratio": ratio,
                "target_ratio": target, "error_budget": error_budget,
                "burn_rate": burn_rate, "met": ratio >= target}
