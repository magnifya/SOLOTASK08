# obsd

Minimal, dependency-free backend skeleton for **metrics ingestion, alerting and
SLO tracking**. Python 3.10+, standard library only, offline, deterministic
(every time-dependent entry point takes an injected `now_ms`).

The lane goal is a metrics collection, alerting and SLO backend that grows
towards time-series writes and downsampling, label indexing and a query subset,
aggregation and sliding windows, alert rules with silencing and inhibition,
notification routing and dedup, SLO and error-budget math, retention and
compaction, multi-tenant quota, access control and audit, and consistent reads
under high availability. This seed implements the first, smallest *real* slice of
each of those areas.

## Run

```bash
# HTTP API (data dir flag is global and comes BEFORE the subcommand)
python3 -m obsd --data-dir ./obsd_data serve --host 127.0.0.1 --port 8080

# test suite
python3 -m unittest discover -s tests -v
```

Both commands are run offline with the system interpreter; no third-party
package is imported anywhere.

## CLI

`python3 -m obsd [--data-dir ./obsd_data] <command> ...` prints exactly one line
of JSON on stdout. On error it prints one line of JSON (`{"error":"..."}`) on
stderr and exits non-zero.

| Command | Example |
| --- | --- |
| `serve` | `python3 -m obsd --data-dir ./obsd_data serve --host 127.0.0.1 --port 8080` |
| `quota-set` | `python3 -m obsd quota-set --tenant acme --max-series 1000 --max-points 1000000` |
| `quota-get` | `python3 -m obsd quota-get --tenant acme` |
| `write` | `python3 -m obsd write --tenant acme --metric latency_ms --label host=a --sample 1000:12.5 --now-ms 2000` |
| `query` | `python3 -m obsd query --tenant acme --metric latency_ms --start 0 --end 5000 --step 1000 --agg avg` |
| `query` (grouped) | `python3 -m obsd query --tenant acme --metric latency_ms --agg avg --group-by '["host"]'` |
| `query` (matchers) | `python3 -m obsd query --tenant acme --metric latency_ms --matchers '[{"key":"host","op":"=~","value":"api-.*"}]'` |
| `query` (sliding window) | `python3 -m obsd query --tenant acme --metric latency_ms --start 0 --end 5000 --step 1000 --window-ms 5000 --agg avg` |
| `rule-add` | `python3 -m obsd rule-add --tenant acme --metric latency_ms --comparator "<" --threshold 10 --window-ms 60000 --for-ms 30000 --agg avg --severity warning` |
| `eval` | `python3 -m obsd eval --now-ms 68000` |
| `alerts` | `python3 -m obsd alerts --tenant acme --state firing` |
| `silence-add` | `python3 -m obsd silence-add --tenant acme --starts-ms 0 --ends-ms 90000 --reason maintenance` |
| `slo set` | `python3 -m obsd slo set --tenant acme --name availability --metric latency_ms --good-comparator ">=" --threshold 1 --target-ratio 0.9 --window-ms 60000` |
| `slo status` | `python3 -m obsd slo status --name availability --now-ms 60000` |

## HTTP API

Errors are always JSON: `{"error":"..."}` with status 400 (bad request),
404 (unknown resource) or 409 (write conflict or quota exceeded).

| Method | Path | Request | Response |
| --- | --- | --- | --- |
| GET | `/healthz` | – | `200 {"ok":true}` |
| POST | `/v1/series` | `{"tenant","metric","labels","samples":[[ts,value],...]}` | `202 {"written":n,"duplicates":m,"series_id":"..."}`, `409` on conflicting timestamp or quota exceeded |
| GET | `/v1/query` | `?tenant=&metric=&label.k=v&start=&end=&step=&agg=&group_by=&window=&matchers=` (group_by and matchers are JSON arrays; matchers holds `{"key","op","value"}` objects) | `200 {"series":[{"labels":{...},"points":[[ts,value\|null],...]}]}` |
| POST | `/v1/quotas` | `{"tenant","max_series":n\|null,"max_points":n\|null}` | `200 {"tenant","max_series","max_points","series","points"}`; invalid tenant/limits give `400` and leave config untouched |
| GET | `/v1/quotas` | `?tenant=` | `200 {"tenant","max_series","max_points","series","points"}` (unconfigured tenant reports `null` limits and real usage) |
| POST | `/v1/rules` | rule object | `201` stored rule |
| GET | `/v1/rules` | `?tenant=` | `200 {"rules":[...]}` |
| POST | `/v1/evaluate` | `{"now_ms":n}` | `200 {"firing":[...],"silenced":[...],"inhibited":[...],"resolved":[...]}` |
| GET | `/v1/alerts` | `?tenant=&state=` | `200 {"alerts":[...]}` |
| POST | `/v1/silences` | `{"tenant","labels","starts_ms","ends_ms","reason"}` | `201` stored silence |
| POST | `/v1/inhibitions` | `{"source_severity","target_severity","same_labels"}` | `201` stored inhibition |
| POST | `/v1/slos` | `{"tenant","name","metric","labels","good_comparator","threshold","target_ratio","window_ms"}` | `201` stored SLO |
| GET | `/v1/slos/status` | `?tenant=&name=&now_ms=` | `200` SLO status object |
| GET | `/v1/stats` | – | `200 {"store":{...},"alerts":n}` |

## Data model

A series identity is `(tenant, metric, sorted(label pairs))`. The canonical
identity string joins the tenant, the metric and every `key=value` label pair
(sorted by key) with `\n`:

```
acme\nlatency_ms\nhost=a\nregion=eu
```

`series_id = sha256(canonical_identity).hexdigest()`, so the same logical series
always maps to the same 64-character id, independent of label insertion order.

Storage layout (all writes atomic via temp file + `os.replace`):

```
<data-dir>/series.json              registry: series_id -> tenant/metric/labels
<data-dir>/points/<series_id>.jsonl one {"t":ts,"v":value} object per line
<data-dir>/quotas.json              tenant -> {"max_series","max_points"} limits
<data-dir>/rules.json               <data-dir>/alerts.json
<data-dir>/slos.json                <data-dir>/silences.json
<data-dir>/inhibitions.json         <data-dir>/counters.json
```

A directory written before quotas existed simply has no `quotas.json`, which
means every tenant is unlimited; limits and usage are reloaded together on
construction so a reopen always reports a consistent snapshot.

`SeriesStore` and `AlertEngine` each guard their state with a
`threading.RLock`, so the threaded HTTP server can serve concurrent requests; all
state is reloaded from disk on construction, so a restart preserves series,
points, rules, alerts, silences, inhibitions and SLOs.

## Semantics and formulas

**Idempotent write.** `write(tenant, metric, labels, samples, now=None,
overwrite=False)` stores `(timestamp_ms, value)` pairs. Writing an existing
`(series, timestamp)` with the *same* value is a duplicate: it changes nothing
and is counted in `duplicates`. A *different* value for a stored timestamp is a
conflict: it raises `ObsError` (`409` over HTTP) unless `overwrite=True`, in
which case the stored value is replaced and counted in `written`. A timestamp
greater than the injected `now` (when `now` is given) is rejected.

**Bucketing.** `query(..., step_ms=step, agg=agg)` assigns a sample with
timestamp `t` to bucket `b = t - (t % step)`, i.e. buckets are
left-closed/right-open (`[b, b+step)`) on epoch multiples. Every bucket from the
first non-empty bucket to the last non-empty bucket that also lies inside
`[start_ms, end_ms]` is emitted, and buckets with no sample are emitted as
`[b, null]`. Bucket value = `agg` over the samples in the bucket, where `agg` is
one of `sum`, `avg`, `min`, `max`, `count`. A series whose range contains no
sample is still listed with an empty `points` list. With `agg` and *no*
`step_ms` the whole `[start_ms, end_ms]` range is a single bucket. With neither,
raw samples are returned in `[timestamp, value]` order. `rollup(tenant, metric,
labels, window_ms, agg)` is `query` with `step_ms = window_ms` over the whole
stored range, with empty buckets dropped; output is ordered by `series_id`, so
it is deterministic.

**Label matchers.** In addition to exact `labels` / `label.k=v` filtering,
`query` accepts `matchers` (`matchers` on `GET /v1/query`, `--matchers` on the
CLI): a JSON array of objects containing exactly `key` (non-empty string),
`op` and `value` (string, may be empty). Ops are `=` / `!=` (exact whole-string
equality and negation) and `=~` / `!~` (Python `re` syntax matched against the
*whole* label value via full match, case-sensitive and Unicode-aware, and its
negation). Omitting the parameter, passing Python `None`, or sending `[]` adds
no condition; an explicit JSON `null` or empty text is rejected (HTTP/CLI).
Matchers combine with each other and with the exact label filters by logical
AND — including several matchers on the same key — apply to raw series labels
before bucketing or grouping (labels absent from `group_by` count too), and are
restricted to the requested `tenant` and `metric`; `tenant`, `metric` and
`series_id` are not implicit labels. A missing key never satisfies `=`/`=~` but
always satisfies `!=`/`!~`, so `=~ ""` selects only series where the key exists
with an empty value (grouping still separates a missing key from an empty
value). `api-.*` matches `api-a` but not `xapi-a`. Duplicate matchers never
duplicate series, matcher order is irrelevant, and contradictory matchers
yield an empty result. Invalid JSON, a non-array, non-object elements, missing
or extra fields, wrong types, an empty `key`, an unsupported `op` or an invalid
regex raise `ObsError` (`400` over HTTP, one JSON error line on stderr for the
CLI); every matcher is validated even when no candidate series exists.

**Grouped query.** `query(..., agg=agg, group_by=[keys...])` aggregates *across*
series: matching series (same tenant/metric/label filters and `[start_ms,
end_ms]` range as above) are bucketed into groups by the values of the given
label keys, and the raw in-range samples of all series in a group are pooled
before `agg` is applied — samples from different series at the same timestamp
count individually. `group_by=[]` merges every matching series into one group.
Each result row carries only `labels` (the group keys actually present on the
group's series — a missing key and an empty-string value form different
groups) and `points`; rows are ordered lexicographically by their sorted label
pairs. Bucketing follows the same rules as the per-series query (epoch-aligned
left-closed buckets with `null` gaps, or a single point at the group's
earliest sample without `step_ms`). A group whose series have no sample in
range is still listed with empty `points`; no matching series at all yields an
empty `series` array. `group_by` must be a list of distinct non-empty strings
and requires `agg`; violations raise `ObsError` (`400` over HTTP, one JSON
error line on stderr for the CLI).

**Sliding-window query.** `query(..., start_ms=start, end_ms=end, step_ms=step,
agg=agg, window_ms=window)` switches to a separate mode the moment `window_ms`
is supplied (it is optional in every binding: `window` on `GET /v1/query`,
`--window-ms` on the CLI). In this mode `start_ms`, `end_ms`, `step_ms` and
`agg` are all required; `start_ms` and `end_ms` must be integer milliseconds,
`step_ms` and `window_ms` positive integer milliseconds, and `end_ms >=
start_ms` (bools, fractions and missing/unknown values are rejected). Evaluation
times are `t = start + k*step` for `k = 0, 1, ...`, keeping only times
`t <= end`; the output point timestamp is the evaluation time itself. The step
need not divide the window and windows are never aligned to fixed buckets.

Each evaluation aggregates the raw samples in the half-open interval
`(t - window_ms, t]`: the left edge is excluded (a sample exactly one window
length earlier does not count), the right edge included (a sample at `t`
does). `start_ms` only constrains the output grid — the first window at
`start` also reads history older than `start`; no window ever reads a sample
newer than its own evaluation time. The aggregation uses the same meanings as
bucketing (`sum`, `avg`, `min`, `max`, `count`), and a window containing no
sample emits `[t, null]` for *every* aggregation, including `count`.

Every matching registered series is returned on the full time grid even when
all of its windows are empty; no matching series yields an empty `series`
array. The Python per-series rows keep `series_id`; HTTP and CLI keep the
existing result shape. With `group_by`, all raw samples of the group's series
that fall in each window are pooled before aggregating (samples sharing a
timestamp across series count individually, `avg` divides by the total sample
count), a missing group key and an empty-string value stay distinct groups,
and each group keeps the full grid, fully null when no sample ever falls in a
window. Group filtering, ordering and validation follow the ordinary grouped
rules. The whole query reads one coherent snapshot, so concurrent writes or
retention sweeps can never mix old and new data; queries never alter samples,
counters or configuration, and the same history gives identical results after
reopening the store. Omitting `window_ms` preserves the previous query
behaviour exactly — `rollup`, write idempotency/overwrite, quotas, alerts and
SLOs are unaffected.

**Retention.** `enforce_retention(tenant, cutoff_ms)` removes every sample of
that tenant with `timestamp_millis < cutoff_ms` and rewrites the affected point
files. `stats()` returns `{"series","points","writes","tenants"}`.

**Per-tenant quotas.** `set_quota(tenant, max_series, max_points)` sets the
tenant-wide limits; each limit is a non-negative integer or `null` (omitted
fields over HTTP also mean `null`, i.e. unlimited), and `0` forbids adding any.
`get_quota(tenant)` returns
`{"tenant","max_series","max_points","series","points"}` — the configured
limits and the current usage in one snapshot. Configuring a quota never creates
a series, and an unconfigured tenant is unlimited. Limits apply to all of a
tenant's metrics together: `series` is the number of registered series and
`points` the total number of *distinct timestamps* across those series, not
physical point-file lines. Label order, same-value replays, overwritten points
and a timestamp repeated inside one batch add no occupancy.

Every write path (`SeriesStore.write`, `POST /v1/series`, the CLI `write`) is
checked after the existing input validation and conflict rules, using the
batch's net increase: a batch is rejected (ObsError / HTTP `409` / CLI JSON
error with non-zero exit) when *either* dimension it increases would exceed its
limit, and the rejection leaves no new series, samples or write-count change.
Limits may be lowered below current usage; afterwards only the dimensions a
write actually increases are checked — an already-over-limit dimension blocks
nothing by itself, so pure duplicates and overwrites keep succeeding. Retention
frees the occupancy of deleted points, while empty series stay registered.
Overwrites rewrite the point file so one timestamp never occupies two lines; on
reopen, any duplicate timestamp lines (including those left by older builds)
collapse to the last value, so historical re-overwrites never double-count.

**Rule shape.** `{"id","tenant","metric","labels","comparator" ∈ {">",">=","<",
"<=","==","!="},"threshold","for_ms","window_ms","agg","severity" ∈ {"info",
"warning","critical"},"annotations"}`. `add_rule` validates every field and
raises `ObsError` for anything malformed (`id` is generated when omitted).

**Evaluation window.** Let `step = window_ms`, `t = now_ms` and
`bucket_start(t) = t - (t % step)`. The window is
`[bucket_start(t) - step, t]`, i.e. the current bucket plus the whole previous
bucket. The rule condition holds for a series when *every* non-empty bucket in
that window satisfies `comparator(value, threshold)` — one violating bucket
anywhere in the window stops the condition, and it keeps stopping it until that
bucket has left the window.

**`for_ms`.** Let `run_start` be the timestamp of the first bucket of the
current unbroken run of satisfying buckets (a violating bucket resets the run).
The rule fires only when the condition holds *and*
`bucket_start(now_ms) - run_start >= for_ms`. With `for_ms = 0`, one satisfied
bucket is enough; with `for_ms = window_ms`, the current and the previous bucket
must both be satisfied. Before the hold time is reached the alert is recorded
with `firing = false` and appears in none of the four output lists.

**Alert identity and dedup.** Alert identity is `(rule_id, series_id)`, distinct
from the alert id. The first time a condition holds an alert id
`alert-NNNNN` is minted for that identity and reused forever: re-evaluating an
unchanged condition returns the *same* `id` and increments `occurrences` (the
number of evaluations in which the condition held). `since_ms` is `run_start`,
not the evaluation time, so dedup does not depend on the wall clock.

**Resolution.** When the condition stops holding, the alert's state becomes
`resolved`, `firing` becomes `false`, `resolved_ms` is set once, and the alert is
reported in the `resolved` list of that evaluation. If the same condition holds
again later, the *same alert id* is reused and `occurrences` keeps counting.
Because the condition covers the whole window, a series that produced one bad
bucket stays unresolved until that bucket has left the window.

**Silence vs inhibition precedence.** A condition that holds and whose `for_ms`
is satisfied is first checked against active silences: a silence is active when
`starts_ms <= now_ms <= ends_ms`, the tenant matches and every silence label
matcher is present in the series labels. Inside an active silence the alert state
is `silenced` (it is *not* firing) and the silence id and reason are recorded.
Only if no silence matches is inhibition checked: an inhibition
`(source_severity, target_severity, same_labels)` marks a `target_severity` alert
as `inhibited` while an alert of at least `source_severity` is currently
`firing` in the same tenant with *exactly the same label set*. Silence therefore
wins over inhibition. A silenced or inhibited alert still counts its
`occurrences` and returns to `firing` as soon as the silence/inhibition no longer
applies. `list_alerts(state=None)` filters by `firing`, `silenced`, `inhibited`
or `resolved`.

**SLO and error budget.** `set_slo(tenant, name, metric, labels,
good_comparator, threshold, target_ratio, window_ms)` defines a good-events
ratio SLO. `slo_status(name, now_ms, tenant=None)` counts every stored sample of
the matching series inside `[now_ms - window_ms, now_ms]`:

```
total          = number of samples in the window (nulls excluded)
good           = samples where compare(value, good_comparator, threshold)
bad            = total - good
ratio          = good / total                             (0.0 when total == 0)
error_budget   = max(0.0, 1 - (1 - ratio) / (1 - target_ratio))   (target_ratio < 1)
               = 1.0 if ratio >= 1.0 else 0.0              (target_ratio == 1)
burn_rate      = (bad / total) / (1 - target_ratio)        (0.0 when target_ratio >= 1 or total == 0)
met            = ratio >= target_ratio
```

`ratio` is the observed good ratio, `target_ratio ∈ (0,1]` the objective,
`burn_rate` how fast the budget is being spent relative to the allowed bad rate
(`1.0` means exactly on budget, `> 1.0` means burning too fast) and
`error_budget` the *fraction of the allowed error budget still unspent*:
`1.0` means untouched, `0.0` means exhausted or exceeded. For any `ratio` that
meets the target the two written forms of `error_budget` are identical, since
`1 - (1-ratio)/(1-target) = (ratio - target)/(1 - target)` and
`1 - ratio/target = (target - ratio)/target` are both non-negative only inside
their own regime; the first form is the one used, because it degrades sensibly
when the ratio beats the target (more budget left) instead of pinning to `0`.
With `total == 0` the ratio is `0.0`, so the budget counts as exhausted.

Examples: 9 good + 1 bad with `target_ratio = 0.9` gives `ratio = 0.9`,
`error_budget = 0.0`, `burn_rate = 0.1/0.1 = 1.0`, `met = true` (exactly on
budget). 98 good + 2 bad with `target_ratio = 0.95` gives `ratio = 0.98`,
`burn_rate = 0.02/0.05 = 0.4` and `error_budget = 1 - 0.02/0.05 = 0.6` (60% of
the budget still unspent). 90 good + 10 bad with `target_ratio = 0.99` gives
`ratio = 0.9`, `burn_rate = 10.0`, `error_budget = 0.0`, `met = false`.

**Determinism.** No module calls `time.time()` inside decision logic: `now_ms` is
passed into `evaluate`, `slo_status` and `write` (the HTTP layer uses the wall
clock only when a caller omits `now_ms` on `GET /v1/slos/status`, and
`/v1/slos/status` requires it in the CLI).

## Layout

```
obsd/__init__.py    public exports
obsd/tsdb.py        SeriesStore
obsd/alerts.py      AlertEngine
obsd/http_app.py    ThreadingHTTPServer + create_server
obsd/cli.py         command line interface
obsd/__main__.py    python3 -m obsd entry point
tests/              unittest suite (tsdb, alerts, http)
```

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Covered: series identity and label-order independence, idempotent re-write,
conflicting-timestamp rejection and `overwrite`, range filtering, epoch-aligned
buckets, null buckets, all five aggregations, deterministic rollups,
sliding-window grids with left-open/right-closed windows, null empty windows
(including count), full grids for empty series and groups, window argument
validation across Python/HTTP/CLI, window snapshot consistency under
concurrent writes and retention, retention,
restart safety, rule validation, `for_ms` timing, dedup with occurrence
counting, resolution and re-firing (including the window that keeps a bad bucket
blocking), silence scoping and expiry, inhibition by severity and exact label
set, SLO/error-budget/burn-rate math (including the empty window), quota
configuration/validation, net-increase enforcement and whole-batch rejection,
zero/lowered limits with duplicates and overwrites, tenant isolation, quota
release under retention, quota/usage consistency after reopen (including
collapsed duplicate timestamp lines), concurrent writes/reconfiguration/pruning,
and the live HTTP surface over a real socket.

Everything is deterministic: the suite injects every timestamp it uses, and the
modules never read the wall clock inside decision logic.
