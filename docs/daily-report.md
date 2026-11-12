# Daily coverage report (air_harvest.daily)

An independent, read-only report over observations already accepted by the
collection pipeline.  It answers, per station, pollutant and local day: how
many of the expected hourly samples were actually covered, how many were
ND-only, and whether a descriptive daily mean has enough numeric coverage to
be published.  All data remain synthetic demo data; the report never emits
health or regulatory judgments and introduces no database server or external
interface.

## Environment requirements

- Python 3.11+ and the Python standard library only (no new third-party
  dependency).
- Timezone handling uses `zoneinfo` together with the operating system's IANA
  timezone database.  On Linux/macOS this is provided by system packages such
  as `tzdata` (for example `apt install tzdata` in a minimal container).  A
  plan whose timezone name cannot be loaded is rejected with a located error;
  no silent fallback to UTC occurs.
- The report opens an existing SQLite database through a read-only file URI.
  It never runs table creation, never creates a missing database, and never
  writes to the collection tables.

## Command line

```sh
python -m air_harvest.daily \
  --db run/air.sqlite \
  --plan data/daily-plan.example.json \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --output run/daily-report.json
```

Dates are `YYYY-MM-DD` calendar dates; the range is inclusive at both ends.
Invalid dates or an inverted range fail with exit code 2 before any output is
written.  The output directory must already exist; the command does not create
directories.

## Plan document

UTF-8 JSON, validated before the database is opened:

```json
{
  "schema_version": 1,
  "min_valid_percent": 75,
  "stations": [
    {"station": "River & Park", "timezone": "America/New_York",
     "pollutants": ["PM2.5", "PM10"]}
  ]
}
```

Validation rules (errors name the offending path, e.g.
`plan.stations[0].timezone`):

- `schema_version`: must be the integer `1`.  Booleans are not integers and
  the string `"1"` is rejected.
- `min_valid_percent`: integer from 1 through 100 inclusive; booleans rejected.
- `stations`: non-empty array.  Each `station` is a non-empty, unique string
  matched exactly against the identifiers stored by the collector; no data for
  a station is required.  Unknown keys are rejected.
- `timezone`: non-empty IANA name that `zoneinfo.ZoneInfo` can load.
- `pollutants`: non-empty array without duplicates, limited to the three
  contract pollutants `PM2.5`, `PM10`, `NO2`.

The plan never modifies the collection database and does not require the
planned stations or pollutants to have any observations.  The report always
covers every planned station, pollutant and date in the requested range, even
when no records exist.

## Sampling geometry and local-date attribution

1. The sampling grid is built from UTC hour-aligned slots.  Each slot is the
   half-open interval `[h, h+1h)` in UTC.
2. An observation enters the unique slot containing its absolute UTC instant.
   An instant exactly on a boundary enters the later slot (`01:00:00Z` belongs
   to `[01:00, 02:00)`).
3. A slot's local date is the calendar date obtained by converting the slot
   *start* `h` (not the observation wall time) into the station timezone.  This
   rule is unambiguous for non-integer offsets and for DST transitions.
4. The expected slots of a local day are all UTC hour-aligned slots whose
   converted start falls on that local date.  The count therefore follows
   timezone history instead of being fixed at 24: ordinary days have 24, a
   spring-forward day 23 (e.g. `America/New_York` 2026-03-08) and a fall-back
   day 25 (2026-11-01).  Expected slots are derived purely from the plan and
   the timezone database; they are never inferred from stored observations.
5. A local date with no expected slot at all (a historical gap larger than a
   day in the zone's timeline) is still emitted, marked `no_expected_hours`,
   with zero counts and no division and no published mean.

## Slot states and the role of ND

Each expected slot is in exactly one mutually exclusive state:

- `numeric`: at least one accepted `measured` observation exists.  The slot's
  `hour_mean` is the unweighted arithmetic mean of the `measured`
  concentrations only.  ND records inside the slot are counted but never
  treated as zero and never enter the mean.
- `nd_only`: the slot has observations but every one of them is `ND`.
- `missing`: the slot has no accepted observation.

A real measured zero is a valid numeric observation.  Only the accepted
business facts in the `observations` table are used.  Re-weighting by raw
bulletin rows, `sources.row_count` or source counts does not happen.
Equivalent-unit and equivalent-instant duplicates were already collapsed to a
single business fact during ingest, so they gain no extra weight here.  ND is
not a numeric detection limit and is never converted to zero.

## Daily statistics

For every station, pollutant and local day:

- `expected_hours`: number of UTC hour slots attributed to the local day.
- `numeric_hours`, `nd_only_hours`, `missing_hours`: counts of the three slot
  states; they sum to `expected_hours`.
- `received_hours = numeric_hours + nd_only_hours`.
- `descriptive_mean`: the equal-weighted arithmetic mean of the **numeric
  hourly means**, not the mean of every raw value.  A slot with three values
  therefore contributes once, exactly as a slot with one value does.  When no
  numeric hour exists it is `null`, regardless of ND or missing counts.
- Numeric coverage uses integer arithmetic only: numerator
  `numeric_hours`, denominator `expected_hours`.  ND-only hours do not improve
  coverage.  The threshold is met exactly when
  `numeric_hours * 100 >= expected_hours * min_valid_percent`; no rounded
  percentage is ever compared.
- `status` / `published_mean`:
  - `no_expected_hours` when `expected_hours == 0`; no division, no mean.
  - `published` when the threshold is met; `published_mean` equals the
    descriptive mean.
  - `insufficient_coverage` otherwise; `published_mean` is `null` while the
    descriptive mean (if any) and all hourly evidence are retained.
  A failed threshold is a normal, successful report outcome, not an error.

### Exact arithmetic

No aggregation value passes through a binary `float`.  Hour means and daily
means are computed with exact rationals (`fractions.Fraction`) built from the
decimal strings stored in SQLite.  The daily mean is computed from the exact
hourly rationals in one step; hourly means are never rounded first and then
averaged.  Only the final display values are rendered to six decimal places
with `Decimal.quantize(..., ROUND_HALF_UP)`, for example `48.888889`.
Coverage decisions use the integer comparison above, never a rounded percent.

## Output JSON

UTF-8 JSON, two-space indentation, trailing newline.  With identical database
contents, plan bytes and date arguments the bytes are identical across runs;
no current timestamp, hostname or absolute machine path is included.

Top level:

- `schema_version`: report schema version (`1`).
- `report`: report identifier string.
- `plan_sha256`: SHA256 hex digest of the exact plan bytes read from disk.
- `request`: `start_date`, `end_date` (inclusive) and `min_valid_percent`.
- `stations`: one block per planned station, in plan order, containing
  `station`, `timezone` and a `pollutants` array.  Each pollutant holds a
  `days` array covering the full requested range.
- `sources`: every source digest referenced by the included observations, with
  the saved `label` and `row_count`, sorted by digest.

Each day object contains `date`, `status`, the six hour counters
(`expected_hours`, `numeric_hours`, `nd_only_hours`, `missing_hours`,
`received_hours`), the `numeric_coverage` object (`numerator`, `denominator`,
`min_valid_percent`, `threshold_met`), `descriptive_mean`, `published_mean`
and `slots`.

Each slot object contains:

- `slot_start_utc`, `slot_end_utc`: the `[h, h+1h)` UTC bounds, `...Z` form.
- `local_start`: the slot start converted to the station timezone, including
  its UTC offset.  During a fall-back the two repeated local wall times remain
  distinguishable (e.g. `2026-11-01T01:00:00-04:00` and
  `2026-11-01T01:00:00-05:00`).
- `state`, `measured_count`, `nd_count`, `hour_mean` (six-decimal string for
  numeric slots, otherwise `null`).
- `observations`: every accepted business fact in the slot, each with its
  business key fields (`station`, `observed_at` UTC, `pollutant`), the saved
  normalized `concentration_ug_m3` (`null` for ND), `qualifier` and
  `source_digest`.  ND observations are listed even though they do not
  participate in the numeric mean.

## Snapshot, provenance and write safety

- The database is opened with a `mode=ro` URI and a read transaction is pinned
  immediately.  All observation and source rows used by a report come from one
  SQLite read snapshot, so a collector committing mid-run cannot mix new
  observations with old source rows.  A new run after the commit sees the new
  snapshot in full.
- Each observation's `source_digest` resolves to an entry in the report's
  `sources`, allowing every daily number to be traced to its hour evidence and
  the first accepted source registered by the collector.  First-source
  attribution follows the existing ingest rules; duplicate sources never
  rewrite it.
- The report cannot reconstruct original HTML that was not saved, and it does
  not enumerate sources that only ever appeared as duplicates without a
  referenced observation.
- The full report is serialized before any output write.  A temporary file in
  the target directory is `fsync`ed and then installed with `os.replace`, so an
  existing report is either fully replaced or left byte-for-byte intact.
  Configuration, validation, database, serialization or replacement failures
  all exit non-zero without touching a previous report.  A corrected plan or
  argument can be rerun directly; no database or report deletion is needed.
  This delivery contains only the single-file report replacement; it does not
  extend the collection-side publishing protocol.

## Synthetic-data notice

All fixtures and numbers are original synthetic demonstrations for a
fictional monitoring network.  The report describes data coverage and
descriptive statistics only; it is not a health assessment, a regulatory
compliance result or an official statistic, and it defines no thresholds beyond
the caller-supplied numeric coverage percentage.
