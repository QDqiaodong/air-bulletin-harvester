# Multi-shard merge warehouse (air_harvest.merge)

An independent, offline, single-writer merge step over several existing
collector SQLite databases.  Different operators collect the same synthetic
bulletins independently; their databases can contain overlapping observations
and disagree about a concentration.  The merge reads each database from one
pinned read-only snapshot, reconciles witnesses on the business key
`station + UTC instant + pollutant`, requires an explicit manifest resolution
for every conflicting key, and publishes one self-contained SQLite warehouse
that keeps **every** witness — equal supporters and losing candidates alike —
plus the full logical snapshot of every input database.

It uses only the Python standard library and SQLite.  It never calls the
collector's table-creation or upgrade logic, never creates a missing input
database, takes no file order as priority, and performs no max/average/newest
auto-resolution.  All data are original synthetic demonstrations; the module
emits no health or regulatory judgment and introduces no service or
time-travel replay.

## Command line and Python entry points

```sh
# Generate synthetic example shards and a matching manifest through the real
# HTML parser and ingest path.
python -m air_harvest.merge_demo --dir run/merge-example

# Merge into a new self-contained warehouse.
python -m air_harvest.merge \
  --manifest run/merge-example/manifest.json \
  --output run/merge-example/warehouse.sqlite
```

The equivalent Python call is
`air_harvest.merge.run_merge(manifest_path, output_path)`, which returns the
same summary object the CLI prints as stable JSON.  `capture_snapshot` and the
validation helpers are also importable for embedding.

## Manifest contract

UTF-8 JSON with exactly three top-level keys:

```json
{
  "schema_version": 1,
  "shards": [
    {"shard_id": "operator-a", "path": "shards/operator-a.sqlite"},
    {"shard_id": "operator-b", "path": "shards/operator-b.sqlite"}
  ],
  "resolutions": [
    {
      "station": "River & Park",
      "observed_at": "2026-01-01T00:00:00Z",
      "pollutant": "PM2.5",
      "selected_shard": "operator-b",
      "reason": "operator-b re-check accepted for the synthetic demo",
      "expected": [
        {"shard_id": "operator-a", "qualifier": "measured", "value_ug_m3": "25"},
        {"shard_id": "operator-b", "qualifier": "measured", "value_ug_m3": "30"}
      ]
    }
  ]
}
```

- `schema_version`: only the integer `1`.  Booleans and strings are rejected.
- `shards`: non-empty array; each item has exactly `shard_id` and `path`.
  - `shard_id`: non-empty string without leading/trailing whitespace, unique
    across the manifest with case-sensitive comparison.
  - `path`: non-empty path relative to the manifest's own directory.  Absolute
    paths, drive letters, backslashes, NUL bytes, references that resolve
    outside the manifest directory, and symbolic links (at any path component)
    are rejected.  A literal `..` that resolves back inside the directory is
    allowed.  Two shards that resolve to the same physical file are rejected,
    including hard links; the manifest file itself is also protected.
  - Every shard database must already exist.  It is opened through a
    `mode=ro` URI and never created or modified.
- `resolutions`: array, may be empty.  Each item has exactly `station`,
  `observed_at`, `pollutant`, `selected_shard`, `expected`, `reason`.
  `reason` is a non-empty string without surrounding whitespace.
  `expected` is a non-empty array; each item has exactly `shard_id`,
  `qualifier`, `value_ug_m3`; shard ids are unique inside one `expected`.
  `value_ug_m3` is a decimal string for `measured` and JSON `null` for `ND`.

### Input database verification

Each shard must already contain the collector's `sources` and
`observations` tables with the contract columns, the composite primary key on
`(station, observed_at, pollutant)` and the foreign key
`observations.source_digest -> sources(digest)`.  Every row is additionally
validated: business-key components and source digests are non-empty;
pollutants are `PM2.5`, `PM10`, `NO2`; qualifiers are `measured` or `ND`;
`ND` carries a NULL concentration and never a value; `measured` carries a
finite, nonnegative decimal string within the same normalized bound the
collector enforces (`>= 1e-12` when nonzero, `<= 1e9 ug/m3`); timestamps
carry an offset and normalize to UTC; every `source_digest` resolves to a
row in the same shard's `sources`; and no two observations normalize to the
same canonical business key inside one shard (different offset spellings of
one instant are one key).  An illegal fact fails the whole run as a data
error — it is not treated as an adjudicable inter-shard conflict.

## Snapshot semantics

Each shard is read through one read-only URI connection with a deferred read
transaction pinned immediately.  All `sources` and `observations` rows used
from that shard come from the same SQLite snapshot, even if a collector
commits while the merge is reading.  There is deliberately no cross-shard
transaction and no claim that the shards share a real-world point in time.

A **logical snapshot** is defined independently of the database file's binary
hash: a canonical JSON document (format id
`air_harvest.merge.shard_snapshot/v1`) containing every `sources` field
(`digest`, `label`, `row_count`) and every `observations` field (`station`,
`observed_at`, `pollutant`, `value_ug_m3`, `qualifier`, `source_digest`),
with fixed row ordering (sources by `digest, label, row_count`; observations
by the stored business-key columns) and sorted, compact JSON keys.  Its
SHA256 is the logical snapshot digest.  The binary SHA256 of the SQLite file
is never used as a fact summary.  Source information proves only the first
accepted association; it cannot reconstruct original HTML or enumerate a
full duplicate-bulletin history.

## Reconciliation rules

- The business key is `station`, the full UTC instant (microsecond
  precision is retained; observations one microsecond apart stay separate)
  and `pollutant`.  Different offset spellings of one instant merge onto one
  key.
- A candidate value's identity is the qualifier plus the canonical
  concentration.  `measured` decimals compare by exact numeric value
  (`25` and `25.0` are the same candidate; normalization goes through
  `decimal.Decimal`, never `float`).  `ND` (NULL) is a distinct candidate
  from a real measured `0`.
- If every shard witnessing a key carries the same candidate, the merge
  produces one final fact and retains all shard witnesses as its supporters.
- If a key has two or more distinct candidates it is a conflict.  Nothing is
  picked automatically: the run fails unless a manifest resolution names the
  key, a `selected_shard` that actually witnesses it, a reason, and an
  `expected` set that is **exactly** the full witness set captured from the
  inputs — every shard, not just the distinct values, with the exact
  qualifier and canonical concentration each shard holds.
- Resolution failures name the business key and the reason: duplicate
  resolution keys, unresolved conflicts, missing/extra/mismatched `expected`
  entries, unknown or non-witnessing `selected_shard`, a resolution for a
  key that does not exist or has no conflict.  Because the expected set is
  matched against the captured snapshot, a stale resolution silently stops
  applying as soon as an input database changes.  Resolution array order has
  no semantic effect.

## Warehouse data model

The output is a new SQLite database carrying its own format id
`air_harvest.merge/v1` in `merge_meta`.  Explicit primary keys, foreign keys,
`UNIQUE` constraints and indexes are declared; the load ends with
`PRAGMA foreign_key_check` and `PRAGMA integrity_check` before publication.

| Table / view | Grain and purpose |
| --- | --- |
| `merge_meta` | Format id, schema version, manifest SHA256, relative manifest label. |
| `manifest_bytes` | The exact original manifest bytes and their SHA256. |
| `shards` | One row per input shard: id, relative path label, manifest ordinal, logical snapshot SHA256, source/observation counts. |
| `shard_snapshots` | The full canonical snapshot JSON per shard and its SHA256; lets every input snapshot be rebuilt after the input files are moved away. |
| `sources` | Every source row of every shard, keyed by `(shard_id, digest)`.  The same digest with different labels or `row_count` in different shards is retained per shard; sources with no final-fact reference are retained too. |
| `business_keys` | Every distinct canonical `(station, observed_at_utc, pollutant)`, with stable integer ids. |
| `candidates` | Every distinct canonical candidate value per key (`qualifier`, canonical `value_ug_m3`, NULL for ND). |
| `witnesses` | One row per shard observation: shard, key, candidate, first `source_digest`, raw stored timestamp and ordinal.  `UNIQUE (shard_id, key_id)` records that each shard witnesses a key at most once; a composite foreign key keeps the digest inside the shard's own `sources`. |
| `resolutions` | Conflict key, selected shard and reason. |
| `resolution_expected` | The full expected witness set recorded with each resolution. |
| `final_facts` | One final fact per business key, pointing at the chosen candidate and (for conflicts) its resolution. |
| `v_final_fact_trace` | Final fact joined to **all** witnesses with `witness_role` = `supporter` (candidate equals the final value, including every equal-value shard) or `loser` (conflict-only, a different candidate). |
| `v_conflicts` | Every conflict key with candidate count, selected shard and reason. |

### Example queries

```sql
-- The final facts and every shard behind each one, including losing values.
SELECT station, observed_at_utc, pollutant, final_qualifier,
       final_value_ug_m3, shard_id, witness_role, source_digest
FROM v_final_fact_trace
ORDER BY station, observed_at_utc, pollutant, shard_id;

-- All conflicts, why they were decided and which shard was selected.
SELECT * FROM v_conflicts;

-- The full expected witness set attached to a resolution.
SELECT r.key_id, re.shard_id, re.qualifier, re.value_ug_m3
FROM resolutions r JOIN resolution_expected re USING (resolution_id)
ORDER BY r.key_id, re.shard_id;

-- Per-shard sources, including the same digest carrying different labels.
SELECT shard_id, digest, label, row_count FROM sources
ORDER BY digest, shard_id;

-- Rebuild an input shard's logical snapshot after moving the inputs away.
SELECT shard_id, snapshot_sha256, canonical_json FROM shard_snapshots;
```

Fact grain: exactly one final fact per canonical business key.  A final fact
is not a per-shard row: trace to shards through `v_final_fact_trace`, where
equal-value shards are supporters and conflict losers remain fully
queryable with their candidate and source.

## Determinism and the stdout summary

With identical input logical snapshots and identical manifest bytes, repeated
builds print byte-identical stdout and produce identical logical database
content for queries ordered by the stable business keys (SQLite file bytes
are not required to match).  Reordering the `shards` or `resolutions` arrays
does not change any merged fact or adjudication; the stored manifest SHA256
changes because the raw bytes change.  The stdout JSON contains at least:
`format`, `schema_version`, `shard_count`, `input_witness_count`,
`business_key_count`, `distinct_candidate_count`, `conflict_key_count`,
`resolved_key_count`, `final_fact_count`, `manifest_sha256` and the
per-shard `snapshot_sha256` map.

## Write safety and failure recovery

Single-writer, offline, full-replace builds.  Validation, snapshot capture,
reconciliation and the entire database load happen inside a private
`.tmp` database in the output's own directory; foreign-key and integrity
checks run and the connection closes before `os.replace` installs the
result.  The output must not alias the manifest or any input shard:
symlink outputs and hard-link aliases (same device/inode) are refused.  Any
configuration, read, reconciliation, write or replacement failure exits
non-zero, leaves the input files and any previous output untouched, and
removes its temporary file.  A corrected manifest can be rerun directly with
no manual cleanup.  Only normal exception safety is provided; there is no
power-loss recovery or concurrent publication protocol.  Once replacement
has succeeded, a later stdout failure does not unpublish the database.

## Synthetic-data notice

All shards, manifests, resolutions and numbers are original synthetic
demonstrations for a fictional monitoring network.  The warehouse records
data provenance and descriptive reconciliation only; it is not a health
assessment, a regulatory determination or an official statistic.
