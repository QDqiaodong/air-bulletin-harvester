# Conditional collection cache (air_harvest.conditional)

An opt-in incremental acquisition protocol over the existing append-only
collector database.  It keeps the original collector semantics (business key
`station + UTC instant + pollutant`, unit normalization, NULL for `ND`, first
accepted source attribution, conflict rejection) and adds a per-URL cache with
conditional requests, exact original bytes and generation numbers.  Everything
is standard library + SQLite, loopback HTTP only, and synthetic demo data; no
health or regulatory judgment is made.

The legacy `air_harvest.store.ingest`, `python -m air_harvest --input/--url`,
`python -m air_harvest.daily` and `python -m air_harvest.merge` paths are
unchanged and never require the extra tables.

## Commands

```sh
python -m air_harvest fetch --url http://127.0.0.1:PORT/bulletin \
  --db run/conditional.sqlite [--label operator-a]
python -m air_harvest status --db run/conditional.sqlite
python -m air_harvest export --db run/conditional.sqlite \
  --output run/original.html --url http://127.0.0.1:PORT/bulletin
python -m air_harvest export --db run/conditional.sqlite \
  --output run/original.html --digest <source-sha256>
python -m air_harvest.conditional_demo --dir run/conditional-demo --clean
```

Every command prints one deterministic, sorted JSON document on stdout.  Any
failure prints `fetch|status|export failed: ...` on stderr and exits 2.  A
failed command never creates a receipt file and never modifies an existing
export.  A receipt failure *after* a successful commit does not roll the commit
back; the data stays published and a retry observes it.

## URL contract

The raw caller string is never used as identity, stored as content or turned
into a path.  Before any socket opens, the URL is rejected unless:

- the scheme is exactly `http` (plain text; no `https`, `ftp`, `file`);
- the host is `127.0.0.1`, `localhost` or `::1` (case-insensitive);
- no userinfo/credentials are present;
- no fragment (`#...`) is present;
- no control characters or whitespace are present;
- the port, if present, is `1..65535` (absent means 80).

Cache identity is `url_key = SHA256(canonical_url)` where the canonical URL is
`http://<lowercase host>:<effective port><path or / ?query>`: no fragment, no
userinfo.  `HTTP://LocalHost/b` and `http://localhost:80/b` are therefore one
cache entry.  Only this canonical form is persisted in `fetch_state.url`; it
is bounded, contains no credentials or fragment, and no absolute filesystem
path ever appears in receipts or status.

## Response contract (200 / 304)

- Redirect statuses (`301`, `302`, `303`, `305`, `307`, `308`, and other
  `3xx`) are never followed.  The client uses `http.client` directly, so there
  is no library-level redirect handling to disable.  Any status other than
  `200` or `304` fails with `unexpected HTTP status`.
- The response body is bounded to 2 MiB.  A declared `Content-Length` above
  that is rejected before reading; otherwise at most 2 MiB + 1 is read.
- A body shorter than its declared `Content-Length` (`IncompleteRead`), a body
  whose length differs from the declared length, a body that is not valid
  UTF-8, a `charset` other than UTF-8, a non-identity `Content-Encoding`, or
  conflicting duplicate headers all fail the fetch.
- A `200` body must parse under the v1 bulletin contract.  A `304` must carry
  no body, must answer a request that actually sent `If-None-Match` or
  `If-Modified-Since`, must have a stored validator to compare against, and
  any returned ETag/Last-Modified must equal the stored value.

Validators:

- `ETag` is preferred over `Last-Modified`.  Weak tags are retained verbatim,
  including the `W/` prefix.  A malformed entity tag fails the fetch rather
  than being stored.
- `Last-Modified` is parsed and re-serialized as an IMF-fixdate in GMT.
- Exactly one validator is stored, in `fetch_state.validator_kind` /
  `validator_value`.  A `200` that advertises neither clears both fields; no
  conditional request is sent afterwards, and a `304` can no longer be
  accepted.

## Storage model

The two original tables are created only if absent and are never altered:

| Table | Columns |
| --- | --- |
| `sources` | `digest` PK, `label`, `row_count` (unchanged) |
| `observations` | key, `value_ug_m3` (TEXT, NULL for ND), `qualifier`, `source_digest` FK (unchanged) |
| `source_content` | `digest` PK/FK, `byte_length`, `raw_bytes` BLOB |
| `fetch_state` | `url_key` PK, `url`, `source_digest` FK, `generation`, `validator_kind`, `validator_value`, `last_http_status`, `last_row_count`, `last_inserted`, `last_duplicates` |

A database is only accepted if its structure is exactly one of:

- the legacy collector schema (`sources`, `observations`);
- the conditional schema (`sources`, `observations`, `source_content`,
  `fetch_state`).

Anything else — missing tables, extra tables/views/triggers, wrong column
shapes, missing foreign keys, or a partially created cache schema — raises
`UnknownSchema` before any data is read or written.  A legacy database gains
the two new tables on its first successful conditional `200`; it never gains
fabricated bytes for sources that were stored before this feature existed.
`status` and `export` on such a source fail with `original bytes ... cannot be
reconstructed` rather than synthesizing anything from the parsed rows.

## Fetch algorithm and transaction boundaries

1. Validate the URL and compute `url_key` / canonical URL.
2. Read the request-start state in a separate short read-only connection:
   `(generation, source_digest, validator_kind, validator_value)`.  A missing
   database, missing `fetch_state`, or cold start yields generation `0`.  No
   lock is held after this step.
3. Send the conditional GET (if a validator exists) and read the full response
   *without any database transaction open*.
4. For `200`: decode and parse the body, then open one short
   `BEGIN IMMEDIATE` transaction which (a) verifies/creates the schema, (b)
   re-reads the state and compare-and-swaps on
   `(generation, source_digest)` — a mismatch raises `stale_response`, (c)
   applies the observation batch with the existing conflict rules, (d) inserts
   the original bytes into `source_content` if that digest is new, (e) inserts
   or updates `fetch_state` with `generation = start.generation + 1`, and (f)
   commits.  Any failure before commit rolls back the rows, the source
   registration, the archived bytes, the generation and even schema creation.
5. For `304`: perform the checks above, then open a read-only snapshot and
   confirm the state still equals the request-start `(generation,
   source_digest)` and that the cached bytes, source row, parsed rows and
   stored observations all agree.  Nothing is written; the operation reports
   `result = "unchanged"`, the same generation, and zero inserted/duplicates.

Cold start uses the same compare-and-swap: the first client to commit wins and
a slower concurrent first fetch gets `stale_response` rather than overwriting
the winner's generation 1.

A late `200` or `304` whose request-start generation/digest no longer matches
raises `StaleResponse` (message begins `stale_response:`).  It is never
retried, never re-requested and never partially applied.

First-source semantics are unchanged: `INSERT OR IGNORE INTO sources` keeps the
label and row count of the first accepted source; duplicate content never
rewrites attribution.  Replayed identical content still advances the URL
generation (it is a successful `200` observation) but inserts nothing.

## Receipts

`fetch` (accepted):

```json
{"result": "accepted", "url": "http://127.0.0.1:8080/bulletin",
 "url_sha256": "...", "http_status": 200, "generation": 1,
 "source_sha256": "...", "rows": 4, "inserted": 3,
 "duplicates": 1, "total": 3}
```

`fetch` (unchanged):

```json
{"result": "unchanged", "url": "http://127.0.0.1:8080/bulletin",
 "url_sha256": "...", "http_status": 304, "generation": 1,
 "source_sha256": "...", "rows": 4, "inserted": 0,
 "duplicates": 0, "total": 3}
```

No receipt contains a timestamp, elapsed time, hostname or absolute path.
`total` is the number of stored observations after the operation.

## Status

`status --db` is strictly offline and read-only; it never creates a missing
database (`FileNotFoundError`, exit 2) and never writes.  It opens a read-only
URI with a pinned snapshot, validates the structure, and for every URL proves
`SHA256(raw_bytes) == source digest`, `byte_length` matches, the bytes parse,
`row_count` matches, and every stored observation agrees with the parsed rows.
Any mismatch raises `CacheCorruption` and no `status` document is printed.

```json
{"schema_version": 1, "report": "air_harvest.status/v1",
 "urls": [{"url": "...", "url_sha256": "...", "http_status": 200,
           "generation": 1, "source_sha256": "...", "source_label": "...",
           "row_count": 4, "content_bytes": 819,
           "last_fetch": {"rows": 4, "inserted": 3, "duplicates": 1}}],
 "totals": {"url_count": 1, "source_count": 1, "observation_count": 3,
            "archived_source_count": 1, "unarchived_source_count": 0,
            "rows": 4, "inserted": 3, "duplicates": 1}}
```

`last_fetch` records what the most recent accepted `200` for that URL added:
rows parsed, inserted observations and duplicates, plus the current cumulative
generation.  A `304` report always shows zero inserted and duplicate counts for
that operation; it never rewrites `last_fetch`.  `archived_source_count` counts
sources with stored bytes; `unarchived_source_count` counts legacy sources
whose bytes were never stored.  URLs are ordered by `url_key`, so output is
byte-stable for identical database contents.

## Export

`export --db --output (--url U | --digest D)` writes the exact stored original
bytes with a same-directory temporary file, `fsync` and `os.replace`.  It
refuses to write to a symlink, to the database itself (same inode or same
resolved path) or to a directory, requires the output's parent to exist, and
verifies the selected source before touching the output.  On any failure an
existing output remains byte-for-byte intact and the temporary file is
removed.  The receipt reports `source_sha256`, `source_label`, `row_count` and
`byte_length` only.

## Compatibility with daily report and merge

`daily` opens any accepted database read-only with a pinned snapshot and only
reads `sources` and `observations`, so it works unchanged on conditional-cache
databases.  `merge` checks only that the original two tables have the collector
shape; it ignores the extra cache tables, so conditional databases can still be
used as merge shards.  The merge path itself is unmodified.

## Failure classes

| Class | Example message | Effect |
| --- | --- | --- |
| URL contract | `HTTP acquisition is limited to local fixture endpoints` | no socket, no DB |
| redirect/status | `unexpected HTTP status 302` | no DB |
| transport | `short read: ...`, `... length does not match ...` | no DB |
| encoding | `response body is not valid UTF-8` | no DB |
| contract | `response body violates the bulletin contract: ...` | no DB |
| validator | `malformed ETag response header: ...` | no DB |
| 304 | `a 304 response requires ...`, `304 validator does not match ...` | no writes |
| stale | `stale_response: ...` | no writes, no retry |
| conflict | `conflicting observation: (...)` | full rollback |
| schema | `unknown database structure ...` | no writes |
| corruption | `stored bytes ...`, `... cannot be reconstructed` | no writes |
| export | `output ...: symbolic-link outputs are not accepted` | old output intact |
