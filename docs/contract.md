# Bulletin v1 contract

Input: UTF-8 HTML with exactly one `table#observations`; column names and order
are `station, observed_at, pollutant, value, unit, qualifier`. Nested inline text
and HTML entities are supported. Nested tables and malformed HTML are outside
the contract. File or local HTTP body limit: 2 MiB; timeout: 5 seconds; redirects
are rejected. HTTP transport is for a caller-controlled local fixture, not an
approved connector to an external publisher. No external API is required.

Station is a nonempty identifier; pollutant is PM2.5, PM10 or NO2. Timestamps
must include a timezone and are canonicalized to UTC. Values must be finite,
nonnegative decimals; nonzero source values must be >=1e-12, and normalized
concentrations must be <=1e9 ug/m3. These are engineering input bounds, not
health thresholds. mg/m3 is multiplied by 1000. Normalized values are decimal
strings in SQLite to retain decimal precision. `ND` means not detected, must
have an empty value, and remains NULL rather than zero. It is not interpreted
as a regulatory threshold or a numeric detection limit.

Business key: station + UTC instant + pollutant. Same normalized value and
qualifier is a duplicate; conflicting observations reject and roll back the
entire batch, including its source registration. There is no correction policy.
Source SHA256 links each observation to its first accepted source; duplicate
sources are registered but do not rewrite this first-source attribution.

Output: `sources` (content SHA256, caller label, parsed row count) and
`observations` (key, concentration, qualifier, source digest). A collection
summary reports parsed rows, inserted observations, duplicates and total stored.
A valid header-only bulletin is accepted as zero rows; missing contracts fail.
