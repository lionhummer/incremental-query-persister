# Verification

The release suite has 18 tests, including actual TanStack QueryClient dehydration/hydration and SQLite transactions across separate connections. `npm run check` runs type checking, tests and the declaration-producing build. CI uses Node 22 and 24, then checks formatting, executes the example and benchmark, and packs the release archive.

`npm run bench` initializes 1,000 rows with an 8,192-character ASCII data field, then changes one row on each of 30 updates. Both totals include initialization. Whole-client JSON serialization records 257,264,758 bytes; incremental row serialization records 8,546,662 bytes across 1,030 row writes, a 96.68% reduction. The script asserts the row-write count and that the incremental payload is less than one twentieth of the baseline.

The memory adapter excludes disk latency, bridge overhead, compression and production workload effects. Metadata serialization and snapshot copying still happen. Local timing is diagnostic only. No production throughput or universal speedup is claimed.
