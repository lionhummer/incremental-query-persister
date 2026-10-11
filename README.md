# Incremental Query Persister

Persist changed query rows instead of serializing the entire cache on every update. Includes atomic SQLite storage, strict byte/count budgets, bounded pending work and generation fencing around clears. No runtime npm dependencies.

```sh
npm install @lionhummer/incremental-query-persister
```

Requires Node 22.13+ for the SQLite adapter. The core uses standard JavaScript APIs and can run in a browser with a custom `RowStore`.

## Quick start

```ts
import {
  createIncrementalPersister,
  MemoryRowStore,
} from "@lionhummer/incremental-query-persister";

const persister = createIncrementalPersister({ store: new MemoryRowStore() });
persister.persistClient({
  timestamp: Date.now(),
  buster: "schema-v1",
  clientState: {
    mutations: [],
    queries: [
      {
        queryHash: "settings",
        queryKey: ["settings"],
        state: { data: { theme: "dark" }, dataUpdatedAt: 1, status: "success" },
      },
    ],
  },
});
await persister.flush();
console.log(await persister.restoreClient());
await persister.close();
```

`MemoryRowStore` is an example adapter: its rows do not survive process exit. For durable local storage:

```ts
import { createIncrementalPersister } from "@lionhummer/incremental-query-persister";
import { SqliteRowStore } from "@lionhummer/incremental-query-persister/sqlite";

const store = new SqliteRowStore("cache.sqlite", "account-123");
const persister = createIncrementalPersister({ store });
// Persist or restore query snapshots here.
await persister.close();
store.close();
```

SQLite runs synchronously inside the adapter. Use a worker when blocking the host's event loop is unacceptable. Node may emit an experimental warning for its built-in SQLite API.

## TanStack integration

The core implements the query-only persister contract structurally; it does not bundle TanStack. Supply its `PersistedClient` type when integrating with the persistence helpers:

```ts
import type {
  PersistedClient,
  Persister,
} from "@tanstack/query-persist-client-core";
import {
  createIncrementalPersister,
  MemoryRowStore,
} from "@lionhummer/incremental-query-persister";

const persister = createIncrementalPersister<PersistedClient>({
  store: new MemoryRowStore(),
  shouldPersist: (query) => query.state.status === "success",
});
const adapter: Persister = persister;
```

Configure dehydration to exclude mutations. Nonempty mutation snapshots are rejected so a mutation outbox cannot silently disappear. TanStack's restore helper should enforce maximum age and the application's buster. The standalone `restoreClient()` returns stored metadata without applying an expiration policy.

Keep query data immutable. The persister recognizes unchanged object data by identity, tracks query metadata separately, and holds weak references to data objects. Mutating a data object in place can hide a change. It shallow-copies incoming snapshots, not arbitrary data trees.

## Storage and scheduling

| Option            | Default         | Meaning                                              |
| ----------------- | --------------- | ---------------------------------------------------- |
| `maxRowBytes`     | 2 MiB           | Maximum UTF-8 JSON bytes per query row               |
| `maxBytes`        | 16 MiB          | Total retained row payload bytes                     |
| `maxRows`         | 500             | Maximum retained rows                                |
| `idleMs`          | 2,000           | Debounce after an incoming snapshot                  |
| `maxWaitMs`       | 30,000          | Maximum debounce window                              |
| `minIntervalMs`   | 60,000          | Minimum interval between successful scheduled writes |
| `protectedHashes` | `[]`            | Rows protected from budget eviction                  |
| `isWriter`        | all calls write | Host-provided writer ownership predicate             |
| `onError`         | none            | Report automatic flush failures                      |

The minimum interval takes precedence over the debounce window. Explicit `flush()` bypasses scheduling and drains the latest snapshot. Only one storage operation executes at a time; updates received while it runs replace one pending snapshot. These limits bound row payload storage, not total process memory, SQLite file size or WAL growth.

Oversized or JSON-unserializable data rows are omitted. Budget eviction removes the largest unprotected rows first. If protected rows alone exceed a budget, the write fails atomically with `BUDGET`. Restore also rejects a stored snapshot exceeding configured budgets.

A query disappearing from the in-memory cache remains on disk. Use `removeQueries(hashes)` for explicit deletion. An observed query excluded by `shouldPersist` is removed. Changing the buster resets prior rows in the same atomic write.

## Session lifecycle and failures

Await `removeClient()` before switching account state. It drops pending snapshots, blocks new input during the clear, waits for older writes, and advances the storage generation. A stale generation is rejected at the storage commit boundary with `FENCED`. The application must stop its old snapshot producer; fencing cannot identify stale account data submitted as a new operation after a clear.

Use one writer per namespace. `isWriter` is an integration hook, not a distributed lock. Other windows can restore, but their writes and deletion calls are ignored when the predicate returns false. Storage namespaces isolate accounts; the host still owns authorization and lifecycle.

`flush()` rejects on failure and exposes it through `persister.error`. Automatic failures call `onError`; that callback must not throw. Failed snapshots are not automatically replayed. Submit a fresh snapshot to retry. Bookkeeping changes only after an acknowledged atomic write. `close()` stops new snapshots and awaits a final flush; close the underlying storage afterwards.

For a browser, native bridge or remote backend, implement the small `RowStore` interface. Its `write` must atomically compare the supplied generation, apply resets/deletes/upserts and update metadata. Its `clear` must atomically delete rows and increment the generation. Bound external calls with your adapter's timeout/cancellation policy; the core awaits storage promises without imposing its own deadline. See [the adapter guide](docs/adapting.md).

## Verification and benchmark

```sh
npm ci
npm run check
npm run format:check
npm run example
npm run bench
```

The 18 tests cover failures, backpressure, clear/write races, budget enforcement, namespace isolation, generation fencing across SQLite connections and a real TanStack dehydration/hydration round trip.

The reproducible benchmark uses 1,000 rows with roughly 8 KB of data each and 30 updates changing one row per update. Including the initial write, it records **8,546,662 incremental row bytes versus 257,264,758 whole-snapshot bytes: 96.68% fewer serialized payload bytes**. This measures a sparse-update workload and a memory adapter, not production disk throughput. Workloads changing every row will benefit less. Timing varies by machine. See [verification details](docs/verification.md).

Licensed under MIT OR Apache-2.0.
