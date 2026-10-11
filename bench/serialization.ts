import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import {
  createIncrementalPersister,
  MemoryRowStore,
  type QueryRow,
  type PersistedClient,
  type RowChange,
} from "../src/index.js";
const ROWS = 1000,
  UPDATES = 30,
  PAYLOAD = 8192;
let transmitted = 0,
  rowWrites = 0;
class MeasuringStore extends MemoryRowStore {
  override async write(c: RowChange) {
    for (const [, raw] of c.upserts) {
      transmitted += Buffer.byteLength(raw);
      rowWrites++;
    }
    return super.write(c);
  }
}
const store = new MeasuringStore(),
  p = createIncrementalPersister({
    store,
    maxRows: ROWS,
    maxBytes: 32 * 1024 * 1024,
    idleMs: 60000,
  });
let rows: QueryRow[] = Array.from({ length: ROWS }, (_, i) => ({
  queryHash: `row-${i}`,
  queryKey: [i],
  state: {
    data: { text: "x".repeat(PAYLOAD) },
    dataUpdatedAt: 1,
    status: "success",
  },
}));
const snapshot = (): PersistedClient => ({
  timestamp: 1,
  buster: "bench",
  clientState: { queries: rows, mutations: [] },
});
let wholeBytes = 0,
  wholeMs = 0,
  incrementalMs = 0;
for (let i = 0; i <= UPDATES; i++) {
  if (i)
    rows = rows.map((r, j) =>
      j === i - 1
        ? {
            ...r,
            state: {
              ...r.state,
              data: { text: "y".repeat(PAYLOAD) },
              dataUpdatedAt: i + 1,
            },
          }
        : r,
    );
  let start = performance.now();
  wholeBytes += Buffer.byteLength(JSON.stringify(snapshot()));
  wholeMs += performance.now() - start;
  start = performance.now();
  p.persistClient(snapshot());
  await p.flush();
  incrementalMs += performance.now() - start;
}
await p.close();
console.log(
  JSON.stringify(
    {
      workload: {
        rows: ROWS,
        updates: UPDATES,
        payloadBytes: PAYLOAD,
        changedRowsPerUpdate: 1,
        initialWriteIncluded: true,
      },
      wholeSnapshotBytes: wholeBytes,
      incrementalRowBytes: transmitted,
      rowWrites,
      reductionPercent: Number(
        (100 * (1 - transmitted / wholeBytes)).toFixed(2),
      ),
      wholeSerializationMs: Number(wholeMs.toFixed(2)),
      incrementalIncludingMemoryStoreMs: Number(incrementalMs.toFixed(2)),
    },
    null,
    2,
  ),
);
assert.equal(rowWrites, ROWS + UPDATES);
assert.ok(transmitted < wholeBytes / 20);
assert.equal((await store.read()).rows.length, ROWS);
