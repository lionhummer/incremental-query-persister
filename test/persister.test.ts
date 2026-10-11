import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createIncrementalPersister,
  MemoryRowStore,
  type QueryRow,
  type PersistedClient,
  type RowChange,
  type RowStore,
} from "../src/index.js";
const row = (hash = "a", data: unknown = { value: 1 }, time = 1): QueryRow => ({
  queryHash: hash,
  queryKey: [hash],
  state: { data, dataUpdatedAt: time, status: "success" },
});
const snapshot = (queries: QueryRow[], buster = "v1"): PersistedClient => ({
  timestamp: 1,
  buster,
  clientState: { queries, mutations: [] },
});
class Store extends MemoryRowStore {
  writes: RowChange[] = [];
  override async write(c: RowChange) {
    this.writes.push(c);
    return super.write(c);
  }
}
const fixture = (
  options: Partial<Parameters<typeof createIncrementalPersister>[0]> = {},
) => {
  const store = new Store();
  const p = createIncrementalPersister({ store, idleMs: 60000, ...options });
  return { p, store };
};
test("unchanged object rows cost no additional writes; one changed row writes alone", async () => {
  const { p, store } = fixture(),
    a = row(),
    b = row("b");
  p.persistClient(snapshot([a, b]));
  await p.flush();
  p.persistClient(snapshot([a, b]));
  await p.flush();
  assert.equal(store.writes.length, 1);
  p.persistClient(snapshot([a, row("b", { value: 2 }, 2)]));
  await p.flush();
  assert.deepEqual(
    store.writes[1]?.upserts.map(([h]) => h),
    ["b"],
  );
  await p.close();
});
test("memory eviction retains disk rows and explicit removal deletes", async () => {
  const { p, store } = fixture();
  p.persistClient(snapshot([row(), row("b")]));
  await p.flush();
  p.persistClient(snapshot([row("b")]));
  await p.flush();
  assert.equal((await store.read()).rows.length, 2);
  await p.removeQueries(["a"]);
  assert.deepEqual(
    (await store.read()).rows.map(([h]) => h),
    ["b"],
  );
  await p.close();
});
test("latest pending snapshot replaces earlier pending work", async () => {
  const { p, store } = fixture();
  for (let i = 0; i < 30; i++)
    p.persistClient(snapshot([row("a", { value: i }, i)]));
  await p.flush();
  assert.equal(store.writes.length, 1);
  assert.equal(
    JSON.parse((await store.read()).rows[0]![1]).state.data.value,
    29,
  );
  await p.close();
});
test("only acknowledged rows are reused after a failed write", async () => {
  const base = new MemoryRowStore();
  let fail = true,
    calls = 0;
  const store: RowStore = {
    read: () => base.read(),
    clear: () => base.clear(),
    write: async (c) => {
      calls++;
      if (fail) throw new Error("disk failed");
      return base.write(c);
    },
  };
  const { p } = fixture({ store });
  const client = snapshot([row()]);
  p.persistClient(client);
  await assert.rejects(p.flush(), /disk failed/);
  fail = false;
  p.persistClient(client);
  await p.flush();
  assert.equal(calls, 2);
  assert.equal((await base.read()).rows.length, 1);
  await p.close();
});
test("in-flight backpressure serializes writes and drains the latest queued snapshot", async () => {
  const base = new MemoryRowStore();
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>((r) => (release = r)),
    started = new Promise<void>((r) => (entered = r));
  let calls = 0,
    active = 0,
    maximum = 0;
  const store: RowStore = {
    read: () => base.read(),
    clear: () => base.clear(),
    write: async (c) => {
      active++;
      maximum = Math.max(maximum, active);
      if (calls++ === 0) {
        entered();
        await gate;
      }
      const r = await base.write(c);
      active--;
      return r;
    },
  };
  const { p } = fixture({ store });
  p.persistClient(snapshot([row()]));
  const flush = p.flush();
  await started;
  for (let i = 2; i <= 20; i++)
    p.persistClient(snapshot([row("a", { value: i }, i)]));
  release();
  await flush;
  assert.equal(calls, 2);
  assert.equal(maximum, 1);
  assert.equal(
    JSON.parse((await base.read()).rows[0]![1]).state.data.value,
    20,
  );
  await p.close();
});
test("clear waits for a late acknowledged write and leaves no stale rows", async () => {
  const base = new MemoryRowStore();
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>((r) => (release = r)),
    started = new Promise<void>((r) => (entered = r));
  const store: RowStore = {
    read: () => base.read(),
    clear: () => base.clear(),
    write: async (c) => {
      entered();
      await gate;
      return base.write(c);
    },
  };
  const { p } = fixture({ store });
  p.persistClient(snapshot([row()]));
  const write = p.flush();
  await started;
  const clear = p.removeClient();
  assert.throws(() => p.persistClient(snapshot([row()])), /clearing/);
  release();
  await write;
  await clear;
  assert.equal((await base.read()).rows.length, 0);
  await p.close();
});
test("storage generation fences a write after another session clears", async () => {
  const store = new MemoryRowStore();
  const { p } = fixture({ store });
  await p.restoreClient();
  await store.clear();
  p.persistClient(snapshot([row()]));
  await assert.rejects(
    p.flush(),
    (e) => e instanceof Error && "code" in e && e.code === "FENCED",
  );
  assert.equal((await store.read()).rows.length, 0);
  await p.close();
});
test("buster changes atomically replace the prior namespace contents", async () => {
  const { p, store } = fixture();
  p.persistClient(snapshot([row()]));
  await p.flush();
  p.persistClient(snapshot([row("b")], "v2"));
  await p.flush();
  assert.deepEqual(
    (await store.read()).rows.map(([h]) => h),
    ["b"],
  );
  await p.close();
});
test("row, total byte and count ceilings evict unprotected largest rows", async () => {
  const { p, store } = fixture({ maxRows: 2, maxBytes: 500, maxRowBytes: 300 });
  p.persistClient(
    snapshot([
      row("huge", { v: "x".repeat(500) }),
      row("small"),
      row("larger", { v: "x".repeat(100) }),
      row("third"),
    ]),
  );
  await p.flush();
  const kept = await store.read();
  assert.ok(kept.rows.length <= 2);
  assert.ok(kept.rows.reduce((n, [, s]) => n + Buffer.byteLength(s), 0) <= 500);
  assert.ok(!kept.rows.some(([h]) => h === "huge"));
  await p.close();
});
test("protected rows cannot silently exceed the total budget", async () => {
  const { p, store } = fixture({ maxRows: 1, protectedHashes: ["a", "b"] });
  p.persistClient(snapshot([row(), row("b")]));
  await assert.rejects(
    p.flush(),
    (e) => e instanceof Error && "code" in e && e.code === "BUDGET",
  );
  assert.equal(store.writes.length, 0);
  await p.close();
});
test("read-only consumers restore but never overwrite", async () => {
  const store = new MemoryRowStore();
  const owner = fixture({ store }).p;
  owner.persistClient(snapshot([row()]));
  await owner.flush();
  const satellite = fixture({ store, isWriter: () => false }).p;
  satellite.persistClient(snapshot([row("partial")]));
  await satellite.flush();
  assert.equal(
    (await satellite.restoreClient())?.clientState.queries[0]?.queryHash,
    "a",
  );
  await owner.close();
  await satellite.close();
});
test("mutation outboxes and timer overflows are rejected", async () => {
  assert.throws(
    () =>
      createIncrementalPersister({
        store: new MemoryRowStore(),
        idleMs: 2 ** 31,
      }),
    /timer/,
  );
  const { p } = fixture();
  assert.throws(
    () =>
      p.persistClient({
        ...snapshot([]),
        clientState: { queries: [], mutations: [{}] },
      }),
    /outbox/,
  );
  await p.close();
});
test("restore seeds row identity and avoids rewriting hydrated objects", async () => {
  const store = new Store();
  const first = fixture({ store }).p;
  first.persistClient(snapshot([row()]));
  await first.flush();
  await first.close();
  const second = fixture({ store }).p;
  const restored = (await second.restoreClient())!;
  second.persistClient(restored);
  await second.flush();
  assert.equal(store.writes.length, 1);
  await second.close();
});
test("changed metadata writes even when data identity and timestamp match", async () => {
  const { p, store } = fixture();
  const a = row();
  p.persistClient(snapshot([a]));
  await p.flush();
  const changed = { ...a, state: { ...a.state, errorUpdatedAt: 10 } };
  p.persistClient(snapshot([changed]));
  await p.flush();
  assert.equal(store.writes.length, 2);
  await p.close();
});
test("restore rejects storage beyond configured budgets", async () => {
  const store = new MemoryRowStore();
  const owner = fixture({ store }).p;
  owner.persistClient(snapshot([row(), row("b")]));
  await owner.flush();
  const reader = fixture({ store, maxRows: 1 }).p;
  await assert.rejects(reader.restoreClient(), /restore budgets/);
  await owner.close();
  await reader.close();
});

test("real TanStack dehydration, typed persister and hydration round-trip", async () => {
  const { QueryClient, dehydrate, hydrate } =
    await import("@tanstack/query-core");
  type Client = import("@tanstack/query-persist-client-core").PersistedClient;
  const p = createIncrementalPersister<Client>({
    store: new MemoryRowStore(),
  }) satisfies import("@tanstack/query-persist-client-core").Persister;
  const first = new QueryClient();
  first.setQueryData(["settings"], { theme: "dark" });
  p.persistClient({
    timestamp: 1,
    buster: "v1",
    clientState: dehydrate(first),
  });
  await p.flush();
  const restored = await p.restoreClient();
  const second = new QueryClient();
  hydrate(second, restored!.clientState);
  assert.deepEqual(second.getQueryData(["settings"]), { theme: "dark" });
  await p.close();
  first.clear();
  second.clear();
});

test("read-only consumers cannot clear the shared namespace", async () => {
  const store = new MemoryRowStore();
  const owner = fixture({ store }).p;
  owner.persistClient(snapshot([row()]));
  await owner.flush();
  const reader = fixture({ store, isWriter: () => false }).p;
  await reader.removeQueries(["a"]);
  await reader.removeClient();
  assert.equal((await store.read()).rows.length, 1);
  await owner.close();
  await reader.close();
});

test("SQLite preserves namespaces and rejects stale writes across connections", async () => {
  const { SqliteRowStore } = await import("../src/sqlite.js");
  const { mkdtempSync, rmSync } = await import("node:fs");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const dir = mkdtempSync(join(tmpdir(), "row-persister-"));
  const path = join(dir, "cache.sqlite");
  const a = new SqliteRowStore(path, "alice"),
    b = new SqliteRowStore(path, "alice"),
    other = new SqliteRowStore(path, "bob");
  try {
    const p = fixture({ store: a }).p;
    p.persistClient(snapshot([row()]));
    await p.flush();
    await p.close();
    assert.equal((await b.read()).rows.length, 1);
    assert.equal((await other.read()).rows.length, 0);
    const stale = await a.read();
    await b.clear();
    assert.equal(
      await a.write({
        generation: stale.generation,
        buster: "old",
        timestamp: 0,
        reset: false,
        upserts: [["stale", "{}"]],
        removes: [],
      }),
      false,
    );
    assert.equal((await a.read()).rows.length, 0);
  } finally {
    a.close();
    b.close();
    other.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
