/** Structurally compatible with a query-only TanStack persisted client. */
export interface QueryRow {
  queryHash: string;
  queryKey: readonly unknown[];
  state: {
    data?: unknown;
    dataUpdatedAt: number;
    status: string;
  };
}
export interface PersistedClient {
  timestamp: number;
  buster: string;
  clientState: { queries: readonly QueryRow[]; mutations: readonly unknown[] };
}
export interface RowSnapshot {
  generation: number;
  buster: string | null;
  timestamp: number;
  rows: readonly (readonly [string, string])[];
}
export interface RowChange {
  generation: number;
  buster: string;
  timestamp: number;
  reset: boolean;
  upserts: readonly (readonly [string, string])[];
  removes: readonly string[];
}
/** write must atomically reject a stale generation. clear increments it. */
export interface RowStore {
  read(): Promise<RowSnapshot>;
  write(change: RowChange): Promise<boolean>;
  clear(): Promise<void>;
}
export class PersistenceError extends Error {
  constructor(
    public readonly code: "INVALID" | "BUDGET" | "FENCED",
    message: string,
  ) {
    super(message);
    this.name = "PersistenceError";
  }
}
export class MemoryRowStore implements RowStore {
  private generation = 0;
  private buster: string | null = null;
  private timestamp = 0;
  private rows = new Map<string, string>();
  async read(): Promise<RowSnapshot> {
    return {
      generation: this.generation,
      buster: this.buster,
      timestamp: this.timestamp,
      rows: [...this.rows],
    };
  }
  async write(c: RowChange): Promise<boolean> {
    if (c.generation !== this.generation) return false;
    const next = c.reset ? new Map<string, string>() : new Map(this.rows);
    for (const hash of c.removes) next.delete(hash);
    for (const [hash, body] of c.upserts) next.set(hash, body);
    this.rows = next;
    this.buster = c.buster;
    this.timestamp = c.timestamp;
    return true;
  }
  async clear(): Promise<void> {
    this.generation++;
    this.rows.clear();
    this.buster = null;
    this.timestamp = 0;
  }
}
interface Held {
  reference?: WeakRef<object>;
  primitive?: unknown;
  metadata: string;
  bytes: number;
}
export interface Options {
  store: RowStore;
  maxRowBytes?: number;
  maxBytes?: number;
  maxRows?: number;
  idleMs?: number;
  maxWaitMs?: number;
  minIntervalMs?: number;
  protectedHashes?: readonly string[];
  shouldPersist?: (row: QueryRow) => boolean;
  isWriter?: () => boolean;
  onError?: (error: unknown) => void;
  now?: () => number;
}
export interface Persister<Client extends PersistedClient = PersistedClient> {
  readonly error: unknown;
  persistClient(client: Client): void;
  restoreClient(): Promise<Client | undefined>;
  removeClient(): Promise<void>;
  /** Drain queued snapshots; rejects on failure. */
  flush(): Promise<void>;
  /** Explicit disk deletion; absence from memory alone does not remove a row. */
  removeQueries(hashes: readonly string[]): Promise<void>;
  /** Await a final flush and stop accepting snapshots. */
  close(): Promise<void>;
}
const encoder = new TextEncoder();
const bytes = (s: string) => encoder.encode(s).byteLength;
function metadata(row: QueryRow): string {
  return JSON.stringify({ ...row, state: { ...row.state, data: undefined } });
}
function remember(row: QueryRow, n: number): Held {
  const data = row.state.data;
  return {
    metadata: metadata(row),
    bytes: n,
    ...(data !== null && typeof data === "object"
      ? { reference: new WeakRef(data) }
      : typeof data !== "string" || data.length <= 128
        ? { primitive: data }
        : {}),
  };
}
function matches(held: Held | undefined, row: QueryRow): boolean {
  if (!held || held.metadata !== metadata(row)) return false;
  return held.reference
    ? held.reference.deref() === row.state.data
    : Object.hasOwn(held, "primitive") &&
        Object.is(held.primitive, row.state.data);
}
export function createIncrementalPersister<
  Client extends PersistedClient = PersistedClient,
>(options: Options): Persister<Client> {
  const maxRow = options.maxRowBytes ?? 2 * 1024 * 1024,
    maxBytes = options.maxBytes ?? 16 * 1024 * 1024,
    maxRows = options.maxRows ?? 500;
  const idle = options.idleMs ?? 2000,
    maxWait = options.maxWaitMs ?? 30000,
    minInterval = options.minIntervalMs ?? 60000;
  for (const n of [maxRow, maxBytes, maxRows])
    if (!Number.isSafeInteger(n) || n <= 0)
      throw new PersistenceError(
        "INVALID",
        "Budgets must be positive safe integers",
      );
  for (const n of [idle, maxWait, minInterval])
    if (!Number.isSafeInteger(n) || n < 0 || n > 2_147_483_647)
      throw new PersistenceError("INVALID", "Invalid timer duration");
  const now = options.now ?? Date.now,
    protectedHashes = new Set(options.protectedHashes ?? []);
  let held = new Map<string, Held>(),
    generation: number | undefined,
    buster: string | null = null,
    timestamp = 0;
  let pending: PersistedClient | undefined,
    firstQueued: number | undefined,
    lastWritten: number | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined,
    closed = false,
    clearing = false;
  // Operations are serialized even after failure; callers retain the rejection.
  let failure: unknown;
  let chain = Promise.resolve();
  let flushing: Promise<void> | undefined;
  const serial = <T>(fn: () => Promise<T>): Promise<T> => {
    const task = chain.then(fn);
    chain = task.then(
      () => {},
      () => {},
    );
    return task;
  };
  function cancelTimer() {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  }
  function schedule() {
    cancelTimer();
    if (!pending || closed || flushing) return;
    const at = now();
    firstQueued ??= at;
    const due = Math.max(
      lastWritten === undefined ? at : lastWritten + minInterval,
      Math.min(at + idle, firstQueued + maxWait),
    );
    timer = setTimeout(
      () => {
        timer = undefined;
        void flush().catch((e) => options.onError?.(e));
      },
      Math.max(0, due - at),
    );
  }
  async function load(): Promise<RowSnapshot> {
    const s = await options.store.read();
    if (!Number.isSafeInteger(s.generation) || s.generation < 0)
      throw new PersistenceError("INVALID", "Invalid storage generation");
    generation = s.generation;
    buster = s.buster;
    timestamp = s.timestamp;
    held = new Map(
      s.rows.map(([hash, raw]) => [hash, { metadata: "", bytes: bytes(raw) }]),
    );
    return s;
  }
  async function write(client: PersistedClient): Promise<void> {
    if (options.isWriter && !options.isWriter()) return;
    if (generation === undefined) await load();
    const reset = buster !== client.buster;
    const next = reset ? new Map<string, Held>() : new Map(held);
    const upserts: Array<[string, string]> = [];
    const seen = new Set<string>();
    for (const row of client.clientState.queries) {
      if (!row.queryHash || seen.has(row.queryHash))
        throw new PersistenceError(
          "INVALID",
          "Query hashes must be unique and nonempty",
        );
      seen.add(row.queryHash);
      if (options.shouldPersist && !options.shouldPersist(row)) {
        next.delete(row.queryHash);
        continue;
      }
      const old = reset ? undefined : held.get(row.queryHash);
      if (matches(old, row)) continue;
      let raw: string;
      try {
        raw = JSON.stringify(row);
      } catch {
        next.delete(row.queryHash);
        continue;
      }
      const n = bytes(raw);
      if (n > maxRow) {
        next.delete(row.queryHash);
        continue;
      }
      upserts.push([row.queryHash, raw]);
      next.set(row.queryHash, remember(row, n));
    }
    let total = [...next.values()].reduce((n, r) => n + r.bytes, 0);
    for (const [hash, row] of [...next]
      .filter(([h]) => !protectedHashes.has(h))
      .sort((a, b) => b[1].bytes - a[1].bytes)) {
      if (total <= maxBytes && next.size <= maxRows) break;
      next.delete(hash);
      total -= row.bytes;
    }
    if (total > maxBytes || next.size > maxRows)
      throw new PersistenceError(
        "BUDGET",
        "Protected rows exceed the snapshot budget",
      );
    const kept = upserts.filter(([hash]) => next.has(hash)),
      removes = [...held.keys()].filter((hash) => !next.has(hash));
    if (!reset && !kept.length && !removes.length) return;
    const change: RowChange = {
      generation: generation!,
      buster: client.buster,
      timestamp: client.timestamp,
      reset,
      upserts: kept,
      removes,
    };
    try {
      if (!(await options.store.write(change)))
        throw new PersistenceError(
          "FENCED",
          "Storage was cleared by another session; restore before retrying",
        );
    } catch (e) {
      generation = undefined;
      held = new Map();
      buster = null;
      throw e;
    }
    // Bookkeeping describes acknowledged writes only.
    held = next;
    buster = client.buster;
    timestamp = client.timestamp;
    lastWritten = now();
    failure = undefined;
  }
  function flush(): Promise<void> {
    cancelTimer();
    if (flushing) return flushing;
    flushing = serial(async () => {
      while (pending) {
        const client = pending;
        pending = undefined;
        firstQueued = undefined;
        await write(client);
      }
    })
      .catch((e) => {
        failure = e;
        throw e;
      })
      .finally(() => {
        flushing = undefined;
        schedule();
      });
    return flushing;
  }
  return {
    get error() {
      return failure;
    },
    persistClient(client) {
      if (closed || clearing)
        throw new PersistenceError(
          "INVALID",
          "Persister is closed or clearing",
        );
      if (options.isWriter && !options.isWriter()) return;
      if (client.clientState.mutations.length)
        throw new PersistenceError(
          "INVALID",
          "This persister stores queries only; use a separate mutation outbox",
        );
      if (
        !Number.isFinite(client.timestamp) ||
        typeof client.buster !== "string"
      )
        throw new PersistenceError("INVALID", "Invalid snapshot metadata");
      pending = {
        ...client,
        clientState: {
          mutations: [],
          queries: client.clientState.queries.map((q) => ({
            ...q,
            queryKey: [...q.queryKey],
            state: { ...q.state },
          })),
        },
      };
      schedule();
    },
    restoreClient() {
      return serial(async () => {
        const s = await load();
        if (s.buster === null) return undefined;
        if (
          s.rows.length > maxRows ||
          s.rows.reduce((n, [, raw]) => n + bytes(raw), 0) > maxBytes ||
          s.rows.some(([, raw]) => bytes(raw) > maxRow)
        )
          throw new PersistenceError(
            "BUDGET",
            "Stored snapshot exceeds restore budgets",
          );
        const queries = s.rows.map(([, raw]) => JSON.parse(raw) as QueryRow);
        held = new Map(
          queries.map((q) => [
            q.queryHash,
            remember(q, bytes(JSON.stringify(q))),
          ]),
        );
        return {
          timestamp: s.timestamp,
          buster: s.buster,
          clientState: { mutations: [], queries },
        } as unknown as Client;
      });
    },
    removeClient() {
      if (options.isWriter && !options.isWriter()) return Promise.resolve();
      clearing = true;
      cancelTimer();
      pending = undefined;
      firstQueued = undefined;
      return serial(async () => {
        await options.store.clear();
        generation = undefined;
        held = new Map();
        buster = null;
        lastWritten = undefined;
      }).finally(() => {
        clearing = false;
      });
    },
    flush,
    removeQueries(hashes) {
      if (options.isWriter && !options.isWriter()) return Promise.resolve();
      return serial(async () => {
        if (generation === undefined) await load();
        if (buster === null) return;
        if (
          !(await options.store.write({
            generation: generation!,
            buster,
            timestamp,
            reset: false,
            upserts: [],
            removes: hashes,
          }))
        )
          throw new PersistenceError("FENCED", "Storage generation changed");
        for (const h of hashes) held.delete(h);
      });
    },
    async close() {
      closed = true;
      await flush();
      cancelTimer();
    },
  };
}
